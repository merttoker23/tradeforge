"""USD-M snapshot + buffered absolute deltas, bounded and fail closed."""

import json
from collections import deque
from datetime import datetime, timedelta
from decimal import Decimal
from heapq import nlargest, nsmallest
from uuid import NAMESPACE_URL, uuid5

from tradeforge.domain.common import utc
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.domain.market import OrderBookSnapshot, StreamState
from tradeforge.market.bars import canonical
from tradeforge.market.exchange_info import milliseconds
from tradeforge.market.streams import MarketEvent, _counter, decimal_field


def levels(value: object, limit: int) -> dict[Decimal, Decimal]:
    if not isinstance(value, list) or len(value) > limit:
        raise ValueError("INVALID_DEPTH_LEVELS")
    result: dict[Decimal, Decimal] = {}
    for pair in value:
        if not isinstance(pair, list) or len(pair) != 2:
            raise ValueError("INVALID_DEPTH_LEVEL")
        price = decimal_field({"p": pair[0]}, "p")
        quantity = decimal_field({"q": pair[1]}, "q")
        if price <= 0 or price in result:
            raise ValueError("INVALID_OR_DUPLICATE_DEPTH_PRICE")
        result[price] = quantity
    return result


class LocalOrderBook:
    def __init__(
        self,
        key: InstrumentKey,
        *,
        max_pending: int = 1024,
        max_levels: int = 5000,
        ttl: timedelta = timedelta(seconds=5),
    ) -> None:
        if (
            type(max_pending) is not int
            or not 1 <= max_pending <= 10000
            or type(max_levels) is not int
            or not 2 <= max_levels <= 10000
            or not timedelta(0) < ttl <= timedelta(minutes=1)
        ):
            raise ValueError("INVALID_BOOK_BUDGET")
        self.key, self.max_pending, self.max_levels, self.ttl = key, max_pending, max_levels, ttl
        self.pending: deque[MarketEvent] = deque(maxlen=max_pending)
        self.generation = -1
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.last = 0
        self.initial = 0
        self.source_at: datetime | None = None
        self.observed_at: datetime | None = None
        self.state = StreamState.UNSYNCED
        self._installed = False

    def begin(self, generation: int) -> None:
        _counter(generation)
        if generation <= self.generation:
            raise ValueError("OLD_BOOK_GENERATION")
        self.generation = generation
        self.invalidate()

    def invalidate(self) -> None:
        self.pending.clear()
        self.bids.clear()
        self.asks.clear()
        self._installed = False
        self.state = StreamState.UNSYNCED

    def ingest(self, event: MarketEvent) -> bool:
        if event.generation != self.generation:
            return False
        try:
            if event.symbol != self.key.symbol or "@depth" not in event.stream.name:
                raise ValueError("DEPTH_IDENTITY_MISMATCH")
            if not utc(event.event_time) <= utc(event.received_at):
                raise ValueError("INVALID_DEPTH_TIME")
            row = json.loads(event.payload_json)
            if row.get("s") != self.key.symbol:
                raise ValueError("DEPTH_IDENTITY_MISMATCH")
            first, last = _counter(row.get("U")), _counter(row.get("u"))
            _counter(row.get("pu"))
            if first > last:
                raise ValueError("INVALID_DEPTH_RANGE")
            levels(row.get("b"), self.max_levels)
            levels(row.get("a"), self.max_levels)
            if self._installed:
                return self._apply(event)
            if len(self.pending) == self.max_pending:
                self.invalidate()
            self.pending.append(event)
            return True
        except (ValueError, TypeError, KeyError):
            self.invalidate()
            raise ValueError("INVALID_DEPTH_EVENT") from None

    def install(self, payload: object, observed_at: datetime, *, generation: int) -> None:
        utc(observed_at)
        if generation != self.generation:
            raise ValueError("OLD_BOOK_GENERATION")
        try:
            if not isinstance(payload, dict):
                raise ValueError("INVALID_DEPTH_SNAPSHOT")
            last = _counter(payload.get("lastUpdateId"))
            source = milliseconds(payload.get("E"))
            if source > observed_at:
                raise ValueError("FUTURE_DEPTH_SNAPSHOT")
            self.bids = levels(payload.get("bids"), self.max_levels)
            self.asks = levels(payload.get("asks"), self.max_levels)
            self.bids = {p: q for p, q in self.bids.items() if q}
            self.asks = {p: q for p, q in self.asks.items() if q}
            self.last = self.initial = last
            self.source_at, self.observed_at = source, observed_at
            self.state, self._installed = StreamState.UNSYNCED, True
            self._check()
            waiting = tuple(self.pending)
            self.pending.clear()
            for event in waiting:
                if not self._installed:
                    break
                self._apply(event)
        except (ValueError, TypeError, KeyError):
            self.invalidate()
            raise ValueError("INVALID_DEPTH_SNAPSHOT") from None

    def _check(self) -> None:
        if (
            not self.bids
            or not self.asks
            or max(self.bids) >= min(self.asks)
            or len(self.bids) > self.max_levels
            or len(self.asks) > self.max_levels
        ):
            raise ValueError("INVALID_LOCAL_BOOK")

    def _apply(self, event: MarketEvent) -> bool:
        row = json.loads(event.payload_json)
        first, last, previous = row["U"], row["u"], row["pu"]
        if last < self.last or (self.state is StreamState.READY and last == self.last):
            return False
        bridge = self.state is StreamState.UNSYNCED
        if (bridge and not first <= self.last <= last) or (not bridge and previous != self.last):
            self.invalidate()
            self.pending.append(event)
            return False
        if self.source_at is not None and event.event_time < self.source_at:
            self.invalidate()
            return False
        for current, wire in ((self.bids, "b"), (self.asks, "a")):
            for price, quantity in levels(row[wire], self.max_levels).items():
                if quantity:
                    current[price] = quantity
                else:
                    current.pop(price, None)
        try:
            self._check()
        except ValueError:
            self.invalidate()
            return False
        self.last, self.state = last, StreamState.READY
        self.source_at = event.event_time
        self.observed_at = max(self.observed_at or event.received_at, event.received_at)
        return True

    def state_at(self, as_of: datetime) -> StreamState:
        utc(as_of)
        state = self.state
        available = self.observed_at is not None and self.observed_at <= as_of
        if not available:
            state = StreamState.UNSYNCED
        elif self.source_at is not None and as_of - self.source_at > self.ttl:
            state = StreamState.STALE if state is StreamState.READY else state
        return state

    def snapshot(self, as_of: datetime, *, limit: int | None = None) -> OrderBookSnapshot:
        if limit is not None and (type(limit) is not int or not 1 <= limit <= 10000):
            raise ValueError("INVALID_BOOK_DEPTH_LIMIT")
        state = self.state_at(as_of)
        available = self.observed_at is not None and self.observed_at <= as_of
        visible = available and state in (StreamState.READY, StreamState.STALE)
        source = self.source_at if available else as_of
        observed = self.observed_at if available else as_of
        values = dict(
            **self.key.model_dump(),
            schema_version=1,
            source_at=source,
            observed_at=observed,
            as_of=as_of,
            captured_at=observed,
            initial_update_id=self.initial,
            last_update_id=self.last,
            bids=(
                tuple(sorted(self.bids.items(), reverse=True))
                if limit is None
                else tuple((p, self.bids[p]) for p in nlargest(limit, self.bids))
            )
            if visible
            else (),
            asks=(
                tuple(sorted(self.asks.items()))
                if limit is None
                else tuple((p, self.asks[p]) for p in nsmallest(limit, self.asks))
            )
            if visible
            else (),
            stream_state=state,
        )
        identifier = canonical(
            {
                "key": self.key.key,
                "last": self.last,
                "as_of": as_of.isoformat(),
                "generation": self.generation,
            }
        )
        values["event_id"] = uuid5(NAMESPACE_URL, identifier)
        return OrderBookSnapshot.model_validate(values, context={"now": as_of})
