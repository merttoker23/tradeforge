"""Exact signed base-asset taker flow, bounded and availability-aware."""

import json
import math
from collections import deque
from collections.abc import Iterator
from datetime import datetime, timedelta
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from tradeforge.domain.common import DataQuality, utc
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.domain.market import TradeFlowWindow
from tradeforge.market.exchange_info import milliseconds
from tradeforge.market.streams import MarketEvent, _counter, decimal_field


class TradeFlow:
    def __init__(
        self,
        key: InstrumentKey,
        *,
        capacity: int = 4096,
        period_seconds: int = 60,
        max_future: float = 0,
    ) -> None:
        if (
            type(capacity) is not int
            or not 1 <= capacity <= 100_000
            or type(period_seconds) is not int
            or not 1 <= period_seconds <= 86400
            or not math.isfinite(max_future)
            or not 0 <= max_future <= 60
        ):
            raise ValueError("INVALID_FLOW_BUDGET")
        self.key, self.capacity, self.period_seconds = key, capacity, period_seconds
        # Exchange timestamps may lead the local clock by the trusted clock
        # skew the decoder accepted; that skew must not invalidate the flow.
        self._future = timedelta(seconds=max_future)
        self._trades: deque[tuple[int, datetime, datetime, Decimal, bool]] = deque(maxlen=capacity)
        self._gaps: deque[tuple[datetime, datetime]] = deque(maxlen=capacity)
        self._floor: datetime | None = None
        self._started: datetime | None = None
        self._last: int | None = None
        self._generation = -1
        self._invalid = False

    @property
    def generation(self) -> int:
        return self._generation

    def begin(self, generation: int, at: datetime) -> None:
        _counter(generation)
        if generation <= self._generation:
            raise ValueError("OLD_FLOW_GENERATION")
        # A new stream generation does not invalidate the retained trades. The
        # aggregate trade id is contiguous per symbol, so the first trade after
        # the reconnect proves whether a trade was actually missed. Only a real
        # discontinuity is a gap; a reconnect alone is not missing evidence and
        # must not restart the availability window.
        self._generation = generation
        if self._started is None:
            self._started = utc(at)
        self._invalid = False

    def invalidate(self, *, discard: bool = False) -> None:
        self._invalid = True
        if discard:
            self._trades.clear()
            self._gaps.clear()
            self._floor = self._started = None
            self._last = None

    def ingest(self, event: MarketEvent) -> bool:
        if event.generation != self._generation:
            return False
        try:
            if event.symbol != self.key.symbol or "@aggTrade" not in event.stream.name:
                raise ValueError("FLOW_IDENTITY_MISMATCH")
            row = json.loads(event.payload_json)
            identifier = _counter(row.get("a"))
            at = milliseconds(row.get("T"))
            quantity = decimal_field(row, "q")
            decimal_field(row, "p")
            maker = row.get("m")
            if (
                type(maker) is not bool
                or quantity <= 0
                or row.get("s") != self.key.symbol
                or not at <= utc(event.event_time) <= utc(event.received_at) + self._future
            ):
                raise ValueError("INVALID_FLOW_TRADE")
            item = (identifier, at, event.received_at, quantity, maker)
            if self._last is not None and identifier <= self._last:
                old = next((t for t in self._trades if t[0] == identifier), None)
                if old is not None and (old[1], old[3:]) != (item[1], item[3:]):
                    raise ValueError("CONFLICTING_FLOW_TRADE")
                return False
            if self._trades and at < self._trades[-1][1]:
                raise ValueError("REORDERED_FLOW_TIME")
            if self._last is not None and identifier != self._last + 1:
                self._gaps.append((at, event.received_at))
            if len(self._trades) == self.capacity:
                self._floor = self._trades[0][1]
            self._trades.append(item)
            self._last = identifier
            return True
        except (ValueError, TypeError, KeyError):
            self.invalidate()
            raise ValueError("INVALID_FLOW_EVENT") from None

    def _window(
        self, start: datetime, as_of: datetime
    ) -> Iterator[tuple[int, datetime, datetime, Decimal, bool]]:
        for trade in reversed(self._trades):
            if trade[1] <= start:
                break
            if trade[1] <= as_of and trade[2] <= as_of:
                yield trade

    def _validity(self, start: datetime, as_of: datetime, has_trades: bool) -> DataQuality:
        if self._invalid:
            return DataQuality.UNKNOWN
        if (self._floor is not None and self._floor > start) or any(
            start < a <= as_of and b <= as_of for a, b in self._gaps
        ):
            return DataQuality.GAP
        if self._started is None or self._started > start:
            return DataQuality.MISSING
        return DataQuality.OK if has_trades else DataQuality.STALE

    def validity_at(self, as_of: datetime) -> DataQuality:
        utc(as_of)
        start = as_of - timedelta(seconds=self.period_seconds)
        return self._validity(start, as_of, next(self._window(start, as_of), None) is not None)

    def snapshot(self, as_of: datetime) -> TradeFlowWindow:
        utc(as_of)
        start = as_of - timedelta(seconds=self.period_seconds)
        items = list(self._window(start, as_of))
        items.reverse()
        quality = self._validity(start, as_of, bool(items))
        source = max((t[1] for t in items), default=as_of)
        observed = max((t[2] for t in items), default=as_of)
        return TradeFlowWindow.model_validate(
            dict(
                **self.key.model_dump(),
                schema_version=1,
                event_id=uuid5(NAMESPACE_URL, f"{self.key.key}/{as_of}/{items}/{quality}"),
                source_at=source,
                observed_at=observed,
                as_of=as_of,
                period_seconds=self.period_seconds,
                count=len(items),
                signed_taker_flow=sum((-t[3] if t[4] else t[3] for t in items), Decimal(0)),
                agg_volume=sum((t[3] for t in items), Decimal(0)),
                source_start=min((t[1] for t in items), default=source),
                source_end=source,
                validity=quality,
            ),
            context={"now": as_of},
        )
