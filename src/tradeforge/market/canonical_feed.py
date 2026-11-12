"""Nonblocking canonical consumer plus separate async recovery/storage worker."""

import asyncio
import math
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Protocol

from tradeforge.domain.common import DataQuality
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.domain.market import StreamState, Timeframe
from tradeforge.market.market_state import BarView, MarketState
from tradeforge.market.order_book import LocalOrderBook
from tradeforge.market.quality import bar_quality
from tradeforge.market.streams import PARTIAL_DEPTH, MarketEvent, StreamSpec
from tradeforge.market.trade_flow import TradeFlow

CANONICAL_BAR_REASONS = frozenset(
    f"CANONICAL_{tf.value.upper()}_{quality.value}"
    for tf in Timeframe
    for quality in DataQuality
    if quality is not DataQuality.OK
) | {"CANONICAL_REPAIR_PENDING", "CANONICAL_DATA_NOT_READY"}


class Recovery(Protocol):
    async def bars(
        self,
        key: InstrumentKey,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
        *,
        valid: Callable[[], bool] = lambda: True,
    ) -> bool: ...
    async def depth(self, book: LocalOrderBook) -> bool: ...


class CanonicalFeed:
    def __init__(
        self,
        *,
        capacity: int = 96,
        max_symbols: int = 1024,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        max_future: float = 0,
    ) -> None:
        if not math.isfinite(max_future) or not 0 <= max_future <= 60:
            raise ValueError("INVALID_MAX_FUTURE")
        self.state = MarketState(capacity, max_series=max_symbols * 3)
        self.max_symbols, self.clock = max_symbols, clock
        # The trusted clock skew of the gateway. A local book and flow must
        # tolerate the same future-stamped evidence their decoder accepted,
        # or a slightly lagging clock would invalidate every stream.
        self.max_future = max_future
        self.keys: dict[str, InstrumentKey] = {}
        self.books: dict[str, LocalOrderBook] = {}
        self.flows: dict[str, TradeFlow] = {}
        self._generations: dict[str, int] = {}
        self._repair: dict[tuple[str, Timeframe], int] = {}
        self._dirty: set[tuple[str, Timeframe]] = set()
        self.storage_ready = True
        self.admission_ready = True
        self.storage_error: str | None = None
        self._epoch = 0
        self._loss_version = 0

    def _need_repair(self, symbol: str, tf: Timeframe) -> None:
        self._loss_version += 1
        self._repair[(symbol, tf)] = self._loss_version

    def configure(
        self,
        keys: tuple[InstrumentKey, ...],
        deep: tuple[str, ...],
        *,
        streams: tuple[StreamSpec, ...] | None = None,
    ) -> None:
        if len(keys) > self.max_symbols or not set(deep) <= {k.symbol for k in keys}:
            raise ValueError("CANONICAL_SYMBOL_BUDGET")
        new = {k.symbol: k for k in keys}
        unchanged = {s for s, key in new.items() if self.keys.get(s) == key}
        deep_symbols = set(deep)
        stream_names = {stream.name for stream in streams} if streams is not None else None

        def retained(name: str) -> bool:
            stream = StreamSpec(name)
            return (
                stream.symbol in unchanged
                and (stream_names is None or name in stream_names)
                and (
                    not any(part in name for part in ("@depth", "@aggTrade"))
                    or stream.symbol in deep_symbols
                )
            )

        repairs = {
            pair: version
            for pair, version in self._repair.items()
            if retained(f"{pair[0].lower()}@kline_{pair[1].value}")
        }
        generations = {
            name: generation for name, generation in self._generations.items() if retained(name)
        }
        if (
            new == self.keys
            and deep_symbols == self.books.keys()
            and generations == self._generations
            and repairs == self._repair
        ):
            return
        self.keys = new
        self._epoch += 1
        self.state.retain(keys)
        # Replanning fences pending recovery, but unchanged sockets do not emit
        # SYNCING again. Preserve their exact state; removed streams are fenced.
        self._generations = generations
        self.books = {
            s: (
                self.books[s]
                if s in unchanged and s in self.books
                else LocalOrderBook(new[s], max_future=self.max_future)
            )
            for s in deep
        }
        self.flows = {
            s: (
                self.flows[s]
                if s in unchanged and s in self.flows
                else TradeFlow(new[s], max_future=self.max_future)
            )
            for s in deep
        }
        # A newly selected symbol may already have a connected depth or
        # aggregate trade stream. Its local book and flow must start with the
        # current stream generation; otherwise every later event is fenced and
        # the evidence window stays permanently unavailable.
        for symbol, book in self.books.items():
            if book.generation < 0:
                generation = next(
                    (
                        self._generations[name]
                        for suffix in ("depth@100ms", "depth", "depth@500ms", PARTIAL_DEPTH)
                        if (name := f"{symbol.lower()}@{suffix}") in self._generations
                    ),
                    None,
                )
                if generation is not None:
                    book.begin(generation)
        for symbol, flow in self.flows.items():
            if flow.generation < 0:
                generation = self._generations.get(f"{symbol.lower()}@aggTrade")
                if generation is not None:
                    flow.begin(generation, self.clock())
        self._repair = repairs
        self._dirty = {pair for pair in self._dirty if pair[0] in unchanged}

    def socket_state(
        self, streams: tuple[StreamSpec, ...], state: str, generation: int,
        *, discard_flow: bool = False,
    ) -> None:
        self._epoch += 1
        for stream in streams:
            symbol = stream.symbol
            if symbol not in self.keys:
                continue
            self._generations.pop(stream.name, None)
            if state == "SYNCING":
                self._generations[stream.name] = generation
            if "@kline_" in stream.name:
                tf = Timeframe(stream.name.split("_", 1)[1])
                self._need_repair(symbol, tf)
                self.state.mark(self.keys[symbol], tf, DataQuality.STALE, self.clock())
            elif "@depth" in stream.name and symbol in self.books:
                book = self.books[symbol]
                book.invalidate()
                if state == "SYNCING":
                    book.begin(max(generation, book.generation + 1))
            elif "@aggTrade" in stream.name and symbol in self.flows:
                flow = self.flows[symbol]
                flow.invalidate(discard=discard_flow)
                if state == "SYNCING":
                    flow.begin(max(generation, flow.generation + 1), self.clock())

    def socket_streams(
        self,
        added: tuple[StreamSpec, ...],
        removed: tuple[StreamSpec, ...],
        generation: int,
    ) -> None:
        """Apply a live subscription delta without restarting the connection.

        A retained stream keeps its local book, flow and candle series, so a
        discretionary rebalance never interrupts the evidence of a symbol that
        stays selected. Only a newly subscribed stream starts from scratch.
        """
        for stream in removed:
            self._generations.pop(stream.name, None)
        for stream in added:
            symbol = stream.symbol
            if symbol is None or symbol not in self.keys:
                continue
            self._generations[stream.name] = generation
            if "@kline_" in stream.name:
                interval = Timeframe(stream.name.split("_", 1)[1])
                # A newly subscribed candle stream has no retained history.
                self._need_repair(symbol, interval)
                self.state.mark(
                    self.keys[symbol], interval, DataQuality.MISSING, self.clock()
                )
            elif "@depth" in stream.name and symbol in self.books:
                book = self.books[symbol]
                book.invalidate()
                book.begin(max(generation, book.generation + 1))
            elif "@aggTrade" in stream.name and symbol in self.flows:
                flow = self.flows[symbol]
                flow.invalidate()
                flow.begin(max(generation, flow.generation + 1), self.clock())

    def lost(self, event: MarketEvent) -> None:
        symbol = event.symbol
        if symbol not in self.keys:
            return
        name = event.stream.name
        if "@kline_" in name:
            tf = Timeframe(name.split("_", 1)[1])
            self._need_repair(symbol, tf)
            self.state.mark(self.keys[symbol], tf, DataQuality.GAP, self.clock())
        elif "@depth" in name and symbol in self.books:
            self.books[symbol].invalidate()
        elif "@aggTrade" in name and symbol in self.flows:
            flow = self.flows[symbol]
            flow.begin(flow.generation + 1, self.clock())

    def consume(self, event: MarketEvent) -> bool:
        if (
            event.symbol not in self.keys
            or self._generations.get(event.stream.name) != event.generation
        ):
            return False
        try:
            name, symbol = event.stream.name, event.symbol
            if "@kline_" in name:
                accepted = self.state.ingest(event, self.keys[symbol])
                if accepted:
                    self._dirty.add((symbol, Timeframe(name.split("_", 1)[1])))
                return accepted
            if "@depth" in name and symbol in self.books:
                book = self.books[symbol]
                return book.ingest(replace(event, generation=book.generation))
            if "@aggTrade" in name and symbol in self.flows:
                flow = self.flows[symbol]
                return flow.ingest(replace(event, generation=flow.generation))
            return False
        except ValueError:
            self.lost(event)
            return False

    def symbol_ready(self, symbol: str, at: datetime) -> bool:
        if not self.admission_ready or not self.storage_ready:
            return False
        if symbol in self.books and self.books[symbol].state_at(at) is not StreamState.READY:
            return False
        if symbol in self.flows and self.flows[symbol].validity_at(at) is not DataQuality.OK:
            return False
        return not self.bar_rejections(symbol, at)

    def bar_rejections(self, symbol: str, at: datetime) -> tuple[str, ...]:
        """Bounded source reasons, independent of order-book/flow readiness."""
        if symbol not in self.keys:
            # Global quote members need no canonical subscriptions. Deep alpha
            # inputs still require their selected bars, book and flow evidence.
            return ()
        reasons = []
        prefix = f"{symbol.lower()}@kline_"
        intervals = (Timeframe.M1, Timeframe.M5, Timeframe.M15)
        for interval in intervals:
            if prefix + interval.value in self._generations:
                quality = self.state.view(self.keys[symbol], interval, at).quality
                if quality is not DataQuality.OK:
                    reasons.append(f"CANONICAL_{interval.value.upper()}_{quality.value}")
        if not reasons and any((symbol, interval) in self._repair for interval in intervals):
            reasons.append("CANONICAL_REPAIR_PENDING")
        return tuple(reasons)

    def ready(self, at: datetime) -> bool:
        if not self.admission_ready or not self.storage_ready or self._repair:
            return False
        for name in self._generations:
            stream = StreamSpec(name)
            if stream.symbol is not None and "@kline_" in name:
                if (
                    self.state.view(
                        self.keys[stream.symbol], Timeframe(name.split("_", 1)[1]), at
                    ).quality
                    is not DataQuality.OK
                ):
                    return False
        return all(book.state_at(at) is StreamState.READY for book in self.books.values()) and all(
            flow.validity_at(at) is DataQuality.OK for flow in self.flows.values()
        )

    async def maintain(
        self,
        recovery: Recovery,
        save: Callable[[BarView], None],
        at: datetime,
        *,
        recover_depth: bool = True,
    ) -> None:
        # Depth bridging cannot wait for the full selected candle repair sweep.
        if recover_depth:
            await self.maintain_depth(recovery)
        # A final kline can be absent even when the socket remains connected.
        # Quality inspection must schedule repair independently of disconnects.
        for name in tuple(self._generations):
            spec = StreamSpec(name)
            member = spec.symbol
            if member is not None and member in self.keys and "@kline_" in name:
                interval = Timeframe(name.split("_", 1)[1])
                if (member, interval) not in self._repair and self.state.view(
                    self.keys[member], interval, at
                ).quality is not DataQuality.OK:
                    self._need_repair(member, interval)
        repairs = 0
        repairs_by_book = sorted(
            self._repair.items(),
            key=lambda item: not (
                item[0][0] in self.books
                and self.books[item[0][0]].state_at(at) is StreamState.READY
            ),
        )
        for (symbol, tf), loss_version in repairs_by_book:
            stream = f"{symbol.lower()}@kline_{tf.value}"
            generation = self._generations.get(stream)
            if generation is None or symbol not in self.keys:
                continue
            key = self.keys[symbol]
            end = datetime.fromtimestamp(int(at.timestamp()) // tf.seconds * tf.seconds, tz=UTC)
            start = end - timedelta(seconds=tf.seconds * min(96, self.state.capacity))
            # Readiness checks every retained candle. Repair the first retained
            # hole before refreshing the tail, within the existing REST budget.
            bars = self.state.view(key, tf, at).bars
            step = timedelta(seconds=tf.seconds)
            hole = None
            for left, right in zip(bars, bars[1:], strict=False):
                if right.open_at - left.open_at > step:
                    hole = left.open_at + step
                    end = min(
                        right.open_at,
                        hole + step * min(8000, self.state.capacity),
                    )
                    start = hole
                    break
            if hole is None and bars:
                # No interior hole: only the candles that closed while the
                # stream was interrupted are missing. Re-reading the whole
                # retained window would re-observe every candle with a new
                # source time, which grows the revision history without
                # adding evidence and slows every later history read.
                hole = max(bar.open_at for bar in bars) + step
                if hole >= end:
                    # The retained series already reaches the current period.
                    complete = True
                else:
                    complete = False
                    start = hole
            else:
                complete = False
            # A close may have arrived while a cold 5m series was waiting for
            # its first recovery turn. Fill its missing setup history as well.
            if tf is Timeframe.M5 and hole is not None and len(bars) < 8:
                start = end - step * min(64, self.state.capacity)
                complete = False

            def valid(
                stream: str = stream,
                symbol: str = symbol,
                tf: Timeframe = tf,
                generation: int | None = generation,
                key: InstrumentKey = key,
                version: int = loss_version,
            ) -> bool:
                return (
                    self.keys.get(symbol) == key
                    and generation == self._generations.get(stream)
                    and self._repair.get((symbol, tf)) == version
                )

            if complete:
                if valid():
                    view = self.state.view(key, tf, self.clock())
                    if bar_quality(view.bars, tf, self.clock()) is DataQuality.OK:
                        # The retained series already reaches the current
                        # period and the interruption left no hole. The stream
                        # resumed, so the recorded failure must not keep the
                        # series unavailable while a repair is still pending.
                        self.state.mark(key, tf, DataQuality.OK, self.clock())
                    if self.state.view(key, tf, self.clock()).quality is DataQuality.OK:
                        self._repair.pop((symbol, tf), None)
                    continue
            if repairs >= 16:
                break
            repairs += 1
            if await recovery.bars(key, tf, start, end, valid=valid):
                if valid():
                    if self.state.view(key, tf, self.clock()).quality is DataQuality.OK:
                        self._repair.pop((symbol, tf), None)
                    self._dirty.add((symbol, tf))
            elif valid():
                # Unavailable symbols must not delay every other repair.
                self._repair[(symbol, tf)] = self._repair.pop((symbol, tf))
        for symbol, tf in sorted(tuple(self._dirty)):
            if symbol not in self.keys:
                self._dirty.discard((symbol, tf))
                continue
            view = self.state.view(self.keys[symbol], tf, self.clock())
            self._dirty.discard((symbol, tf))
            try:
                await asyncio.to_thread(save, view)
            except Exception as error:
                from tradeforge.runtime.progress import safe_failure

                self._dirty.add((symbol, tf))
                self.storage_ready = False
                self.storage_error = safe_failure(error)
                return
        self.storage_ready = True
        self.storage_error = None

    async def maintain_depth(self, recovery: Recovery) -> None:
        for symbol, book in tuple(self.books.items()):
            prefix = f"{symbol.lower()}@depth"
            if book.state is StreamState.UNSYNCED and book.pending and any(
                prefix + suffix in self._generations for suffix in ("@100ms", "", "@500ms")
            ):
                await recovery.depth(book)
