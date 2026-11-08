"""Nonblocking canonical consumer plus separate async recovery/storage worker."""

import asyncio
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Protocol

from tradeforge.domain.common import DataQuality
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.domain.market import StreamState, Timeframe
from tradeforge.market.market_state import BarView, MarketState
from tradeforge.market.order_book import LocalOrderBook
from tradeforge.market.streams import MarketEvent, StreamSpec
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
    ) -> None:
        self.state = MarketState(capacity, max_series=max_symbols * 3)
        self.max_symbols, self.clock = max_symbols, clock
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

        def retained(name: str) -> bool:
            stream = StreamSpec(name)
            return (
                stream.symbol in unchanged
                and (streams is None or stream in streams)
                and (
                    not any(part in name for part in ("@depth", "@aggTrade"))
                    or stream.symbol in deep
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
            and set(deep) == set(self.books)
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
            s: self.books[s] if s in unchanged and s in self.books else LocalOrderBook(new[s])
            for s in deep
        }
        self.flows = {
            s: self.flows[s] if s in unchanged and s in self.flows else TradeFlow(new[s])
            for s in deep
        }
        self._repair = repairs
        self._dirty = {pair for pair in self._dirty if pair[0] in unchanged}

    def socket_state(self, streams: tuple[StreamSpec, ...], state: str, generation: int) -> None:
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
                flow.invalidate()
                if state == "SYNCING":
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
        if (
            not self.admission_ready
            or not self.storage_ready
            or any(s == symbol for s, _ in self._repair)
        ):
            return False
        if symbol in self.books and self.books[symbol].state_at(at) is not StreamState.READY:
            return False
        if symbol in self.flows and self.flows[symbol].snapshot(at).validity is not DataQuality.OK:
            return False
        return not self.bar_rejections(symbol, at)

    def bar_rejections(self, symbol: str, at: datetime) -> tuple[str, ...]:
        """Bounded source reasons, independent of order-book/flow readiness."""
        if symbol not in self.keys:
            # Global quote members need no canonical subscriptions. Deep alpha
            # inputs still require their selected bars, book and flow evidence.
            return ()
        reasons = []
        for name in sorted(self._generations):
            if StreamSpec(name).symbol == symbol and "@kline_" in name:
                interval = Timeframe(name.split("_", 1)[1])
                quality = self.state.view(self.keys[symbol], interval, at).quality
                if quality is not DataQuality.OK:
                    reasons.append(f"CANONICAL_{interval.value.upper()}_{quality.value}")
        if not reasons and any(s == symbol for s, _ in self._repair):
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
            flow.snapshot(at).validity is DataQuality.OK for flow in self.flows.values()
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
        for (symbol, tf), loss_version in sorted(tuple(self._repair.items())):
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
            for left, right in zip(bars, bars[1:], strict=False):
                if right.open_at - left.open_at > step:
                    start = left.open_at + step
                    end = min(
                        right.open_at,
                        start + step * min(8000, self.state.capacity),
                    )
                    break

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

            if await recovery.bars(key, tf, start, end, valid=valid):
                if valid():
                    if self.state.view(key, tf, self.clock()).quality is DataQuality.OK:
                        self._repair.pop((symbol, tf), None)
                    self._dirty.add((symbol, tf))
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
            if book.state is StreamState.UNSYNCED and any(
                name.startswith(symbol.lower() + "@depth") for name in self._generations
            ):
                await recovery.depth(book)
