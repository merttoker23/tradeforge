"""Deterministic market subscription budgets and bounded priority shedding."""

from collections import deque
from dataclasses import dataclass

from tradeforge.market.streams import GLOBAL_STREAMS, PARTIAL_DEPTH, MarketEvent, StreamSpec


@dataclass(frozen=True)
class SubscriptionPlan:
    eligible: frozenset[str]
    groups: tuple[tuple[StreamSpec, ...], ...]
    omitted: tuple[str, ...]
    protected: frozenset[str]


def plan_subscriptions(
    eligible: frozenset[str],
    *,
    watched: tuple[str, ...] = (),
    deep: tuple[str, ...] = (),
    protected: tuple[str, ...] = (),
    max_sockets: int = 8,
    streams_per_socket: int = 1024,
    partial_depth: bool = False,
) -> SubscriptionPlan:
    if not 1 <= max_sockets <= 32 or not 2 <= streams_per_socket <= 1024:
        raise ValueError("INVALID_SUBSCRIPTION_BUDGET")
    if not eligible or not set(watched + deep + protected) <= eligible:
        raise ValueError("INVALID_SUBSCRIPTION_UNIVERSE")
    groups: list[list[StreamSpec]] = []
    added: set[StreamSpec] = set()
    omitted = []

    def add(spec: StreamSpec, required: bool) -> None:
        if spec in added:
            return
        for group in groups:
            if (
                group[0].route == spec.route
                and len(group) < streams_per_socket
                and (group[0].name in GLOBAL_STREAMS) == (spec.name in GLOBAL_STREAMS)
            ):
                group.append(spec)
                added.add(spec)
                return
        if len(groups) < max_sockets:
            groups.append([spec])
            added.add(spec)
        elif required:
            raise ValueError("PROTECTED_MARKET_BUDGET_EXCEEDED")
        else:
            omitted.append(spec.name)

    for name in GLOBAL_STREAMS:
        add(StreamSpec(name), True)
    # Every eligible pair must deliver its closed 15m bar. Optional tiers only
    # add higher-frequency or depth evidence on top of this mandatory sweep.
    for symbol in sorted(eligible):
        add(StreamSpec(f"{symbol.lower()}@kline_15m"), True)
    full_book = ("bookTicker", "depth@100ms", "aggTrade")
    strategy_book = (PARTIAL_DEPTH, "aggTrade") if partial_depth else full_book
    protected_set = frozenset(protected)
    for symbols, required, suffixes in (
        (protected, True, full_book),
        # The deep tier reads the closed 15m features and the 5m setup bars.
        # It does not read the 1m series, so it must not depend on a
        # one-minute candle stream that is only kept for display.
        (deep, False, (*strategy_book, "kline_5m")),
        (watched, False, ("bookTicker", "kline_1m", "kline_5m")),
    ):
        for symbol in dict.fromkeys(symbols):
            for suffix in suffixes:
                if suffix == PARTIAL_DEPTH and symbol in protected_set:
                    continue
                add(StreamSpec(f"{symbol.lower()}@{suffix}"), required)
    return SubscriptionPlan(
        eligible, tuple(tuple(g) for g in groups), tuple(omitted), frozenset(protected)
    )


@dataclass(frozen=True)
class QueuedMarket:
    event: MarketEvent
    priority: int
    size: int


class MarketQueue:
    """Only market data enters here. Never account, execution, risk or protection commands.

    Priority 3 reserves observations of protected symbols, not actual protection
    events. Those travel independently. A loss always increments a visible counter.
    """

    def __init__(self, capacity: int = 4096, *, max_bytes: int = 16 * 1024 * 1024) -> None:
        if type(capacity) is not int or not 1 <= capacity <= 100000:
            raise ValueError("INVALID_MARKET_QUEUE_CAPACITY")
        if type(max_bytes) is not int or not 1 <= max_bytes <= 64 * 1024 * 1024:
            raise ValueError("INVALID_MARKET_BYTE_BUDGET")
        self.capacity, self.dropped = capacity, 0
        self.max_bytes, self.byte_size = max_bytes, 0
        self.evicted: tuple[MarketEvent, ...] = ()
        self._queues: list[deque[QueuedMarket]] = [deque() for _ in range(4)]

    def __len__(self) -> int:
        return sum(map(len, self._queues))

    def put(self, event: MarketEvent, *, priority: int) -> bool:
        if (
            not isinstance(event, MarketEvent)
            or type(priority) is not int
            or not 0 <= priority <= 3
        ):
            raise ValueError("MARKET_OBSERVATION_REQUIRED")
        size = len(event.payload_json.encode())
        removed: list[MarketEvent] = []
        self.evicted = ()
        if size > self.max_bytes:
            self.dropped += 1
            self.evicted = (event,)
            return False
        while len(self) >= self.capacity or self.byte_size + size > self.max_bytes:
            self.dropped += 1
            lowest = next(i for i, q in enumerate(self._queues) if q)
            if lowest > priority:
                self.evicted = (*removed, event)
                return False
            victim = self._queues[lowest].popleft()
            self.byte_size -= victim.size
            removed.append(victim.event)
        self.evicted = tuple(removed)
        self.byte_size += size
        self._queues[priority].append(QueuedMarket(event, priority, size))
        return True

    def get(self) -> QueuedMarket:
        for queue in reversed(self._queues):
            if queue:
                item = queue.popleft()
                self.byte_size -= item.size
                return item
        raise IndexError("EMPTY_MARKET_QUEUE")

    def discard_stream(self, stream: StreamSpec) -> None:
        for priority, queue in enumerate(self._queues):
            retained = deque()
            for item in queue:
                if item.event.stream == stream:
                    self.byte_size -= item.size
                else:
                    retained.append(item)
            self._queues[priority] = retained

    def clear(self) -> None:
        for queue in self._queues:
            queue.clear()
        self.byte_size = 0
        self.evicted = ()
