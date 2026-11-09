"""Registry-driven public ingestion with truthful, per-symbol stream readiness."""

import asyncio
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from tradeforge.domain.common import utc
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.intelligence.models import RegimePolicy
from tradeforge.market.canonical_feed import CanonicalFeed
from tradeforge.market.clock_sync import ClockSync
from tradeforge.market.instrument_registry import RegistryView
from tradeforge.market.socket_manager import AiohttpConnector, SocketManager, StreamFailure, pause
from tradeforge.market.streams import GLOBAL_STREAMS, MarketEvent, StreamSpec
from tradeforge.market.subscription_budget import MarketQueue, SubscriptionPlan, plan_subscriptions
from tradeforge.market.universe_refresh import UniverseRefresher


class MarketGateway:
    def __init__(
        self,
        clock_sync: ClockSync,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
        capacity: int = 4096,
        stale_after: float = 15,
        environment: str = "live",
    ) -> None:
        if environment not in {"testnet", "live"}:
            raise ValueError("BINANCE_ENVIRONMENT_INVALID")
        self.environment = environment
        if not 0 < stale_after <= 60:
            raise ValueError("INVALID_STREAM_TTL")
        self.clock_sync, self.clock, self.monotonic = clock_sync, clock, monotonic
        self.stale_after = stale_after
        self.queue = MarketQueue(capacity)
        self.view = RegistryView(None, False, "UNIVERSE_UNAVAILABLE")
        self.eligible: frozenset[str] = frozenset()
        self.plan: SubscriptionPlan | None = None
        self._generations: dict[str, int] = {}
        self._latest: dict[tuple[str, str], tuple[MarketEvent, float]] = {}
        self.rejected = 0
        self.rejection_reasons: dict[str, int] = {}
        self.clock_attempts = self.clock_failures = 0
        self.socket_reconnects = self.socket_failures = 0
        self.active_socket_reconnects = self.active_socket_failures = 0
        self.quote_attempts = self.quote_failures = 0
        self.quote_last_reason = "NOT_SAMPLED"
        self.quote_last_failure_at = ""
        self._epoch = 0
        self.canonical = CanonicalFeed(clock=clock)

    def _clear_queue(self) -> None:
        while len(self.queue):
            self.canonical.lost(self.queue.get().event)
        self.queue.clear()

    def update_universe(
        self,
        view: RegistryView,
        *,
        watched: tuple[str, ...] = (),
        deep: tuple[str, ...] = (),
        protected: tuple[str, ...] = (),
    ) -> None:
        self.view = view
        observation = view.observation
        if not view.ready or observation is None or observation.metadata is None:
            self.plan = None
            self.eligible = frozenset()
            self._latest.clear()
            self._generations.clear()
            self._clear_queue()
            self.canonical.configure((), ())
            return
        members = frozenset(i.symbol for i in observation.metadata.instruments)
        new_plan = plan_subscriptions(
            members,
            watched=watched,
            deep=deep,
            protected=protected,
            streams_per_socket=100 if self.environment == "testnet" else 1024,
        )
        if self.eligible != members:
            # A membership/plan change is a new ingestion epoch, not a cache merge.
            self._latest.clear()
            self._generations.clear()
            self._clear_queue()
        elif self.plan != new_plan:
            # Fence only changed socket groups. Global market streams retain
            # their original generations and source timestamps across selection.
            old_groups = set(self.plan.groups) if self.plan else set()
            retained = {s.name for group in new_plan.groups if group in old_groups for s in group}
            self._generations = {n: g for n, g in self._generations.items() if n in retained}
            self._latest = {k: v for k, v in self._latest.items() if k[0] in retained}
            self._clear_queue()
        self.eligible, self.plan = members, new_plan
        selected = {s.symbol for group in new_plan.groups for s in group if s.symbol}
        deep_symbols = {s.symbol for group in new_plan.groups for s in group if "@depth" in s.name}
        self.canonical.configure(
            tuple(
                InstrumentKey.model_validate(
                    {name: getattr(i, name) for name in InstrumentKey.model_fields}
                )
                for i in observation.metadata.instruments
                if i.symbol in selected
            ),
            tuple(sorted(s for s in deep_symbols if s is not None)),
            streams=tuple(s for group in new_plan.groups for s in group),
        )

    def socket_state(self, streams: tuple[StreamSpec, ...], state: str, generation: int) -> None:
        self.canonical.socket_state(streams, state, generation)
        for stream in streams:
            self._generations.pop(stream.name, None)
            if state == "SYNCING":
                self._generations[stream.name] = generation
            for key in [k for k in self._latest if k[0] == stream.name]:
                del self._latest[key]
        self._clear_queue()

    def ingest(self, event: MarketEvent) -> bool:
        key = (event.stream.name, event.symbol)
        now, mono = utc(self.clock()), self.monotonic()
        reason = (
            "UNIVERSE_UNAVAILABLE"
            if self.plan is None
            else "INELIGIBLE_SYMBOL"
            if event.symbol not in self.eligible
            else "STALE_GENERATION"
            if self._generations.get(event.stream.name) != event.generation
            else "INVALID_EVENT_TIME"
            if not event.event_time <= event.received_at <= now
            else "STALE_EVENT"
            if (now - event.event_time).total_seconds() > self.stale_after
            else None
        )
        if reason:
            self.rejected += 1
            self.rejection_reasons[reason] = self.rejection_reasons.get(reason, 0) + 1
            return False
        previous = self._latest.get(key)
        if previous:
            old = previous[0]
            if event.event_time < old.event_time or (
                event.exchange_sequence is not None
                and old.exchange_sequence is not None
                and event.exchange_sequence <= old.exchange_sequence
            ):
                self.rejected += 1
                self.rejection_reasons["OUT_OF_ORDER"] = (
                    self.rejection_reasons.get("OUT_OF_ORDER", 0) + 1
                )
                return False
            if event.event_time == old.event_time and event.exchange_sequence is None:
                if event.payload_json != old.payload_json and event.exchange_sequence is None:
                    self._latest.pop(key)
                else:
                    return False
                self.rejected += 1
                self.rejection_reasons["CONFLICTING_DUPLICATE"] = (
                    self.rejection_reasons.get("CONFLICTING_DUPLICATE", 0) + 1
                )
                return False
        self._latest[key] = event, mono
        assert self.plan is not None
        priority = (
            3
            if event.symbol in self.plan.protected
            else 0
            if event.stream.name in GLOBAL_STREAMS
            else 2
            if "@depth" in event.stream.name or "@aggTrade" in event.stream.name
            else 1
        )
        accepted = self.queue.put(event, priority=priority)
        for dropped in self.queue.evicted:
            self._latest.pop((dropped.stream.name, dropped.symbol), None)
            self.canonical.lost(dropped)
        return accepted

    def fresh(self, name: str, symbol: str) -> bool:
        now, mono = utc(self.clock()), self.monotonic()
        sample = self._latest.get((name, symbol))
        if sample is None:
            return False
        event, received_mono = sample
        return (
            self._generations.get(name) == event.generation
            and 0 <= mono - received_mono <= self.stale_after
            and 0 <= (now - event.event_time).total_seconds() <= self.stale_after
            and abs((now - event.received_at).total_seconds() - (mono - received_mono)) <= 1
        )

    def symbol_ready(self, symbol: str) -> bool:
        """Symbol evidence is independent of unrelated changed-only ticker members.

        Only activated streams (present in ``_generations``) block readiness; a
        planned but not yet connected stream must not hold a symbol back.
        """
        return bool(
            self.status()["data_ready"]
            and symbol in self.eligible
            and all(
                self.fresh(name, symbol) for name in GLOBAL_STREAMS if name in self._generations
            )
            and self.plan is not None
            and all(
                self.fresh(s.name, symbol)
                for group in self.plan.groups
                for s in group
                if s.symbol == symbol and s.name in self._generations
            )
            and self.canonical.symbol_ready(symbol, self.clock())
        )

    def status(self) -> dict[str, str | int | float | bool]:
        now, mono = utc(self.clock()), self.monotonic()
        covered = sum(
            all(self.fresh(name, symbol) for name in GLOBAL_STREAMS) for symbol in self.eligible
        )
        coverage = covered / len(self.eligible) if self.eligible else 0.0
        streams_fresh = all(
            any(self.fresh(name, symbol) for symbol in self.eligible) for name in GLOBAL_STREAMS
        )
        observation = self.view.observation
        reason = "OK"
        if not self.clock_sync.ready(now, mono):
            reason = "CLOCK_UNSAFE"
        elif not self.view.ready or observation is None or self.plan is None:
            reason = "UNIVERSE_UNAVAILABLE"
        elif not observation.observed_at <= now < observation.expires_at:
            reason = "UNIVERSE_STALE"
        elif not self.eligible or not covered:
            reason = "STREAMS_STALE"
        return {
            "state": "READY" if reason == "OK" else "STALE",
            "reason": reason,
            "data_ready": reason == "OK",
            "transport_ready": reason in {"OK", "STREAMS_STALE"} and streams_fresh,
            "analysis_coverage_ready": reason == "OK"
            and coverage >= RegimePolicy().minimum_coverage,
            "trading_ready": False,
            "canonical_ready": self.canonical.ready(now),
            "clock_attempts": self.clock_attempts,
            "clock_failures": self.clock_failures,
            "socket_reconnects": self.socket_reconnects + self.active_socket_reconnects,
            "socket_failures": self.socket_failures + self.active_socket_failures,
            "quote_attempts": self.quote_attempts,
            "quote_failures": self.quote_failures,
            "quote_last_reason": self.quote_last_reason,
            "quote_last_failure_at": self.quote_last_failure_at,
            "eligible": len(self.eligible),
            "covered": covered,
            "coverage": coverage,
            "queued": len(self.queue),
            "queued_payload_bytes": self.queue.byte_size,
            "dropped": self.queue.dropped,
            "rejected": self.rejected,
            "rejected_generation": self.rejection_reasons.get("STALE_GENERATION", 0),
            "rejected_time": self.rejection_reasons.get("INVALID_EVENT_TIME", 0),
            "rejected_stale": self.rejection_reasons.get("STALE_EVENT", 0),
            "rejected_sequence": self.rejection_reasons.get("OUT_OF_ORDER", 0),
            "rejected_conflict": self.rejection_reasons.get("CONFLICTING_DUPLICATE", 0),
            "subscriptions": sum(map(len, self.plan.groups)) if self.plan else 0,
            "omitted": len(self.plan.omitted) if self.plan else 0,
        }

    def telemetry(self) -> tuple[dict[str, object], datetime]:
        """Only validated current-generation observations constitute coverage evidence."""
        now = utc(self.clock())
        sample: dict[str, object] = dict(self.status())
        observation = self.view.observation
        events = [
            event
            for (name, _), (event, received_mono) in self._latest.items()
            if name in GLOBAL_STREAMS
            and self._generations.get(name) == event.generation
            and 0 <= (now - event.event_time).total_seconds() <= self.stale_after
            and 0 <= self.monotonic() - received_mono <= self.stale_after
            and abs((now - event.received_at).total_seconds() - (self.monotonic() - received_mono))
            <= 1
        ]
        samples = {(e.stream.name, e.symbol): e for e in events}
        coverage_deadlines = [
            min(
                samples[name, symbol].event_time + timedelta(seconds=self.stale_after)
                for name in GLOBAL_STREAMS
            ).isoformat()
            for symbol in sorted(self.eligible)
            if all((name, symbol) in samples for name in GLOBAL_STREAMS)
        ]
        deadline = now + timedelta(seconds=self.stale_after)
        if observation is not None:
            deadline = min(deadline, observation.expires_at)
        sample.update(
            measured=bool(events),
            coverage_deadlines=coverage_deadlines,
            universe_version=observation.version_hash if observation else None,
            clock_safe=self.clock_sync.ready(now, self.monotonic()),
            source_at=min(e.event_time for e in events).isoformat() if events else None,
            watched=sorted(
                {
                    s.symbol
                    for group in self.plan.groups
                    for s in group
                    if s.symbol is not None and "@kline_5m" in s.name
                }
            )
            if self.plan
            else [],
            deep=sorted(
                {
                    s.symbol
                    for group in self.plan.groups
                    for s in group
                    if s.symbol is not None and "@depth" in s.name
                }
            )
            if self.plan
            else [],
            protected=sorted(self.plan.protected) if self.plan else [],
        )
        return sample, deadline

    async def run(
        self,
        refresher: UniverseRefresher,
        connector: AiohttpConnector,
        stop: asyncio.Event,
        *,
        consume: Callable[[MarketEvent], None] | None = None,
        watched: tuple[str, ...] = (),
        deep: tuple[str, ...] = (),
        selection: Callable[[], tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]]
        | None = None,
    ) -> None:
        """Metadata I/O runs in a worker; ingestion never waits on synchronous DB work.

        Consumers must be nonblocking. Canonical memory is updated here; its
        recovery and persistence worker runs separately from market ingestion.
        """
        managers: dict[
            tuple[StreamSpec, ...], tuple[asyncio.Task[None], asyncio.Event, SocketManager]
        ] = {}
        tokens: dict[tuple[StreamSpec, ...], int] = {}
        current_plan: SubscriptionPlan | None = None

        async def sync_clock() -> None:
            while not stop.is_set():
                delay = 30.0
                for attempt in range(3):
                    self.clock_attempts += 1
                    try:
                        sent = self.clock()
                        server = await connector.server_time()
                        if self.clock_sync.observe(sent, self.clock(), server, self.monotonic()):
                            break
                        self.clock_failures += 1
                        if self.clock_sync.reason != "CLOCK_RTT":
                            break
                    except StreamFailure as error:
                        self.clock_failures += 1
                        delay = max(delay, error.retry_after)
                        break
                    except Exception:
                        self.clock_failures += 1
                    if attempt < 2:
                        await pause(stop, 0.25 * 2**attempt)
                    if stop.is_set():
                        return
                await pause(stop, delay)

        async def metadata() -> None:
            nonlocal current_plan
            while not stop.is_set():
                try:
                    view = await asyncio.to_thread(refresher.tick)
                except Exception:
                    view = RegistryView(None, False, "UNIVERSE_UNAVAILABLE")
                chosen_watch, chosen_deep, protected = (
                    selection() if selection else (watched, deep, ())
                )
                if chosen_watch or chosen_deep or protected:
                    observation = view.observation
                    available = (
                        {i.symbol for i in observation.metadata.instruments}
                        if (observation and observation.metadata)
                        else set()
                    )
                    self.update_universe(
                        view,
                        watched=tuple(
                            s
                            for s in dict.fromkeys((*chosen_watch, *chosen_deep))
                            if s in available
                        ),
                        deep=tuple(s for s in dict.fromkeys(chosen_deep) if s in available),
                        protected=tuple(s for s in protected if s in available),
                    )
                else:
                    self.update_universe(view)
                if self.plan != current_plan:
                    groups = set(self.plan.groups) if self.plan else set()
                    membership_changed = current_plan is not None and (
                        self.plan is None or current_plan.eligible != self.plan.eligible
                    )
                    retiring = []
                    for group in list(managers):
                        if group in groups and not membership_changed:
                            continue
                        task, group_stop, manager = managers.pop(group)
                        tokens.pop(group, None)  # Fence before cancellation can run callbacks.
                        group_stop.set()
                        task.cancel()
                        retiring.append(task)
                        self.socket_reconnects += manager.reconnects
                        self.socket_failures += manager.failures
                    await asyncio.gather(*retiring, return_exceptions=True)
                    current_plan = self.plan
                    if self.plan:
                        for group in self.plan.groups:
                            if group in managers:
                                continue
                            self._epoch += 1
                            tokens[group] = self._epoch

                            def state(
                                value: str,
                                generation: int,
                                streams: tuple[StreamSpec, ...] = group,
                                expected_epoch: int = self._epoch,
                            ) -> None:
                                if tokens.get(streams) == expected_epoch:
                                    self.socket_state(streams, value, generation)

                            def event(
                                value: MarketEvent,
                                expected_epoch: int = self._epoch,
                                streams: tuple[StreamSpec, ...] = group,
                            ) -> None:
                                if tokens.get(streams) == expected_epoch:
                                    self.ingest(value)

                            manager = SocketManager(
                                group,
                                connector.open,
                                eligible=lambda: self.eligible,
                                on_event=event,
                                on_state=state,
                                clock=self.clock,
                                monotonic=self.monotonic,
                                ack_timeout=15 if self.environment == "testnet" else 5,
                            )
                            # A replacement manager must never reuse an old generation.
                            manager.generation = self._epoch << 32
                            group_stop = asyncio.Event()
                            managers[group] = (
                                asyncio.create_task(manager.run(group_stop)),
                                group_stop,
                                manager,
                            )
                self.active_socket_reconnects = sum(m.reconnects for _, _, m in managers.values())
                self.active_socket_failures = sum(m.failures for _, _, m in managers.values())
                await pause(stop, 1)

        clock_task = asyncio.create_task(sync_clock())
        metadata_task = asyncio.create_task(metadata())
        try:
            while not stop.is_set():
                if (
                    metadata_task.done()
                    or clock_task.done()
                    or any(task.done() for task, _, _ in managers.values())
                ):
                    raise RuntimeError("GATEWAY_WORKER_STOPPED")
                # Bound each batch to avoid starving PING/receive and metadata tasks.
                for _ in range(min(len(self.queue), 256)):
                    value = self.queue.get().event
                    self.canonical.consume(value)
                    if consume is not None:
                        consume(value)
                await pause(stop, 0.01)
        finally:
            self._epoch += 1
            tokens.clear()
            tasks = [clock_task, metadata_task, *(t for t, _, _ in managers.values())]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._generations.clear()
            self._latest.clear()
            self._clear_queue()
