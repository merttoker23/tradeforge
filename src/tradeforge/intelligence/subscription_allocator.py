"""Stable optional surveillance and mandatory position-feed reservation.

The returned routed plan is a desired subscription set. The ingestion owner applies
its deltas and must verify actual stream readiness/loss using verify_monitoring;
having a desired subscription alone never establishes protection readiness.
"""

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Literal
from uuid import NAMESPACE_URL, uuid5

from tradeforge.domain.common import utc
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.domain.market import UniverseSnapshot
from tradeforge.intelligence.models import FeatureSnapshot, digest
from tradeforge.intelligence.opportunity import RankedOpportunity, SurveillancePolicy
from tradeforge.intelligence.ranking import rank_market
from tradeforge.market.streams import GLOBAL_STREAMS, StreamSpec
from tradeforge.market.subscription_budget import SubscriptionPlan, plan_subscriptions


def stream_names(plan: SubscriptionPlan) -> frozenset[str]:
    return frozenset(s.name for group in plan.groups for s in group)


@dataclass(frozen=True)
class SurveillanceAllocation:
    universe: UniverseSnapshot
    ranked: tuple[RankedOpportunity, ...]
    plan: SubscriptionPlan
    subscribe: frozenset[str]
    unsubscribe: frozenset[str]
    source_feature_hash: str
    source_universe_hash: str
    policy_hash: str
    reason_codes: tuple[str, ...]
    status: Literal["WATCHING", "NO_TRADE"]
    trading_allowed: Literal[False] = field(default=False, init=False)

    def verify_monitoring(
        self, active: set[str] | frozenset[str], *, lost: set[str] | frozenset[str] = frozenset()
    ) -> None:
        required = {
            f"{symbol.lower()}@{suffix}"
            for symbol in self.plan.protected
            for suffix in ("bookTicker", "depth@100ms", "aggTrade")
        }
        if not required <= active or required & lost:
            raise ValueError("POSITION_MONITORING_LOST")


class SubscriptionAllocator:
    def __init__(self, policy: SurveillancePolicy | None = None) -> None:
        self.policy = policy or SurveillancePolicy()
        self.latest: SurveillanceAllocation | None = None
        self._watch: dict[InstrumentKey, datetime] = {}
        self._deep: dict[InstrumentKey, datetime] = {}
        self._cooldowns: dict[tuple[str, InstrumentKey], datetime] = {}
        self._last_rebalance: datetime | None = None
        self._last_input: tuple[datetime, str, datetime, str] | None = None

    @property
    def retained_count(self) -> int:
        return len(self._watch) + len(self._deep) + len(self._cooldowns)

    def _select(
        self,
        tier: str,
        current: dict[InstrumentKey, datetime],
        candidates: tuple[RankedOpportunity, ...],
        budget: int,
        now: datetime,
        due: bool,
        cooldowns: dict[tuple[str, InstrumentKey], datetime],
    ) -> dict[InstrumentKey, datetime]:
        p = self.policy
        scores = {r.instrument: r.score for r in candidates if r.score is not None}
        selected = {k: at for k, at in current.items() if k in scores}
        for key in current.keys() - selected.keys():
            cooldowns[tier, key] = now + timedelta(seconds=p.cooldown_seconds)
        if not due:
            return selected
        for row in candidates:
            key, score = row.instrument, row.score
            if key in selected or cooldowns.get((tier, key), now) > now or score is None:
                continue
            if len(selected) < budget:
                selected[key] = now
        rotations = 0
        for row in candidates:
            key, score = row.instrument, row.score
            if key in selected or cooldowns.get((tier, key), now) > now or score is None:
                continue
            removable = [
                k for k, at in selected.items() if (now - at).total_seconds() >= p.dwell_seconds
            ]
            if not removable or rotations >= p.rotation_limit:
                break
            worst = min(removable, key=lambda k: (scores[k], k.key))
            old_score = scores[worst]
            assert old_score is not None
            if score > old_score + p.hysteresis:
                del selected[worst]
                cooldowns[tier, worst] = now + timedelta(seconds=p.cooldown_seconds)
                selected[key] = now
                rotations += 1
        return selected

    def _plan(
        self,
        eligible: tuple[InstrumentKey, ...],
        positions: tuple[InstrumentKey, ...],
        watch: dict[InstrumentKey, datetime],
        deep: dict[InstrumentKey, datetime],
        ranked: tuple[RankedOpportunity, ...],
    ) -> SubscriptionPlan:
        p = self.policy
        members = frozenset(k.symbol for k in (*eligible, *positions))
        if not members:
            return SubscriptionPlan(
                members, (tuple(StreamSpec(n) for n in GLOBAL_STREAMS),), (), frozenset()
            )
        priority = {r.instrument: n for n, r in enumerate(ranked)}
        while True:
            plan = plan_subscriptions(
                members,
                watched=tuple(k.symbol for k in sorted(watch, key=lambda k: priority[k])),
                deep=tuple(k.symbol for k in sorted(deep, key=lambda k: priority[k])),
                protected=tuple(k.symbol for k in positions),
                max_sockets=p.max_sockets,
                streams_per_socket=p.streams_per_socket,
            )
            if not plan.omitted:
                return plan
            # Never publish a partially subscribed optional tier. Capacity goes to
            # required feeds, then watch, then discretionary depth.
            if deep:
                del deep[max(deep, key=lambda k: priority[k])]
            elif watch:
                del watch[max(watch, key=lambda k: priority[k])]

    def update(
        self,
        snapshot: FeatureSnapshot,
        *,
        now: datetime,
        positions: tuple[InstrumentKey, ...] | None,
        queue_pressure: float = 0.0,
        paused: bool = False,
    ) -> SurveillanceAllocation:
        now = utc(now)
        p, frame = self.policy, snapshot.frame
        if self.latest and now < self.latest.universe.as_of:
            raise ValueError("CLOCK_ROLLBACK")
        if positions is None:
            raise ValueError("POSITION_STATE_UNKNOWN")
        if type(positions) is not tuple or len(set(positions)) != len(positions):
            raise ValueError("INVALID_POSITION_SET")
        if len(positions) > p.max_positions:
            raise ValueError("POSITION_BUDGET_EXCEEDED")
        positions = tuple(
            sorted((InstrumentKey.model_validate(k) for k in positions), key=lambda k: k.key)
        )
        if (
            type(queue_pressure) is not float
            or not math.isfinite(queue_pressure)
            or not 0 <= queue_pressure <= 1
            or type(paused) is not bool
        ):
            raise ValueError("INVALID_SURVEILLANCE_CONTROL")
        if len(frame.universe.eligible) > snapshot.policy.max_symbols:
            raise ValueError("SYMBOL_BUDGET_EXHAUSTED")
        source_hash = snapshot.digest
        incoming = (
            frame.as_of,
            source_hash,
            frame.universe.observed_at,
            frame.universe.version_hash,
        )
        if self._last_input:
            old_at, old_hash, universe_at, universe_hash = self._last_input
            if frame.as_of < old_at:
                raise ValueError("INPUT_ROLLBACK")
            if frame.as_of == old_at and source_hash != old_hash:
                raise ValueError("CONFLICTING_INPUT")
            if frame.universe.observed_at < universe_at or (
                frame.universe.observed_at == universe_at
                and frame.universe.version_hash != universe_hash
            ):
                raise ValueError("UNIVERSE_ROLLBACK")
        ranked = rank_market(snapshot, p, now=now)
        optional = tuple(r for r in ranked if r.score is not None and r.instrument not in positions)
        eligible = set(frame.universe.eligible)
        cooldowns = {
            key: until
            for key, until in self._cooldowns.items()
            if key[1] in eligible and until > now
        }
        due = (
            self._last_rebalance is None
            or (now - self._last_rebalance).total_seconds() >= p.rebalance_seconds
        )
        reasons = []
        if paused:
            reasons.append("PAUSED")
        if queue_pressure >= p.pressure_threshold:
            reasons.append("QUEUE_PRESSURE")
        watch = self._select(
            "watch", self._watch, () if paused else optional, p.watch, now, due, cooldowns
        )
        deep_candidates = tuple(r for r in optional if r.instrument in watch)
        deep = self._select(
            "deep",
            self._deep,
            () if paused or "QUEUE_PRESSURE" in reasons else deep_candidates,
            p.deep,
            now,
            due,
            cooldowns,
        )
        desired_watch, desired_deep = set(watch), set(deep)
        plan = self._plan(frame.universe.eligible, positions, watch, deep, ranked)
        if set(watch) != desired_watch or set(deep) != desired_deep:
            reasons.append("OPTIONAL_CAPACITY_SHED")
            for tier, removed in (
                ("watch", desired_watch - watch.keys()),
                ("deep", desired_deep - deep.keys()),
            ):
                for key in removed:
                    cooldowns[tier, key] = now + timedelta(seconds=p.cooldown_seconds)
        # Eligible positions appear in both tier projections. Positions outside the
        # tradable universe remain mandatory in plan.protected, never re-eligible.
        required = eligible & set(positions)
        watched = tuple(sorted(set(watch) | required, key=lambda k: k.key))
        deep_keys = tuple(sorted(set(deep) | required, key=lambda k: k.key))
        allocation_hash = digest(
            {
                "universe": frame.universe.version_hash,
                "as_of": now.isoformat(),
                "policy": p.policy_hash,
                "source": source_hash,
                "watch": [k.key for k in watched],
                "deep": [k.key for k in deep_keys],
                "protected": [k.key for k in positions],
            }
        )
        universe = UniverseSnapshot(
            schema_version=1,
            event_id=uuid5(NAMESPACE_URL, allocation_hash),
            source_at=frame.source_at,
            observed_at=frame.observed_at,
            as_of=now,
            eligible=frame.universe.eligible,
            watched=watched,
            deep=deep_keys,
            filtering_rules=frame.universe.filtering_rules,
            version_hash=allocation_hash,
        )
        previous_names = stream_names(self.latest.plan) if self.latest else frozenset()
        current_names = stream_names(plan)
        result = SurveillanceAllocation(
            universe,
            ranked,
            plan,
            current_names - previous_names,
            previous_names - current_names,
            source_hash,
            frame.universe.version_hash,
            p.policy_hash,
            tuple(reasons) or (("CANDIDATES_SELECTED",) if watch else ("NO_TRADE",)),
            "WATCHING" if watch else "NO_TRADE",
        )
        # No state changes before every validation and mandatory capacity check pass.
        self._watch, self._deep, self._cooldowns = watch, deep, cooldowns
        self.latest, self._last_input = result, incoming
        if due:
            self._last_rebalance = now
        return result
