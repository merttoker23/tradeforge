"""Continuous, closed-only PostgreSQL consumer. No network client or order authority."""

import time
from collections.abc import Callable
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import text

from tradeforge.domain.common import DataQuality, utc
from tradeforge.domain.market import ClosedBar, MarketSnapshot, RegimeLabel
from tradeforge.intelligence.inputs import MarketFrame, MarketObservation
from tradeforge.intelligence.models import (
    REGIME_ALGORITHM_VERSION,
    FeatureSnapshot,
    RegimePolicy,
    digest,
)
from tradeforge.intelligence.opportunity import SurveillancePolicy
from tradeforge.intelligence.ranking import rank_market
from tradeforge.intelligence.regime import RegimeEngine
from tradeforge.intelligence.warmup import seed_history
from tradeforge.market.bars import bar_content_hash, bar_hash, canonical
from tradeforge.market.instrument_registry import InstrumentRegistry
from tradeforge.persistence.db import Database
from tradeforge.persistence.repositories import instrument_id
from tradeforge.runtime.progress import RuntimeProgress
from tradeforge.runtime.storage import save_runtime

HISTORY_QUERY = text("""
                SELECT bars.input_hash,bars.content_hash,bars.observed_at,bars.payload
                FROM unnest(CAST(:ids AS uuid[])) AS ids(id)
                CROSS JOIN LATERAL (
                    SELECT DISTINCT ON(open_at) input_hash,content_hash,observed_at,payload,open_at
                    FROM closed_bar_observations
                    WHERE instrument_id=ids.id AND timeframe=:tf
                        AND observed_at<=:at AND close_at<=:at
                    ORDER BY open_at DESC,observed_at DESC LIMIT 33
                ) bars
                ORDER BY ids.id,bars.open_at
            """)

# Count probe: detects any change (new bars, revisions, backfills) in the
# observable history for our instruments without returning full row payloads.
# The probe is bounded to the analysis window. An unbounded count scans every
# retained period of every instrument (millions of rows) on each tick and
# exhausts the statement budget before the analysis can run.
COUNT_PROBE = text("""
            SELECT count(*) FROM closed_bar_observations bars
            WHERE bars.instrument_id IN (
                SELECT id FROM unnest(CAST(:ids AS uuid[])) AS ids(id))
                AND bars.timeframe=:tf
                AND bars.open_at>=:start
                AND bars.observed_at<=:at AND bars.close_at<=:at
        """)

MAX_OPPORTUNITIES = 64

# The benchmark instrument every relative (BTC-relative) feature is measured
# against. A frame without it cannot estimate beta or relative strength.
BENCHMARK_SYMBOL = "BTCUSDT"

# Analysis counters describe the last completed strategy evaluation. A tick
# replaces the worker state, but these counters describe the analysed pairs
# and must not be erased by an unrelated pipeline step.
ANALYSIS_COUNTERS = (
    "candidate_count",
    "reason_counts",
    "process_reason_counts",
    "last_hour",
    "evaluated_count",
    "analysis_symbols",
    "analyzed_symbols",
    "evaluation_total",
    "analysis_cycle_total",
    "last_analysis_at",
    "symbol_results",
    "symbol_summaries",
)


def frame_coverage(observations: tuple[MarketObservation, ...], eligible: tuple[Any, ...]) -> float:
    """Share of the eligible universe measured on the frame's closed period."""
    return len(observations) / len(eligible) if eligible else 0.0


def publish_block(
    observations: tuple[MarketObservation, ...],
    eligible: tuple[Any, ...],
    policy: RegimePolicy,
) -> str | None:
    """Reject a frame whose closed period or benchmark is still incomplete.

    A cross-sectional frame is only meaningful when every member is measured
    on the same closed period. Closed bars reach the store one instrument at
    a time, so a newly closed period is complete only after the universe
    reported it. Publishing a partial period would replace a consistent scan
    with a partial one and, whenever the benchmark is still missing, would
    destroy every relative (BTC-relative) feature of the universe.
    """
    if frame_coverage(observations, eligible) < policy.minimum_coverage:
        return "WAITING_HISTORY_SWEEP"
    if any(getattr(key, "symbol", None) == BENCHMARK_SYMBOL for key in eligible) and not any(
        row.instrument.symbol == BENCHMARK_SYMBOL for row in observations
    ):
        # The benchmark defines the relative feature set. Without it the
        # engine cannot estimate beta, so the period is not publishable.
        return "FRAME_BENCHMARK_MISSING"
    return None


class QuantWorker:
    def __init__(
        self,
        db: Database,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        require_gateway: bool = False,
    ) -> None:
        self.db, self.clock = db, clock
        self.require_gateway = require_gateway
        self.detector = RegimeEngine()
        self._before_current: RegimeEngine | None = None
        self.loaded = False
        self.history: list[ClosedBar] = []
        self.rebuild_current = False
        self.market_samples: tuple[MarketSnapshot, ...] | None = None
        self.progress = RuntimeProgress(clock=clock)
        self.history_metrics: dict[str, int | float] = {}
        self._rings: dict[tuple[str, ...], dict[datetime, ClosedBar]] = {}
        self._bar_cache: dict[str, ClosedBar] = {}
        self._max_observed_at: datetime | None = None
        self._db_total_count: int | None = None
        self._history_dirty = True
        self._last_persisted_version: str | None = None
        self._history_version: str | None = None
        self._history_signature: tuple[str, ...] | None = None
        self._analyzed_history_signature: tuple[str, ...] | None = None
        self._history_seed_key: tuple[object, ...] | None = None
        self._history_seed: RegimeEngine | None = None
        self.state: dict[str, Any] = self._state("STARTING", False, 0)

    def _state(self, reason: str, ready: bool, processed: int) -> dict[str, Any]:
        latest = self.detector.latest
        now = utc(self.clock())
        period = self.detector.policy.timeframe.seconds
        max_age = period * self.detector.policy.max_age_periods
        source_fresh = bool(
            latest and 0 <= (now - latest.frame.source_at).total_seconds() <= max_age
        )
        tradeable = bool(
            source_fresh
            and latest is not None
            and latest.state.label in {RegimeLabel.RANGE, RegimeLabel.TREND_EXPANSION}
        )
        state = {
            **self.progress.state,
            **self.history_metrics,
            "evaluated_at": now.isoformat(),
            "alive": True,
            "data_ready": ready,
            "analysis_ready": ready,
            "trading_ready": False,
            "live": False,
            "orders_enabled": False,
            "reason": reason,
            "processed": processed,
            "last_source_at": latest.frame.source_at.isoformat() if latest else None,
            "source_fresh": source_fresh,
            "regime_tradeable": tradeable,
            "analysis_timeframe": self.detector.policy.timeframe.value,
            "max_source_age_seconds": max_age,
            "regime_label": latest.state.label.value if latest else "UNKNOWN",
            "regime_reasons": list(latest.reason_codes) if latest else [],
            "regime_metrics": dict(latest.state.metrics) if latest else {},
        }
        # The last completed evaluation survives a tick that produced no new
        # strategy evidence. Only a newer evaluation may replace it.
        previous = getattr(self, "state", None) or {}
        for key in ANALYSIS_COUNTERS:
            if key in previous:
                state[key] = previous[key]
        return state

    def status(self) -> dict[str, Any]:
        state = dict(self.state)
        age = (utc(self.clock()) - datetime.fromisoformat(state["evaluated_at"])).total_seconds()
        if not 0 <= age <= 15:
            state.update(
                data_ready=False,
                analysis_ready=False,
                execution_ready=False,
                source_fresh=False,
                regime_tradeable=False,
                reason="QUANT_TICK_STALE",
            )
        return state

    def _load(self, now: datetime) -> None:
        policy = self.detector.policy
        period = timedelta(seconds=policy.timeframe.seconds)
        cursor = now
        history: list[FeatureSnapshot] = []
        for _ in range(policy.confirmation + policy.cooldown + 1):
            with self.db.transaction() as c:
                payload = c.scalar(
                    text(
                        "SELECT payload FROM regime_snapshots WHERE as_of<=:at "
                        "ORDER BY as_of DESC LIMIT 1"
                    ),
                    {"at": cursor},
                )
            if payload is None:
                break
            snapshot = FeatureSnapshot.model_validate_json(canonical(payload), context={"now": now})
            if not history:
                self.detector.latest = snapshot
            if snapshot.policy != policy or snapshot.algorithm_version != REGIME_ALGORITHM_VERSION:
                break
            if history and history[-1].frame.source_at - snapshot.frame.source_at != period:
                break
            history.append(snapshot)
            # Revisions of this closed period share its source time. Only an
            # earlier, distinct period can supply another confirmation.
            cursor = snapshot.frame.source_at - timedelta(microseconds=1)
        if history:
            restored = RegimeEngine(policy)
            for snapshot in reversed(history):
                if snapshot is history[0]:
                    self._before_current = deepcopy(restored)
                restored.restore_confirmation(snapshot)
            self.detector = restored
        self.loaded = True

    @property
    def _history_limit(self) -> int:
        return self.detector.policy.window + 1

    def _parse(self, row: Any, now: datetime) -> ClosedBar:
        bar = self._bar_cache.get(row.input_hash)
        if bar is None:
            bar = ClosedBar.model_validate_json(row.payload, context={"now": now})
            self._bar_cache[row.input_hash] = bar
            self.history_metrics["history_parsed"] = (
                self.history_metrics.get("history_parsed", 0) + 1
            )
        if bar.as_of > now:
            raise ValueError("FUTURE_HISTORY_INPUT")
        return bar

    def _store(self, bar: ClosedBar, input_hash: str) -> None:
        ring = self._rings.setdefault(bar.key, {})
        old = ring.get(bar.open_at)
        if old is not None:
            if bar.observed_at < old.observed_at or old == bar:
                return
            if bar.observed_at == old.observed_at:
                raise ValueError("CONFLICTING_HISTORY_REVISION")
        elif len(ring) >= self._history_limit:
            oldest = min(ring)
            if bar.open_at <= oldest:
                return
            removed = ring.pop(oldest)
            self._bar_cache.pop(bar_hash(removed), None)
        if old is not None:
            self._bar_cache.pop(bar_hash(old), None)
        ring[bar.open_at] = bar
        self._bar_cache[input_hash] = bar
        self._history_signature = None
        self._history_dirty = True

    def _load_full(self, rows: tuple[Any, ...] | list[Any], now: datetime) -> None:
        self._rings.clear()
        # Reuse parsed rows by input hash, then release those outside the ring.
        retained = {}
        for row in rows:
            bar = self._parse(row, now)
            self._rings.setdefault(bar.key, {})[bar.open_at] = bar
            retained[row.input_hash] = bar
        self._bar_cache = retained
        self._history_signature = self._signature(rows)
        if rows:
            self._max_observed_at = max(row.observed_at for row in rows)
        self._history_dirty = True

    def _signature(self, rows: tuple[Any, ...] | list[Any]) -> tuple[str, ...]:
        return tuple(sorted(row.content_hash for row in rows))

    def _current_signature(self) -> tuple[str, ...]:
        return tuple(
            sorted(bar_content_hash(bar) for ring in self._rings.values() for bar in ring.values())
        )

    def _refresh_history(self) -> None:
        if not self._history_dirty:
            return
        self.history = [
            bar for key in sorted(self._rings) for _, bar in sorted(self._rings[key].items())
        ]
        if self._history_signature is None:
            self._history_signature = self._current_signature()
        self._history_dirty = False

    def _frame(self, now: datetime) -> tuple[MarketFrame | None, str]:
        self.rebuild_current = False
        self.history_metrics["history_parsed"] = 0
        self.progress.advance("UNIVERSE_INPUT")
        view = InstrumentRegistry(self.db).view(now)
        if not view.ready or view.observation is None or view.observation.universe is None:
            return None, view.reason
        universe = view.observation.universe
        if len(universe.eligible) > self.detector.policy.max_symbols:
            return None, "SYMBOL_BUDGET_EXHAUSTED"
        metadata_changed = self._history_version != universe.version_hash
        if metadata_changed:
            self._rings.clear()
            self._bar_cache.clear()
            self._max_observed_at = None
            self._db_total_count = None
            self._history_signature = None
            self._analyzed_history_signature = None
            self._history_dirty = True
        self._history_version = universe.version_hash
        with self.db.transaction() as c:
            sweep = c.execute(
                text(
                    "SELECT payload,observed_at FROM runtime_observations "
                    "WHERE service='market_gateway' "
                    "ORDER BY observed_at DESC,sample_id DESC LIMIT 1"
                ),
            ).first()
            # The heartbeat is a single upserted row. A concurrent newer pulse
            # must not disappear behind the timestamp captured before DB I/O.
            # Capture evaluation time after reading it; future evidence still fails.
            now = utc(self.clock())
            if sweep is not None and sweep.observed_at > now:
                sweep = None
            if self.require_gateway and (
                sweep is None
                or sweep.payload.get("clock_safe") is not True
                or (now - sweep.observed_at).total_seconds() > 15
            ):
                return None, "GATEWAY_TELEMETRY_UNSAFE"
            sweep_incomplete = sweep is not None and sweep.payload.get("history_complete") is False
            self.progress.advance("HISTORY_QUERY")
            query_started = time.perf_counter()
            # The primary-key prefix lets each instrument stop at its latest
            # 33 closed periods; unrelated symbols and old retained history do
            # not join a global sort on every two-second analysis tick.
            identifiers = [instrument_id(k) for k in universe.eligible]
            timeframe = self.detector.policy.timeframe
            if self._analyzed_history_signature is None and self.detector.latest is not None:
                previous_rows = c.execute(
                    HISTORY_QUERY,
                    {
                        "ids": identifiers,
                        "tf": timeframe.value,
                        "at": self.detector.latest.frame.as_of,
                    },
                ).all()
                self._analyzed_history_signature = self._signature(list(previous_rows))
            # Change detection: compare the live DB count against the count
            # captured at the last full load. Any difference means new,
            # revised, or backfilled evidence arrived and we must reload the
            # bounded window. When the count is unchanged, only an incremental
            # probe for recently observed revisions is needed.
            window_start = now - timedelta(seconds=timeframe.seconds * (self._history_limit + 1))
            live_count = c.execute(
                COUNT_PROBE,
                {
                    "ids": identifiers,
                    "tf": timeframe.value,
                    "at": now,
                    "start": window_start,
                },
            ).scalar()
            if self._max_observed_at is None or live_count != self._db_total_count:
                # Full bounded reload: first load, metadata change, or
                # evidence count changed (new/backfilled/revised bars).
                rows = c.execute(
                    HISTORY_QUERY,
                    {"ids": identifiers, "tf": timeframe.value, "at": now},
                ).all()
                self._load_full(list(rows), now)
                self._db_total_count = live_count
                self.history_metrics.update(
                    history_query_ms=round((time.perf_counter() - query_started) * 1000, 3),
                    history_rows=len(rows),
                    history_new_rows=len(rows),
                    history_backfill_count=0,
                )
            else:
                # Count unchanged: probe only for recently observed revisions.
                # _db_total_count is not None here, so a full load has run and
                # _max_observed_at is set.
                cursor = self._max_observed_at - timedelta(seconds=max(60, timeframe.seconds // 4))
                new_rows = c.execute(
                    text("""
                        SELECT bars.input_hash,bars.content_hash,bars.observed_at,bars.payload
                        FROM closed_bar_observations bars
                        WHERE bars.instrument_id IN (
                            SELECT id FROM unnest(CAST(:ids AS uuid[])) AS ids(id))
                            AND bars.timeframe=:tf
                            AND bars.observed_at>:cursor AND bars.observed_at<=:at
                            AND bars.close_at<=:at
                        ORDER BY bars.instrument_id,bars.open_at,bars.observed_at
                    """),
                    {"ids": identifiers, "tf": timeframe.value, "at": now, "cursor": cursor},
                ).all()
                if new_rows:
                    # Merge new/revised observations into the bounded rings.
                    parsed = 0
                    for row in new_rows:
                        if row.input_hash not in self._bar_cache:
                            parsed += 1
                        bar = self._parse(row, now)
                        self._store(bar, row.input_hash)
                        self._max_observed_at = max(
                            self._max_observed_at or row.observed_at, row.observed_at
                        )
                    self.history_metrics.update(
                        history_query_ms=round((time.perf_counter() - query_started) * 1000, 3),
                        history_rows=len(new_rows),
                        history_new_rows=len(new_rows),
                        history_backfill_count=0,
                        history_parsed=parsed,
                    )
                else:
                    self.history_metrics.update(
                        history_query_ms=round((time.perf_counter() - query_started) * 1000, 3),
                        history_rows=0,
                        history_new_rows=0,
                        history_backfill_count=0,
                    )
            markets = c.execute(
                text(
                    "SELECT instrument_id,payload FROM runtime_market "
                    "WHERE observed_at<=:at AND source_at>=:start"
                ),
                {"at": now, "start": now - timedelta(seconds=15)},
            ).all()
        quotes = {
            row.instrument_id: MarketSnapshot.model_validate_json(
                canonical(row.payload), context={"now": now}
            )
            for row in markets
        }
        if self.market_samples is not None:
            quotes = {
                instrument_id(m): m
                for m in self.market_samples
                if m.observed_at <= now and 0 <= (now - m.source_at).total_seconds() <= 15
            }
        allowed = {k.key: k for k in universe.eligible}
        self.progress.advance("HISTORY_PARSE")
        parse_started = time.perf_counter()
        self._refresh_history()
        signature = self._history_signature or ()
        history_changed = (
            self._analyzed_history_signature is not None
            and signature != self._analyzed_history_signature
        )
        series: dict[tuple[str, ...], list[ClosedBar]] = {
            key: list(ring.values()) for key, ring in self._rings.items() if key[:-1] in allowed
        }
        self.history_metrics.update(
            history_parse_ms=round((time.perf_counter() - parse_started) * 1000, 3),
        )
        seconds = self.detector.policy.timeframe.seconds
        current_close = datetime.fromtimestamp(
            int(now.timestamp()) // seconds * seconds, UTC
        ) - timedelta(milliseconds=1)
        current_members = 0
        for bars in series.values():
            bars.sort(key=lambda b: b.open_at)
            if (
                len(bars) >= 2
                and bars[-1].close_at == current_close
                and bars[-1].open_at - bars[-2].open_at == timedelta(seconds=seconds)
            ):
                current_members += 1
        current_coverage = current_members / len(universe.eligible) if universe.eligible else 0.0
        self.history_metrics.update(
            history_current_members=current_members,
            history_current_coverage=current_coverage,
        )
        # Partial history never blocks per-pair analysis. Coverage below the
        # regime policy remains visible as LOW_COVERAGE and makes the market
        # regime non-tradeable, but already-finalized bars are still evidence.
        self.history_metrics["history_sweep_complete"] = (
            current_coverage >= self.detector.policy.minimum_coverage
        )
        self.history_metrics["gateway_history_incomplete"] = sweep_incomplete
        if sweep_incomplete and current_members == 0:
            # The gateway is still sweeping the current period and no member
            # has delivered it yet. An older complete period must not be
            # published as if it were the current measurement.
            return None, "WAITING_HISTORY_SWEEP"
        closed = max((b.close_at for bars in series.values() for b in bars), default=None)
        if closed is None:
            return None, "CLOSED_BARS_MISSING"
        if (
            not 0
            <= (now - closed).total_seconds()
            <= (self.detector.policy.timeframe.seconds * self.detector.policy.max_age_periods)
        ):
            return None, "ANALYSIS_STALE"
        if self.detector.latest is not None and closed <= self.detector.latest.frame.source_at:
            if closed < self.detector.latest.frame.source_at:
                return None, "WAITING_CLOSED_BAR"
            if now <= self.detector.latest.frame.as_of:
                return None, "WAITING_CLOSED_BAR"
            self.rebuild_current = (
                history_changed
                or not self.detector.series_count
                or self.detector.latest.frame.universe.version_hash != universe.version_hash
            )
            known = {f.instrument.key: f.sample_count for f in self.detector.latest.features}
            known_stress = {
                f.instrument.key
                for f in self.detector.latest.features
                if f.spread_bps is not None and f.funding is not None
            }
            previous_cost = {f.instrument.key: f for f in self.detector.latest.features}
            fresh_cost = {m.key: m for m in quotes.values() if m.quality is DataQuality.OK}
            # Cost evidence repairs a period only on improved coverage or a new
            # stress observation. Time alone must never rewrite a closed bar.
            enrich_cost = len(fresh_cost) > len(known_stress)
            policy = self.detector.policy
            for cost_key, cost_sample in fresh_cost.items():
                before = previous_cost.get(cost_key)
                spread = float(
                    20000
                    * (cost_sample.best_ask - cost_sample.best_bid)
                    / (cost_sample.best_ask + cost_sample.best_bid)
                )
                if (
                    abs(float(cost_sample.funding)) >= policy.stress_funding
                    and (
                        before is None
                        or before.funding is None
                        or abs(before.funding) < policy.stress_funding
                    )
                ) or (
                    spread >= policy.stress_spread_bps
                    and (
                        before is None
                        or before.spread_bps is None
                        or before.spread_bps < policy.stress_spread_bps
                    )
                ):
                    self.rebuild_current = True
            for history in series.values():
                ordered = sorted(history, key=lambda b: b.open_at)
                count = 0
                for a, b in zip(ordered[-2::-1], ordered[:0:-1], strict=True):
                    if b.open_at - a.open_at != timedelta(seconds=b.timeframe.seconds):
                        break
                    count += 1
                if ordered[-1].close_at == closed and count > known.get(ordered[-1].key[:-1], 0):
                    self.rebuild_current = True
                key = allowed[ordered[-1].key[:-1]]
                market = quotes.get(instrument_id(key))
                if (
                    ordered[-1].close_at == closed
                    and count > 0
                    and key.key not in known_stress
                    and market is not None
                    and market.quality is DataQuality.OK
                    and enrich_cost
                ):
                    # Newly recovered broad cost evidence repairs the same
                    # period once, without adding its return a second time.
                    self.rebuild_current = True
            if not self.rebuild_current:
                return None, "WAITING_CLOSED_BAR"
        observations: list[MarketObservation] = []
        for bars in series.values():
            bars.sort(key=lambda b: b.open_at)
            bars = bars[-2:]
            if len(bars) != 2 or bars[-1].close_at != closed:
                continue
            previous, current = bars
            if current.open_at - previous.open_at != timedelta(seconds=current.timeframe.seconds):
                continue
            quality = (
                DataQuality.OK
                if (now - current.close_at).total_seconds()
                <= current.timeframe.seconds * self.detector.policy.max_age_periods
                else DataQuality.STALE
            )
            key = allowed[current.key[:-1]]
            observations.append(
                MarketObservation(
                    previous=previous,
                    current=current,
                    market=quotes.get(instrument_id(key)),
                    quality=quality,
                )
            )
        if not observations:
            return None, "CONTIGUOUS_BARS_MISSING"
        frame_observations = tuple(observations)
        coverage = frame_coverage(frame_observations, universe.eligible)
        self.history_metrics["history_frame_members"] = len(frame_observations)
        self.history_metrics["history_frame_coverage"] = coverage
        block = publish_block(frame_observations, universe.eligible, self.detector.policy)
        if block is not None:
            return None, block
        frame = MarketFrame(
            schema_version=1,
            event_id=uuid5(
                NAMESPACE_URL, digest([closed.isoformat(), now.isoformat(), universe.version_hash])
            ),
            source_at=closed,
            observed_at=now,
            as_of=now,
            timeframe=self.detector.policy.timeframe,
            universe=universe,
            observations=tuple(observations),
        )
        return frame, "ANALYZED"

    def _seed_candidate(self, candidate: RegimeEngine, frame: MarketFrame) -> None:
        key = (
            self._history_signature,
            frame.source_at,
            frame.universe.version_hash,
            candidate.policy.model_dump_json(),
        )
        cached = self._history_seed
        if cached is None or self._history_seed_key != key:
            seed_history(candidate, self.history, frame)
            # Cache closed-return statistics only, before the current period is
            # applied. Never reuse quotes, regime confirmations or current returns.
            cached = RegimeEngine(candidate.policy)
            cached._series = deepcopy(candidate._series)
            cached._seed_source = candidate._seed_source
            cached._warmup_hash = candidate._warmup_hash
            self._history_seed, self._history_seed_key = cached, key
        else:
            candidate._series = deepcopy(cached._series)
            candidate._seed_source = cached._seed_source
            candidate._warmup_hash = cached._warmup_hash

    def _persist(self, snapshot: FeatureSnapshot) -> None:
        at = snapshot.state.as_of
        if snapshot.input_version == self._last_persisted_version:
            return
        identifier = uuid5(NAMESPACE_URL, snapshot.input_version)
        ranked = rank_market(snapshot, SurveillancePolicy(), now=at)
        # Evidence storage is bounded: only the highest-priority candidates are
        # retained per regime snapshot instead of every eligible symbol.
        selected = ranked[:MAX_OPPORTUNITIES]
        expiry = snapshot.frame.source_at + timedelta(
            seconds=snapshot.policy.timeframe.seconds * snapshot.policy.max_age_periods
        )
        with self.db.transaction() as c:
            c.execute(
                text("""
                INSERT INTO regime_snapshots(snapshot_id,input_version,as_of,payload)
                VALUES(:id,:version,:at,CAST(:payload AS jsonb)) ON CONFLICT DO NOTHING
            """),
                {
                    "id": identifier,
                    "version": snapshot.input_version,
                    "at": at,
                    "payload": canonical(snapshot.model_dump(mode="json")),
                },
            )
            for row in selected:
                payload = {
                    **row.model_dump(mode="json"),
                    "source_at": snapshot.frame.source_at.isoformat(),
                    "expires_at": expiry.isoformat(),
                    "trading_allowed": False,
                }
                c.execute(
                    text("""
                    INSERT INTO opportunities
                        (opportunity_id,instrument_id,regime_snapshot_id,observed_at,payload)
                    VALUES(:id,:instrument,:regime,:at,CAST(:payload AS jsonb))
                    ON CONFLICT DO NOTHING
                """),
                    {
                        "id": uuid5(identifier, row.instrument.symbol),
                        "instrument": instrument_id(row.instrument),
                        "regime": identifier,
                        "at": at,
                        "payload": canonical(payload),
                    },
                )
        self._last_persisted_version = snapshot.input_version

    def tick(self, *, persist: bool = True) -> dict[str, Any]:
        now = utc(self.clock())
        started = time.perf_counter()
        self.history_metrics = {
            "history_query_ms": 0.0,
            "history_parse_ms": 0.0,
            "history_rows": 0,
            "history_parsed": 0,
        }
        try:
            if not self.loaded:
                self.progress.advance("QUANT_LOAD")
                self._load(now)
            frame, reason = self._frame(now)
            processed = 0
            if frame is not None:
                self.progress.advance("REGIME_ANALYSIS")
                # A revised current candle replaces that period's contribution;
                # it must neither reset prior confirmations nor count twice.
                before = self.detector
                candidate = deepcopy(
                    self._before_current or RegimeEngine(self.detector.policy)
                    if self.rebuild_current
                    else self.detector
                )
                # Rehydrate actual return evidence on first frame after restart.
                # Once running, the engine retains its gap/revision semantics.
                if self.rebuild_current or not candidate.series_count:
                    self._seed_candidate(candidate, frame)
                snapshot = candidate.update(frame)
                self.progress.advance("REGIME_STORAGE")
                self._persist(snapshot)
                self.detector = candidate
                if not self.rebuild_current:
                    self._before_current = before
                self._analyzed_history_signature = self._history_signature
                processed = 1
            latest = self.detector.latest
            fresh = bool(
                latest
                and 0
                <= (now - latest.frame.source_at).total_seconds()
                <= self.detector.policy.timeframe.seconds * self.detector.policy.max_age_periods
            )
            # Readiness describes the evidence pipeline, not the strategy's
            # permission to trade. TRANSITION/UNKNOWN is a strategy filter and
            # must never stop per-pair analysis or turn health into an alarm.
            blocked = reason in {
                "UNIVERSE_MISSING",
                "UNIVERSE_STALE",
                "SYMBOL_BUDGET_EXHAUSTED",
                "GATEWAY_TELEMETRY_UNSAFE",
                "CLOSED_BARS_MISSING",
                "ANALYSIS_STALE",
                "CONTIGUOUS_BARS_MISSING",
                # A period that is not yet complete cannot be analysed. The
                # last complete scan stays visible, but it must not be
                # presented as a fresh measurement of the current period.
                "WAITING_HISTORY_SWEEP",
                "FRAME_BENCHMARK_MISSING",
            }
            if not fresh:
                ready = False
                if latest is not None and not blocked:
                    reason = "ANALYSIS_STALE"
            else:
                ready = not blocked
            if latest is None and not fresh and not blocked:
                reason = "ANALYSIS_NOT_OBSERVED"
            self.progress.state["healthy"] = True
            self.progress.complete(
                data_ready=ready, analysis_ready=ready, execution_ready=False, reason=reason
            )
            self.history_metrics["tick_ms"] = round((time.perf_counter() - started) * 1000, 3)
            self.state = self._state(reason, ready, processed)
            if persist:
                self.progress.advance("RUNTIME_STORAGE")
                save_runtime(self.db, "quant_core", now, now + timedelta(seconds=15), self.state)
        except Exception as error:
            self.progress.fail(error)
            self.progress.state["healthy"] = False
            self.history_metrics["tick_ms"] = round((time.perf_counter() - started) * 1000, 3)
            # Driver messages, SQL, URLs and payloads never escape this boundary.
            self.state = self._state("QUANT_STORAGE_OR_INPUT_UNAVAILABLE", False, 0)
        return self.state
