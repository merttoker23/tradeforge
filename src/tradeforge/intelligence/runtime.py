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
from tradeforge.intelligence.models import FeatureSnapshot, digest
from tradeforge.intelligence.opportunity import SurveillancePolicy
from tradeforge.intelligence.ranking import rank_market
from tradeforge.intelligence.regime import RegimeEngine
from tradeforge.intelligence.warmup import seed_history
from tradeforge.market.bars import canonical
from tradeforge.market.instrument_registry import InstrumentRegistry
from tradeforge.persistence.db import Database
from tradeforge.persistence.repositories import instrument_id
from tradeforge.runtime.progress import RuntimeProgress
from tradeforge.runtime.storage import save_runtime

HISTORY_QUERY = text("""
                SELECT bars.input_hash,bars.payload FROM unnest(CAST(:ids AS uuid[])) AS ids(id)
                CROSS JOIN LATERAL (
                    SELECT DISTINCT ON(open_at) input_hash,payload,open_at
                    FROM closed_bar_observations
                    WHERE instrument_id=ids.id AND timeframe=:tf
                        AND observed_at<=:at AND close_at<=:at
                    ORDER BY open_at DESC,observed_at DESC LIMIT 33
                ) bars
                ORDER BY ids.id,bars.open_at
            """)


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
        self._bar_cache: dict[str, ClosedBar] = {}
        self._history_version: str | None = None
        self._history_signature: tuple[str, ...] | None = None
        self._analyzed_history_signature: tuple[str, ...] | None = None
        self._history_seed_key: tuple[object, ...] | None = None
        self._history_seed: RegimeEngine | None = None
        self.state: dict[str, Any] = self._state("STARTING", False, 0)

    def _state(self, reason: str, ready: bool, processed: int) -> dict[str, Any]:
        latest = self.detector.latest
        return {
            **self.progress.state,
            **self.history_metrics,
            "evaluated_at": self.clock().isoformat(),
            "alive": True,
            "data_ready": ready,
            "trading_ready": False,
            "live": False,
            "orders_enabled": False,
            "reason": reason,
            "processed": processed,
            "last_source_at": latest.frame.source_at.isoformat() if latest else None,
            "regime_label": latest.state.label.value if latest else "UNKNOWN",
            "regime_reasons": list(latest.reason_codes) if latest else [],
        }

    def status(self) -> dict[str, Any]:
        state = dict(self.state)
        age = (utc(self.clock()) - datetime.fromisoformat(state["evaluated_at"])).total_seconds()
        if not 0 <= age <= 15:
            state.update(
                data_ready=False,
                analysis_ready=False,
                execution_ready=False,
                reason="QUANT_TICK_STALE",
            )
        return state

    def _load(self, now: datetime) -> None:
        with self.db.transaction() as c:
            payload = c.scalar(
                text(
                    "SELECT payload FROM regime_snapshots WHERE as_of<=:at "
                    "ORDER BY as_of DESC LIMIT 1"
                ),
                {"at": now},
            )
        if payload is not None:
            # Cursor survives crashes; statistical history warms up conservatively
            # after restart. Never claim recovered confidence from a heartbeat.
            self.detector.latest = FeatureSnapshot.model_validate_json(
                canonical(payload), context={"now": now}
            )
        self.loaded = True

    def _frame(self, now: datetime) -> tuple[MarketFrame | None, str]:
        self.rebuild_current = False
        self.progress.advance("UNIVERSE_INPUT")
        view = InstrumentRegistry(self.db).view(now)
        if not view.ready or view.observation is None or view.observation.universe is None:
            return None, view.reason
        universe = view.observation.universe
        if len(universe.eligible) > self.detector.policy.max_symbols:
            return None, "SYMBOL_BUDGET_EXHAUSTED"
        metadata_changed = self._history_version != universe.version_hash
        if metadata_changed:
            self._bar_cache = {}
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
            if self._analyzed_history_signature is None and self.detector.latest is not None:
                previous_rows = c.execute(
                    HISTORY_QUERY,
                    {
                        "ids": identifiers,
                        "tf": self.detector.policy.timeframe.value,
                        "at": self.detector.latest.frame.as_of,
                    },
                ).all()
                self._analyzed_history_signature = tuple(row.input_hash for row in previous_rows)
            rows = c.execute(
                HISTORY_QUERY,
                {
                    "ids": identifiers,
                    "tf": self.detector.policy.timeframe.value,
                    "at": now,
                },
            ).all()
            self.history_metrics.update(
                history_query_ms=round((time.perf_counter() - query_started) * 1000, 3),
                history_rows=len(rows),
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
        series: dict[tuple[str, ...], list[ClosedBar]] = {}
        self.progress.advance("HISTORY_PARSE")
        parse_started = time.perf_counter()
        signature = tuple(row.input_hash for row in rows)
        history_changed = (
            self._analyzed_history_signature is not None
            and signature != self._analyzed_history_signature
        )
        parsed: dict[str, ClosedBar] = {}
        self.history = []
        parsed_count = 0
        for row in rows:
            bar = self._bar_cache.get(row.input_hash)
            if bar is None:
                bar = ClosedBar.model_validate_json(row.payload, context={"now": now})
                parsed_count += 1
            if bar.as_of > now:
                raise ValueError("FUTURE_HISTORY_INPUT")
            if bar.key[:-1] in allowed:
                series.setdefault(bar.key, []).append(bar)
                self.history.append(bar)
                parsed[row.input_hash] = bar
        self._bar_cache = parsed
        self._history_signature = signature
        self.history_metrics.update(
            history_parsed=parsed_count,
            history_parse_ms=round((time.perf_counter() - parse_started) * 1000, 3),
        )
        seconds = self.detector.policy.timeframe.seconds
        current_close = datetime.fromtimestamp(
            int(now.timestamp() - 2) // seconds * seconds, UTC
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
        # An unfinished global sweep is not an analysis barrier once enough
        # members prove the same finalized current period. Older members never
        # count toward this bypass, even within the normal analysis age limit.
        if sweep_incomplete and current_coverage < self.detector.policy.minimum_coverage:
            return None, "WAITING_HISTORY_SWEEP"
        closed = (
            current_close
            if sweep_incomplete
            else max((b.close_at for bars in series.values() for b in bars), default=None)
        )
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
            # Changed-only partial quote sets may alternate forever. Enrich on
            # improved coverage, or coalesce equal/lower coverage for 15 seconds.
            # Candle revisions/new closes above are never delayed by this limit.
            enrich_cost = (
                len(fresh_cost) > len(known_stress)
                or (now - self.detector.latest.frame.as_of).total_seconds() >= 15
            )
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
        observations = []
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
        identifier = uuid5(NAMESPACE_URL, snapshot.input_version)
        ranked = rank_market(snapshot, SurveillancePolicy(), now=at)
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
            for row in ranked:
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
            ready = bool(
                fresh
                and latest
                and latest.state.label not in {RegimeLabel.UNKNOWN, RegimeLabel.TRANSITION}
                and reason in {"ANALYZED", "WAITING_CLOSED_BAR"}
            )
            if not fresh and latest is not None:
                reason = "ANALYSIS_STALE"
            elif fresh and latest and reason in {"ANALYZED", "WAITING_CLOSED_BAR"}:
                if latest.state.label is RegimeLabel.UNKNOWN:
                    reason = "REGIME_UNKNOWN"
                elif latest.state.label is RegimeLabel.TRANSITION:
                    reason = "REGIME_TRANSITION"
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
