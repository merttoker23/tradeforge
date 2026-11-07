"""Incremental fixed-policy global regime; ingestion is CPU-only and bounded."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from statistics import median
from uuid import NAMESPACE_URL, uuid5

from tradeforge.domain.common import DataQuality, ReasonCode
from tradeforge.domain.market import MarketRegimeState, RegimeLabel
from tradeforge.intelligence.breadth import summarize
from tradeforge.intelligence.change_points import Cusum
from tradeforge.intelligence.correlation import RollingPair
from tradeforge.intelligence.inputs import MarketFrame, MarketObservation
from tradeforge.intelligence.models import (
    REGIME_ALGORITHM_VERSION,
    FeatureSnapshot,
    InstrumentFeatures,
    RegimePolicy,
    feature_input_version,
)
from tradeforge.intelligence.relative_strength import relative_strength
from tradeforge.intelligence.volatility import RollingVolatility


@dataclass
class _Series:
    vol: RollingVolatility
    pair: RollingPair
    cusum: Cusum
    last_close: Decimal | None = None


class RegimeEngine:
    def __init__(self, policy: RegimePolicy | None = None) -> None:
        self.policy = policy or RegimePolicy()
        self._series: dict[tuple[str, ...], _Series] = {}
        self.latest: FeatureSnapshot | None = None
        self._stable = self._candidate = RegimeLabel.UNKNOWN
        self._streak = self._cooldown = 0
        self._seed_source: datetime | None = None
        self._warmup_hash: str | None = None

    @property
    def series_count(self) -> int:
        return len(self._series)

    def _new_pair(self) -> RollingPair:
        return RollingPair(self.policy.window, self.policy.minimum_history)

    def _new_series(self) -> _Series:
        p = self.policy
        return _Series(
            RollingVolatility(p.window, p.alpha, p.return_clip),
            self._new_pair(),
            Cusum(p.cusum_drift, p.cusum_threshold),
        )

    def _fresh(self, row: MarketObservation, frame: MarketFrame) -> bool:
        age = (frame.as_of - row.current.source_at).total_seconds()
        return (
            row.quality is DataQuality.OK
            and age <= self.policy.max_age_periods * frame.timeframe.seconds
        )

    def _features(self, frame: MarketFrame, reasons: list[str]) -> tuple[InstrumentFeatures, ...]:
        usable = [r for r in frame.observations if self._fresh(r, frame)]
        returns = {r.instrument.key: r.log_return for r in usable}
        # A missing period destroys continuity; do not reuse pre-gap warmup.
        self._series = {k: s for k, s in self._series.items() if k in returns}
        revised = {
            r.instrument.key
            for r in usable
            if r.instrument.key in self._series
            and self._series[r.instrument.key].last_close != r.previous.close
        }
        if revised:
            reasons.append("OVERLAP_REVISION")
            for key in revised:
                del self._series[key]
            if any(
                r.instrument.symbol == "BTCUSDT" and r.instrument.key in revised for r in usable
            ):
                # Every beta used that benchmark history, even on unchanged assets.
                for series in self._series.values():
                    series.pair = self._new_pair()
        btc = next((r.log_return for r in usable if r.instrument.symbol == "BTCUSDT"), None)
        output = []
        for row in usable:
            key, value = row.instrument.key, returns[row.instrument.key]
            stats = self._series.get(key)
            if stats is None:
                stats = self._series[key] = self._new_series()
            stats.vol.add(value)
            stats.last_close = row.current.close
            changed = stats.cusum.add(value)
            if btc is None:
                stats.pair = self._new_pair()
            else:
                stats.pair.add(btc, value)
            market = row.market
            if market is not None and not market.costs_fresh(frame.as_of, 15):
                market = None
            beta = stats.pair.beta
            output.append(
                InstrumentFeatures(
                    instrument=row.instrument,
                    log_return=value,
                    sample_count=stats.vol.count,
                    volatility=stats.vol.volatility,
                    return_q90=stats.vol.quantile(0.9) or 0.0,
                    efficiency=stats.vol.efficiency,
                    direction=stats.vol.direction,
                    beta=beta,
                    correlation=stats.pair.correlation,
                    relative_return=relative_strength(value, btc, beta),
                    spread_bps=row.spread_bps if market else None,
                    funding=float(market.funding) + 0.0 if market else None,
                    change_point=changed,
                )
            )
        return tuple(output)

    def _classify(
        self, frame: MarketFrame, features: tuple[InstrumentFeatures, ...], reasons: list[str]
    ) -> tuple[RegimeLabel, dict[str, float]]:
        p = self.policy
        section = summarize(
            tuple(f.log_return for f in features),
            eligible=len(frame.universe.eligible),
            neutral=p.neutral_return,
        )
        ready = [f for f in features if f.sample_count >= p.minimum_history]
        denominator = len(frame.universe.eligible)
        warm_coverage = len(ready) / denominator if denominator else 0.0
        stress = [f for f in features if f.spread_bps is not None and f.funding is not None]
        stress_coverage = len(stress) / denominator if denominator else 0.0
        metrics = {
            "coverage": section.coverage,
            "warm_coverage": warm_coverage,
            "stress_coverage": stress_coverage,
            "advancing": section.advancing,
            "declining": section.declining,
            "dispersion": section.dispersion,
            "volatility": median(f.volatility for f in features) if features else 0.0,
            "efficiency": median(f.efficiency for f in ready) if ready else 0.0,
            "return_q90": median(f.return_q90 for f in features) if features else 0.0,
        }
        correlations = [
            f.correlation
            for f in features
            if f.correlation is not None and f.instrument.symbol != "BTCUSDT"
        ]
        cross_assets = sum(k.symbol != "BTCUSDT" for k in frame.universe.eligible)
        metrics["correlation_coverage"] = len(correlations) / cross_assets if cross_assets else 0.0
        if correlations:
            metrics["btc_correlation"] = median(correlations)
        if any(f.beta is None for f in features):
            reasons.append("BTC_HISTORY_MISSING")
        if any(f.change_point for f in features):
            reasons.append("CHANGE_POINT")
        if any(not self._fresh(r, frame) for r in frame.observations):
            reasons.append("STALE_OR_INVALID_INPUT")
        stale = (
            frame.as_of - frame.source_at
        ).total_seconds() > p.max_age_periods * frame.timeframe.seconds
        if stale:
            reasons.append("STALE_INPUT")
        if (frame.as_of - frame.universe.observed_at).total_seconds() > p.universe_ttl_seconds:
            reasons.append("STALE_UNIVERSE")
        if section.coverage < p.minimum_coverage:
            reasons.append("LOW_COVERAGE")
        if stress_coverage < p.minimum_coverage:
            reasons.append("STRESS_DATA_MISSING")
        stressed: set[tuple[str, ...]] = set()
        stress_groups = {
            "LIQUIDITY_STRESS": {
                f.instrument.key
                for f in stress
                if f.spread_bps is not None and f.spread_bps >= p.stress_spread_bps
            },
            "FUNDING_STRESS": {
                f.instrument.key
                for f in stress
                if f.funding is not None and abs(f.funding) >= p.stress_funding
            },
            "RETURN_SHOCK": {
                f.instrument.key for f in features if abs(f.log_return) >= p.stress_return
            },
        }
        for reason, members in stress_groups.items():
            metrics[reason.lower() + "_breadth"] = (
                len(members) / denominator if denominator else 0.0
            )
            if members:
                reasons.append(reason)
                stressed.update(members)
        metrics["stress_breadth"] = len(stressed) / denominator if denominator else 0.0
        market_stress = metrics["stress_breadth"] >= p.minimum_stress_breadth or any(
            f.instrument.symbol == "BTCUSDT" and f.instrument.key in stressed for f in features
        )
        if metrics["volatility"] >= p.stress_vol or section.dispersion >= p.stress_dispersion:
            reasons.append("VOLATILITY_STRESS")
            market_stress = True
        if market_stress:
            return RegimeLabel.DISORDER, metrics
        if any(
            r in reasons
            for r in ("LOW_COVERAGE", "STRESS_DATA_MISSING", "STALE_INPUT", "STALE_UNIVERSE")
        ):
            return RegimeLabel.UNKNOWN, metrics
        if warm_coverage < p.minimum_coverage:
            reasons.append("INSUFFICIENT_HISTORY")
            return RegimeLabel.UNKNOWN, metrics
        if (
            metrics["volatility"] <= p.compression_vol
            and metrics["return_q90"] <= p.compression_vol
        ):
            reasons.append("COMPRESSED_VOLATILITY")
            return RegimeLabel.VOL_COMPRESSION, metrics
        trend_up = sum(f.direction > 0 for f in ready) / len(ready)
        trend_down = sum(f.direction < 0 for f in ready) / len(ready)
        if (
            metrics["efficiency"] >= p.trend_efficiency
            and max(trend_up, trend_down) >= p.trend_breadth
        ):
            reasons.append("DIRECTIONAL_BREADTH")
            return RegimeLabel.TREND_EXPANSION, metrics
        reasons.append("RANGE_EVIDENCE")
        return RegimeLabel.RANGE, metrics

    def _label(self, target: RegimeLabel, reasons: list[str]) -> RegimeLabel:
        if target in (RegimeLabel.UNKNOWN, RegimeLabel.DISORDER):
            self._stable = target
            self._candidate, self._streak = RegimeLabel.UNKNOWN, 0
            self._cooldown = self.policy.cooldown
            return target
        self._cooldown = max(0, self._cooldown - 1)
        if target == self._stable:
            self._candidate, self._streak = target, 0
            return target
        self._streak = self._streak + 1 if target == self._candidate else 1
        self._candidate = target
        if self._streak >= self.policy.confirmation and self._cooldown == 0:
            self._stable, self._streak = target, 0
            self._cooldown = self.policy.cooldown
            return target
        reasons.append("HYSTERESIS")
        return RegimeLabel.TRANSITION

    def update(self, frame: MarketFrame) -> FeatureSnapshot:
        if frame.timeframe is not self.policy.timeframe:
            raise ValueError("POLICY_TIMEFRAME_MISMATCH")
        if len(frame.universe.eligible) > self.policy.max_symbols:
            raise ValueError("SYMBOL_BUDGET_EXHAUSTED")
        previous_hash = self.latest.digest if self.latest else "0" * 64
        reasons: list[str] = []
        if self.latest:
            old = self.latest.frame
            incoming, known = frame.universe, old.universe
            if incoming.observed_at < known.observed_at or incoming.as_of < known.as_of:
                raise ValueError("UNIVERSE_ROLLBACK")
            if incoming.observed_at == known.observed_at and (
                incoming.version_hash != known.version_hash
                or set(incoming.eligible) != set(known.eligible)
            ):
                raise ValueError("CONFLICTING_UNIVERSE")
            if frame.source_at < old.source_at or frame.as_of < old.as_of:
                raise ValueError("OUT_OF_ORDER_FRAME")
            if frame.source_at == old.source_at:
                if frame == old:
                    return self.latest
                raise ValueError("CONFLICTING_FRAME")
            if (frame.source_at - old.source_at).total_seconds() != frame.timeframe.seconds:
                if self._seed_source == frame.source_at:
                    reasons.append("HISTORY_RESTORED")
                else:
                    self._series.clear()
                    reasons.append("DATA_GAP")
        self._seed_source = None
        features = self._features(frame, reasons)
        target, metrics = self._classify(frame, features, reasons)
        label = self._label(target, reasons)
        input_version = feature_input_version(
            frame,
            self.policy.policy_hash,
            previous_hash,
            self._warmup_hash,
            algorithm_version=REGIME_ALGORITHM_VERSION,
        )
        reason = (
            ReasonCode.INSUFFICIENT_EVIDENCE
            if label in (RegimeLabel.UNKNOWN, RegimeLabel.TRANSITION)
            else ReasonCode.NO_TRADE
        )
        state = MarketRegimeState(
            schema_version=1,
            event_id=uuid5(NAMESPACE_URL, input_version),
            source_at=frame.source_at,
            observed_at=frame.observed_at,
            as_of=frame.as_of,
            label=label,
            metrics=tuple(sorted(metrics.items())),
            reason_codes=(reason,),
            input_version=input_version,
        )
        result = FeatureSnapshot(
            algorithm_version=REGIME_ALGORITHM_VERSION,
            policy=self.policy,
            policy_hash=self.policy.policy_hash,
            previous_hash=previous_hash,
            warmup_hash=self._warmup_hash,
            frame=frame,
            features=features,
            state=state,
            reason_codes=tuple(dict.fromkeys(reasons)),
        )
        self.latest = result
        self._warmup_hash = None
        return result
