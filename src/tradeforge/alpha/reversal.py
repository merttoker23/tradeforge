"""Versioned range reversal evidence; no forecasts, decisions or order capability."""

from datetime import datetime, timedelta
from decimal import Decimal
from statistics import median
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from tradeforge.alpha.base import AlphaCandidate, AlphaResult, MomentumInput, instrument_stress
from tradeforge.alpha.momentum import _trend
from tradeforge.domain.common import DataQuality, Positive, StrictModel, UnitFloat, utc
from tradeforge.domain.decision import AlphaFamily, DecisionState, Side
from tradeforge.domain.market import RegimeLabel, StreamState, TradeFlowWindow
from tradeforge.intelligence.models import digest


class ReversalPolicy(StrictModel):
    version: Literal[1] = 1
    minimum_history: Annotated[int, Field(ge=8, le=64)] = 8
    ttl_seconds: Annotated[int, Field(ge=1, le=3600)] = 900
    micro_age_seconds: Annotated[int, Field(ge=1, le=60)] = 5
    synchronization_seconds: Annotated[int, Field(ge=0, le=5)] = 1
    spread_bps: Positive = Decimal("5")
    minimum_depth_quote: Positive = Decimal("10000")
    minimum_stretch: Positive = Decimal("0.5")
    maximum_stretch: Positive = Decimal("2.5")
    minimum_wick: Annotated[UnitFloat, Field(gt=0)] = 0.35
    minimum_flow: Annotated[UnitFloat, Field(gt=0)] = 0.2
    trend_efficiency: Annotated[UnitFloat, Field(gt=0)] = 0.8
    max_setups: Annotated[int, Field(ge=1, le=100_000)] = 4096
    max_instruments: Annotated[int, Field(ge=1, le=100_000)] = 4096

    @model_validator(mode="after")
    def coherent(self) -> Self:
        if self.minimum_stretch >= self.maximum_stretch:
            raise ValueError("INCOHERENT_STRETCH_POLICY")
        return self

    @property
    def digest(self) -> str:
        return digest(self.model_dump(mode="json"))


class ReversalInput(MomentumInput):
    """Same closed multi-timeframe contract plus a preceding aggressive-flow sample."""

    previous_flow: TradeFlowWindow

    @model_validator(mode="after")
    def preceding_flow(self) -> Self:
        old, current = self.previous_flow, self.flow
        if old.key != self.instrument.key:
            raise ValueError("FLOW_IDENTITY_MISMATCH")
        if old.as_of > self.as_of:
            raise ValueError("FUTURE_INPUT")
        if old.source_end > current.source_start or old.source_start >= old.source_end:
            raise ValueError("OVERLAPPING_OR_EMPTY_FLOW")
        return self


class ReversalAlpha:
    def __init__(self, policy: ReversalPolicy | None = None) -> None:
        self.policy = policy or ReversalPolicy()
        self._latest: dict[tuple[str, ...], tuple[datetime, str]] = {}
        self._emitted: dict[str, tuple[datetime, int]] = {}

    def _gate(self, d: ReversalInput, side: Side, now: datetime) -> str | None:
        p, f = self.policy, d.features
        if d.quality is not DataQuality.OK:
            return "DATA_UNAVAILABLE"
        for bars in (d.setup_bars, d.medium_bars, d.higher_bars):
            if len(bars) < p.minimum_history:
                return "INSUFFICIENT_HISTORY"
            if (now - bars[-1].close_at).total_seconds() >= bars[-1].timeframe.seconds:
                return "STALE_BARS"
        if (now - f.state.source_at).total_seconds() > (
            f.policy.max_age_periods * f.frame.timeframe.seconds
        ):
            return "STALE_REGIME"
        universe = f.frame.universe
        if (
            d.instrument not in universe.eligible
            or (now - universe.observed_at).total_seconds() > f.policy.universe_ttl_seconds
        ):
            return "UNIVERSE_UNAVAILABLE"
        if f.state.label is not RegimeLabel.RANGE:
            return "REGIME_NOT_ALLOWED"
        rows = [r for r in f.frame.observations if r.instrument == d.instrument]
        features = [r for r in f.features if r.instrument == d.instrument]
        if (
            len(rows) != 1
            or rows[0].quality is not DataQuality.OK
            or len(features) != 1
            or features[0].sample_count < f.policy.minimum_history
        ):
            return "FEATURE_UNAVAILABLE"
        sign = 1 if side is Side.LONG else -1
        feature = features[0]
        stress = instrument_stress(d)
        if stress:
            return stress
        if any(
            _trend(bars, -sign, p.trend_efficiency) for bars in (d.medium_bars, d.higher_bars)
        ) or (feature.direction == -sign and feature.efficiency >= p.trend_efficiency):
            return "STRONG_TREND_FADE"
        m, b, flow, old = d.market, d.book, d.flow, d.previous_flow
        if not m.costs_fresh(now, p.micro_age_seconds) or not m.volume_fresh(
            now, p.micro_age_seconds
        ):
            return "MARKET_UNAVAILABLE"
        if b.stream_state is not StreamState.READY or any(
            (now - at).total_seconds() > p.micro_age_seconds for at in (b.source_at, b.captured_at)
        ):
            return "BOOK_UNAVAILABLE"
        spread = 20000 * (m.best_ask - m.best_bid) / (m.best_ask + m.best_bid)
        book_spread = 20000 * (b.asks[0][0] - b.bids[0][0]) / (b.asks[0][0] + b.bids[0][0])
        if max(spread, book_spread) > p.spread_bps:
            return "SPREAD_GATE"
        if (
            any(w.validity is not DataQuality.OK or w.agg_volume <= 0 for w in (flow, old))
            or (now - flow.source_end).total_seconds() > p.micro_age_seconds
            or (now - flow.source_at).total_seconds() > p.micro_age_seconds
        ):
            return "FLOW_UNAVAILABLE"
        times = (m.source_at, b.source_at, b.captured_at, flow.source_end)
        if (
            (max(times) - min(times)).total_seconds() > p.synchronization_seconds
            or abs(m.best_bid - b.bids[0][0]) > (m.best_bid * p.spread_bps / 10000)
            or abs(m.best_ask - b.asks[0][0]) > (m.best_ask * p.spread_bps / 10000)
            or (flow.source_start - old.source_end).total_seconds() > p.synchronization_seconds
            or flow.source_start >= flow.source_end
            or old.period_seconds != flow.period_seconds
        ):
            return "MICROSTRUCTURE_DESYNC"
        mid = (m.best_bid + m.best_ask) / 2
        band = mid * p.spread_bps / 10000
        if (
            min(
                sum((price * qty for price, qty in levels if abs(price - mid) <= band), Decimal(0))
                for levels in (b.bids, b.asks)
            )
            < p.minimum_depth_quote
        ):
            return "LIQUIDITY_GATE"
        current_ratio = sign * flow.signed_taker_flow / flow.agg_volume
        previous_ratio = sign * old.signed_taker_flow / old.agg_volume
        if previous_ratio >= 0 or current_ratio < Decimal(str(p.minimum_flow)):
            return "FLOW_NOT_STABILIZED"
        return None

    def evaluate(self, d: ReversalInput, *, side: Side, now: datetime) -> AlphaResult:
        utc(now)
        if side not in (Side.LONG, Side.SHORT):
            raise ValueError("DIRECTIONAL_SIDE_REQUIRED")
        if d.as_of > now:
            raise ValueError("FUTURE_INPUT")
        identity, input_hash = (*d.instrument.key, side.value), d.digest
        previous = self._latest.get(identity)
        if previous is not None:
            if d.as_of < previous[0]:
                raise ValueError("OUT_OF_ORDER_ALPHA")
            if d.as_of == previous[0] and input_hash != previous[1]:
                raise ValueError("CONFLICTING_ALPHA_INPUT")

        def reject(reason: str) -> AlphaResult:
            return AlphaResult(candidate=None, reason_codes=(reason,))

        p, bars = self.policy, d.setup_bars
        if identity not in self._latest and len(self._latest) >= p.max_instruments:
            return reject("INSTRUMENT_CAPACITY_EXHAUSTED")
        self._latest[identity] = d.as_of, input_hash
        if len(bars) < p.minimum_history:
            return reject("INSUFFICIENT_HISTORY")
        sign = 1 if side is Side.LONG else -1
        found: tuple[int, Decimal, Decimal, bool] | None = None
        for i in range(max(4, len(bars) - 3), len(bars)):
            before = bars[max(0, i - 8) : i]
            failed = False
            if len(before) >= 5:
                prior_boundary = (
                    min(b.low for b in before[:-1])
                    if sign > 0
                    else max(b.high for b in before[:-1])
                )
                if sign * (before[-1].close - prior_boundary) < 0:
                    before, failed = before[:-1], True
            boundary = min(b.low for b in before) if sign > 0 else max(b.high for b in before)
            bar = bars[i]
            extreme = bar.low if sign > 0 else bar.high
            if sign * (boundary - extreme) > 0 and sign * (bar.close - boundary) > 0:
                found = i, boundary, median(b.high - b.low for b in before), failed
                break
        anchor = bars[found[0]] if found else bars[-1]
        expires = anchor.close_at + timedelta(seconds=p.ttl_seconds)
        if now >= expires:
            return reject("EXPIRED")
        gate = self._gate(d, side, now)
        if gate:
            return reject(gate)
        if found is None:
            return reject("NO_SWEEP_RECLAIM")
        i, boundary, baseline, failed = found
        extreme = anchor.low if sign > 0 else anchor.high
        if (
            baseline <= 0
            or not p.minimum_stretch <= sign * (boundary - extreme) / baseline <= p.maximum_stretch
        ):
            return reject("STRETCH_GATE")
        span = anchor.high - anchor.low
        wick = (
            min(anchor.open, anchor.close) - anchor.low
            if sign > 0
            else anchor.high - max(anchor.open, anchor.close)
        )
        if span <= 0 or wick / span < Decimal(str(p.minimum_wick)):
            return reject("WEAK_WICK_REJECTION")
        after = bars[i + 1 :]
        trigger, stop = (anchor.high if sign > 0 else anchor.low), extreme
        if any(sign * (b.close - boundary) <= 0 for b in after):
            return reject("RECLAIM_INVALIDATED")
        if any((b.high >= trigger if sign > 0 else b.low <= trigger) for b in after):
            return reject("TRIGGER_ALREADY_PASSED")
        if any((b.low <= stop if sign > 0 else b.high >= stop) for b in after):
            return reject("STOP_ALREADY_BREACHED")
        entry = d.market.best_ask if sign > 0 else d.market.best_bid
        exit_quote = d.market.best_bid if sign > 0 else d.market.best_ask
        if sign * (entry - trigger) >= 0:
            return reject("TRIGGER_ALREADY_PASSED")
        if sign * (exit_quote - stop) <= 0:
            return reject("STOP_ALREADY_BREACHED")
        target = trigger + 2 * (trigger - stop)
        if target <= 0:
            return reject("INVALID_TARGET_GEOMETRY")
        state = (
            DecisionState.ARMED
            if after and sign * (after[-1].close - anchor.close) > 0
            else DecisionState.WATCHING
        )
        groups = (
            ("price_structure", float(wick / span)),
            ("participation", 0.0),
            ("aggressive_flow", float(sign * d.flow.signed_taker_flow / d.flow.agg_volume)),
        )
        setup_id = digest(
            (
                d.instrument.key,
                AlphaFamily.REVERSAL.value,
                side.value,
                anchor.open_at.isoformat(),
                p.digest,
            )
        )
        rank = 1 if state is DecisionState.ARMED else 0
        self._emitted = {k: v for k, v in self._emitted.items() if v[0] > now}
        old = self._emitted.get(setup_id)
        if old is not None and rank <= old[1]:
            return reject("DUPLICATE_SETUP")
        if old is None and len(self._emitted) >= p.max_setups:
            return reject("SETUP_CAPACITY_EXHAUSTED")
        reasons = (
            "SWING_SWEEP_RECLAIM",
            "WICK_REJECTION",
            "NORMALIZED_STRETCH",
            "FLOW_STABILIZED",
            *(("FAILED_BREAKOUT",) if failed else ()),
        )
        candidate = AlphaCandidate(
            setup_id=setup_id,
            instrument=d.instrument,
            alpha_family=AlphaFamily.REVERSAL,
            side=side,
            state=state,
            observed_at=d.as_of,
            expires_at=expires,
            trigger=trigger,
            stop_proposal=stop,
            take_profit_proposal=target,
            invalidations=("CLOSE_THROUGH_RANGE", "DATA_OR_REGIME_INVALID", "EXPIRED"),
            source_feature_hash=d.features.digest,
            input_hash=input_hash,
            policy_hash=p.digest,
            regime_version=d.features.state.input_version,
            universe_version=d.features.frame.universe.version_hash,
            reason_codes=reasons,
            ordinal_quality=0.6 * groups[0][1] + 0.2 * groups[2][1],
            evidence_groups=groups,
            spread_bps=20000
            * (d.market.best_ask - d.market.best_bid)
            / (d.market.best_ask + d.market.best_bid),
        )
        self._emitted[setup_id] = expires, rank
        return AlphaResult(candidate=candidate, reason_codes=reasons)
