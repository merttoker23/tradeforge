"""Closed-bar breakout/pullback evidence with fixed, grouped ordinal weights."""

from datetime import datetime, timedelta
from decimal import Decimal
from statistics import median

from tradeforge.alpha.base import (
    AlphaCandidate,
    AlphaResult,
    MomentumInput,
    MomentumPolicy,
    instrument_stress,
)
from tradeforge.domain.common import DataQuality, utc
from tradeforge.domain.decision import DecisionState, Side
from tradeforge.domain.market import ClosedBar, RegimeLabel, StreamState
from tradeforge.intelligence.models import digest


def _trend(bars: tuple[ClosedBar, ...], sign: int, efficiency: float) -> bool:
    moves = [b.close - a.close for a, b in zip(bars, bars[1:], strict=False)]
    distance = sum((abs(m) for m in moves), Decimal(0))
    change = sign * (bars[-1].close - bars[0].close)
    return distance > 0 and change > 0 and change / distance >= Decimal(str(efficiency))


class MomentumAlpha:
    """One emission per setup/stage; suppression is process-local, never order dedup."""

    def __init__(self, policy: MomentumPolicy | None = None) -> None:
        self.policy = policy or MomentumPolicy()
        self._emitted: dict[str, tuple[datetime, int]] = {}
        self._latest: dict[tuple[str, ...], tuple[datetime, str]] = {}

    def _gate(self, d: MomentumInput, side: Side, now: datetime) -> str | None:
        p, f = self.policy, d.features
        if d.quality is not DataQuality.OK:
            return "DATA_UNAVAILABLE"
        for bars in (d.setup_bars, d.medium_bars, d.higher_bars):
            if len(bars) < p.minimum_history:
                return "INSUFFICIENT_HISTORY"
            # Allow the most recent CLOSED higher period, never its in-progress successor.
            if (now - bars[-1].close_at).total_seconds() >= bars[-1].timeframe.seconds:
                return "STALE_BARS"
        if (
            now - f.state.source_at
        ).total_seconds() > f.policy.max_age_periods * f.frame.timeframe.seconds:
            return "STALE_REGIME"
        universe = f.frame.universe
        if (
            d.instrument not in universe.eligible
            or (now - universe.observed_at).total_seconds() > f.policy.universe_ttl_seconds
        ):
            return "UNIVERSE_UNAVAILABLE"
        if f.state.label is not RegimeLabel.TREND_EXPANSION:
            return "REGIME_NOT_ALLOWED"
        rows = [r for r in f.frame.observations if r.instrument == d.instrument]
        features = [r for r in f.features if r.instrument == d.instrument]
        if len(rows) != 1 or len(features) != 1 or rows[0].quality is not DataQuality.OK:
            return "FEATURE_UNAVAILABLE"
        feature = features[0]
        stress = instrument_stress(d)
        if stress:
            return stress
        if (
            feature.sample_count < f.policy.minimum_history
            or feature.beta is None
            or feature.relative_return is None
        ):
            return "RELATIVE_STRENGTH_UNKNOWN"
        sign = 1 if side is Side.LONG else -1
        if feature.direction != sign or any(
            not _trend(bars, sign, p.trend_efficiency) for bars in (d.medium_bars, d.higher_bars)
        ):
            return "TREND_MISALIGNED"
        if sign * feature.relative_return <= 0:
            return "RELATIVE_STRENGTH_OPPOSED"
        m, book, flow = d.market, d.book, d.flow
        if not m.costs_fresh(now, p.micro_age_seconds) or not m.volume_fresh(
            now, p.micro_age_seconds
        ):
            return "MARKET_UNAVAILABLE"
        if book.stream_state is not StreamState.READY or any(
            (now - at).total_seconds() > p.micro_age_seconds
            for at in (book.source_at, book.captured_at)
        ):
            return "BOOK_UNAVAILABLE"
        spread = 20000 * (m.best_ask - m.best_bid) / (m.best_ask + m.best_bid)
        book_spread = (
            20000 * (book.asks[0][0] - book.bids[0][0]) / (book.asks[0][0] + book.bids[0][0])
        )
        if max(spread, book_spread) > p.spread_bps:
            return "SPREAD_GATE"
        # Count only executable nearby liquidity, not distant large spoof-like levels.
        mid = (m.best_bid + m.best_ask) / 2
        band = mid * p.spread_bps / 10000
        depths = [
            sum((price * qty for price, qty in levels if abs(price - mid) <= band), Decimal(0))
            for levels in (book.bids, book.asks)
        ]
        if min(depths) < p.minimum_depth_quote:
            return "LIQUIDITY_GATE"
        if (
            flow.validity is not DataQuality.OK
            or any(
                (now - at).total_seconds() > p.micro_age_seconds
                for at in (flow.source_at, flow.source_end)
            )
            or flow.agg_volume <= 0
        ):
            return "FLOW_UNAVAILABLE"
        if sign * flow.signed_taker_flow / flow.agg_volume < Decimal(str(p.minimum_flow)):
            return "FLOW_OPPOSED"
        return None

    def evaluate(self, d: MomentumInput, *, side: Side, now: datetime) -> AlphaResult:
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

        if identity not in self._latest and len(self._latest) >= self.policy.max_instruments:
            return reject("INSTRUMENT_CAPACITY_EXHAUSTED")
        # Rejected newer evidence also fences replay: a stale/gapped update must
        # not allow an older good snapshot to resurrect a setup afterwards.
        self._latest[identity] = d.as_of, input_hash
        bars, p = d.setup_bars, self.policy
        if len(bars) < p.minimum_history:
            return reject("INSUFFICIENT_HISTORY")
        sign = 1 if side is Side.LONG else -1
        # Search the three latest closed bars; an older breakout is no longer actionable.
        found: tuple[int, Decimal, Decimal] | None = None
        for i in range(max(4, len(bars) - 3), len(bars)):
            before = bars[max(0, i - 8) : i]
            boundary = max(b.high for b in before) if sign > 0 else min(b.low for b in before)
            if sign * (bars[i].close - boundary) > 0:
                found = i, boundary, median(b.high - b.low for b in before)
                break
        anchor = bars[found[0]] if found else bars[-1]
        # Receipt/revision timestamps can move; the closed period cannot. Late
        # recovery and replay must never renew the lifetime of this setup.
        expires = anchor.close_at + timedelta(seconds=p.ttl_seconds)
        if now >= expires:
            return reject("EXPIRED")
        gate = self._gate(d, side, now)
        if gate:
            return reject(gate)
        state = DecisionState.DISCOVERED
        reasons = ["TREND_ALIGNED", "RELATIVE_STRENGTH_ALIGNED"]
        price_quality, participation = 0.3, 0.0
        if found:
            index, boundary, baseline_range = found
            breakout, after = bars[index], bars[index + 1 :]
            if any(sign * (b.close - boundary) <= 0 for b in after):
                return reject("BREAKOUT_INVALIDATED")
            if (
                baseline_range <= 0
                or breakout.high - breakout.low < p.expansion_ratio * baseline_range
            ):
                return reject("VOLATILITY_NOT_EXPANDING")
            baseline_volume = median(b.volume for b in bars[max(0, index - 8) : index])
            if baseline_volume <= 0 or breakout.volume < p.volume_ratio * baseline_volume:
                return reject("WEAK_PARTICIPATION")
            participation = min(
                1.0, float(breakout.volume / (p.volume_ratio * baseline_volume)) - 1.0
            )
            state, price_quality = DecisionState.WATCHING, 0.6
            reasons += ["BREAKOUT_CLOSED", "VOLATILITY_EXPANSION", "PARTICIPATION_CONFIRMED"]
            extreme = breakout.high if sign > 0 else breakout.low
            distance = sign * (extreme - boundary)
            if after:
                if any(sign * (b.close - extreme) >= 0 for b in after):
                    return reject("TRIGGER_ALREADY_PASSED")
                retreat = min(b.low for b in after) if sign > 0 else max(b.high for b in after)
                depth = sign * (extreme - retreat) / distance
                if depth > Decimal(str(p.maximum_pullback)):
                    return reject("PULLBACK_TOO_DEEP")
                if depth >= Decimal(str(p.minimum_pullback)):
                    # Must close back towards continuation after the retreat.
                    if sign * (bars[-1].close - bars[-2].close) > 0:
                        state, price_quality = DecisionState.ARMED, 1.0
                        reasons.append("PULLBACK_CONFIRMED")
            trigger = extreme
            stop = (
                (min(b.low for b in after) if sign > 0 else max(b.high for b in after))
                if after
                else boundary
            )
        else:
            trigger = max(b.high for b in bars) if sign > 0 else min(b.low for b in bars)
            stop = min(b.low for b in bars) if sign > 0 else max(b.high for b in bars)
        entry_quote = d.market.best_ask if sign > 0 else d.market.best_bid
        exit_quote = d.market.best_bid if sign > 0 else d.market.best_ask
        if sign * (entry_quote - trigger) >= 0:
            return reject("TRIGGER_ALREADY_PASSED")
        if sign * (exit_quote - stop) <= 0:
            return reject("STOP_ALREADY_BREACHED")
        flow_quality = float(sign * d.flow.signed_taker_flow / d.flow.agg_volume)
        groups = (
            ("price_structure", price_quality),
            ("participation", participation),
            ("aggressive_flow", flow_quality),
        )
        setup_id = digest((d.instrument.key, side.value, anchor.open_at.isoformat(), p.digest))
        rank = (DecisionState.DISCOVERED, DecisionState.WATCHING, DecisionState.ARMED).index(state)
        self._emitted = {k: v for k, v in self._emitted.items() if v[0] > now}
        old = self._emitted.get(setup_id)
        if old is not None and rank <= old[1]:
            return reject("DUPLICATE_SETUP")
        if old is None and len(self._emitted) >= p.max_setups:
            return reject("SETUP_CAPACITY_EXHAUSTED")
        candidate = AlphaCandidate(
            setup_id=setup_id,
            instrument=d.instrument,
            side=side,
            state=state,
            observed_at=d.as_of,
            expires_at=expires,
            trigger=trigger,
            stop_proposal=stop,
            take_profit_proposal=trigger + 2 * (trigger - stop),
            invalidations=(
                "CLOSE_BACK_THROUGH_BREAKOUT",
                "PULLBACK_DEPTH_EXCEEDED",
                "DATA_OR_REGIME_INVALID",
                "EXPIRED",
            ),
            source_feature_hash=d.features.digest,
            input_hash=input_hash,
            policy_hash=p.digest,
            regime_version=d.features.state.input_version,
            universe_version=d.features.frame.universe.version_hash,
            reason_codes=tuple(reasons),
            ordinal_quality=0.6 * price_quality + 0.2 * participation + 0.2 * flow_quality,
            evidence_groups=groups,
            spread_bps=20000
            * (d.market.best_ask - d.market.best_bid)
            / (d.market.best_ask + d.market.best_bid),
        )
        self._emitted[setup_id] = expires, rank
        self._latest[identity] = d.as_of, input_hash
        return AlphaResult(candidate=candidate, reason_codes=candidate.reason_codes)
