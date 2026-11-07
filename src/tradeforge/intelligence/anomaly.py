"""Danger gates precede ordinal scoring, including symmetric pump/dump checks."""

from decimal import Decimal

from tradeforge.intelligence.inputs import MarketObservation
from tradeforge.intelligence.models import InstrumentFeatures, RegimePolicy
from tradeforge.intelligence.opportunity import SurveillancePolicy


def danger_reasons(
    row: MarketObservation,
    feature: InstrumentFeatures,
    policy: SurveillancePolicy,
    regime: RegimePolicy,
) -> tuple[str, ...]:
    reasons = []
    # Compare Decimals directly: a finite Decimal ratio may overflow binary floats.
    previous, current = row.previous.volume, row.current.volume
    if previous <= 0 or current <= 0:
        reasons.append("VOLUME_MISSING")
    elif current / previous < Decimal(str(policy.minimum_volume_ratio)):
        reasons.append("ILLIQUID_VOLUME")
    if abs(feature.log_return) >= min(policy.shock_return, regime.stress_return):
        reasons.append("PRICE_SHOCK")
    if (
        previous > 0
        and current / previous >= Decimal(str(policy.pump_volume_ratio))
        and abs(feature.log_return) >= policy.pump_return
    ):
        reasons.append("PUMP_OR_DUMP")
    if feature.spread_bps is not None and feature.spread_bps >= min(
        policy.max_spread_bps, regime.stress_spread_bps
    ):
        reasons.append("COST_OR_LIQUIDITY_STRESS")
    if feature.funding is not None and abs(feature.funding) >= min(
        policy.max_funding, regime.stress_funding
    ):
        reasons.append("FUNDING_STRESS")
    if not policy.min_volatility <= feature.volatility < policy.max_volatility:
        reasons.append("UNSUITABLE_VOLATILITY")
    return tuple(reasons)
