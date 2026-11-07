"""Adverse price rounding, quantity floor, then independent risk/filter checks."""

from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from tradeforge.domain.common import NonNegative, Positive, StrictModel
from tradeforge.domain.decision import Side
from tradeforge.risk.limits import RiskLimits
from tradeforge.risk.models import EntryRequest


class SizedEntry(StrictModel):
    qty: Positive
    entry_price: Positive
    stop_price: Positive
    risk: Positive
    notional: Positive
    margin: Positive
    costs: NonNegative


def size_entry(
    request: EntryRequest,
    *,
    equity: Decimal,
    realized_vol: Decimal,
    policy: RiskLimits,
) -> SizedEntry | None:
    f = request.filters
    long = request.side is Side.LONG
    entry = (request.entry_price / f.tick_size).to_integral_value(
        rounding=ROUND_CEILING if long else ROUND_FLOOR
    ) * f.tick_size
    stop = (request.stop_price / f.tick_size).to_integral_value(
        rounding=ROUND_FLOOR if long else ROUND_CEILING
    ) * f.tick_size
    if not f.min_price <= stop <= f.max_price or not f.min_price <= entry <= f.max_price:
        return None
    unit_risk = abs(entry - stop) + request.cost_per_unit
    if unit_risk <= 0:
        return None
    budget = equity * policy.max_initial_risk * min(Decimal(1), policy.target_vol / realized_vol)
    budget *= request.liquidity_scale
    qty = min(request.requested_qty, budget / unit_risk, f.max_qty)
    qty = (qty / f.step_size).to_integral_value(rounding=ROUND_FLOOR) * f.step_size
    risk, notional = qty * unit_risk, qty * entry
    if (
        not f.min_qty <= qty <= f.max_qty
        or notional < f.min_notional
        or qty <= 0
        or risk > budget
        or risk > equity * policy.max_initial_risk
    ):
        return None
    return SizedEntry(
        qty=qty,
        entry_price=entry,
        stop_price=stop,
        risk=risk,
        notional=notional,
        margin=notional / request.leverage,
        costs=qty * request.cost_per_unit,
    )
