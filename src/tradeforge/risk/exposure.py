"""Gross concentration cannot be hidden by leverage, netting or opposite-side hedges."""

from decimal import Decimal

from tradeforge.domain.common import Money, NonNegative, StrictModel
from tradeforge.domain.decision import Side
from tradeforge.risk.models import AccountSnapshot, EntryRequest
from tradeforge.risk.position_sizer import SizedEntry


class PortfolioExposure(StrictModel):
    heat: NonNegative
    gross_notional: NonNegative
    delta: Money
    beta_delta: Money
    symbol_notional: NonNegative
    side_notional: NonNegative
    cluster_notional: NonNegative
    positions: int


def exposure(
    request: EntryRequest,
    account: AccountSnapshot,
    sized: SizedEntry,
    groups: tuple[tuple[str, ...], ...],
) -> PortfolioExposure:
    zero = Decimal(0)
    facts = {f.symbol: f for f in account.factors}
    rows = [
        (
            p.symbol,
            p.side,
            p.qty * max(p.entry_price, p.mark_price),
            p.qty
            * (
                max(abs(p.entry_price - p.stop_price), abs(p.mark_price - p.stop_price))
                + p.cost_per_unit
            ),
        )
        for p in (*account.positions, *account.pending)
        if p.stop_price is not None
    ]
    rows.append((request.instrument.symbol, request.side, sized.notional, sized.risk))
    group_values = [sum((n for s, _, n, _ in rows if s in g), zero) for g in groups]
    delta = sum((n * (1 if side is Side.LONG else -1) for _, side, n, _ in rows), zero)
    beta = zero
    for symbol, side, n, _ in rows:
        factor = facts[symbol].beta
        assert factor is not None  # Governor rejected missing factors before calling.
        beta += n * factor * (1 if side is Side.LONG else -1)
    return PortfolioExposure(
        heat=sum((r for _, _, _, r in rows), zero),
        gross_notional=sum((n for _, _, n, _ in rows), zero),
        delta=delta,
        beta_delta=beta,
        symbol_notional=max(
            sum((n for s, _, n, _ in rows if s == symbol), zero)
            for symbol in {s for s, _, _, _ in rows}
        ),
        side_notional=max(
            sum((n for _, side, n, _ in rows if side is direction), zero)
            for direction in (Side.LONG, Side.SHORT)
        ),
        cluster_notional=max(group_values, default=zero),
        positions=len(rows),
    )
