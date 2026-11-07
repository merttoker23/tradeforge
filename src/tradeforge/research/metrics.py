"""Cost-inclusive scorecards; simulation never grants evidence of live edge."""

from collections import defaultdict
from decimal import Decimal
from typing import Literal

from tradeforge.domain.common import Count, Money, NonNegative, StrictModel, Text
from tradeforge.execution.paper_broker import PaperPosition, Trade
from tradeforge.research.fill_simulator import ZERO

Mode = Literal["BACKTEST", "PAPER", "OOS"]
Status = Literal["NEGATIVE", "UNPROVEN"]


class Scorecard(StrictModel):
    mode: Mode
    status: Status
    live_eligible: Literal[False] = False
    closed_trades: Count
    open_positions: Count
    gross_pnl: Money
    net_pnl: Money
    unrealized_pnl: Money
    fees: NonNegative
    funding: Money
    slippage_cost: NonNegative
    net_ev: Money | None
    drawdown: NonNegative
    hit_rate: NonNegative | None
    profit_factor: NonNegative | None
    payoff_ratio: NonNegative | None
    worst_trade: Money | None


class GroupScore(StrictModel):
    dimension: Literal["symbol", "strategy", "regime", "hour"]
    value: Text
    scorecard: Scorecard


class MarkedPosition(StrictModel):
    position: PaperPosition
    unrealized_pnl: Money


def outcome_key(item: Trade | PaperPosition) -> tuple[object, ...]:
    intent = item.intent_id if isinstance(item, Trade) else item.order.intent.intent_id
    return item.instrument.key, intent


def scorecard(
    trades: tuple[Trade, ...],
    *,
    mode: Mode,
    open_positions: int = 0,
    unrealized_pnl: Decimal = ZERO,
    open_fees: Decimal = ZERO,
    open_funding: Decimal = ZERO,
    open_slippage: Decimal = ZERO,
    equity: tuple[Decimal, ...] = (),
    incomplete: frozenset[tuple[object, ...]] = frozenset(),
) -> Scorecard:
    # Partial exits from one position are one outcome, not independent wins.
    outcomes: dict[tuple[object, ...], Decimal] = defaultdict(lambda: ZERO)
    for t in trades:
        outcomes[outcome_key(t)] += t.net_pnl
    values = tuple(value for key, value in outcomes.items() if key not in incomplete)
    wins = tuple(p for p in values if p > 0)
    losses = tuple(-p for p in values if p < 0)
    net = sum(outcomes.values(), ZERO)
    peak = drawdown = running = ZERO
    path = equity
    if not path:
        points = []
        for t in sorted(trades, key=lambda t: t.closed_at):
            running += t.net_pnl
            points.append(running)
        path = tuple(points)
    for point in path:
        peak = max(peak, point)
        drawdown = max(drawdown, peak - point)
    win_sum, loss_sum = sum(wins, ZERO), sum(losses, ZERO)
    return Scorecard(
        mode=mode,
        status="NEGATIVE" if net < 0 else "UNPROVEN",
        closed_trades=len(values),
        open_positions=open_positions,
        gross_pnl=sum((t.gross_pnl for t in trades), ZERO),
        net_pnl=net,
        unrealized_pnl=unrealized_pnl,
        fees=sum((t.fees for t in trades), open_fees),
        funding=sum((t.funding for t in trades), open_funding),
        slippage_cost=sum((t.slippage_cost for t in trades), open_slippage),
        net_ev=sum(values, ZERO) / len(values) if values else None,
        drawdown=drawdown,
        hit_rate=Decimal(len(wins)) / len(values) if values else None,
        profit_factor=win_sum / loss_sum if loss_sum else None,
        payoff_ratio=(win_sum / len(wins)) / (loss_sum / len(losses)) if wins and losses else None,
        worst_trade=min(values) if values else None,
    )


def group_keys(item: Trade | PaperPosition) -> tuple[tuple[str, str], ...]:
    alpha = item.alpha if isinstance(item, Trade) else item.order.alpha
    regime = item.regime if isinstance(item, Trade) else item.order.regime
    return (
        ("symbol", item.instrument.symbol),
        ("strategy", alpha.value),
        ("regime", regime.value),
        ("hour", str(item.opened_at.hour)),
    )


def grouped_scores(
    trades: tuple[Trade, ...],
    *,
    mode: Mode,
    marks: tuple[MarkedPosition, ...] = (),
    equity_paths: dict[tuple[str, str], tuple[Decimal, ...]] | None = None,
) -> tuple[GroupScore, ...]:
    groups: dict[tuple[str, str], list[Trade]] = defaultdict(list)
    for t in trades:
        for key in group_keys(t):
            groups[key].append(t)
    open_groups: dict[tuple[str, str], list[MarkedPosition]] = defaultdict(list)
    for mark in marks:
        for key in group_keys(mark.position):
            open_groups[key].append(mark)
            groups.setdefault(key, [])
    return tuple(
        GroupScore.model_validate(
            dict(
                dimension=dimension,
                value=value,
                scorecard=scorecard(
                    tuple(items),
                    mode=mode,
                    open_positions=len(open_groups[(dimension, value)]),
                    incomplete=frozenset(
                        outcome_key(m.position) for m in open_groups[(dimension, value)]
                    ),
                    unrealized_pnl=sum(
                        (m.unrealized_pnl for m in open_groups[(dimension, value)]), ZERO
                    ),
                    open_fees=sum(
                        (m.position.entry_fees for m in open_groups[(dimension, value)]), ZERO
                    ),
                    open_funding=sum(
                        (m.position.funding for m in open_groups[(dimension, value)]), ZERO
                    ),
                    open_slippage=sum(
                        (m.position.entry_slippage for m in open_groups[(dimension, value)]), ZERO
                    ),
                    equity=(equity_paths or {}).get((dimension, value), ()),
                ),
            )
        )
        for (dimension, value), items in sorted(groups.items())
    )
