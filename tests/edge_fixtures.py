"""Deterministic closed-trade evidence and hand-defined costs; no exchange activity."""

from datetime import timedelta
from decimal import Decimal as D
from functools import lru_cache
from uuid import UUID

from tests.decision_fixtures import NOW
from tradeforge.domain.decision import AlphaFamily, Side


def inputs(**patch):
    from tradeforge.decision.cost_model import CostInput, Friction

    normal = Friction(
        spread_bps=D("2"), slippage_bps=D("3"), funding_bps=D("1"), latency_bps=D("1")
    )
    stress = Friction(
        spread_bps=D("4"), slippage_bps=D("6"), funding_bps=D("2"), latency_bps=D("2")
    )
    return CostInput.model_validate(
        dict(
            notional=D("1000"),
            initial_risk=D("10"),
            entry_role="maker",
            exit_role="taker",
            maker_fee_bps=D("2"),
            taker_fee_bps=D("5"),
            normal=normal,
            stress=stress,
            observed_at=NOW,
            expires_at=NOW + timedelta(minutes=1),
            source_version="a" * 64,
        )
        | patch
    )


def bucket(**patch):
    from tradeforge.decision.stats import Bucket

    return Bucket.model_validate(
        dict(
            symbol="BTCUSDT",
            regime="RANGE",
            alpha_family=AlphaFamily.REVERSAL,
            side=Side.LONG,
            environment="paper",
        )
        | patch
    )


def trades(
    wins=450,
    losses=550,
    payoff="3",
    loss="1",
    *,
    start=None,
    split="TRAIN",
    predicted_p=0.45,
    offset=0,
):
    from tradeforge.decision.cost_model import RealizedCost
    from tradeforge.decision.stats import TradeOutcome

    start = start or NOW - timedelta(days=1)
    costs = RealizedCost(
        maker_fees=D("0"),
        taker_fees=D("0.1"),
        spread=D("0.1"),
        slippage=D("0"),
        funding=D("0"),
        latency=D("0"),
    )
    return tuple(
        TradeOutcome(
            trade_id=UUID(int=offset + i + 1),
            bucket=bucket(),
            split=split,
            opened_at=start + timedelta(seconds=i * 3),
            closed_at=start + timedelta(seconds=i * 3 + 1),
            observed_at=start + timedelta(seconds=i * 3 + 2),
            initial_risk=D("10"),
            forecast_at=start + timedelta(seconds=i * 3),
            gross_R=D(payoff) if (i * 37) % (wins + losses) < wins else -D(loss),
            costs=costs,
            forecast_net_R=D("0.2"),
            forecast_p_win=predicted_p,
            analysis_version="a" * 64,
        )
        for i in range(wins + losses)
    )


def analyze(rows=None, **patch):
    from tradeforge.decision.edge import analyze_edge

    if rows is None:
        rows = trades() + trades(180, 220, start=NOW - timedelta(hours=1), split="OOS", offset=2000)
    return analyze_edge(
        rows,
        **(
            dict(bucket=bucket(), cost=inputs(), at=NOW, feature_hash="a" * 64, regime_version="r1")
            | patch
        ),
    )


@lru_cache(maxsize=32)
def candidate_analysis(symbol, family, side, regime, feature_hash, at):
    from tests.decision_fixtures import change

    b = bucket(symbol=symbol, alpha_family=family, side=side, regime=regime)
    rows = trades() + trades(180, 220, start=NOW - timedelta(hours=1), split="OOS", offset=2000)
    return analyze(
        tuple(change(r, bucket=b) for r in rows),
        bucket=b,
        feature_hash=feature_hash,
        regime_version=regime,
        at=at,
        cost=inputs(initial_risk=D("100")),
    )
