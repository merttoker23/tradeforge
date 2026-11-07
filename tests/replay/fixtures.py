"""Small recorded normalized tape; quantities and fees are explicit, never calibrated."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal as D
from uuid import NAMESPACE_URL, uuid5

from tradeforge.domain.decision import AlphaFamily, Side
from tradeforge.domain.execution import ExecutionIntent, PositionMode, StopPlan
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.domain.market import RegimeLabel, UniverseSnapshot

T = datetime(2026, 1, 1, tzinfo=UTC)
KEY = InstrumentKey(
    symbol="BTCUSDT",
    exchange="BINANCE",
    market_type="USD_M",
    quote="USDT",
    margin_asset="USDT",
    contract_type="PERPETUAL",
)


def event(n, **patch):
    at = T + timedelta(seconds=n)
    return dict(
        schema_version=1,
        event_id=uuid5(NAMESPACE_URL, f"tape/{n}"),
        source_at=at,
        observed_at=at,
        as_of=at,
        sequence=n,
        **patch,
    )


def universe(n=0, eligible=(KEY,)):
    return UniverseSnapshot(
        **event(n),
        eligible=eligible,
        watched=eligible,
        deep=eligible,
        filtering_rules=("point-in-time",),
        version_hash="a" * 64,
    )


def quote(n=1, bid="99", ask="100", qty="0.5", **patch):
    from tradeforge.research.fill_simulator import FeeSchedule, Quote

    fields = dict(
        **event(n),
        **KEY.model_dump(),
        best_bid=D(bid),
        best_ask=D(ask),
        bid_qty=D(qty),
        ask_qty=D(qty),
        fees=FeeSchedule(
            version="fees1",
            observed_at=T,
            effective_at=T,
            maker_rate=D("0.0002"),
            taker_rate=D("0.0005"),
        ),
    )
    return Quote(**{**fields, **patch})


def order(n=1, qty="1", side=Side.LONG, name="entry", **patch):
    from tradeforge.execution.paper_broker import PaperOrder

    at = T + timedelta(seconds=n)
    stop = D("95") if side is Side.LONG else D("105")
    target = D("110") if side is Side.LONG else D("90")
    intent = ExecutionIntent(
        **event(n),
        **KEY.model_dump(),
        intent_id=uuid5(NAMESPACE_URL, name),
        client_order_id=name,
        decision_id=uuid5(NAMESPACE_URL, "decision/" + name),
        side=side,
        qty=D(qty),
        price=D("100"),
        position_mode=PositionMode.ONE_WAY,
        reduce_only=False,
        stop_plan=StopPlan(stop_price=stop, qty=D(qty)),
        max_slippage_bps=D("300"),
        expires_at=at + timedelta(seconds=60),
        created_at=at,
        policy_version="risk1",
        risk_snapshot_id=uuid5(NAMESPACE_URL, "risk"),
    )
    return PaperOrder(
        **{
            **dict(
                intent=intent,
                take_profit=target,
                alpha=AlphaFamily.MOMENTUM,
                regime=RegimeLabel.RANGE,
                decision_hash="b" * 64,
            ),
            **patch,
        }
    )
