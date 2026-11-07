"""Official account v3/config shapes and strict guardian exchange validation."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal as D

import pytest

from tests.integration.test_execution_binance import client
from tests.risk_fixtures import account, change, factor
from tests.universe_fixtures import payload, symbol


def frame():
    from tradeforge.execution.broker import ExecutionContext
    from tradeforge.market.exchange_info import parse_exchange_info

    at = datetime.now(UTC)
    metadata = parse_exchange_info(payload(symbol()), at).instruments[0]
    return ExecutionContext(
        as_of=at,
        metadata=metadata,
        account=account(as_of=at, trading_day=at.date(), factors=(factor("BTCUSDT", as_of=at),)),
        mark_price=D("100"),
        bid=D("99.99"),
        ask=D("100.01"),
        leverage=D("1"),
        max_notional=D("10000"),
    )


def exchange_replies():
    at = int(datetime.now(UTC).timestamp() * 1000)
    return [
        (200, {"serverTime": at}),
        # V3 deliberately has no canTrade. This belongs to accountConfig.
        (
            200,
            {
                "totalInitialMargin": "0",
                "totalMaintMargin": "0",
                "totalWalletBalance": "10000",
                "totalUnrealizedProfit": "0",
                "totalMarginBalance": "10000",
                "totalPositionInitialMargin": "0",
                "totalOpenOrderInitialMargin": "0",
                "totalCrossWalletBalance": "10000",
                "totalCrossUnPnl": "0",
                "availableBalance": "10000",
                "maxWithdrawAmount": "10000",
                "assets": [],
                "positions": [],
            },
        ),
        (200, {"dualSidePosition": False}),
        (
            200,
            [
                {
                    "symbol": "BTCUSDT",
                    "marginType": "ISOLATED",
                    "isAutoAddMargin": False,
                    "leverage": 1,
                    "maxNotionalValue": "10000",
                }
            ],
        ),
        (200, []),
        (
            200,
            {
                "feeTier": 0,
                "canTrade": True,
                "canDeposit": False,
                "canWithdraw": False,
                "dualSidePosition": False,
                "multiAssetsMargin": False,
                "updateTime": 0,
            },
        ),
        (200, []),  # Algo open orders also reserve exposure, separately from /openOrders.
        (200, payload(symbol())),
        (
            200,
            {
                "symbol": "BTCUSDT",
                "bidPrice": "99.99",
                "bidQty": "10",
                "askPrice": "100.01",
                "askQty": "10",
                "time": at,
            },
        ),
        (
            200,
            {
                "symbol": "BTCUSDT",
                "markPrice": "100",
                "indexPrice": "100",
                "estimatedSettlePrice": "100",
                "lastFundingRate": "0",
                "interestRate": "0",
                "nextFundingTime": at + 3600000,
                "time": at,
            },
        ),
    ]


def test_official_v3_and_boolean_symbol_config_allow_verified_flat_testnet_context():
    from tradeforge.execution.context import verify_exchange_context

    c, calls = client(exchange_replies())
    context = frame()
    context = change(
        context, environment="testnet", account=change(context.account, environment="testnet")
    )
    verified = verify_exchange_context(c, context, stream_ready=True)
    assert verified.one_way and verified.isolated and verified.clock_ready and verified.stream_ready
    assert verified.bid == D("99.99")
    assert any(x[1] == "/fapi/v1/accountConfig" for x in calls)
    assert any(x[1] == "/fapi/v1/openAlgoOrders" for x in calls)


@pytest.mark.parametrize(
    "index,patch",
    [
        (1, {"availableBalance": "1"}),
        (1, {"positions": [{"symbol": "BTCUSDT", "positionAmt": "0.1"}]}),
        (2, {"dualSidePosition": True}),
        (5, {"multiAssetsMargin": True}),
        (5, {"canTrade": False}),
    ],
)
def test_exchange_account_discrepancies_cannot_be_overridden_by_db_flags(index, patch):
    from tradeforge.execution.context import verify_exchange_context
    from tradeforge.persistence.db import EntryHalt

    replies = exchange_replies()
    replies[index] = (200, {**replies[index][1], **patch})
    c, _ = client(replies)
    context = frame()
    context = change(
        context, environment="testnet", account=change(context.account, environment="testnet")
    )
    with pytest.raises(EntryHalt):
        verify_exchange_context(c, context, stream_ready=True)


def test_expired_context_and_market_spread_widening_fail_before_submission(seeded):
    import json

    from sqlalchemy import text

    from tests.execution_fixtures import setup_execution
    from tradeforge.execution.order_manager import validate_context
    from tradeforge.persistence.db import EntryHalt
    from tradeforge.risk.governor import RiskAssessment

    db, intent, context, at = setup_execution(seeded)
    with db.transaction() as c:
        body = c.scalar(
            text(
                "SELECT payload FROM audit_log "
                "WHERE event_type='execution.authorization.v1' LIMIT 1"
            )
        )
    quant = RiskAssessment.model_validate_json(json.dumps(body["quant"]))
    with pytest.raises(EntryHalt):
        validate_context(intent, change(context, as_of=at - timedelta(seconds=6)), quant, at=at)
    with pytest.raises(EntryHalt):
        validate_context(intent, change(context, bid=D("95"), ask=D("105")), quant, at=at)
