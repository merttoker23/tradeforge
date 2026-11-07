"""Terminal account numbers and executions must come from Demo evidence only."""

import json

import pytest

from tests.unit.test_demo_readiness import demo_client
from tradeforge.execution.binance_client import Response


def test_demo_terminal_uses_real_positions_orders_and_reports_missing_pnl():
    from tradeforge.execution.preflight import preflight

    client, calls = demo_client(
        patch={
            "/fapi/v3/positionRisk": [
                {
                    "symbol": "BTCUSDT",
                    "positionAmt": "2",
                    "entryPrice": "100",
                    "markPrice": "105",
                    "unRealizedProfit": "10",
                    "positionSide": "BOTH",
                }
            ],
            "/fapi/v1/openAlgoOrders": [
                {
                    "symbol": "BTCUSDT",
                    "orderType": "STOP_MARKET",
                    "side": "SELL",
                    "triggerPrice": "90",
                    "quantity": "2",
                    "algoStatus": "NEW",
                }
            ],
            "/fapi/v1/income": [],
            "/fapi/v1/userTrades": [
                {
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "price": "100",
                    "qty": "2",
                    "commission": "0.1",
                    "commissionAsset": "USDT",
                    "realizedPnl": "0",
                    "time": 1792713600000,
                }
            ],
        }
    )
    result = preflight(client, terminal=True)
    account = result["terminal"]
    assert account["environment"] == "BINANCE_FUTURES_DEMO"
    assert account["positions"][0]["entry_price"] == "100"
    assert account["stops"][0]["stop_price"] == "90"
    assert account["history"][0]["commission"] == "0.1"
    assert account["total_pnl"] is None and account["total_risk"] is None
    assert account["daily_pnl"] == "0"
    assert all(method == "GET" for method, *_ in calls)


def test_incomplete_history_never_fabricates_account_pnl():
    from tradeforge.execution.preflight import preflight

    client, _ = demo_client(
        patch={
            "/fapi/v1/income": [
                {
                    "symbol": "BTCUSDT",
                    "incomeType": "REALIZED_PNL",
                    "income": "3",
                    "asset": "USDT",
                    "time": 0,
                }
            ]
            * 1000
        }
    )
    data = preflight(client, terminal=True)["terminal"]
    assert data["daily_pnl"] is None
    assert data["total_pnl"] is None
    assert "INCOME_HISTORY_INCOMPLETE" in data["reason_codes"]


@pytest.mark.parametrize("failure", ["invalid_key", "disconnect"])
def test_invalid_demo_key_and_disconnection_are_safe_read_only_failures(failure):
    from tradeforge.execution.preflight import preflight

    client, calls = demo_client()

    def fail(method, path, params, headers):
        assert method == "GET"
        if failure == "disconnect":
            raise OSError("PRIVATE_SENTINEL")
        return Response(401, {}, b'{"code":-2015,"msg":"PRIVATE_SENTINEL"}')

    client.transport = fail
    result = preflight(client, terminal=True)
    assert result["ready"] is False and result["orders_enabled"] is False
    assert "PRIVATE_SENTINEL" not in json.dumps(result)
    assert result["equity"] is None
