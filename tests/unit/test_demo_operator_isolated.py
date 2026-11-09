"""Isolated Testnet-manual-terminal regression tests (no exchange or DB calls).

Functions are compiled from the delivered source because this portable test can
run without a production PostgreSQL driver or credentials. Integration tests
must still be run in the actual Tradeforge environment.
"""

import ast
import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

SOURCE = Path(__file__).resolve().parents[2] / "src/tradeforge/dashboard/demo_operator.py"


class AckUnknown(Exception):
    pass


class ExchangeRejected(Exception):
    pass


def module_functions(*names):
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    selected = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert len(selected) == len(names)
    unit = ast.Module(
        body=[
            ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0),
            *selected,
        ],
        type_ignores=[],
    )
    ast.fix_missing_locations(unit)
    g = dict(
        re=re,
        Decimal=Decimal,
        InvalidOperation=InvalidOperation,
        uuid4=uuid4,
        Any=object,
        datetime=datetime,
        timedelta=timedelta,
        UTC=UTC,
        _SYMBOL=re.compile(r"[A-Z0-9_]{2,40}\Z"),
        _CLIENT_ID=re.compile(r"[A-Za-z0-9_-]{1,36}\Z"),
        AckUnknown=AckUnknown,
        ExchangeRejected=ExchangeRejected,
    )
    exec(compile(unit, str(SOURCE), "exec"), g)
    return g


def test_manual_market_order_has_no_strategy_gate():
    m = module_functions("_positive_decimal", "_order_params")
    params = m["_order_params"](
        {"symbol": "BTCUSDT", "side": "BUY", "type": "MARKET", "quantity": "0.001"}
    )
    assert params["newClientOrderId"].startswith("tfmanual_")
    assert params["quantity"] == "0.001"
    assert params["newOrderRespType"] == "ACK"
    assert "price" not in params and "timeInForce" not in params
    assert "leverage" not in params


def test_limit_order_reduce_only_and_custom_position_side():
    m = module_functions("_positive_decimal", "_order_params")
    data = {
        "symbol": "BTCUSDT",
        "side": "SELL",
        "type": "LIMIT",
        "quantity": "1.250",
        "price": "92000.125",
        "timeInForce": "IOC",
        "positionSide": "BOTH",
        "reduceOnly": True,
    }
    params = m["_order_params"](data)
    assert params["price"] == "92000.125"
    assert params["timeInForce"] == "IOC"
    assert params["reduceOnly"] == "true"
    with pytest.raises(ValueError, match="REDUCE_ONLY_ONE_WAY_REQUIRED"):
        m["_order_params"]({**data, "positionSide": "LONG"})


@pytest.mark.parametrize("quantity", ["0", "-1", "NaN", "Infinity", "1e9", "", "abc", None, 1])
def test_invalid_quantities_are_rejected(quantity):
    m = module_functions("_positive_decimal", "_order_params")
    with pytest.raises(ValueError):
        m["_order_params"](
            {"symbol": "BTCUSDT", "side": "BUY", "type": "MARKET", "quantity": quantity}
        )


def test_no_hidden_live_order_or_unvalidated_fields():
    m = module_functions("_positive_decimal", "_order_params")
    with pytest.raises(ValueError):
        m["_order_params"](
            {
                "symbol": "BTCUSDT",
                "side": "BUY",
                "type": "MARKET",
                "quantity": "1",
                "environment": "live",
            }
        )
    with pytest.raises(ValueError):
        m["_order_params"]({"symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT", "quantity": "1"})


def test_emits_testnet_order_once_and_persists_intent_first():
    g = module_functions("_positive_decimal", "_order_params", "_place")
    log = []

    class Client:
        def request(self, method, path, params):
            log.append(("send", method, path, params))
            return {"orderId": 123456, "status": "NEW"}

    def client(db, master, *, read_only):
        assert master == "existing-master" and not read_only
        return Client()

    g["_client"] = client
    g["_audit"] = lambda db, kind, body: log.append(("audit", kind, body))
    status, receipt = g["_place"](
        object(),
        "existing-master",
        {"symbol": "BTCUSDT", "side": "BUY", "type": "MARKET", "quantity": "0.01"},
    )
    assert status == 200 and receipt["order_id"] == "123456"
    assert [row[0] for row in log] == ["audit", "send", "audit"]
    assert log[1][1:3] == ("POST", "/fapi/v1/order")
    assert receipt["environment"] == "testnet"


def test_ack_unknown_no_automatic_retry():
    g = module_functions("_positive_decimal", "_order_params", "_place")
    sends = []

    class Client:
        def request(self, method, path, params):
            sends.append(params)
            raise AckUnknown("ACK_UNKNOWN")

    g["_client"] = lambda db, master, *, read_only: Client()
    g["_audit"] = lambda db, kind, body: None
    status, body = g["_place"](
        object(),
        "master",
        {"symbol": "BTCUSDT", "side": "SELL", "type": "MARKET", "quantity": "0.5"},
    )
    assert status == 202 and body["status"] == "ACK_UNKNOWN" and body["retry_safe"] is False
    assert len(sends) == 1


def test_cancel_is_single_testnet_delete():
    g = module_functions("_cancel")
    messages = []

    class Client:
        def request(self, method, path, params):
            messages.append((method, path, params))
            return {"symbol": "BTCUSDT", "status": "CANCELED"}

    g["_client"] = lambda db, master, *, read_only: Client()
    g["_audit"] = lambda db, kind, body: None
    status, body = g["_cancel"](
        object(), "master", {"symbol": "BTCUSDT", "client_order_id": "tfmanual_abc"}
    )
    assert status == 200 and body["status"] == "CANCELED"
    assert messages == [
        ("DELETE", "/fapi/v1/order", {"symbol": "BTCUSDT", "origClientOrderId": "tfmanual_abc"})
    ]


def test_account_viewer_get_only_and_no_guardian():
    g = module_functions("_read_account")
    requests = []
    answers = {
        "/fapi/v3/account": {"totalMarginBalance": "10000.0", "availableBalance": "9900.0"},
        "/fapi/v3/balance": [{"asset": "USDT", "availableBalance": "9900.0"}],
        "/fapi/v3/positionRisk": [{"symbol": "BTCUSDT", "positionAmt": "0"}],
        "/fapi/v1/openOrders": [],
        "/fapi/v1/openAlgoOrders": [],
    }

    class Client:
        def request(self, method, path):
            requests.append((method, path))
            return answers[path]

    g["_client"] = lambda db, master, *, read_only: Client() if read_only else None
    g["number"] = Decimal
    g["terminal_account"] = lambda client, account, positions, orders, algos: {
        "environment": "BINANCE_FUTURES_DEMO",
        "positions": [],
    }
    status = g["_read_account"](object(), "master")
    assert status["connection"] == status["balance"] == "OK"
    assert status["equity"] == "10000.0" and status["position_count"] == 0
    assert status["guardian"] == "NOT_REQUIRED" and status["orders_enabled"] is False
    assert all(method == "GET" for method, _ in requests)


def test_aiohttp_routes_and_read_only_mode(tmp_path):
    import asyncio

    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    g = module_functions("attach_demo_operator")
    g.update(
        web=web,
        asyncio=asyncio,
        Path=Path,
        _INTERVAL_SECONDS=20,
        _read_account=lambda db, master: {
            "as_of": datetime.now(UTC).isoformat(),
            "equity": "1000.0",
            "available_balance": "999.0",
            "position_count": 0,
        },
        _audit=lambda *args: None,
        select_environment=lambda db: SimpleNamespace(name="testnet"),
        AuthError=Exception,
        __file__=str(SOURCE),
    )
    app = web.Application()
    app.router.add_get(
        "/",
        lambda req: web.Response(
            text="<!doctype html><html><head></head><body></body></html>", content_type="text/html"
        ),
    )
    g["attach_demo_operator"](app, object(), None, read_only_loopback=True)

    async def run():
        async with TestClient(TestServer(app)) as client:
            response = await client.get("/")
            html = await response.text()
            assert "demo_operator.js" in html and "demo_operator.css" in html
            response = await client.get("/api/demo/account")
            data = await response.json()
            assert data["environment"] == "testnet"
            assert data["orders_enabled"] is False
            response = await client.post("/api/demo/order", json={"symbol": "BTCUSDT"})
            assert response.status == 403

    asyncio.run(run())
