"""Read-only account viewer: untrusted browser input has no exchange write route."""

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from tradeforge.dashboard import demo_operator


def test_testnet_client_rejects_production_environment(monkeypatch):
    monkeypatch.setattr(demo_operator, "select_environment", lambda db: SimpleNamespace(name="live"))
    with pytest.raises(ValueError, match="TESTNET_ENVIRONMENT_REQUIRED"):
        demo_operator._client(object(), "some-master", read_only=True)


def test_missing_encryption_master_rejects_account_read(monkeypatch):
    monkeypatch.setattr(demo_operator, "select_environment", lambda db: SimpleNamespace(name="testnet"))
    with pytest.raises(ValueError, match="BINANCE_MASTER_REQUIRED"):
        demo_operator._client(object(), None, read_only=True)


def test_read_account_never_mutates_exchange(monkeypatch):
    calls = []

    class DummyClient:
        def request(self, method, path, params=None):
            calls.append((method, path))
            bodies = {
                "/fapi/v3/account": {"totalMarginBalance":"1000", "availableBalance":"900"},
                "/fapi/v3/balance": [{"asset":"USDT", "availableBalance":"900"}],
                "/fapi/v3/positionRisk": [{"symbol":"BTCUSDT","positionAmt":"0"}],
                "/fapi/v1/openOrders": [],
                "/fapi/v1/openAlgoOrders": [],
            }
            return bodies[path]

    monkeypatch.setattr(demo_operator, "_client", lambda *args, **kwargs: DummyClient())
    monkeypatch.setattr(demo_operator, "terminal_account", lambda *args: {})
    account = demo_operator._read_account(object(), "master")
    assert account["equity"] == "1000"
    assert account["available_balance"] == "900"
    assert account["orders_enabled"] is False  # Read-only evidence is not execution authority.
    assert "guardian" not in account
    assert len(calls) == 5
    assert all(method == "GET" for method, path in calls)


def test_account_endpoint_is_get_only_and_form_does_not_exist(monkeypatch):
    monkeypatch.setattr(demo_operator, "select_environment", lambda db: SimpleNamespace(name="testnet"))
    monkeypatch.setattr(demo_operator, "_read_account", lambda db, master: {
        "as_of":"2026-10-22T00:00:00Z","equity":"1000", "available_balance":"900", "position_count":0,
    })
    monkeypatch.setattr(demo_operator, "_audit", lambda *args: None)
    app = web.Application()

    async def home(request):
        return web.Response(text="<html><head></head><body></body></html>", content_type="text/html")

    app.router.add_get("/", home)
    demo_operator.attach_demo_operator(app, object(), "master")

    async def run():
        async with TestClient(TestServer(app)) as client:
            html = await (await client.get("/")).text()
            assert "demo_status.js" in html
            assert "demo_operator.js" not in html
            assert (await client.get("/api/demo/account")).status == 200
            for method, path in (("POST","/api/demo/order"), ("POST","/api/demo/cancel"),
                                 ("GET","/api/demo/order")):
                assert (await client.request(method, path)).status == 404

    asyncio.run(run())


def test_no_manual_authority_hidden_in_source():
    source = Path(demo_operator.__file__).read_text(encoding="utf-8")
    assert "_place(" not in source
    assert "_cancel(" not in source
    assert 'add_post("/api/demo/order"' not in source
    assert "newClientOrderId" not in source
