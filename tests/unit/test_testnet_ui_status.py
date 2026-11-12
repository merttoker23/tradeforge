"""Terminal and account diagnostics do not expose manual order authority."""

import asyncio
from pathlib import Path
from types import SimpleNamespace

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from tradeforge.dashboard import demo_operator

ROOT = Path(__file__).resolve().parents[2]
STATIC = ROOT / "src/tradeforge/dashboard/static"


async def account_and_routes(monkeypatch, *, environment="testnet", master="valid"):
    monkeypatch.setattr(demo_operator, "select_environment", lambda db: SimpleNamespace(name=environment))
    monkeypatch.setattr(demo_operator, "_read_account", lambda *args: {
        "environment": "BINANCE_FUTURES_DEMO", "as_of": "2026-10-22T00:00:00Z",
        "equity": "1000", "available_balance": "1000", "position_count": 0,
    })
    monkeypatch.setattr(demo_operator, "_audit", lambda *args: None)
    app = web.Application()

    async def home(request):
        return web.Response(text="<html><head></head><body></body></html>", content_type="text/html")

    app.router.add_get("/", home)
    demo_operator.attach_demo_operator(app, object(), master)
    async with TestClient(TestServer(app)) as client:
        html = await (await client.get("/")).text()
        account_response = await client.get("/api/demo/account")
        account = await account_response.json()
        order = await client.post("/api/demo/order", json={"symbol": "BTCUSDT"})
        cancel = await client.post("/api/demo/cancel", json={"symbol": "BTCUSDT"})
        lookup = await client.get("/api/demo/order?symbol=BTCUSDT")
        return html, account, order.status, cancel.status, lookup.status


def test_no_manual_order_capability_on_testnet(monkeypatch):
    html, account, order, cancel, lookup = asyncio.run(account_and_routes(monkeypatch))
    assert account["environment"] == "testnet"
    assert "manual_orders_available" not in account
    assert "orders_enabled" not in account
    assert "demo_operator.js" not in html
    assert "demo_status.js" in html
    assert (order, cancel, lookup) == (404, 404, 404)


def test_no_manual_order_capability_on_live_or_without_credentials(monkeypatch):
    for environment, master in (("live", "valid"), ("testnet", None)):
        _, account, order, cancel, lookup = asyncio.run(
            account_and_routes(monkeypatch, environment=environment, master=master)
        )
        assert account["environment"] == environment
        assert (order, cancel, lookup) == (404, 404, 404)


def test_strategy_evidence_is_read_only():
    script = (STATIC / "demo_status.js").read_text(encoding="utf-8")
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    assert "tf-pretrade-preview" in script
    assert "Strateji değerlendirmesi değil" in script
    assert "ready_deep_count" in script
    assert "fetch(" not in script and "POST" not in script
    assert "execution-summary" in html and "Piyasa terminali" in html
    assert "demo-manual-order" not in html
    assert not (STATIC / "demo_operator.js").exists()
    assert not (STATIC / "demo_operator.css").exists()
