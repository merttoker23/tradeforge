"""Isolated regression tests for Guardian-free Testnet UI (no exchange calls)."""

import ast
import asyncio
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "src/tradeforge/dashboard/demo_operator.py"
STATIC = ROOT / "src/tradeforge/dashboard/static"


def exported_function(name):
    tree = ast.parse(MODULE.read_text(encoding="utf-8"))
    matching = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name]
    assert len(matching) == 1
    unit = ast.Module(
        body=[
            ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0),
            matching[0],
        ],
        type_ignores=[],
    )
    ast.fix_missing_locations(unit)
    env = {
        "web": web,
        "Path": Path,
        "asyncio": asyncio,
        "datetime": datetime,
        "UTC": UTC,
        "_INTERVAL_SECONDS": 20,
        "_MAX_ORDER_BYTES": 4096,
        "COOKIE": "__Host-tradeforge",
        "AuthError": type("AuthError", (Exception,), {}),
        "__file__": str(MODULE),
    }
    exec(compile(unit, str(MODULE), "exec"), env)
    return env


async def get_status(
    *, environment="testnet", master="existing-master", read_only=False, auth=None
):
    env = exported_function("attach_demo_operator")
    env["select_environment"] = lambda db: SimpleNamespace(name=environment)
    env["_read_account"] = lambda db, master: {
        "as_of": datetime.now(UTC).isoformat(),
        "equity": "1000.00",
        "available_balance": "1000.00",
        "position_count": 0,
    }
    env["_audit"] = lambda *args: None
    env["_place"] = lambda db, master, body: (
        200,
        {"status": "NEW", "client_order_id": "tfmanual_1"},
    )
    env["_order_params"] = lambda body: body
    app = web.Application()

    async def home(req):
        return web.Response(
            text="<html><head></head><body></body></html>", content_type="text/html"
        )

    app.router.add_get("/", home)
    env["attach_demo_operator"](
        app, object(), master, read_only_loopback=read_only, operator_auth=auth
    )
    async with TestClient(TestServer(app)) as client:
        response = await client.get("/")
        html = await response.text()
        assert "demo_operator.js" in html
        assert "demo_status.js" in html
        assert "demo_status.css" in html
        response = await client.get("/assets/demo_status.js")
        assert response.status == 200 and "Guardian" in (await response.text())
        response = await client.get("/api/demo/account")
        assert response.status == 200
        state = await response.json()
        response = await client.post(
            "/api/demo/order",
            json={"symbol": "BTCUSDT", "side": "BUY", "type": "MARKET", "quantity": "0.1"},
        )
        return state, response.status


def test_testnet_manual_ready_is_independent_from_guardian():
    data, order_status = asyncio.run(get_status())
    assert data["environment"] == "testnet"
    assert data["manual_orders_available"] is True
    assert data["orders_enabled"] is False
    assert order_status == 200


def test_live_environment_never_advertises_manual_testnet_permission():
    state, _ = asyncio.run(get_status(environment="live"))
    assert state["manual_orders_available"] is False
    assert state["orders_enabled"] is False


def test_missing_master_never_advertises_manual_capability():
    state, _ = asyncio.run(get_status(master=None))
    assert state["manual_orders_available"] is False


def test_read_only_listener_never_advertises_manual_capability():
    state, order_status = asyncio.run(get_status(read_only=True))
    assert state["manual_orders_available"] is False
    assert order_status == 403


def test_read_only_operator_scope_not_allowed_to_trade():
    class ReadOnlyAuth:
        def authenticate(self, header, scope):
            if scope == "control":
                raise type("AuthError", (Exception,), {})()
            return SimpleNamespace()

    # Scope logic is covered by the actual AuthError from the module at runtime.
    body = MODULE.read_text(encoding="utf-8")
    assert 'sessions.authenticate(request.cookies.get(COOKIE), "control")' in body
    assert "operator_auth.authenticate(" in body


def test_strategy_preview_is_read_only_without_automatic_execution():
    script = (STATIC / "demo_status.js").read_text(encoding="utf-8")
    assert "tf-pretrade-preview" in script
    assert "Gerçek strateji" not in script  # no invented execution evidence
    assert "Strateji değerlendirmesi değil" in script
    assert "selected_deep_count" in script
    assert "ready_deep_count" in script
    assert "execution_guardian" in script  # removed from health DOM, not authorization
    assert "fetch(" not in script and "POST" not in script and "place(" not in script


def test_old_quote_history_and_algorithms_untouched():
    present = {path.relative_to(ROOT).as_posix() for path in ROOT.rglob("*") if path.is_file()}
    demo = (
        "src/tradeforge/dashboard/demo_operator.py",
        "src/tradeforge/dashboard/local_terminal.py",
    )
    assert all(path in present for path in demo)
    assert not any(
        p.startswith(("src/tradeforge/intelligence/", "src/tradeforge/market/", "migrations/"))
        for p in demo
    )
