"""Exercise the canonical production factory, not an isolated WebSocket route."""

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from aiohttp import ClientSession, WSServerHandshakeError, web

from tests.unit.test_dashboard_sessions import session_auth
from tradeforge.dashboard.api import create_app
from tradeforge.dashboard.sessions import COOKIE


class Model:
    def snapshot(self):
        return {"status": "DEGRADED", "sections": {}, "as_of": datetime.now(UTC).isoformat()}

    def terminal_symbols(self, *, search="", offset=0):
        return {"items": [{"symbol": search or "BTCUSDT"}], "has_more": False}

    def terminal_chart(self, symbol, timeframe):
        return {"symbol": symbol, "timeframe": timeframe, "bars": []}


async def reply(ws, identifier):
    while True:
        frame = await ws.receive_json(timeout=3)
        if frame.get("id") == identifier:
            return frame


async def serve(app):
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    return runner, f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"


def test_production_factory_socket_bootstrap_reads_projections_and_assets():
    async def run():
        sessions = session_auth()
        demanded = []

        async def demand(symbol, timeframe):
            demanded.append((symbol, timeframe))

        app = create_app(
            Model(),
            None,
            sessions=sessions,
            external_origin="https://terminal.example",
            chart_demand=demand,
        )
        runner, root = await serve(app)
        headers = {"Host": "terminal.example", "Cookie": COOKIE + "=" + sessions.login("x" * 64)}
        try:
            async with ClientSession(headers=headers) as client:
                async with client.get(root + "/") as response:
                    html = await response.text()
                    assert "/assets/realtime.js" in html
                    assert "wss://terminal.example" in response.headers["Content-Security-Policy"]
                async with client.get(root + "/assets/realtime.js") as response:
                    assert response.status == 200
                async with client.ws_connect(
                    root + "/api/realtime", origin="https://terminal.example"
                ) as ws:
                    hello = await ws.receive_json(timeout=3)
                    assert hello["type"] == "hello" and hello["protocol"] == 2
                    assert hello["session"]["scopes"] == ["control", "read"]
                    snapshot = await ws.receive_json(timeout=3)
                    assert snapshot["type"] == "snapshot"
                    for identifier, operation, params in [
                        (1, "symbols", {"search": "ETH", "offset": 0}),
                        (2, "chart", {"symbol": "ETHUSDT", "timeframe": "15m"}),
                    ]:
                        await ws.send_json(
                            {"type": "request", "id": identifier, "op": operation, "params": params}
                        )
                        frame = await reply(ws, identifier)
                        assert frame["type"] == "response" and frame["ok"] is True
                    assert demanded == [("ETHUSDT", "15m")]
        finally:
            await runner.cleanup()

    asyncio.run(run())


def test_read_only_socket_rejects_retired_operations_and_expired_session_requests():
    async def run():
        at = [1_000_000]
        sessions = session_auth(lambda: at[0])
        app = create_app(Model(), sessions=sessions, external_origin="https://terminal.example")
        runner, root = await serve(app)
        headers = {"Host": "terminal.example", "Cookie": COOKIE + "=" + sessions.login("x" * 64)}
        try:
            async with ClientSession(headers=headers) as client:
                with pytest.raises(WSServerHandshakeError):
                    await client.ws_connect(root + "/api/realtime", origin="https://evil.example")
                async with client.ws_connect(
                    root + "/api/realtime", origin="https://terminal.example"
                ) as ws:
                    await ws.receive_json(timeout=3)
                    await ws.receive_json(timeout=3)
                    at[0] += sessions.lifetime + 1
                    await ws.send_json({"type": "request", "id": 1, "op": "snapshot", "params": {}})
                    frame = await ws.receive(timeout=3)
                    assert frame.type.name in {"CLOSE", "CLOSING", "CLOSED"}
        finally:
            await runner.cleanup()

        runner, root = await serve(create_app(Model(), None, read_only_loopback=True))
        try:
            async with ClientSession() as client:
                async with client.ws_connect(root + "/api/realtime", origin=root) as ws:
                    assert (await ws.receive_json(timeout=3))["session"]["scopes"] == ["read"]
                    await ws.receive_json(timeout=3)
                    await ws.send_json({"type": "request", "id": 1, "op": "command", "params": {}})
                    frame = await reply(ws, 1)
                    assert frame["ok"] is False and frame["status"] == 400
        finally:
            await runner.cleanup()

    asyncio.run(run())


def test_server_pushes_changed_subscriptions_and_quotes_bypass_a_slow_chart(monkeypatch):
    import threading

    from tradeforge.dashboard import api, realtime

    async def run():
        started, release = threading.Event(), threading.Event()
        revision = [1]

        class ChangingModel(Model):
            def terminal_chart(self, symbol, timeframe):
                if symbol == "SLOWUSDT":
                    started.set()
                    assert release.wait(3)
                return {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "bars": [],
                    "revision": revision[0],
                }

        clock = [0.0]
        monkeypatch.setattr(api, "time", SimpleNamespace(monotonic=lambda: clock[0]))
        monkeypatch.setattr(realtime, "SNAPSHOT_INTERVAL", 0.03)
        quote = SimpleNamespace(
            symbol="BTCUSDT",
            mark_price="101",
            volume=None,
            source_at=datetime.now(UTC),
            source="PUBLIC",
            environment="mainnet",
        )
        app = create_app(ChangingModel(), read_only_loopback=True, market_feed=lambda: (quote,))
        runner, root = await serve(app)
        try:
            async with ClientSession() as client:
                async with client.ws_connect(root + "/api/realtime", origin=root) as ws:
                    await ws.receive_json(timeout=3)
                    await ws.receive_json(timeout=3)
                    await ws.send_json(
                        {
                            "type": "request",
                            "id": 1,
                            "op": "chart",
                            "params": {"symbol": "BTCUSDT", "timeframe": "15m"},
                        }
                    )
                    assert (await reply(ws, 1))["data"]["revision"] == 1
                    revision[0], clock[0] = 2, 10.0
                    while True:
                        frame = await ws.receive_json(timeout=3)
                        if frame["type"] == "chart":
                            assert frame["id"] == 1 and frame["data"]["revision"] == 2
                            break
                    await ws.send_json(
                        {
                            "type": "request",
                            "id": 2,
                            "op": "chart",
                            "params": {"symbol": "SLOWUSDT", "timeframe": "15m"},
                        }
                    )
                    assert await asyncio.to_thread(started.wait, 2)
                    await ws.send_json(
                        {
                            "type": "request",
                            "id": 3,
                            "op": "quotes",
                            "params": {"symbols": ["BTCUSDT"]},
                        }
                    )
                    assert (await reply(ws, 3))["data"]["items"][0]["price"] == "101"
                    release.set()
        finally:
            release.set()
            await runner.cleanup()

    asyncio.run(run())


@pytest.mark.parametrize(
    "params",
    [
        {"symbol": "BTCUSDT", "timeframe": []},
        {"symbol": "../secret", "timeframe": "15m"},
        {"symbol": "BTCUSDT", "timeframe": "1s"},
    ],
)
def test_invalid_chart_subscription_is_rejected_before_model_access(params):
    from tradeforge.dashboard.realtime import RealtimeHub

    with pytest.raises(ValueError):
        RealtimeHub.validate_request({"type": "request", "id": 1, "op": "chart", "params": params})


def test_socket_disconnect_does_not_release_db_admission_before_thread_finishes():
    import threading

    async def run():
        started, release, symbols_started = threading.Event(), threading.Event(), threading.Event()

        class BlockingModel(Model):
            def terminal_chart(self, symbol, timeframe):
                started.set()
                assert release.wait(3)
                return super().terminal_chart(symbol, timeframe)

            def terminal_symbols(self, **kwargs):
                symbols_started.set()
                return super().terminal_symbols(**kwargs)

        runner, root = await serve(create_app(BlockingModel(), read_only_loopback=True))
        try:
            async with ClientSession() as client:
                first = await client.ws_connect(root + "/api/realtime", origin=root)
                second = await client.ws_connect(root + "/api/realtime", origin=root)
                for ws in (first, second):
                    await ws.receive_json(timeout=3)
                    await ws.receive_json(timeout=3)
                await first.send_json(
                    {
                        "type": "request",
                        "id": 1,
                        "op": "chart",
                        "params": {"symbol": "BTCUSDT", "timeframe": "15m"},
                    }
                )
                assert await asyncio.to_thread(started.wait, 2)
                await first.close()
                await second.send_json(
                    {
                        "type": "request",
                        "id": 1,
                        "op": "symbols",
                        "params": {"search": "", "offset": 0},
                    }
                )
                assert not await asyncio.to_thread(symbols_started.wait, 0.1)
                release.set()
                assert (await reply(second, 1))["ok"] is True
                await second.close()
        finally:
            release.set()
            await runner.cleanup()

    asyncio.run(run())


def test_invalid_search_rejects_only_that_request_and_keeps_dashboard_connected():
    async def run():
        runner, root = await serve(create_app(Model(), read_only_loopback=True))
        try:
            async with ClientSession() as client:
                async with client.ws_connect(root + "/api/realtime", origin=root) as ws:
                    await ws.receive_json(timeout=3)
                    await ws.receive_json(timeout=3)
                    await ws.send_json(
                        {
                            "type": "request",
                            "id": 1,
                            "op": "symbols",
                            "params": {"search": "BTC/USDT", "offset": 0},
                        }
                    )
                    frame = await reply(ws, 1)
                    assert frame["ok"] is False and frame["status"] == 400
                    assert not ws.closed
                    await ws.send_json(
                        {
                            "type": "request",
                            "id": 2,
                            "op": "symbols",
                            "params": {"search": "ETH", "offset": 0},
                        }
                    )
                    assert (await reply(ws, 2))["data"]["items"][0]["symbol"] == "ETH"
        finally:
            await runner.cleanup()

    asyncio.run(run())
