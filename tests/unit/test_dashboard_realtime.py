"""Read-only realtime transport contracts; never interacts with Binance orders."""

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from aiohttp import ClientSession, web

from tradeforge.dashboard.auth import AuthError, Principal
from tradeforge.dashboard.realtime import MAX_SYMBOLS, RealtimeHub


class FakeSession:
    def __init__(self) -> None:
        self.calls = []

    def authenticate(self, cookie, scope):
        self.calls.append((cookie, scope))
        if cookie != "valid":
            raise AuthError(401)
        return Principal("test", frozenset({"read"}))


def test_cookie_bound_socket_requires_exact_origin():
    session = FakeSession()
    hub = RealtimeHub(None, sessions=session, origin="https://tradeforge.example")
    for origin in (None, "https://evil.example", "http://tradeforge.example"):
        request = SimpleNamespace(
            headers={"Origin": origin} if origin else {},
            cookies={"__Host-tradeforge": "valid"},
        )
        with pytest.raises(AuthError) as error:
            hub.authorize(request)
        assert error.value.status == 403
        assert not session.calls
    hub.authorize(
        SimpleNamespace(
            headers={"Origin": "https://tradeforge.example"},
            cookies={"__Host-tradeforge": "valid"},
        )
    )
    assert session.calls == [("valid", "read")]


def test_csp_restricts_websocket_to_configured_authority():
    async def run():
        hub = RealtimeHub(None, sessions=FakeSession(), origin="https://terminal.example")
        response = web.Response(
            headers={"Content-Security-Policy": "default-src 'none'; connect-src 'self';"}
        )
        await hub.allow_same_origin_socket(SimpleNamespace(host="attacker.example"), response)
        policy = response.headers["Content-Security-Policy"]
        assert "wss://terminal.example" in policy
        assert "attacker.example" not in policy
        assert "default-src 'none'" in policy

    asyncio.run(run())


def test_quote_environment_and_source_expiry_are_preserved():
    now = datetime.now(UTC)
    q = SimpleNamespace(
        symbol="BTCUSDT",
        mark_price="123.45",
        volume="456",
        source_at=now,
        volume_source_at=now,
        source="BINANCE_PUBLIC",
        environment="MAINNET",
    )
    state = {"transport": "OK", "eligible": ["BTCUSDT"]}
    hub = RealtimeHub(None, market_feed=lambda: (q,), market_state=lambda: state)
    rows = hub.quotes(("BTCUSDT",))
    assert rows["type"] == "quotes"
    assert rows["execution_evidence"] is False
    assert rows["items"][0]["environment"] == "MAINNET"
    assert rows["items"][0]["freshness"] == "OK"
    q.source_at = now - timedelta(seconds=30)
    stale = hub.quotes(("BTCUSDT",))["items"][0]
    assert stale["freshness"] == "STALE"
    assert stale["reason"] == "SOURCE_EXPIRED"
    state["transport"] = "UNAVAILABLE"
    assert hub.quotes(("BTCUSDT",))["items"][0]["freshness"] == "STALE"


def test_stream_subscription_is_bounded_and_sequences_are_contiguous():
    async def run():
        quote = SimpleNamespace(
            symbol="BTCUSDT",
            mark_price="1",
            volume=None,
            source_at=datetime.now(UTC),
            volume_source_at=None,
            source="PUBLIC",
            environment="MAINNET",
        )
        hub = RealtimeHub(
            None,
            read_only_loopback=True,
            market_feed=lambda: (quote,),
            market_state=lambda: {"transport": "OK", "eligible": ["BTCUSDT"]},
        )
        app = web.Application()
        hub.install(app)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        root = f"http://127.0.0.1:{port}"
        try:
            async with ClientSession() as client:
                async with client.ws_connect(root + "/api/realtime", origin=root) as ws:
                    hello = await ws.receive_json(timeout=3)
                    snapshot = await ws.receive_json(timeout=3)
                    assert hello["type"] == "hello" and hello["seq"] == 1
                    assert snapshot["type"] == "snapshot" and snapshot["seq"] == 2
                    await ws.send_json({"type": "subscribe", "symbols": ["BTCUSDT"]})
                    frames = []
                    for _ in range(4):
                        frame = await ws.receive_json(timeout=3)
                        frames.append(frame)
                        if frame["type"] == "quotes":
                            break
                    assert frames[-1]["type"] == "quotes"
                    assert frames[-1]["items"][0]["symbol"] == "BTCUSDT"
                    assert [f["seq"] for f in frames] == list(range(3, 3 + len(frames)))
                async with client.ws_connect(root + "/api/realtime", origin=root) as ws:
                    await ws.receive_json(timeout=3)
                    await ws.receive_json(timeout=3)
                    await ws.send_json(
                        {"type": "subscribe", "symbols": ["BTCUSDT"] * (MAX_SYMBOLS + 1)}
                    )
                    result = await ws.receive(timeout=3)
                    assert result.type.name in {"CLOSE", "CLOSING", "CLOSED"}
        finally:
            await runner.cleanup()

    asyncio.run(run())
