"""Loopback operator principal must cover the WebSocket; Binance 400s stay precise."""

import asyncio
from datetime import UTC, datetime

import pytest
from aiohttp import ClientSession, WSServerHandshakeError, web

from tradeforge.dashboard.api import create_app


class Model:
    db = object()

    def snapshot(self):
        return {"status": "DEGRADED", "sections": {}, "as_of": datetime.now(UTC).isoformat()}


async def serve(app):
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    return runner, f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"


def test_loopback_websocket_shares_http_principal_and_rejects_foreign_origin():
    async def run():
        app = create_app(
            Model(), None, auth=None, read_only_loopback=False, binance_master="x" * 32
        )
        runner, root = await serve(app)
        try:
            async with ClientSession() as client:
                async with client.get(root + "/api/session") as response:
                    assert response.status == 200
                    body = await response.json()
                    assert body["subject"] == "loopback"
                    assert body["scopes"] == ["control", "read"]
                # The hub's standalone asset route must agree with /assets/.
                async with client.get(root + "/realtime.js") as response:
                    assert response.status == 200
                async with client.get(root + "/assets/realtime.js") as response:
                    assert response.status == 200
                async with client.ws_connect(root + "/api/realtime", origin=root) as ws:
                    hello = await ws.receive_json(timeout=3)
                    assert hello["type"] == "hello"
                    assert hello["session"]["scopes"] == ["control", "read"]
                    snapshot = await ws.receive_json(timeout=3)
                    assert snapshot["type"] == "snapshot"
                with pytest.raises(WSServerHandshakeError) as error:
                    await client.ws_connect(root + "/api/realtime", origin="https://evil.example")
                assert error.value.status == 403
        finally:
            await runner.cleanup()

    asyncio.run(run())


def test_binance_write_trims_and_returns_specific_error_codes(monkeypatch):
    from tradeforge.runtime import binance_environment

    saved = {}

    def fake_update(db, master, environment, api_key, api_secret):
        saved.update(environment=environment, api_key=api_key, api_secret=api_secret)

    def fake_state(db):
        return {"environment": "testnet", "revision": 1, "credentials": {}}

    monkeypatch.setattr(binance_environment, "update_credentials", fake_update)
    monkeypatch.setattr(binance_environment, "public_state", fake_state)

    async def run():
        app = create_app(
            Model(), None, auth=None, read_only_loopback=False, binance_master="x" * 32
        )
        runner, root = await serve(app)
        try:
            async with ClientSession() as client:
                headers = {"Origin": root}
                async with client.post(
                    root + "/api/binance/settings",
                    json={
                        "environment": " testnet ",
                        "api_key": "  " + "a" * 64 + " \n",
                        "api_secret": "\t" + "b" * 64 + "  ",
                    },
                    headers=headers,
                ) as response:
                    assert response.status == 200, await response.text()
                    assert (await response.json())["environment"] == "testnet"
                assert saved == {
                    "environment": "testnet",
                    "api_key": "a" * 64,
                    "api_secret": "b" * 64,
                }

                # Whitespace-only credentials are still rejected, with a precise code.
                def invalid_update(db, master, environment, api_key, api_secret):
                    raise ValueError("BINANCE_CREDENTIALS_INVALID")

                monkeypatch.setattr(binance_environment, "update_credentials", invalid_update)
                async with client.post(
                    root + "/api/binance/settings",
                    json={"environment": "testnet", "api_key": "x", "api_secret": "y"},
                    headers=headers,
                ) as response:
                    assert response.status == 400
                    assert (await response.json())["error"] == "BINANCE_CREDENTIALS_INVALID"

                # A blocked switch must name the real blocker, not COMMAND_INVALID.
                def blocked_switch(db, environment):
                    raise ValueError("BINANCE_UNPARTITIONED_HISTORY_BLOCKS_SWITCH")

                monkeypatch.setattr(binance_environment, "switch_environment", blocked_switch)
                async with client.post(
                    root + "/api/binance/settings",
                    json={"environment": "live"},
                    headers=headers,
                ) as response:
                    assert response.status == 400
                    assert (await response.json())["error"] == (
                        "BINANCE_UNPARTITIONED_HISTORY_BLOCKS_SWITCH"
                    )
                # An unhashable environment must be a 400, not a 503 from TypeError.
                async with client.post(
                    root + "/api/binance/test",
                    json={"environment": []},
                    headers=headers,
                ) as response:
                    assert response.status == 400
                    assert (await response.json())["error"] == "BINANCE_TEST_INVALID"
                async with client.get(root + "/api/binance/settings") as response:
                    assert response.status == 200
        finally:
            await runner.cleanup()

    asyncio.run(run())


def test_binance_test_reports_a_safe_specific_rejection_reason(monkeypatch):
    from tradeforge.execution import binance_client
    from tradeforge.execution.broker import ExchangeRejected
    from tradeforge.runtime import binance_environment

    recorded = {}
    monkeypatch.setattr(
        binance_environment,
        "load_credentials",
        lambda db, master, env: ("a" * 32, "b" * 32),
    )
    monkeypatch.setattr(
        binance_environment,
        "record_connection_test",
        lambda db, env, connected: recorded.update(environment=env, connected=connected),
    )

    class RejectedClient:
        environment = "testnet"

        def __init__(self, *_args, **_kwargs):
            pass

        def request(self, method, path, params=None, *, signed=True):
            if path == "/fapi/v1/ping":
                return {}
            raise ExchangeRejected("API_KEY_INVALID")

    monkeypatch.setattr(binance_client, "BinanceTestnetClient", RejectedClient)

    async def run():
        app = create_app(
            Model(), None, auth=None, read_only_loopback=False, binance_master="x" * 32
        )
        runner, root = await serve(app)
        try:
            async with ClientSession() as client:
                async with client.post(
                    root + "/api/binance/test",
                    json={"environment": "testnet"},
                    headers={"Origin": root},
                ) as response:
                    assert response.status == 200
                    assert (await response.json()) == {
                        "environment": "testnet",
                        "connected": False,
                        "reason": "BINANCE_TEST_API_KEY_INVALID",
                    }
                assert recorded == {"environment": "testnet", "connected": False}
        finally:
            await runner.cleanup()

    asyncio.run(run())


def test_binance_test_with_no_credentials_reports_disconnected(monkeypatch):
    from tradeforge.runtime import binance_environment

    recorded = {}
    monkeypatch.setattr(binance_environment, "load_credentials", lambda db, master, env: None)
    monkeypatch.setattr(
        binance_environment,
        "record_connection_test",
        lambda db, env, connected: recorded.update(environment=env, connected=connected),
    )

    async def run():
        app = create_app(
            Model(), None, auth=None, read_only_loopback=False, binance_master="x" * 32
        )
        runner, root = await serve(app)
        try:
            async with ClientSession() as client:
                async with client.post(
                    root + "/api/binance/test",
                    json={"environment": " testnet "},
                    headers={"Origin": root},
                ) as response:
                    assert response.status == 200
                    assert (await response.json()) == {
                        "environment": "testnet",
                        "connected": False,
                        "reason": "BINANCE_TEST_CREDENTIALS_MISSING",
                    }
                assert recorded == {"environment": "testnet", "connected": False}
        finally:
            await runner.cleanup()

    asyncio.run(run())
