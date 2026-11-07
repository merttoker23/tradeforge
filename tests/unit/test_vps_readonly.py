"""Read-only loopback is browseable without a tab-specific login, not writable."""

import asyncio
import hashlib
import json
from pathlib import Path

import pytest
from aiohttp import ClientSession, web

from tradeforge.dashboard.api import create_app
from tradeforge.dashboard.auth import Authenticator


def test_loopback_read_only_session_is_not_operator():
    async def run():
        runner = web.AppRunner(create_app(read_only_loopback=True), access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        url = f"http://127.0.0.1:{port}"
        try:
            async with ClientSession() as client:
                for _ in range(2):
                    async with client.get(url + "/api/session") as response:
                        assert response.status == 200
                        assert (await response.json())["scopes"] == ["read"]
                async with client.get(url + "/api/snapshot") as response:
                    assert response.status == 503  # DB absent; never fake OK
                async with client.post(
                    url + "/api/shutdown", json={}, headers={"Origin": url}
                ) as response:
                    assert response.status in {401, 403, 404}
                async with client.post(
                    url + "/api/commands", json={}, headers={"Origin": url}
                ) as response:
                    assert response.status in {401, 403, 404}
                async with client.post(
                    url + "/api/setup/demo", json={}, headers={"Origin": url}
                ) as response:
                    assert response.status in {401, 403, 404}
                async with client.get(
                    url + "/api/session", headers={"Host": "evil.invalid"}
                ) as response:
                    assert response.status == 403
        finally:
            await runner.cleanup()

    asyncio.run(run())


def test_read_only_listener_rejects_operator_writes_and_foreign_origin():
    async def run():
        token = "o" * 64
        auth = Authenticator.from_json(
            json.dumps(
                {
                    "tokens": [
                        {
                            "subject": "operator",
                            "scopes": ["read", "control"],
                            "sha256": hashlib.sha256(token.encode()).hexdigest(),
                        }
                    ]
                }
            )
        )
        runner = web.AppRunner(create_app(auth=auth, read_only_loopback=True), access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
        try:
            async with ClientSession() as client:
                for path in ("/api/market/chart",):
                    async with client.post(
                        url + path,
                        json={},
                        headers={"Authorization": "Bearer " + token, "Origin": url},
                    ) as response:
                        assert response.status == 403
                for headers in (
                    {"Origin": "https://evil.invalid"},
                    {"Sec-Fetch-Site": "cross-site"},
                ):
                    async with client.get(url + "/api/session", headers=headers) as response:
                        assert response.status == 403
        finally:
            await runner.cleanup()

    asyncio.run(run())


def test_dashboard_runtime_rejects_mixed_authority_and_unsafe_config(monkeypatch):
    from apps.dashboard.runtime import build_app, main

    with pytest.raises(ValueError, match="DASHBOARD_CONFIG_INVALID"):
        build_app(auth_file=Path("missing.auth"), read_only_loopback=True)
    monkeypatch.setenv("TRADEFORGE_LIVE", "true")
    monkeypatch.setattr("sys.argv", ["dashboard", "--check"])
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 2
