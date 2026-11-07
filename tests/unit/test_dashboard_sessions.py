"""HTTPS cookie authority remains bounded by origin, expiry and a persistent key."""

import asyncio
import hashlib
import json

import pytest
from aiohttp import ClientSession, CookieJar, web

from tradeforge.dashboard.api import create_app
from tradeforge.dashboard.auth import Authenticator


def session_auth(clock=lambda: 1_000_000):
    from tradeforge.dashboard.sessions import Sessions

    auth = Authenticator.from_json(
        json.dumps(
            {
                "tokens": [
                    {
                        "subject": "operator",
                        "scopes": ["read", "control"],
                        "sha256": hashlib.sha256(b"x" * 64).hexdigest(),
                    }
                ]
            }
        )
    )
    return Sessions(auth, b"s" * 64, clock=clock)


def test_session_survives_restart_but_not_expiry_or_tampering():
    from tradeforge.dashboard.auth import AuthError

    at = [1_000_000]
    sessions = session_auth(lambda: at[0])
    cookie = sessions.login("x" * 64)
    restarted = session_auth(lambda: at[0])
    assert restarted.authenticate(cookie, "control").subject == "operator"
    with pytest.raises(AuthError):
        restarted.authenticate(cookie + "x", "read")
    at[0] += restarted.lifetime + 1
    with pytest.raises(AuthError):
        restarted.authenticate(cookie, "read")


def test_external_dashboard_requires_session_and_rejects_csrf():
    async def run():
        sessions = session_auth()
        runner = web.AppRunner(
            create_app(sessions=sessions, external_origin="https://trade.example.com"),
            access_log=None,
        )
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
        host = {"Host": "trade.example.com"}
        try:
            async with ClientSession(cookie_jar=CookieJar()) as client:
                async with client.get(url + "/api/session", headers=host) as response:
                    assert response.status == 401
                async with client.post(
                    url + "/login", headers=host, data={"token": "x" * 64}, allow_redirects=False
                ) as response:
                    assert response.status == 403
                async with client.post(
                    url + "/login",
                    headers={**host, "Origin": "https://trade.example.com"},
                    data={"token": "x" * 64},
                    allow_redirects=False,
                ) as response:
                    assert response.status == 303
                    cookie = response.cookies["__Host-tradeforge"]
                    assert cookie["secure"] and cookie["httponly"]
                    assert cookie["samesite"] == "Strict" and cookie["path"] == "/"
                    value = cookie.value
                authorized = {**host, "Cookie": "__Host-tradeforge=" + value}
                async with client.get(url + "/api/session", headers=authorized) as response:
                    assert response.status == 200
                for origin in (None, "https://evil.invalid"):
                    headers = dict(authorized)
                    if origin:
                        headers["Origin"] = origin
                    async with client.post(
                        url + "/api/market/chart", headers=headers, json={}
                    ) as response:
                        assert response.status == 403
                async with client.get(
                    url + "/api/session",
                    headers={
                        **authorized,
                        "Host": "evil.invalid",
                        "X-Forwarded-Host": "trade.example.com",
                    },
                ) as response:
                    assert response.status == 403
        finally:
            await runner.cleanup()

    asyncio.run(run())
