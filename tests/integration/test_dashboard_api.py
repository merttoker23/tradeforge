"""HTTP security boundary and honest local dashboard behavior."""

import asyncio
from contextlib import asynccontextmanager

import pytest
from aiohttp import ClientSession, web

from tests.unit.test_dashboard_auth import auth


def chart_request(symbol="BTCUSDT", timeframe="15m"):
    return {"symbol": symbol, "timeframe": timeframe}


@asynccontextmanager
async def server(db=None):
    from tradeforge.dashboard.api import create_app
    from tradeforge.dashboard.control import CommandQueue
    from tradeforge.dashboard.read_model import ReadModel

    app = create_app(ReadModel(db) if db else None, CommandQueue(db) if db else None, auth())
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    try:
        async with ClientSession() as client:
            yield client, f"http://127.0.0.1:{port}"
    finally:
        await runner.cleanup()


def test_unauthorized_writes_are_rejected(migrated):
    async def run():
        async with server(migrated) as (client, url):
            async with client.post(url + "/api/market/chart", json=chart_request()) as response:
                assert response.status == 401
            async with client.post(
                url + "/api/market/chart",
                json=chart_request(),
                headers={"Authorization": "Bearer " + "x" * 48},
            ) as response:
                assert response.status == 401
                assert "x" * 48 not in await response.text()

    asyncio.run(run())


def test_bearer_only_no_cookie_query_or_cross_origin_writes(migrated):
    async def run():
        async with server(migrated) as (client, url):
            for headers, suffix, status in [
                ({"Cookie": "token=" + "o" * 48}, "", 401),
                ({}, "?token=" + "o" * 48, 401),
                (
                    {"Authorization": "Bearer " + "o" * 48, "Origin": "https://evil.invalid"},
                    "",
                    403,
                ),
                ({"Authorization": "Bearer " + "o" * 48, "Host": "evil.invalid"}, "", 403),
            ]:
                async with client.post(
                    url + "/api/market/chart" + suffix, json=chart_request(), headers=headers
                ) as response:
                    assert response.status == status
                    assert "o" * 48 not in await response.text()
            async with client.post(
                url + "/api/market/chart",
                json=chart_request(),
                headers={"Authorization": "Bearer " + "o" * 48, "Origin": url},
            ) as response:
                assert response.status == 202
                assert (await response.json())["status"] == "REQUESTED"

    asyncio.run(run())


def test_unavailable_reads_stale_banner_assets_and_liveness():
    async def run():
        async with server() as (client, url):
            async with client.get(url + "/health") as response:
                assert response.status == 200
                assert (await response.json())["orders_enabled"] is False
            headers = {"Authorization": "Bearer " + "v" * 48}
            async with client.get(url + "/api/snapshot", headers=headers) as response:
                assert response.status == 503
                assert (await response.json())["status"] == "STALE"
                assert response.headers["Cache-Control"] == "no-store"
            async with client.get(url + "/readiness", headers=headers) as response:
                assert response.status == 503
            async with client.get(url) as response:
                body = await response.text()
                assert response.status == 200
                assert "Doğrulanıyor" in body and "Algoritma Kontrol Merkezi" in body
                assert "script-src 'self'" in response.headers["Content-Security-Policy"]
                assert response.headers["X-Frame-Options"] == "DENY"
            async with client.get(url + "/assets/app.js") as response:
                body = await response.text()
                assert "textContent" in body
                assert "localStorage" not in body and "innerHTML" not in body

    asyncio.run(run())


def test_requests_are_bounded_and_errors_do_not_expose_inputs(migrated):
    async def run():
        async with server(migrated) as (client, url):
            headers = {"Authorization": "Bearer " + "o" * 48}
            async with client.post(
                url + "/api/market/chart", data="s" * 4097, headers=headers
            ) as response:
                assert response.status == 413
            async with client.post(
                url + "/api/market/chart", json={"secret": "SENTINEL"}, headers=headers
            ) as response:
                assert response.status == 400
                assert "SENTINEL" not in await response.text()
            async with client.get(url + "/api/session", headers=headers) as response:
                assert response.status == 200
                assert (await response.json())["scopes"] == ["control", "read"]

    asyncio.run(run())


def test_polling_returns_bounded_projection_and_db_failure_is_safe(migrated, monkeypatch):
    async def run():
        async with server(migrated) as (client, url):
            headers = {"Authorization": "Bearer " + "v" * 48}
            async with client.get(url + "/api/snapshot", headers=headers) as response:
                assert response.status == 200
                body = await response.json()
                assert body["poll_seconds"] == 5 and body["row_limit"] == 100
                assert body["status"] == "DEGRADED"

            def fail():
                raise RuntimeError("PASSWORD_SENTINEL")

            monkeypatch.setattr(migrated.sql_engine, "begin", fail)
            # Writes must not succeed if their atomic audit cannot be written.
            async with client.post(
                url + "/api/market/chart",
                json=chart_request(),
                headers={"Authorization": "Bearer " + "o" * 48},
            ) as response:
                assert response.status == 503
                assert "PASSWORD_SENTINEL" not in await response.text()

    asyncio.run(run())


@pytest.mark.parametrize(
    "headers", [{}, {"Host": "evil.invalid"}, {"Origin": "https://evil.invalid"}]
)
def test_all_unauthorized_shapes_have_bounded_audit(migrated, headers):
    from sqlalchemy import text

    async def run():
        async with server(migrated) as (client, url):
            for _ in range(9):
                async with client.get(url + "/api/snapshot", headers=headers) as response:
                    status = response.status
            assert status == 429
        with migrated.transaction() as c:
            count = c.scalar(
                text("SELECT count(*) FROM audit_log WHERE event_type='dashboard.auth.v1'")
            )
        assert count <= 8

    asyncio.run(run())


def test_api_rechecks_source_deadlines_in_cached_responses(migrated, monkeypatch):
    from datetime import UTC, datetime, timedelta

    from tradeforge.dashboard.read_model import ReadModel

    at = datetime.now(UTC)
    body = {
        "status": "DEGRADED",
        "sections": {
            "guardian": {
                "status": "OK",
                "data": [
                    {
                        "ready": True,
                        "unprotected_positions": 0,
                        "freshness": "OK",
                        "fresh_until": (at - timedelta(seconds=1)).isoformat(),
                    }
                ],
            }
        },
        "alarms": [],
    }
    monkeypatch.setattr(ReadModel, "snapshot", lambda self: body)

    async def run():
        async with server(migrated) as (client, url):
            for _ in range(2):
                async with client.get(
                    url + "/api/snapshot", headers={"Authorization": "Bearer " + "v" * 48}
                ) as response:
                    assert response.status == 200
                    result = await response.json()
                    assert result["sections"]["guardian"]["status"] == "STALE"

    asyncio.run(run())
