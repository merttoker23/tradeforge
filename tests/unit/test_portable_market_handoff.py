"""Regression tests for portable direct local handoff and authenticated Compose."""

import asyncio
import json

from aiohttp import ClientSession, web

from tradeforge.runtime import market_link


def test_loopback_handoff_only_accepts_same_host_local_requests(monkeypatch):
    monkeypatch.setattr(
        market_link,
        "memory_payload",
        lambda gateway, persistence, *, quotes_only=False: {
            "markets": [] if not quotes_only else None,
            "quotes": [] if quotes_only else None,
            "selected": "quotes" if quotes_only else "markets",
        },
    )

    async def exercise():
        app = web.Application()
        market_link.add_loopback_memory_route(app, object(), object())
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        base = f"http://127.0.0.1:{port}"
        try:
            async with ClientSession(trust_env=False) as session:
                async with session.get(base + "/market-state") as response:
                    assert response.status == 200
                    assert (await response.json())["selected"] == "markets"
                    assert response.headers.get("Cache-Control") == "no-store"
                async with session.get(base + "/quote-state") as response:
                    assert response.status == 200
                    assert (await response.json())["selected"] == "quotes"
                for bad_headers in (
                    {"Host": "evil.example"},
                    {"Origin": "https://evil.example"},
                    {"Sec-Fetch-Site": "cross-site"},
                    {"Authorization": "Bearer forged"},
                ):
                    async with session.get(base + "/market-state", headers=bad_headers) as response:
                        assert response.status == 403
                async with session.post(base + "/chart-request") as response:
                    assert response.status == 404
        finally:
            await runner.cleanup()

    asyncio.run(exercise())


def test_existing_authenticated_compose_route_is_not_opened(monkeypatch):
    token = "a" * 48
    monkeypatch.setattr(market_link, "memory_payload", lambda *args, **kwargs: {"markets": []})

    async def exercise():
        app = web.Application()
        market_link.add_memory_route(app, object(), object(), token)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            async with ClientSession(trust_env=False) as session:
                url = f"http://127.0.0.1:{port}/market-state"
                async with session.get(url) as response:
                    assert response.status == 403
                async with session.get(
                    url, headers={"Authorization": "Bearer " + token}
                ) as response:
                    assert response.status == 200
        finally:
            await runner.cleanup()

    asyncio.run(exercise())


def test_client_chooses_fixed_local_or_compose_host_without_configuration():
    class Content:
        async def iter_chunked(self, _size):
            yield json.dumps({"markets": []}).encode()

    class Response:
        status = 200
        content = Content()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            pass

    class Session:
        def __init__(self):
            self.calls = []

        def get(self, url, **kwargs):
            self.calls.append((url, kwargs))
            return Response()

    async def exercise():
        session = Session()
        assert (await market_link.fetch_memory(session, None)) == {"markets": []}
        assert session.calls[-1][0] == "http://127.0.0.1:8101/market-state"
        assert session.calls[-1][1]["headers"] == {}
        assert session.calls[-1][1]["allow_redirects"] is False
        assert (await market_link.fetch_memory(session, "x" * 48)) == {"markets": []}
        assert session.calls[-1][0] == "http://market_gateway:8101/market-state"
        assert session.calls[-1][1]["headers"] == {"Authorization": "Bearer " + "x" * 48}

    asyncio.run(exercise())
