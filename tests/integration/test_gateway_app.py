import asyncio

import aiohttp
from aiohttp import web

from tradeforge.market.clock_sync import ClockSync
from tradeforge.market.market_gateway import MarketGateway


def test_gateway_app_health_readiness_are_local_and_truthful():
    from apps.market_gateway.runtime import create_app

    async def scenario():
        gateway = MarketGateway(ClockSync())
        runner = web.AppRunner(create_app(gateway))
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            async with aiohttp.ClientSession() as client:
                for path, status in [("/health", 200), ("/readiness", 503)]:
                    async with client.get(f"http://127.0.0.1:{port}{path}") as response:
                        assert response.status == status
                        body = await response.json()
                        assert body["live"] is False
                        assert body["orders_enabled"] is False
                        assert body["trading_ready"] is False
                        assert body["state"] == ("ALIVE" if path == "/health" else "STALE")
        finally:
            await runner.cleanup()

    asyncio.run(scenario())


def test_liveness_does_not_compute_analytical_readiness(monkeypatch):
    from apps.market_gateway.runtime import create_app

    async def scenario():
        gateway = MarketGateway(ClockSync())
        calls = []
        original = gateway.status

        def analytical_status():
            calls.append(True)
            return original()

        monkeypatch.setattr(gateway, "status", analytical_status)
        runner = web.AppRunner(create_app(gateway))
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            async with aiohttp.ClientSession() as client:
                async with client.get(f"http://127.0.0.1:{port}/health") as response:
                    assert response.status == 200
                    assert (await response.json())["alive"] is True
                    assert calls == []
                async with client.get(f"http://127.0.0.1:{port}/readiness") as response:
                    assert response.status == 503
                    assert (await response.json())["data_ready"] is False
                    assert len(calls) == 1
        finally:
            await runner.cleanup()

    asyncio.run(scenario())
