"""Anonymous browsing can warm public charts without any DB write authority."""

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from aiohttp import ClientSession, web

from tradeforge.market.runtime import GatewayPersistence


def test_public_chart_demand_is_bounded_deduplicated_and_expires():
    at = [datetime(2026, 10, 24, tzinfo=UTC)]
    gateway = SimpleNamespace(clock=lambda: at[0], eligible=frozenset({"BTCUSDT"}))
    worker = GatewayPersistence(gateway, None)
    for _ in range(10):
        worker.request_chart("BTCUSDT", "1m")
    for timeframe in ("5m", "15m", "1h"):
        worker.request_chart("BTCUSDT", timeframe)
    with pytest.raises(ValueError, match="CHART_RATE_LIMITED"):
        worker.request_chart("BTCUSDT", "4h")
    at[0] += timedelta(seconds=31)
    worker.request_chart("BTCUSDT", "4h")
    for symbol, timeframe in (("BTCUSDT", "1s"), ("UNKNOWN", "1m")):
        with pytest.raises(ValueError):
            worker.request_chart(symbol, timeframe)


def test_internal_chart_demand_endpoint_rejects_browser_and_foreign_host():
    from apps.market_gateway.runtime import create_app

    async def run():
        gateway = SimpleNamespace(clock=lambda: datetime.now(UTC), eligible={"BTCUSDT"})
        worker = GatewayPersistence(gateway, None)
        runner = web.AppRunner(create_app(gateway, chart_demand=worker.request_chart))
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/chart-demand?symbol=BTCUSDT&timeframe=1m"
        try:
            async with ClientSession() as client:
                async with client.get(url) as response:
                    assert response.status == 202
                for headers in (
                    {"Host": "evil.invalid"},
                    {"Origin": "http://127.0.0.1"},
                    {"Sec-Fetch-Site": "same-origin"},
                ):
                    async with client.get(url, headers=headers) as response:
                        assert response.status == 403
        finally:
            await runner.cleanup()

    asyncio.run(run())
