"""Selected timeframe warmup stays in the credential-free gateway, with bounded demand."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from tests.universe_fixtures import payload, symbol
from tradeforge.dashboard.auth import Principal
from tradeforge.dashboard.control import CommandQueue
from tradeforge.market.clock_sync import ClockSync
from tradeforge.market.instrument_registry import InstrumentRegistry
from tradeforge.market.market_gateway import MarketGateway
from tradeforge.market.runtime import GatewayPersistence


def test_authenticated_chart_demand_is_deduplicated_and_rate_bounded(migrated):
    person = Principal("reader", frozenset({"read"}))
    queue = CommandQueue(migrated)
    queue.request_chart(person, {"symbol": "BTCUSDT", "timeframe": "1m"})
    queue.request_chart(person, {"symbol": "BTCUSDT", "timeframe": "1m"})
    with migrated.transaction() as c:
        assert (
            c.scalar(text("SELECT count(*) FROM audit_log WHERE event_type='dashboard.chart.v1'"))
            == 1
        )
        assert c.scalar(text("SELECT count(*) FROM execution_intents")) == 0
    for tf in ("5m", "15m", "1h"):
        queue.request_chart(person, {"symbol": "BTCUSDT", "timeframe": tf})
    with pytest.raises(ValueError):
        queue.request_chart(person, {"symbol": "BTCUSDT", "timeframe": "4h"})
    with pytest.raises(ValueError):
        queue.request_chart(person, {"symbol": "BTCUSDT", "timeframe": "1s"})


def test_gateway_only_loads_current_eligible_selected_chart(migrated):
    now = datetime.now(UTC)
    registry = InstrumentRegistry(migrated)
    registry.record(payload(symbol()), now, timedelta(hours=1))
    sync = ClockSync()
    sync.observe(now, now, now, 1)
    gateway = MarketGateway(sync, clock=lambda: now, monotonic=lambda: 1)
    gateway.update_universe(registry.view(now))
    queue = CommandQueue(migrated, clock=lambda: now)
    person = Principal("reader", frozenset({"read"}))
    queue.request_chart(person, {"symbol": "NOTELIGIBLE", "timeframe": "1m"})

    class NeverNetwork:
        async def fetch(self, *args):
            pytest.fail("ineligible chart triggered network")

    worker = GatewayPersistence(gateway, migrated)
    assert asyncio.run(worker.chart_once(NeverNetwork())) is False


def test_read_only_dashboard_get_warms_all_native_timeframes_via_gateway(migrated):
    from aiohttp import ClientSession, web

    from apps.dashboard.runtime import notify_chart_demand
    from apps.market_gateway.runtime import create_app
    from tradeforge.dashboard.api import create_app as dashboard_app
    from tradeforge.dashboard.read_model import ReadModel

    now = datetime.now(UTC)
    registry = InstrumentRegistry(migrated)
    registry.record(payload(symbol()), now, timedelta(hours=1))
    sync = ClockSync()
    sync.observe(now, now, now, 1)
    gateway = MarketGateway(sync, clock=lambda: now, monotonic=lambda: 1)
    gateway.update_universe(registry.view(now))
    worker = GatewayPersistence(gateway, migrated)
    requested = []

    class RecordedPublicClient:
        async def fetch(self, path, params):
            assert path == "/fapi/v1/klines"
            requested.append(params["interval"])
            start = int(params["startTime"])
            seconds = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400}[params["interval"]]
            return [
                [
                    start,
                    "100",
                    "110",
                    "90",
                    "105",
                    "2",
                    start + seconds * 1000 - 1,
                    "0",
                    1,
                    "0",
                    "0",
                    "0",
                ]
            ], now

    async def run():
        market_runner = web.AppRunner(create_app(gateway, chart_demand=worker.request_chart))
        await market_runner.setup()
        market_site = web.TCPSite(market_runner, "127.0.0.1", 0)
        await market_site.start()
        port = market_site._server.sockets[0].getsockname()[1]

        async def notify(symbol, timeframe):
            await notify_chart_demand(symbol, timeframe, port=port)

        dashboard_runner = web.AppRunner(
            dashboard_app(ReadModel(migrated), read_only_loopback=True, chart_demand=notify)
        )
        await dashboard_runner.setup()
        dashboard_site = web.TCPSite(dashboard_runner, "127.0.0.1", 0)
        await dashboard_site.start()
        url = f"http://127.0.0.1:{dashboard_site._server.sockets[0].getsockname()[1]}"
        try:
            async with ClientSession() as client:
                for index, timeframe in enumerate(("1m", "5m", "15m", "1h", "4h")):
                    if index == 4:
                        # A new bounded demand window, without a real sleep.
                        for key in worker.chart_demand:
                            worker.chart_demand[key] -= timedelta(seconds=31)
                    async with client.get(
                        url + f"/api/market/chart?symbol=BTCUSDT&timeframe={timeframe}"
                    ) as response:
                        assert response.status == 200
                    assert await worker.chart_once(RecordedPublicClient())
                    chart = ReadModel(migrated).terminal_chart("BTCUSDT", timeframe)
                    assert chart["bars"] and chart["bars"][0]["close"] == "105"
                async with client.post(
                    url + "/api/market/chart", json={"symbol": "BTCUSDT", "timeframe": "1m"}
                ) as response:
                    assert response.status == 403
        finally:
            await dashboard_runner.cleanup()
            await market_runner.cleanup()

    asyncio.run(run())
    assert requested == ["1m", "5m", "15m", "1h", "4h"]
    with migrated.transaction() as connection:
        assert (
            connection.scalar(
                text("SELECT count(*) FROM audit_log WHERE event_type='dashboard.chart.v1'")
            )
            == 0
        )
        assert connection.scalar(text("SELECT count(*) FROM execution_intents")) == 0
