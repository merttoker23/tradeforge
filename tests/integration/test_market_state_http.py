import asyncio
import json
from datetime import timedelta

import aiohttp
from aiohttp import web

from tests.market_fixtures import START, instrument
from tests.unit.test_market_state_recovery import rows
from tradeforge.domain.market import Timeframe
from tradeforge.market.exchange_info import Reply
from tradeforge.market.market_state import MarketState
from tradeforge.market.recovery import MarketRecovery, PublicMarketClient


def test_loopback_backfill_replays_identical_snapshot_hashes():
    async def scenario():
        requested = []

        async def klines(request):
            requested.append(dict(request.query))
            return web.json_response(rows())

        app = web.Application()
        app.router.add_get("/fapi/v1/klines", klines)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            async with aiohttp.ClientSession() as session:

                async def get(path, params):
                    async with session.get(
                        f"http://127.0.0.1:{port}" + path, params=params
                    ) as response:
                        return Reply(response.status, dict(response.headers), await response.read())

                at = START + timedelta(minutes=3, seconds=1)
                client = PublicMarketClient(get=get, clock=lambda: at, min_interval=0)
                hashes = []
                for _ in range(2):
                    state = MarketState(8)
                    assert await MarketRecovery(state, client).bars(
                        instrument(), Timeframe.M1, START, START + timedelta(minutes=3)
                    )
                    view = state.view(instrument(), Timeframe.M1, at)
                    hashes.append(view.digest)
                    assert json.dumps(view.payload)
                assert hashes[0] == hashes[1]
                assert all(r["interval"] == "1m" and r["symbol"] == "BTCUSDT" for r in requested)
        finally:
            await runner.cleanup()

    asyncio.run(scenario())
