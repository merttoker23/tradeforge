"""Broad recovery retains still-valid members and overlaps independent public endpoints."""

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from tradeforge.market.runtime import GatewayPersistence


def fixture():
    at = [datetime(2026, 11, 3, tzinfo=UTC)]
    symbols = tuple(f"ASSET{i}USDT" for i in range(5))
    gateway = SimpleNamespace(
        clock=lambda: at[0],
        monotonic=lambda: 1,
        clock_sync=SimpleNamespace(ready=lambda *a: True),
        view=SimpleNamespace(
            ready=True,
            observation=SimpleNamespace(
                observed_at=at[0], expires_at=at[0] + timedelta(hours=1), version_hash="v1"
            ),
        ),
        eligible=set(symbols),
        plan=SimpleNamespace(groups=()),
        _epoch=1,
        fresh=lambda *a: False,
        stale_after=15,
    )
    return GatewayPersistence(gateway, None), at, symbols


def rows(symbols, at):
    stamp = int(at.timestamp() * 1000)
    return {
        "/fapi/v1/premiumIndex": [
            dict(
                symbol=s,
                markPrice="100.5",
                indexPrice="100.4",
                lastFundingRate="0.0001",
                time=stamp,
            )
            for s in symbols
        ],
        "/fapi/v1/ticker/bookTicker": [
            dict(symbol=s, bidPrice="100", askPrice="101", time=stamp) for s in symbols
        ],
        "/fapi/v1/ticker/24hr": [dict(symbol=s, volume="123", closeTime=stamp) for s in symbols],
    }


def test_complementary_partial_responses_preserve_members_and_original_source_times():
    worker, at, symbols = fixture()
    first = rows(symbols[:3], at[0])
    worker.rest_samples = worker.rest_markets(
        first["/fapi/v1/ticker/bookTicker"],
        first["/fapi/v1/premiumIndex"],
        first["/fapi/v1/ticker/24hr"],
        at[0],
    )
    old_source = at[0]
    at[0] += timedelta(seconds=3)
    second = rows(symbols[3:], at[0])

    class Client:
        async def fetch(self, path, params):
            return second[path], at[0]

    assert asyncio.run(worker.quote_once(Client()))
    samples = {s.symbol: s for s in worker.rest_samples}
    assert set(samples) == set(symbols)
    assert samples[symbols[0]].source_at == old_source
    assert samples[symbols[4]].source_at == at[0]
    at[0] += timedelta(seconds=13)
    asyncio.run(worker.quote_once(Client()))
    assert {s.symbol for s in worker.rest_samples} == set(symbols[3:])


def test_independent_quote_endpoints_can_progress_while_one_is_waiting():
    async def scenario():
        worker, at, symbols = fixture()
        body = rows(symbols, at[0])
        started = set()
        all_started = asyncio.Event()

        class Client:
            async def fetch(self, path, params):
                started.add(path)
                if len(started) == 3:
                    all_started.set()
                await asyncio.wait_for(all_started.wait(), 0.5)
                return body[path], at[0]

        assert await worker.quote_once(Client())
        assert len(worker.rest_samples) == 5
        assert {s.source_at for s in worker.rest_samples} == {at[0]}

    asyncio.run(scenario())


def test_removed_members_cannot_satisfy_new_universe_cost_coverage():
    worker, at, symbols = fixture()
    old = rows(symbols, at[0])
    worker.rest_samples = worker.rest_markets(
        old["/fapi/v1/ticker/bookTicker"],
        old["/fapi/v1/premiumIndex"],
        old["/fapi/v1/ticker/24hr"],
        at[0],
    )
    worker.gateway.eligible = {"NEWUSDT"}
    worker.gateway.fresh = lambda *a: True
    body = rows(("NEWUSDT",), at[0])

    class Client:
        async def fetch(self, path, params):
            return body[path], at[0]

    assert asyncio.run(worker.quote_once(Client()))
    assert [sample.symbol for sample in worker.rest_samples] == ["NEWUSDT"]
