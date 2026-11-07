"""Metadata caching must not freeze live RAM prices or renew their provenance."""

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from aiohttp import ClientSession, web

from tests.unit.test_dashboard_auth import auth
from tests.unit.test_momentum_alpha import evidence
from tradeforge.dashboard.api import create_app
from tradeforge.dashboard.read_model import ReadModel


@asynccontextmanager
async def quote_server(monkeypatch, *, ram=True, state=None):
    now = [datetime.now(UTC)]
    template = evidence().market
    quote = type(template).model_validate(
        {**template.model_dump(), "source_at": now[0], "observed_at": now[0], "as_of": now[0]},
        context={"now": now[0]},
    )
    quotes = [quote]
    metadata = {
        "items": [
            {
                "symbol": quote.symbol,
                "price": None,
                "volume": None,
                "source_at": None,
                "source": None,
                "freshness": "STALE",
            }
        ],
        "has_more": False,
    }
    calls = []
    model = ReadModel(None)

    def terminal_symbols(**kwargs):
        calls.append(kwargs)
        return metadata

    class CurrentTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now[0]

    monkeypatch.setattr(model, "terminal_symbols", terminal_symbols)
    monkeypatch.setattr("tradeforge.dashboard.api.datetime", CurrentTime)
    app = create_app(
        model,
        auth=auth(),
        market_feed=(lambda: tuple(quotes)) if ram else None,
        market_state=(lambda: state) if state is not None else None,
    )
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    try:
        async with ClientSession(headers={"Authorization": "Bearer " + "v" * 48}) as client:

            async def read():
                async with client.get(f"http://127.0.0.1:{port}/api/market/symbols") as response:
                    assert response.status == 200
                    return (await response.json())["items"][0]

            yield read, now, quotes, metadata, calls
    finally:
        await runner.cleanup()


def test_new_ram_price_is_visible_inside_metadata_cache_without_another_db_read(monkeypatch):
    async def scenario():
        async with quote_server(monkeypatch) as (read, now, quotes, metadata, calls):
            first = await read()
            now[0] += timedelta(milliseconds=100)
            quotes[0] = type(quotes[0]).model_validate(
                {
                    **quotes[0].model_dump(),
                    "mark_price": Decimal(first["price"]) + 1,
                    "source_at": now[0],
                    "observed_at": now[0],
                    "as_of": now[0],
                },
                context={"now": now[0]},
            )
            second = await read()
            assert second["price"] == str(quotes[0].mark_price)
            assert second["source_at"] == now[0].isoformat()
            assert len(calls) == 1
            assert metadata["items"][0]["price"] is None

    asyncio.run(scenario())


def test_empty_cached_page_receives_new_ram_quote_without_waiting_for_db_cache_expiry(monkeypatch):
    async def scenario():
        async with quote_server(monkeypatch) as (read, now, quotes, metadata, calls):
            sample = quotes.pop()
            assert (await read())["price"] is None
            quotes.append(sample)
            assert (await read())["price"] == str(sample.mark_price)
            assert metadata["items"][0]["price"] is None and len(calls) == 1

    asyncio.run(scenario())


def test_missing_authoritative_ram_feed_does_not_fall_back_to_db_prices(monkeypatch):
    async def scenario():
        async with quote_server(monkeypatch) as (read, now, quotes, metadata, calls):
            metadata["items"][0].update(
                price="123", volume="10", source_at=now[0].isoformat(), freshness="OK"
            )
            quotes.clear()
            missing = await read()
            assert missing["price"] is missing["volume"] is None
            assert missing["freshness"] == "STALE"

    asyncio.run(scenario())


@pytest.mark.parametrize("age", [15, 16, -1])
def test_cached_response_rechecks_ram_source_age_and_never_renews_it(monkeypatch, age):
    async def scenario():
        async with quote_server(monkeypatch) as (read, now, quotes, metadata, calls):
            assert (await read())["price"] is not None
            original_source = quotes[0].source_at
            now[0] = original_source + timedelta(seconds=age)
            expired = await read()
            assert expired["price"] == str(quotes[0].mark_price)
            assert expired["freshness"] == "STALE"
            assert expired["source_at"] == original_source.isoformat()
            assert quotes[0].source_at == original_source
            assert len(calls) == 1

    asyncio.run(scenario())


def test_removed_ram_quote_cannot_survive_in_the_metadata_cache(monkeypatch):
    async def scenario():
        async with quote_server(monkeypatch) as (read, now, quotes, metadata, calls):
            assert (await read())["price"] is not None
            quotes.clear()
            missing = await read()
            assert missing["price"] is missing["volume"] is None
            assert missing["freshness"] == "STALE"
            assert len(calls) == 1

    asyncio.run(scenario())


def test_cached_db_only_price_also_expires_at_its_original_source_deadline(monkeypatch):
    async def scenario():
        async with quote_server(monkeypatch, ram=False) as (read, now, quotes, metadata, calls):
            stamp = (now[0] - timedelta(seconds=14.9)).isoformat()
            metadata["items"][0].update(price="123", volume="10", source_at=stamp, freshness="OK")
            assert (await read())["price"] == "123"
            now[0] += timedelta(milliseconds=200)
            expired = await read()
            assert expired["price"] is expired["volume"] is None
            assert expired["freshness"] == "STALE" and expired["source_at"] == stamp
            assert metadata["items"][0]["price"] == "123" and len(calls) == 1

    asyncio.run(scenario())


def test_quotes_remain_available_while_chart_db_worker_is_blocked(monkeypatch):
    import threading

    async def scenario():
        entered, release = threading.Event(), threading.Event()
        sample = evidence().market
        model = ReadModel(None)

        def chart(*args):
            entered.set()
            release.wait(2)
            return {"bars": []}

        monkeypatch.setattr(model, "terminal_chart", chart)
        app = create_app(model, auth=auth(), market_feed=lambda: (sample,))
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
        try:
            async with ClientSession(headers={"Authorization": "Bearer " + "v" * 48}) as client:
                held = asyncio.create_task(
                    client.get(url + "/api/market/chart?symbol=BTCUSDT&timeframe=15m")
                )
                await asyncio.to_thread(entered.wait, 1)
                async with client.get(
                    url + "/api/market/quotes?symbols=" + sample.symbol
                ) as response:
                    assert response.status == 200
                    body = await response.json()
                    assert body["execution_evidence"] is False
                    assert body["items"][0]["price"] == str(sample.mark_price)
                    assert body["items"][0]["source_at"] == sample.source_at.isoformat()
                release.set()
                await held
        finally:
            release.set()
            await runner.cleanup()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "suffix,status",
    [
        ("", 400),
        ("?symbols=BTCUSDT&symbols=ETHUSDT", 400),
        ("?symbols=" + ",".join("A" + str(i) for i in range(129)), 400),
        ("?symbols=bad-input", 400),
    ],
)
def test_ram_quote_endpoint_bounds_and_auth(monkeypatch, suffix, status):
    async def scenario():
        app = create_app(auth=auth(), market_feed=lambda: ())
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/api/market/quotes"
        try:
            async with ClientSession() as client:
                async with client.get(url + "?symbols=BTCUSDT") as response:
                    assert response.status == 401
                async with client.get(
                    url + suffix, headers={"Authorization": "Bearer " + "v" * 48}
                ) as response:
                    assert response.status == status
        finally:
            await runner.cleanup()

    asyncio.run(scenario())


@pytest.mark.parametrize("transport", ["UNAVAILABLE", "CLOCK_UNSAFE", "RECORD_INVALID"])
def test_retained_ram_quotes_expose_transport_staleness_without_renewing_source(
    monkeypatch, transport
):
    async def scenario():
        state = {"transport": transport}
        async with quote_server(monkeypatch, state=state) as (read, now, quotes, metadata, calls):
            sample = quotes[0]
            row = await read()
            assert row["price"] == str(sample.mark_price)
            assert row["freshness"] == "STALE"
            assert row["reason"] == transport
            assert row["source_at"] == sample.source_at.isoformat()
            assert row["execution_evidence"] is False
            state["transport"] = "OK"
            assert (await read())["freshness"] == "OK"
            assert quotes[0].source_at == sample.source_at

    asyncio.run(scenario())


def test_fresh_mark_does_not_refresh_old_volume_provenance(monkeypatch):
    from tradeforge.domain.market import MarketQuote

    async def scenario():
        async with quote_server(monkeypatch) as (read, now, quotes, metadata, calls):
            payload = quotes[0].model_dump(mode="json")
            for key in ("best_bid", "best_ask", "index_price", "funding", "quality"):
                payload.pop(key)
            payload.update(
                environment="mainnet",
                source="binance:ws-mark",
                volume_source_at=(now[0] - timedelta(seconds=16)).isoformat(),
            )
            quotes[0] = MarketQuote.model_validate_json(__import__("json").dumps(payload))
            row = await read()
            assert row["freshness"] == "OK"
            assert row["price"] == str(quotes[0].mark_price)
            assert row["volume"] is None
            assert row["volume_source_at"] == payload["volume_source_at"]

    asyncio.run(scenario())
