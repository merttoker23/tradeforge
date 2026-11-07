"""Use real HTTP fragmentation; one stream.read(n) need not return the full JSON."""

import asyncio
import json

import aiohttp
import pytest
from aiohttp import web

from tradeforge.runtime.market_link import fetch_memory


def fetch_fragmented(
    monkeypatch, payload, *, limit=4_000_000, status=200, split=10, quotes_only=False
):
    async def scenario():
        async def handler(request):
            assert request.headers["Authorization"] == "Bearer offline-internal-test-token"
            response = web.StreamResponse(status=status)
            await response.prepare(request)
            try:
                await response.write(payload[:split])
                await asyncio.sleep(0.02)
                await response.write(payload[split:])
                await response.write_eof()
            except (ConnectionResetError, aiohttp.ClientConnectionError):
                pass  # A rejected/oversized response closes before the final chunk.
            return response

        app = web.Application()
        path = "/quote-state" if quotes_only else "/market-state"
        app.router.add_get(path, handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        monkeypatch.setattr("tradeforge.runtime.market_link.MAX_BODY", limit)
        try:
            async with aiohttp.ClientSession() as client:

                class LocalSession:
                    def get(self, url, **kwargs):
                        assert url == "http://market_gateway:8101" + path
                        assert kwargs["timeout"].total == 3
                        assert kwargs["allow_redirects"] is False
                        return client.get(f"http://127.0.0.1:{port}" + path, **kwargs)

                if quotes_only:
                    return await fetch_memory(
                        LocalSession(), "offline-internal-test-token", quotes_only=True
                    )
                return await fetch_memory(LocalSession(), "offline-internal-test-token")
        finally:
            await runner.cleanup()

    return asyncio.run(scenario())


def test_fragmented_market_handoff_is_read_until_eof_without_changing_source_times(monkeypatch):
    feed = {
        "as_of": "2026-10-25T12:00:00+00:00",
        "status": {"reason": "STREAMS_STALE", "data_ready": False},
        "markets": [{"symbol": "BTCUSDT", "source_at": "2026-10-25T11:59:59+00:00"}],
        "deep": [],
    }
    assert fetch_fragmented(monkeypatch, json.dumps(feed).encode()) == feed


def test_fragmented_oversize_body_is_rejected_even_after_valid_json_prefix(monkeypatch):
    payload = b'{"markets":[]}' + b" " * 200
    with pytest.raises(ValueError, match="MARKET_HANDOFF_CAPACITY"):
        fetch_fragmented(monkeypatch, payload, limit=100, split=len(b'{"markets":[]}'))


def test_exact_size_limit_is_accepted(monkeypatch):
    payload = b'{"markets":[]}'
    assert fetch_fragmented(monkeypatch, payload, limit=len(payload)) == {"markets": []}


@pytest.mark.parametrize("payload", [b'{"markets":', b'{"markets":null}'])
def test_truncated_or_invalid_feed_never_becomes_valid(monkeypatch, payload):
    with pytest.raises(ValueError):
        fetch_fragmented(monkeypatch, payload)


def test_unavailable_response_is_still_rejected(monkeypatch):
    with pytest.raises(ValueError, match="MARKET_HANDOFF_UNAVAILABLE"):
        fetch_fragmented(monkeypatch, b'{"markets":[]}', status=503)


def test_display_handoff_reads_quote_schema_without_analysis_payload(monkeypatch):
    payload = {"quotes": [], "eligible": [], "clock_safe": True}
    assert fetch_fragmented(monkeypatch, json.dumps(payload).encode(), quotes_only=True) == payload
    with pytest.raises(ValueError, match="MARKET_HANDOFF_INVALID"):
        fetch_fragmented(monkeypatch, b'{"markets":[]}', quotes_only=True)


def test_quote_route_retains_internal_auth_and_browser_origin_rejection():
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from tradeforge.runtime.market_link import add_memory_route

    async def scenario():
        now = datetime.now(UTC)
        gateway = SimpleNamespace(
            clock=lambda: now,
            clock_sync=SimpleNamespace(ready=lambda at, mono: True),
            monotonic=lambda: 0,
            eligible=set(),
        )
        persistence = SimpleNamespace(display_quotes=lambda at: ())
        token = "offline-internal-quote-test-token-" + "x" * 16
        app = web.Application()
        add_memory_route(app, gateway, persistence, token)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        base = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
        try:
            async with aiohttp.ClientSession() as client:
                authorized = {"Authorization": "Bearer " + token}
                for path in ("/quote-state", "/market-state"):
                    for headers in (
                        {},
                        {"Authorization": "Bearer wrong"},
                        {**authorized, "Origin": "https://offline.invalid"},
                        {**authorized, "Sec-Fetch-Site": "cross-site"},
                    ):
                        async with client.get(base + path, headers=headers) as response:
                            assert response.status == 403
                async with client.get(base + "/quote-state", headers=authorized) as response:
                    assert response.status == 200
                    assert response.headers["Cache-Control"] == "no-store"
                    assert await response.json() == {
                        "as_of": now.isoformat(),
                        "clock_safe": True,
                        "eligible": [],
                        "quotes": [],
                    }
        finally:
            await runner.cleanup()

    asyncio.run(scenario())


def test_component_provenance_survives_http_memory_frame_and_alpha_boundaries(monkeypatch):
    from datetime import timedelta

    from tests.intelligence_fixtures import engine, frame
    from tests.unit.test_alpha_handoff import feed_for
    from tests.unit.test_market_component_freshness import gateway, rest_rows
    from tests.unit.test_momentum_alpha import change, evidence
    from tradeforge.domain.market import MarketSnapshot, Timeframe
    from tradeforge.intelligence.inputs import MarketFrame
    from tradeforge.market.runtime import GatewayPersistence
    from tradeforge.runtime.market_link import alpha_inputs, memory_payload

    sample = evidence()
    at = sample.as_of
    gw = gateway(at, ("BTCUSDT", "ETHUSDT", "SOLUSDT"))
    persistence = GatewayPersistence(gw, None)
    persistence.rest_samples = tuple(
        persistence.rest_markets(*rest_rows(at, symbol=symbol, volume_age=30), at)[0]
        for symbol in sorted(gw.eligible)
    )
    payload = memory_payload(gw, persistence)
    payload["deep"] = feed_for(sample)["deep"]
    received = fetch_fragmented(monkeypatch, json.dumps(payload).encode(), split=100)
    markets = {
        m["symbol"]: MarketSnapshot.model_validate_json(json.dumps(m)) for m in received["markets"]
    }
    base = frame(31)
    linked = change(
        base,
        observations=tuple(
            change(row, market=markets[row.instrument.symbol]) for row in base.observations
        ),
    )
    parsed = MarketFrame.model_validate_json(linked.model_dump_json())
    result = engine().update(parsed)
    assert dict(result.state.metrics)["stress_coverage"] == 1
    assert all(
        row.market.volume_source_at == at - timedelta(seconds=30) for row in parsed.observations
    )
    assert received["status"]["analysis_coverage_ready"] is True
    assert received["status"]["analysis_volume_covered"] == 0
    monkeypatch.setattr(
        "tradeforge.runtime.market_link.load_bars",
        lambda db, key, tf, at, limit: (
            sample.medium_bars if tf is Timeframe.M15 else sample.setup_bars
        ),
    )
    diagnostics = {}
    assert alpha_inputs(None, sample.features, received, diagnostics=diagnostics) == ()
    assert diagnostics["input_rejections"] == [
        {"symbol": "ETHUSDT", "reason": "VOLUME_UNAVAILABLE"}
    ]
