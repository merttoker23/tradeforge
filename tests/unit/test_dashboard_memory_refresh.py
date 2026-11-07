"""Dashboard RAM polling has a bounded cadence and never renews source timestamps."""

import asyncio
from types import SimpleNamespace

import pytest
from aiohttp import web

from tests.unit.test_momentum_alpha import evidence
from tradeforge.runtime import dashboard_memory as module


def run_poll(monkeypatch, *, duration=0.05, outcomes=("OK", "OK")):
    async def scenario():
        real_sleep = asyncio.sleep
        ready = asyncio.Event()
        clock, starts, delays, observed = [0.0], [], [], []
        sample = evidence().market
        quote = {
            k: v
            for k, v in sample.model_dump(mode="json").items()
            if k not in {"best_bid", "best_ask", "index_price", "funding", "quality"}
        }
        quote.update(
            volume_source_at=sample.source_at.isoformat(),
            environment="mainnet",
            source="binance:ws-mark",
        )
        feed = {"quotes": [quote], "clock_safe": True, "eligible": [sample.symbol]}
        captured = {}

        def create_app(*args, **kwargs):
            captured.update(kwargs)
            return web.Application()

        async def fetch(session, token, *, quotes_only=False):
            assert token == "offline-internal-test-token"
            assert quotes_only is True
            starts.append(clock[0])
            clock[0] += duration
            await real_sleep(0)
            outcome = outcomes[len(starts) - 1]
            if outcome == "ERROR":
                raise OSError("offline failure")
            if outcome == "INVALID":
                return {
                    "quotes": [{"symbol": sample.symbol, "not": "a valid quote"}, quote],
                    "clock_safe": True,
                    "eligible": [sample.symbol],
                }
            return feed

        async def sleep(delay):
            delays.append(delay)
            observed.append(
                (captured["market_feed"](), dict(captured.get("market_state", lambda: {})()))
            )
            if len(delays) == len(outcomes):
                ready.set()
                await asyncio.Event().wait()
            clock[0] += delay
            await real_sleep(0)

        monkeypatch.setattr(module, "create_app", create_app)
        monkeypatch.setattr(module, "fetch_memory", fetch)
        monkeypatch.setattr(
            module, "time", SimpleNamespace(monotonic=lambda: clock[0]), raising=False
        )
        monkeypatch.setattr(module.asyncio, "sleep", sleep)
        app = module.dashboard_app(
            None,
            None,
            sessions=None,
            external_origin="https://offline.invalid",
            internal_token="offline-internal-test-token",
        )
        context = app.cleanup_ctx[0](app)
        await anext(context)
        try:
            await asyncio.wait_for(ready.wait(), timeout=2)
        finally:
            await context.aclose()
        return starts, delays, observed, sample

    return asyncio.run(scenario())


@pytest.mark.parametrize("duration,delay", [(0.05, 0.2), (2, 0.1)])
def test_internal_poll_uses_quarter_second_start_to_start_with_no_overlap(
    monkeypatch, duration, delay
):
    starts, delays, observed, sample = run_poll(monkeypatch, duration=duration)
    assert starts == [0, duration + delay]
    assert delays == [delay, delay]
    assert all(rows[0][0].mark_price == sample.mark_price for rows in observed)
    assert all(rows[0][0].source_at == sample.source_at for rows in observed)


@pytest.mark.parametrize("failure", ["ERROR", "INVALID"])
def test_failed_handoff_retains_timestamped_quote_and_next_valid_sample_recovers(
    monkeypatch, failure
):
    starts, delays, observed, sample = run_poll(monkeypatch, outcomes=("OK", failure, "OK"))
    assert all(rows[0][0].source_at == sample.source_at for rows in observed)
    assert observed[1][1]["transport"] == ("UNAVAILABLE" if failure == "ERROR" else "OK")
    assert observed[2][1]["transport"] == "OK"
    assert starts == [0, 0.25, 0.5]
    assert delays == pytest.approx([0.2] * len(delays))


def test_malformed_quote_is_isolated_from_other_symbols_and_invalidity_is_explicit():
    import json

    from tradeforge.domain.market import MarketQuote

    sample = evidence().market
    payload = sample.model_dump(mode="json")
    for key in ("best_bid", "best_ask", "index_price", "funding", "quality"):
        payload.pop(key)
    payload.update(
        environment="mainnet", source="binance:ws-mark", volume=None, volume_source_at=None
    )
    quote = MarketQuote.model_validate_json(json.dumps(payload))
    cache = module.QuoteCache()
    cache.consume(
        {
            "quotes": [payload, {"symbol": "BTCUSDT", "mark_price": "bad"}, None],
            "eligible": [sample.symbol, "BTCUSDT"],
            "clock_safe": True,
        }
    )
    assert cache.quotes == (quote,)
    assert cache.state["transport"] == "OK"
    assert cache.state["invalid_symbols"] == ["BTCUSDT"]
    assert cache.state["invalid_records"] == 2
    cache.unavailable()
    assert cache.quotes == (quote,)
    assert cache.state["transport"] == "UNAVAILABLE"
    cache.consume({"quotes": [], "eligible": [sample.symbol], "clock_safe": False})
    assert cache.quotes == (quote,)
    assert cache.state["transport"] == "CLOCK_UNSAFE"
    cache.consume({"quotes": [payload], "eligible": [sample.symbol], "clock_safe": True})
    assert cache.state["transport"] == "OK"
    assert cache.quotes[0].source_at == quote.source_at
    cache.consume({"quotes": [payload], "eligible": [], "clock_safe": True})
    assert cache.quotes == ()


def test_safe_handoff_omission_retains_last_quote_explicitly_stale():
    import json

    from tradeforge.domain.market import MarketQuote

    sample = evidence().market
    payload = sample.model_dump(mode="json")
    for key in ("best_bid", "best_ask", "index_price", "funding", "quality"):
        payload.pop(key)
    payload.update(
        environment="mainnet", source="binance:ws-mark", volume=None, volume_source_at=None
    )
    quote = MarketQuote.model_validate_json(json.dumps(payload))
    cache = module.QuoteCache()
    cache.consume({"quotes": [payload], "eligible": [sample.symbol], "clock_safe": True})
    cache.consume({"quotes": [], "eligible": [sample.symbol], "clock_safe": True})
    assert cache.quotes == (quote,)
    assert cache.state["missing_symbols"] == [sample.symbol]


def test_quote_handoff_does_not_build_analysis_or_depth_payload():
    from tradeforge.runtime.market_link import memory_payload

    sample = evidence().market

    class DisplayPersistence:
        def display_quotes(self, at):
            assert at == sample.as_of
            return (sample,)

        @property
        def rest_samples(self):
            raise AssertionError("Display polling must not calculate analysis/depth evidence")

    gateway = SimpleNamespace(
        clock=lambda: sample.as_of,
        clock_sync=SimpleNamespace(ready=lambda at, mono: True),
        monotonic=lambda: 0,
        eligible={sample.symbol},
    )
    payload = memory_payload(gateway, DisplayPersistence(), quotes_only=True)
    assert set(payload) == {"as_of", "clock_safe", "eligible", "quotes"}
    assert payload["quotes"][0]["source_at"] == sample.source_at.isoformat().replace("+00:00", "Z")
