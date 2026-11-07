"""Display continuity never grants composite execution evidence."""

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from tests.unit.test_gateway_streams import NOW, mark, ticker
from tests.universe_fixtures import payload, symbol
from tradeforge.market.clock_sync import ClockSync
from tradeforge.market.exchange_info import parse_exchange_info
from tradeforge.market.instrument_registry import RegistryView
from tradeforge.market.market_gateway import MarketGateway
from tradeforge.market.runtime import GatewayPersistence
from tradeforge.market.socket_manager import WireFrame
from tradeforge.market.streams import GLOBAL_STREAMS, StreamSpec, decode_events
from tradeforge.runtime.market_link import memory_payload


def gateway_fixture():
    at, mono = [NOW], [1.0]
    sync = ClockSync()
    sync.observe(NOW, NOW, NOW, 1)
    gateway = MarketGateway(sync, clock=lambda: at[0], monotonic=lambda: mono[0])
    metadata = parse_exchange_info(payload(symbol(), symbol("ETHUSDT")), NOW)
    view = RegistryView(
        SimpleNamespace(
            metadata=metadata,
            observed_at=NOW,
            expires_at=NOW + timedelta(hours=1),
            version_hash="a" * 64,
        ),
        True,
        "OK",
    )
    gateway.update_universe(view)
    gateway.socket_state(tuple(StreamSpec(s) for s in GLOBAL_STREAMS), "SYNCING", 1)
    return gateway, GatewayPersistence(gateway, None), at, mono, view


def feed(gateway, at, stream, rows, generation=1):
    events = decode_events(
        json.dumps({"stream": stream, "data": rows}),
        route="market",
        received_at=at,
        generation=generation,
        sequence=1,
        eligible=gateway.eligible,
    )
    return [gateway.ingest(event) for event in events]


def test_fresh_mark_survives_stale_ticker_and_book_without_execution_snapshot():
    gateway, worker, at, mono, _ = gateway_fixture()
    feed(gateway, at[0], GLOBAL_STREAMS[0], [ticker()])
    at[0] += timedelta(seconds=16)
    mono[0] += 16
    feed(
        gateway,
        at[0],
        GLOBAL_STREAMS[1],
        [mark(E=int(at[0].timestamp() * 1000), T=int(at[0].timestamp() * 1000) + 1000)],
    )
    body = memory_payload(gateway, worker)
    assert body["markets"] == []
    assert len(body.get("quotes", [])) == 1
    quote = body["quotes"][0]
    assert datetime.fromisoformat(quote["source_at"]) == at[0]
    assert datetime.fromisoformat(quote["volume_source_at"]) == NOW
    assert quote["volume"] == "10"
    assert body["clock_safe"] is True and body["eligible"] == ["BTCUSDT", "ETHUSDT"]
    gateway.clock_sync.invalidate()
    assert memory_payload(gateway, worker)["quotes"] == []


def test_changed_only_ticker_coverage_does_not_block_a_ready_symbol():
    gateway, _, at, _, _ = gateway_fixture()
    feed(gateway, at[0], GLOBAL_STREAMS[0], [ticker()])
    feed(gateway, at[0], GLOBAL_STREAMS[1], [mark(), mark("ETHUSDT")])
    status = gateway.status()
    assert status["covered"] == 1 and status["coverage"] == 0.5
    assert status["data_ready"] is True
    assert status.get("transport_ready") is True
    assert status.get("analysis_coverage_ready") is False
    assert gateway.symbol_ready("BTCUSDT") is True
    assert gateway.symbol_ready("ETHUSDT") is False
    assert status["trading_ready"] is False


def test_watch_replan_preserves_global_evidence_and_generations():
    gateway, worker, at, _, view = gateway_fixture()
    feed(gateway, at[0], GLOBAL_STREAMS[0], [ticker()])
    feed(gateway, at[0], GLOBAL_STREAMS[1], [mark()])
    gateway.update_universe(view, watched=("BTCUSDT",))
    assert gateway._generations[GLOBAL_STREAMS[1]] == 1
    assert len(memory_payload(gateway, worker).get("quotes", [])) == 1
    gateway.socket_state((StreamSpec(GLOBAL_STREAMS[1]),), "SYNCING", 2)
    assert not any(feed(gateway, at[0], GLOBAL_STREAMS[1], [mark()], generation=1))
    assert memory_payload(gateway, worker)["quotes"] == []


def test_slow_clock_probe_retains_good_sample_without_extending_its_ttl():
    sync = ClockSync()
    sync.observe(NOW, NOW, NOW, 1)
    sync.observe(
        NOW + timedelta(seconds=10), NOW + timedelta(seconds=12), NOW + timedelta(seconds=11), 13
    )
    assert sync.ready(NOW + timedelta(seconds=12), 13)
    assert sync.sample_at == NOW and sync.sample_mono == 1
    assert not sync.ready(NOW + timedelta(seconds=61), 62)
    sync.observe(
        NOW + timedelta(seconds=12),
        NOW + timedelta(seconds=12.1),
        NOW + timedelta(seconds=15),
        13.1,
    )
    assert not sync.ready(NOW + timedelta(seconds=12.1), 13.1)


def test_rest_mark_quote_does_not_require_fresh_book_or_ticker():
    gateway, worker, at, _, _ = gateway_fixture()
    stamp = int(at[0].timestamp() * 1000)
    marks = [{"symbol": "BTCUSDT", "markPrice": "101", "time": stamp}]
    tickers = [{"symbol": "BTCUSDT", "volume": "4", "closeTime": stamp - 60000}]
    quotes = worker.rest_quotes(marks, tickers, at[0])
    assert len(quotes) == 1 and quotes[0].mark_price == 101
    assert quotes[0].volume_source_at == NOW - timedelta(seconds=60)
    assert worker.rest_markets([], marks, tickers, at[0]) == ()
    marks[0]["time"] = stamp + 1
    assert worker.rest_quotes(marks, tickers, at[0]) == ()


def test_watch_selection_change_keeps_global_socket_open(monkeypatch):
    gateway, _, _, _, view = gateway_fixture()
    opens = []
    global_generation = []
    stop = asyncio.Event()

    class Refresher:
        calls = 0

        def tick(self):
            self.calls += 1
            return view

    refresher = Refresher()

    class Socket:
        async def send_json(self, command):
            self.command = command
            opens.append(tuple(command["params"]))
            if command["params"] == list(GLOBAL_STREAMS):
                global_generation.append(command["id"])

        async def receive(self):
            await asyncio.sleep(0.001)
            return WireFrame("TEXT", json.dumps({"id": self.command["id"], "result": None}))

        async def pong(self, value):
            pass

    class Connector:
        async def server_time(self):
            return NOW

        @asynccontextmanager
        async def open(self, route):
            yield Socket()

    async def paused(event, seconds):
        await asyncio.sleep(0.005)
        if refresher.calls >= 3:
            stop.set()

    monkeypatch.setattr("tradeforge.market.market_gateway.pause", paused)

    async def scenario():
        await asyncio.wait_for(
            gateway.run(
                refresher,
                Connector(),
                stop,
                selection=lambda: (("BTCUSDT",) if refresher.calls < 2 else ("ETHUSDT",), (), ()),
            ),
            3,
        )

    asyncio.run(scenario())
    assert len(global_generation) == 1
    assert sum(GLOBAL_STREAMS[0] in group for group in opens) == 1
    assert any("ethusdt@kline_1m" in group for group in opens)


def test_fresh_websocket_marks_and_costs_need_no_rest_requests():
    gateway, worker, at, _, _ = gateway_fixture()
    feed(gateway, at[0], GLOBAL_STREAMS[1], [mark(), mark("ETHUSDT")])
    worker.rest_samples = tuple(
        SimpleNamespace(symbol=s, source_at=at[0]) for s in gateway.eligible
    )

    class Client:
        async def fetch(self, *args):
            raise AssertionError("Fresh WS marks must not cause REST fallback")

    assert asyncio.run(worker.quote_once(Client())) is False


def test_mark_only_rest_fallback_avoids_book_and_ticker_queries():
    gateway, worker, at, _, _ = gateway_fixture()
    stamp = int(at[0].timestamp() * 1000)
    worker.rest_samples = worker.rest_markets(
        [
            {"symbol": s, "bidPrice": "100", "askPrice": "101", "time": stamp}
            for s in gateway.eligible
        ],
        [
            {
                "symbol": s,
                "markPrice": "100.5",
                "indexPrice": "100.4",
                "lastFundingRate": "0.0001",
                "time": stamp,
            }
            for s in gateway.eligible
        ],
        [{"symbol": s, "volume": "123", "closeTime": stamp} for s in gateway.eligible],
        at[0],
    )
    paths = []

    class Client:
        async def fetch(self, path, params):
            paths.append(path)
            return [
                {"symbol": "BTCUSDT", "markPrice": "101", "time": int(at[0].timestamp() * 1000)}
            ], at[0]

    assert asyncio.run(worker.quote_once(Client()))
    assert paths == ["/fapi/v1/premiumIndex"]
    assert memory_payload(gateway, worker)["quotes"][0]["mark_price"] == "101"


def test_public_rest_worker_reuses_and_closes_a_single_credential_free_session(monkeypatch):
    from tradeforge.market.recovery import PublicMarketClient

    sessions = []

    class Session:
        def __init__(self, **kwargs):
            assert kwargs["trust_env"] is False
            sessions.append(self)
            self.closed = False

        @asynccontextmanager
        async def get(self, url, **kwargs):
            assert url == "https://fapi.binance.com/fapi/v1/premiumIndex"
            assert kwargs["allow_redirects"] is False

            async def chunks(size):
                yield b"[]"

            yield SimpleNamespace(
                status=200, headers={}, content=SimpleNamespace(iter_chunked=chunks)
            )

        async def close(self):
            self.closed = True

    monkeypatch.setattr("tradeforge.market.recovery.aiohttp.ClientSession", Session)

    async def scenario():
        async with PublicMarketClient(min_interval=0) as client:
            assert (await client.fetch("/fapi/v1/premiumIndex", {}))[0] == []
            assert (await client.fetch("/fapi/v1/premiumIndex", {}))[0] == []
        assert len(sessions) == 1 and sessions[0].closed

    asyncio.run(scenario())


@pytest.mark.parametrize("durations,attempts,ready", [([1.2, 0.2], 2, True), ([1.2], 3, False)])
def test_clock_resampling_is_rapid_bounded_and_never_extends_bad_evidence(
    monkeypatch, durations, attempts, ready
):
    gateway, _, at, mono, view = gateway_fixture()
    gateway.clock_sync.invalidate()
    stop = asyncio.Event()
    delays = []

    class Connector:
        calls = 0

        async def server_time(self):
            duration = durations[min(self.calls, len(durations) - 1)]
            self.calls += 1
            midpoint = at[0] + timedelta(seconds=duration / 2)
            at[0] += timedelta(seconds=duration)
            mono[0] += duration
            return midpoint

        @asynccontextmanager
        async def open(self, route):
            await stop.wait()
            yield None

    async def paused(event, seconds):
        delays.append(seconds)
        if seconds >= 30:
            stop.set()
        await asyncio.sleep(0)

    monkeypatch.setattr("tradeforge.market.market_gateway.pause", paused)
    connector = Connector()
    asyncio.run(gateway.run(SimpleNamespace(tick=lambda: view), connector, stop))
    assert connector.calls == attempts
    assert gateway.clock_sync.ready(at[0], mono[0]) is ready
    assert sum(0.1 < delay < 1 for delay in delays) == attempts - 1


@pytest.mark.parametrize("version_changed", [False, True])
def test_rest_recovery_survives_equivalent_metadata_reload_but_fences_new_version(version_changed):
    gateway, worker, at, _, view = gateway_fixture()

    class Client:
        async def fetch(self, path, params):
            previous = view.observation
            replacement = SimpleNamespace(**vars(previous))
            replacement.version_hash = "b" * 64 if version_changed else previous.version_hash
            gateway.view = RegistryView(replacement, True, "OK")
            return [
                {"symbol": "BTCUSDT", "markPrice": "101", "time": int(at[0].timestamp() * 1000)}
            ], at[0]

    result = asyncio.run(worker.quote_once(Client()))
    assert result is not version_changed
    assert bool(worker.rest_quote_samples) is not version_changed


def test_rest_quote_transport_failure_has_safe_reason_counter_without_secret_text(monkeypatch):
    gateway, worker, _, _, _ = gateway_fixture()
    stop = asyncio.Event()

    async def failed(client):
        raise OSError("do-not-include-exception-transport-contents")

    async def paused(event, seconds):
        stop.set()

    monkeypatch.setattr(worker, "quote_once", failed)
    monkeypatch.setattr("tradeforge.market.runtime.pause", paused)
    asyncio.run(worker.quotes(stop))
    status = gateway.status()
    assert status.get("quote_failures") == 1
    assert status.get("quote_last_reason") == "QUOTE_UNAVAILABLE"
    assert "do-not-include" not in json.dumps(status)
