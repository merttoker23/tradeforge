"""PHASE_16A real PostgreSQL contracts; no public network or order credentials."""

from datetime import UTC, datetime, timedelta

from sqlalchemy import text

from tests.universe_fixtures import payload, symbol
from tradeforge.dashboard.read_model import ReadModel, refresh_freshness
from tradeforge.market.clock_sync import ClockSync
from tradeforge.market.instrument_registry import InstrumentRegistry
from tradeforge.market.market_gateway import MarketGateway


def test_stream_coverage_requires_current_measured_matching_universe(migrated):
    from tradeforge.runtime.storage import save_runtime

    now = datetime.now(UTC) - timedelta(seconds=2)
    registry = InstrumentRegistry(migrated)
    observation = registry.record(payload(symbol()), now, timedelta(hours=1))
    base = dict(
        eligible=1,
        covered=1,
        measured=True,
        clock_safe=True,
        universe_version=observation.version_hash,
        reason="OK",
        data_ready=True,
        coverage_deadlines=[(now + timedelta(seconds=15)).isoformat()],
    )
    assert (
        ReadModel(migrated, clock=lambda: now).snapshot()["sections"]["streams"]["status"]
        == "UNKNOWN"
    )
    save_runtime(migrated, "market_gateway", now, now + timedelta(seconds=15), base)
    result = ReadModel(migrated, clock=lambda: now).snapshot()
    assert result["sections"]["universe"]["data"][0]["stream_coverage"] == 1.0
    assert result["sections"]["streams"]["status"] == "OK"
    assert result["trading_authority"]["status"] == "NOT_READY"
    assert result["sections"]["risk"]["status"] == "UNKNOWN"
    assert result["sections"]["guardian"]["status"] == "UNKNOWN"
    aged = refresh_freshness(result, now + timedelta(seconds=16))
    assert aged["sections"]["streams"]["status"] == "STALE"
    assert aged["sections"]["universe"]["data"][0]["stream_coverage"] is None
    save_runtime(
        migrated,
        "market_gateway",
        now + timedelta(milliseconds=1),
        now + timedelta(seconds=15),
        {**base, "covered": 0, "coverage_deadlines": []},
    )
    partial = ReadModel(migrated, clock=lambda: now + timedelta(seconds=1)).snapshot()
    assert partial["sections"]["streams"]["status"] == "DEGRADED"
    assert partial["sections"]["universe"]["data"][0]["stream_coverage"] == 0.0
    registry.record(
        payload(symbol(), symbol("ETHUSDT")), now + timedelta(seconds=1), timedelta(hours=1)
    )
    changed = ReadModel(migrated, clock=lambda: now + timedelta(seconds=2)).snapshot()
    assert changed["sections"]["universe"]["data"][0]["stream_coverage"] is None


def test_gateway_telemetry_does_not_turn_heartbeat_into_measurement(migrated):
    now = datetime.now(UTC) - timedelta(seconds=1)
    sync = ClockSync()
    sync.observe(now, now, now, 1)
    registry = InstrumentRegistry(migrated)
    registry.record(payload(symbol()), now, timedelta(hours=1))
    gateway = MarketGateway(sync, clock=lambda: now, monotonic=lambda: 1)
    gateway.update_universe(registry.view(now))
    sample, deadline = gateway.telemetry()
    assert sample["measured"] is False
    assert sample["covered"] == 0
    assert deadline <= now + timedelta(seconds=15)


def test_gateway_reports_actual_subscription_tiers_instead_of_registry_defaults(migrated):
    now = datetime.now(UTC) - timedelta(seconds=1)
    sync = ClockSync()
    sync.observe(now, now, now, 1)
    registry = InstrumentRegistry(migrated)
    registry.record(payload(symbol(), symbol("ETHUSDT")), now, timedelta(hours=1))
    gateway = MarketGateway(sync, clock=lambda: now, monotonic=lambda: 1)
    gateway.update_universe(registry.view(now), watched=("BTCUSDT",), deep=("ETHUSDT",))
    sample, _ = gateway.telemetry()
    assert sample["watched"] == ["BTCUSDT"]
    assert sample["deep"] == ["ETHUSDT"]
    assert sample["protected"] == []
    assert sample["measured"] is False


def test_coverage_partial_delay_disconnect_and_reconnect(migrated):
    import json

    from tradeforge.market.streams import GLOBAL_STREAMS, StreamSpec, decode_events
    from tradeforge.runtime.storage import save_runtime

    now = datetime.now(UTC) - timedelta(seconds=25)
    mono = [1.0]
    sync = ClockSync()
    sync.observe(now, now, now, mono[0])
    registry = InstrumentRegistry(migrated)
    registry.record(payload(symbol(), symbol("ETHUSDT")), now, timedelta(hours=1))
    gateway = MarketGateway(sync, clock=lambda: now, monotonic=lambda: mono[0])
    gateway.update_universe(registry.view(now))
    streams = tuple(StreamSpec(name) for name in GLOBAL_STREAMS)
    gateway.socket_state(streams, "SYNCING", 1)

    def feed(member, generation=1):
        for name in GLOBAL_STREAMS:
            row = {"s": member, "E": int(now.timestamp() * 1000)}
            row.update(
                dict(e="24hrTicker", c="100", o="99", h="101", l="98", v="1", q="100", P="1")
                if name == GLOBAL_STREAMS[0]
                else dict(
                    e="markPriceUpdate",
                    p="100",
                    i="100",
                    r="0.001",
                    T=int(now.timestamp() * 1000) + 1000,
                )
            )
            for event in decode_events(
                json.dumps({"stream": name, "data": [row]}),
                route="market",
                received_at=now,
                generation=generation,
                sequence=1,
                eligible=gateway.eligible,
            ):
                gateway.ingest(event)

    feed("BTCUSDT")
    feed("ETHUSDT")
    now += timedelta(seconds=20)
    mono[0] += 20
    feed("BTCUSDT")
    sample, deadline = gateway.telemetry()
    assert sample["covered"] == 1 and deadline > now
    save_runtime(migrated, "market_gateway", now, deadline, sample)
    result = ReadModel(migrated, clock=lambda: now).snapshot()
    assert result["sections"]["streams"]["status"] == "DEGRADED"
    assert result["sections"]["universe"]["data"][0]["stream_coverage"] == 0.5
    gateway.socket_state(streams, "STALE", 1)
    assert gateway.telemetry()[0]["measured"] is False
    gateway.socket_state(streams, "SYNCING", 2)
    feed("BTCUSDT", 1)
    assert gateway.telemetry()[0]["measured"] is False
    feed("BTCUSDT", 2)
    feed("ETHUSDT", 2)
    assert gateway.telemetry()[0]["covered"] == 2


def test_quant_consumes_closed_bars_and_restart_does_not_duplicate(migrated):
    from tests.intelligence_fixtures import frame
    from tradeforge.intelligence.runtime import QuantWorker
    from tradeforge.market.market_state import BarView
    from tradeforge.market.storage import save_view

    f = frame(1)
    now = f.as_of
    # Use the frame's real instrument identities, not a separate discovery list.
    registry = InstrumentRegistry(migrated)
    registry.record(
        payload(*(symbol(k.symbol) for k in f.universe.eligible)), now, timedelta(hours=1)
    )
    for row in f.observations:
        save_view(
            migrated,
            BarView(row.instrument, f.timeframe, now, (row.previous, row.current), row.quality),
        )
    worker = QuantWorker(migrated, clock=lambda: now)
    assert worker.tick()["processed"] == 1
    assert worker.tick()["processed"] == 0
    assert QuantWorker(migrated, clock=lambda: now).tick()["processed"] == 0
    with migrated.transaction() as c:
        assert c.scalar(text("SELECT count(*) FROM regime_snapshots")) == 1
        assert c.scalar(text("SELECT count(*) FROM opportunities")) == 3
        assert c.scalar(text("SELECT count(*) FROM execution_intents")) == 0
    view = ReadModel(migrated, clock=lambda: now).snapshot()
    assert view["sections"]["opportunities"]["data"]
    assert view["sections"]["regime"]["status"] == "OK"
    assert view["sections"]["regime"]["data"][0]["label"] == "UNKNOWN"
    # A running worker cannot renew the timestamp of a completed analysis.
    late = QuantWorker(migrated, clock=lambda: now + timedelta(hours=1)).tick()
    assert late["data_ready"] is False


def test_quant_empty_database_is_alive_but_not_ready(migrated):
    from tradeforge.intelligence.runtime import QuantWorker

    state = QuantWorker(migrated).tick()
    assert state["alive"] is True and state["data_ready"] is False
    assert state["reason"] == "UNIVERSE_MISSING"


def test_quant_production_requires_gateway_clock_and_heartbeat(migrated):
    from tradeforge.intelligence.runtime import QuantWorker
    from tradeforge.runtime.storage import save_runtime

    now = datetime.now(UTC) - timedelta(seconds=1)
    InstrumentRegistry(migrated).record(payload(symbol()), now, timedelta(hours=1))
    worker = QuantWorker(migrated, clock=lambda: now, require_gateway=True)
    assert worker.tick()["reason"] == "GATEWAY_TELEMETRY_UNSAFE"
    save_runtime(
        migrated,
        "market_gateway",
        now,
        now + timedelta(seconds=15),
        {"clock_safe": False, "history_complete": True},
    )
    assert worker.tick()["reason"] == "GATEWAY_TELEMETRY_UNSAFE"


def test_quant_database_disconnect_and_recovery(migrated, monkeypatch):
    from tradeforge.intelligence.runtime import QuantWorker

    worker = QuantWorker(migrated)
    original = worker._load

    def disconnected(now):
        raise OSError("PASSWORD_SENTINEL")

    monkeypatch.setattr(worker, "_load", disconnected)
    assert worker.tick()["reason"] == "QUANT_STORAGE_OR_INPUT_UNAVAILABLE"
    assert "PASSWORD_SENTINEL" not in str(worker.state)
    monkeypatch.setattr(worker, "_load", original)
    assert worker.tick()["reason"] == "UNIVERSE_MISSING"


def test_cached_coverage_ages_per_symbol_without_expiring_service_heartbeat(migrated):
    from tradeforge.runtime.storage import save_runtime

    now = datetime.now(UTC) - timedelta(seconds=1)
    observation = InstrumentRegistry(migrated).record(
        payload(symbol(), symbol("ETHUSDT")), now, timedelta(hours=1)
    )
    sample = dict(
        eligible=2,
        covered=2,
        measured=True,
        clock_safe=True,
        universe_version=observation.version_hash,
        reason="OK",
        coverage_deadlines=[(now + timedelta(seconds=s)).isoformat() for s in (2, 10)],
    )
    save_runtime(migrated, "market_gateway", now, now + timedelta(seconds=15), sample)
    result = ReadModel(migrated, clock=lambda: now).snapshot()
    assert result["sections"]["streams"]["status"] == "OK"
    aged = refresh_freshness(result, now + timedelta(seconds=3))
    assert aged["sections"]["streams"]["status"] == "DEGRADED"
    assert aged["sections"]["universe"]["data"][0]["stream_coverage"] == 0.5
    assert aged["service_health"]["market_gateway"] == "OK"
