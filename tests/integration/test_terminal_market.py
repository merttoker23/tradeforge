"""Selected market queries are bounded and preserve real closed candle provenance."""

from datetime import timedelta

import pytest

from tests.intelligence_fixtures import frame
from tests.universe_fixtures import payload, symbol
from tradeforge.dashboard.read_model import ReadModel
from tradeforge.market.instrument_registry import InstrumentRegistry
from tradeforge.market.market_state import BarView
from tradeforge.market.storage import save_view


def test_terminal_search_and_chart_only_return_stored_closed_bars(migrated):
    f = frame(1)
    InstrumentRegistry(migrated).record(
        payload(*(symbol(k.symbol) for k in f.universe.eligible)), f.as_of, timedelta(hours=1)
    )
    observation = f.observations[0]
    save_view(
        migrated,
        BarView(
            observation.instrument,
            f.timeframe,
            f.as_of,
            (observation.previous, observation.current),
            observation.quality,
        ),
    )
    model = ReadModel(migrated, clock=lambda: f.as_of)
    result = model.terminal_symbols(search=observation.instrument.symbol)
    assert result["items"][0]["symbol"] == observation.instrument.symbol
    assert len(result["items"]) <= 50
    chart = model.terminal_chart(observation.instrument.symbol, f.timeframe.value)
    # Persisted bars without verifiable environment provenance must remain UNKNOWN.
    assert chart["environment"] == "UNKNOWN"
    assert chart["execution_evidence"] is False
    assert len(chart["bars"]) == 2
    assert chart["bars"][-1]["close"] == str(observation.current.close)
    assert all(b["time"] <= int(f.as_of.timestamp()) for b in chart["bars"])
    assert model.terminal_chart(observation.instrument.symbol, "4h")["bars"] == []
    with pytest.raises(ValueError):
        model.terminal_chart("x';SELECT", "1m")
    with pytest.raises(ValueError):
        model.terminal_chart(observation.instrument.symbol, "1s")
    with pytest.raises(ValueError):
        model.terminal_symbols(offset=10001)


def test_display_chart_cache_cannot_change_quant_evidence(migrated):
    from sqlalchemy import text

    from tradeforge.market.chart_history import save_chart

    f = frame(1)
    observation = f.observations[0]
    InstrumentRegistry(migrated).record(
        payload(symbol(observation.instrument.symbol)), f.as_of, timedelta(hours=1)
    )
    view = BarView(
        observation.instrument,
        f.timeframe,
        f.as_of,
        (observation.previous, observation.current),
        observation.quality,
    )
    save_chart(migrated, view)
    with migrated.transaction() as c:
        assert c.scalar(text("SELECT count(*) FROM closed_bar_observations")) == 0
        assert c.scalar(text("SELECT count(*) FROM feature_snapshots")) == 0
        assert c.scalar(text("SELECT count(*) FROM terminal_chart_cache")) == 1
    chart = ReadModel(migrated, clock=lambda: f.as_of).terminal_chart(
        observation.instrument.symbol, f.timeframe.value
    )
    assert len(chart["bars"]) == 2


def test_terminal_ranking_uses_current_regime_without_scanning_old_opportunity_history(migrated):
    from uuid import uuid4

    from sqlalchemy import event, text

    from tests.universe_fixtures import NOW

    at = NOW
    InstrumentRegistry(migrated).record(
        payload(symbol("BTCUSDT"), symbol("ETHUSDT")), at, timedelta(hours=1)
    )
    old, current, future = uuid4(), uuid4(), uuid4()
    with migrated.transaction() as c:
        for snapshot_id, observed, version in (
            (old, at - timedelta(minutes=15), "old"),
            (current, at, "current"),
            (future, at + timedelta(minutes=15), "future"),
        ):
            c.execute(
                text(
                    "INSERT INTO regime_snapshots(snapshot_id,input_version,as_of,payload) "
                    "VALUES (:id,:version,:at,'{}')"
                ),
                {"id": snapshot_id, "version": version, "at": observed},
            )
        # An old high score must not rank above a symbol from the current regime.
        for name, regime_id, score, observed in (
            ("BTCUSDT", old, 100, at - timedelta(minutes=15)),
            ("ETHUSDT", current, 20, at),
            ("BTCUSDT", future, 200, at + timedelta(minutes=15)),
        ):
            c.execute(
                text(
                    "INSERT INTO opportunities(opportunity_id,instrument_id,"
                    "regime_snapshot_id,observed_at,payload) "
                    "SELECT :id,instrument_id,:regime,:at,jsonb_build_object('score',:score) "
                    "FROM instruments WHERE symbol=:symbol"
                ),
                {
                    "id": uuid4(),
                    "regime": regime_id,
                    "at": observed,
                    "score": score,
                    "symbol": name,
                },
            )
        c.execute(
            text(
                "INSERT INTO opportunities(opportunity_id,instrument_id,"
                "regime_snapshot_id,observed_at,payload) "
                "SELECT md5('archived-' || n)::uuid,i.instrument_id,:regime,:at,"
                "jsonb_build_object('score',100) FROM instruments i "
                "CROSS JOIN generate_series(1,4096) n WHERE i.symbol='BTCUSDT'"
            ),
            {"regime": old, "at": at - timedelta(minutes=15)},
        )
    captured = []

    def capture(connection, cursor, statement, parameters, context, executemany):
        if "FROM instruments i JOIN members" in statement:
            captured.append((statement, parameters))

    event.listen(migrated.sql_engine, "before_cursor_execute", capture)
    try:
        result = ReadModel(migrated, clock=lambda: at).terminal_symbols()
    finally:
        event.remove(migrated.sql_engine, "before_cursor_execute", capture)
    assert [item["symbol"] for item in result["items"]] == ["ETHUSDT", "BTCUSDT"]
    assert result["items"][0]["score"] == 20
    assert result["items"][1]["score"] is None
    assert (
        ReadModel(migrated, clock=lambda: at).terminal_symbols(search="btc")["items"][0]["symbol"]
        == "BTCUSDT"
    )
    statement, parameters = captured[0]
    with migrated.transaction() as c:
        # Even on a tiny fixture, the emitted bound query must permit the existing index.
        c.execute(text("SET LOCAL enable_seqscan=off"))
        plan = c.exec_driver_sql(
            "EXPLAIN (ANALYZE, FORMAT JSON) " + statement, parameters
        ).scalar_one()
    assert "opportunity_regime" in str(plan)

    def nodes(node):
        yield node
        for child in node.get("Plans", []):
            yield from nodes(child)

    opportunity_reads = [
        node for node in nodes(plan[0]["Plan"]) if node.get("Relation Name") == "opportunities"
    ]
    assert opportunity_reads
    assert all(node["Actual Rows"] <= 1 for node in opportunity_reads)
