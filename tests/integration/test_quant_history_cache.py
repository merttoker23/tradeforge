"""Bounded point-in-time history reads and revision-aware parsed history reuse."""

import statistics
import time
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import text

from tests.intelligence_fixtures import observation
from tests.universe_fixtures import payload, symbol
from tradeforge.domain.market import ClosedBar, Timeframe
from tradeforge.intelligence.runtime import QuantWorker
from tradeforge.market.bars import bar_hash, canonical
from tradeforge.market.instrument_registry import InstrumentRegistry
from tradeforge.persistence.repositories import add_instrument


def seed(db, count=8, periods=40):
    samples = [observation(periods, "ASSET" + str(i) + "USDT") for i in range(count)]
    now = samples[0].current.as_of
    InstrumentRegistry(db).record(
        payload(*(symbol(s.instrument.symbol) for s in samples)), now, timedelta(hours=1)
    )
    with db.transaction() as connection:
        rows = []
        for sample in samples:
            identifier = add_instrument(connection, sample.instrument)
            for index in range(1, periods + 1):
                bar = observation(index, sample.instrument.symbol).current
                rows.append(
                    {
                        "id": identifier,
                        "tf": "15m",
                        "open": bar.open_at,
                        "close": bar.close_at,
                        "source": bar.source_at,
                        "observed": bar.observed_at,
                        "payload": canonical(bar.model_dump(mode="json")),
                        "hash": bar_hash(bar),
                    }
                )
        connection.execute(
            text("""
            INSERT INTO closed_bar_observations
            (instrument_id,timeframe,open_at,close_at,source_at,observed_at,
             payload,input_hash,content_hash)
            VALUES(:id,:tf,:open,:close,:source,:observed,:payload,:hash,:hash)
        """),
            rows,
        )
    return now, samples


def test_local_history_query_parse_baseline(migrated):
    now, _ = seed(migrated, count=50, periods=128)
    timings, parses = [], []
    for _ in range(3):
        started = time.perf_counter()
        with migrated.transaction() as connection:
            rows = connection.scalars(
                text("""
                SELECT payload FROM (
                    SELECT payload,row_number() OVER (
                        PARTITION BY instrument_id ORDER BY open_at DESC) AS n
                    FROM (SELECT DISTINCT ON(instrument_id,open_at) instrument_id,open_at,payload
                          FROM closed_bar_observations WHERE timeframe=:tf
                            AND observed_at<=:at AND close_at<=:at
                          ORDER BY instrument_id,open_at DESC,observed_at DESC) revisions
                ) bars WHERE n<=33
            """),
                {"tf": "15m", "at": now},
            ).all()
        timings.append((time.perf_counter() - started) * 1000)
        started = time.perf_counter()
        parsed = [ClosedBar.model_validate_json(row, context={"now": now}) for row in rows]
        parses.append((time.perf_counter() - started) * 1000)
    assert len(parsed) == 1650
    print(
        f"BASELINE retained=6400 returned=1650 query_ms={statistics.median(timings):.3f} "
        f"parse_ms={statistics.median(parses):.3f}"
    )
    worker = QuantWorker(migrated, clock=lambda: now)
    worker._frame(now)
    queries, cached_parses = [], []
    for _ in range(3):
        worker._frame(now)
        queries.append(worker.history_metrics["history_query_ms"])
        cached_parses.append(worker.history_metrics["history_parse_ms"])
        assert worker.history_metrics["history_parsed"] == 0
    print(
        f"OPTIMIZED returned=1650 query_ms={statistics.median(queries):.3f} "
        f"parse_ms={statistics.median(cached_parses):.3f}"
    )


def test_unchanged_history_reuses_parsed_rows_and_reports_bounded_work(migrated):
    now, samples = seed(migrated)
    worker = QuantWorker(migrated, clock=lambda: now)
    first = worker.tick()
    assert first["history_rows"] == len(samples) * 33
    assert first["history_parsed"] == len(samples) * 33
    second = worker.tick()
    # Only the short overlap window is re-read; full 33-bar history is not.
    assert 0 < second["history_rows"] <= len(samples)
    assert second["history_parsed"] == 0
    assert second["processed"] == 0
    assert second["tick_ms"] >= second["history_query_ms"] >= 0


def test_late_revision_invalidates_same_period_and_future_revision_waits(migrated):
    from tradeforge.market.market_state import BarView
    from tradeforge.market.storage import save_view

    now, samples = seed(migrated, count=3)
    at = [now]
    worker = QuantWorker(migrated, clock=lambda: at[0])
    assert worker.tick()["processed"] == 1
    original = samples[0].current
    future = now + timedelta(seconds=5)
    revised = ClosedBar.model_validate_json(
        canonical(
            {
                **original.model_dump(mode="json"),
                "event_id": str(uuid4()),
                "observed_at": future.isoformat(),
                "as_of": future.isoformat(),
                "high": str(original.high * 2),
            }
        ),
        context={"now": future},
    )
    save_view(
        migrated,
        BarView(samples[0].instrument, Timeframe.M15, future, (revised,), samples[0].quality),
    )
    assert worker.tick()["processed"] == 0
    assert worker.state["history_parsed"] == 0
    at[0] = future + timedelta(seconds=1)
    assert worker.tick()["processed"] == 1
    assert worker.state["history_parsed"] == 1
    assert worker.detector.latest.frame.source_at == original.close_at
    assert worker.tick()["processed"] == 0


def test_metadata_change_invalidates_cache_and_new_interval_stays_point_in_time(migrated):
    from tradeforge.market.market_state import BarView
    from tradeforge.market.storage import save_view

    now, samples = seed(migrated, count=3)
    at = [now]
    worker = QuantWorker(migrated, clock=lambda: at[0])
    worker.tick()
    at[0] += timedelta(seconds=1)
    InstrumentRegistry(migrated).record(
        payload(*(symbol(s.instrument.symbol) for s in samples)), at[0], timedelta(hours=1)
    )
    assert worker.tick()["history_parsed"] == 99
    assert worker.state["processed"] == 1
    next_sample = observation(41, samples[0].instrument.symbol)
    save_view(
        migrated,
        BarView(
            next_sample.instrument,
            Timeframe.M15,
            next_sample.current.as_of,
            (next_sample.current,),
            next_sample.quality,
        ),
    )
    assert worker.tick()["processed"] == 0
    assert all(bar.observed_at <= at[0] for bar in worker.history)
    at[0] = next_sample.current.as_of
    worker.tick()
    assert worker.state["history_parsed"] == 1
    assert len(worker.history) <= 99
    assert max(bar.close_at for bar in worker.history) == next_sample.current.close_at


def test_revision_invalidation_survives_failed_analysis_persistence(migrated, monkeypatch):
    from tradeforge.market.market_state import BarView
    from tradeforge.market.storage import save_view

    now, samples = seed(migrated, count=3)
    at = [now]
    worker = QuantWorker(migrated, clock=lambda: at[0])
    assert worker.tick()["processed"] == 1
    at[0] += timedelta(seconds=1)
    original = samples[0].current
    revised = ClosedBar.model_validate_json(
        canonical(
            {
                **original.model_dump(mode="json"),
                "event_id": str(uuid4()),
                "observed_at": at[0].isoformat(),
                "as_of": at[0].isoformat(),
                "high": str(original.high * 2),
            }
        ),
        context={"now": at[0]},
    )
    save_view(
        migrated,
        BarView(samples[0].instrument, Timeframe.M15, at[0], (revised,), samples[0].quality),
    )
    persist = worker._persist

    def fail(snapshot):
        raise OSError("offline storage failure")

    monkeypatch.setattr(worker, "_persist", fail)
    assert worker.tick()["processed"] == 0
    monkeypatch.setattr(worker, "_persist", persist)
    assert worker.tick()["processed"] == 1
    assert worker.tick()["processed"] == 0
