"""A full history batch must not perform three round trips per candle."""

from sqlalchemy import event, text

from tests.market_fixtures import bar, instrument
from tradeforge.domain.market import Timeframe
from tradeforge.market.market_state import MarketState
from tradeforge.market.storage import save_view
from tradeforge.persistence.repositories import add_instrument


def test_history_batch_has_bounded_round_trips_and_preserves_every_revision(migrated):
    state = MarketState(64)
    for index in range(33):
        state.ingest_bar(bar(index, delay=(32 - index) * 900))
    view = state.view(instrument(), Timeframe.M15, bar(32).as_of)
    statements = []

    def capture(*args):
        statements.append(args[2])

    event.listen(migrated.sql_engine, "before_cursor_execute", capture)
    try:
        save_view(migrated, view)
    finally:
        event.remove(migrated.sql_engine, "before_cursor_execute", capture)
    assert len(statements) <= 5
    with migrated.transaction() as c:
        assert c.scalar(text("SELECT count(*) FROM closed_bar_observations")) == 33
        assert c.scalar(text("SELECT count(*) FROM closed_bars")) == 33


def test_replay_lookup_does_not_scan_retained_instrument_history(migrated):
    with migrated.transaction() as c:
        identifier = add_instrument(c, instrument())
        c.execute(
            text("""
            INSERT INTO closed_bar_observations
              (instrument_id,timeframe,open_at,close_at,source_at,observed_at,
               payload,input_hash,content_hash)
            SELECT :id,'15m',at,at+interval '899 seconds',at+interval '899 seconds',
                   at+interval '900 seconds'+revision*interval '1 second',
                   payload,encode(sha256(convert_to(payload,'UTF8')),'hex'),
                   encode(sha256(convert_to(payload,'UTF8')),'hex')
            FROM (
              SELECT timestamptz '2026-10-01' + (n/4)*interval '15 minutes' AS at,n%4 AS revision,
                     '{"retained":'||n||'}' AS payload FROM generate_series(1,12000) n
            ) history
            """),
            {"id": identifier},
        )
        c.execute(text("ANALYZE closed_bar_observations"))
    state = MarketState(64)
    for index in range(33):
        state.ingest_bar(bar(index, delay=(32 - index) * 900))
    view = state.view(instrument(), Timeframe.M15, bar(32).as_of)
    lookups = []

    def capture(connection, cursor, statement, parameters, context, executemany):
        if statement.lstrip().startswith("SELECT") and "closed_bar_observations" in statement:
            lookups.append((statement, parameters))

    event.listen(migrated.sql_engine, "before_cursor_execute", capture)
    try:
        save_view(migrated, view)
    finally:
        event.remove(migrated.sql_engine, "before_cursor_execute", capture)
    assert len(lookups) == 2

    def visited_rows(plan):
        own = (
            (
                plan["Actual Rows"]
                + plan.get("Rows Removed by Filter", 0)
                + plan.get("Rows Removed by Index Recheck", 0)
            )
            * plan["Actual Loops"]
            if plan.get("Relation Name") == "closed_bar_observations"
            else 0
        )
        return own + sum(visited_rows(child) for child in plan.get("Plans", ()))

    with migrated.transaction() as c:
        for statement, parameters in lookups:
            plan = c.exec_driver_sql(
                "EXPLAIN (ANALYZE, FORMAT JSON) " + statement, parameters
            ).scalar()
            assert visited_rows(plan[0]["Plan"]) <= 2 * len(view.bars)


def test_history_larger_than_recordset_chunk_is_atomic_and_replayable(migrated):
    state = MarketState(1024)
    for index in range(513):
        state.ingest_bar(bar(index))
    view = state.view(instrument(), Timeframe.M15, bar(512).as_of)
    save_view(migrated, view)
    save_view(migrated, view)
    with migrated.transaction() as c:
        assert c.scalar(text("SELECT count(*) FROM closed_bar_observations")) == 513
        assert c.scalar(text("SELECT count(*) FROM closed_bars")) == 513
