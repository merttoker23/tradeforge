"""Validate the fixed SQL against the real disposable migrated PostgreSQL schema."""

import importlib.util
import json
import shutil
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text

HELPER = Path(__file__).resolve().parents[2] / "deploy/diagnostics/production_diagnostics.py"


@pytest.fixture
def diagnostic_sql():
    assert HELPER.is_file(), "fixed readonly aggregation SQL has not been implemented"
    spec = importlib.util.spec_from_file_location("production_diagnostics_sql", HELPER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_fixed_queries_execute_on_migrated_schema_without_writes(migrated, diagnostic_sql):
    with migrated.transaction() as connection:
        connection.execute(text("SET TRANSACTION READ ONLY"))
        result = {}
        for name, query in diagnostic_sql.QUERIES.items():
            rows = connection.execute(text(query)).scalars().all()
            result[name] = rows
        assert result["pipeline_72h"]
        counts = {row["stage"]: row["count"] for row in result["pipeline_72h"]}
        assert counts["execution_intents"] == counts["fills_observed"] == 0
        assert result["history_72h"] == []
        assert result["candidate_latest"] == []


def test_runtime_latest_is_gauge_not_historical_frequency(migrated, diagnostic_sql):
    with migrated.transaction() as connection:
        connection.execute(
            text("""
            INSERT INTO runtime_observations(service,observed_at,expires_at,payload)
            VALUES ('market_gateway',now()-interval '2 seconds',now()+interval '13 seconds',
                CAST(:payload AS jsonb))
        """),
            {
                "payload": json.dumps(
                    {"eligible": 517, "covered": 411, "reason": "LOW_COVERAGE", "secret": "hidden"}
                )
            },
        )
    with migrated.transaction() as connection:
        connection.execute(text("SET TRANSACTION READ ONLY"))
        row = connection.execute(text(diagnostic_sql.QUERIES["runtime_current"])).scalar_one()
        assert row["service"] == "market_gateway"
        assert row["data"]["eligible"] == 517
        assert row["fresh"] is True
        projected = diagnostic_sql.project_database_row("runtime_current", row)
        assert "hidden" not in json.dumps(projected)
        assert projected["data"]["reason"] == "LOW_COVERAGE"


def test_readonly_transaction_rejects_writes(migrated):
    from sqlalchemy.exc import DBAPIError

    with pytest.raises(DBAPIError, match="read-only transaction"):
        with migrated.sql_engine.begin() as connection:
            connection.execute(text("SET TRANSACTION READ ONLY"))
            connection.execute(
                text("INSERT INTO audit_log(event_type,payload) VALUES ('test','{}')")
            )


@pytest.fixture
def readonly_role(migrated):
    setup = (HELPER.parent / "access_setup.sql").read_text()
    with migrated.sql_engine.begin() as connection:
        name = connection.execute(text("SELECT current_database()")).scalar_one()
        connection.exec_driver_sql(setup.replace("ON DATABASE tradeforge", f'ON DATABASE "{name}"'))
    try:
        yield migrated
    finally:
        with migrated.sql_engine.begin() as connection:
            connection.exec_driver_sql("DROP OWNED BY tf_diagnostics")
            connection.exec_driver_sql("DROP ROLE tf_diagnostics")


def test_administrator_setup_role_passes_gate_and_selects_all_fixed_queries(
    readonly_role,
    diagnostic_sql,
):
    with readonly_role.sql_engine.begin() as connection:
        connection.execute(text("SET TRANSACTION READ ONLY"))
        connection.execute(text("SET LOCAL ROLE tf_diagnostics"))
        assert connection.execute(text(diagnostic_sql.ROLE_CHECK)).scalar_one() == 1
        for query in diagnostic_sql.QUERIES.values():
            connection.execute(text(query)).all()


@pytest.mark.parametrize(
    "unsafe",
    [
        "ALTER ROLE tf_diagnostics SUPERUSER",
        "ALTER ROLE tf_diagnostics INHERIT",
        "ALTER ROLE tf_diagnostics CONNECTION LIMIT -1",
        "GRANT pg_read_all_data TO tf_diagnostics",
        "GRANT INSERT ON public.commands TO tf_diagnostics",
        "GRANT CREATE ON SCHEMA public TO tf_diagnostics",
        "GRANT USAGE ON SEQUENCE public.audit_log_audit_id_seq TO tf_diagnostics",
    ],
)
def test_role_gate_rejects_unsafe_privileges(readonly_role, diagnostic_sql, unsafe):
    from sqlalchemy.exc import DBAPIError

    with readonly_role.sql_engine.begin() as connection:
        connection.exec_driver_sql(unsafe)
    with pytest.raises(DBAPIError, match="division by zero"):
        with readonly_role.sql_engine.begin() as connection:
            connection.execute(text("SET TRANSACTION READ ONLY"))
            connection.execute(text("SET LOCAL ROLE tf_diagnostics"))
            connection.execute(text(diagnostic_sql.ROLE_CHECK))


def test_entire_fixed_psql_protocol_works_as_dedicated_role(readonly_role, diagnostic_sql):
    psql = shutil.which("psql")
    assert psql, "psql client is required for the real diagnostic protocol integration test"
    url = readonly_role.sql_engine.url
    result = subprocess.run(
        [
            psql,
            "-X",
            "-w",
            "-q",
            "-A",
            "-t",
            "-v",
            "ON_ERROR_STOP=1",
            "--host",
            url.host,
            "--port",
            str(url.port),
            "--username",
            url.username,
            "--dbname",
            url.database,
            "--command",
            "SET ROLE tf_diagnostics",
            "--command",
            diagnostic_sql.SQL,
        ],
        capture_output=True,
        check=False,
        timeout=15,
        env={
            "PATH": "/usr/bin:/bin",
            "PGPASSFILE": "/dev/null",
            "PGSERVICEFILE": "/dev/null",
            **({"PGPASSWORD": url.password} if url.password else {}),
        },
    )
    assert result.returncode == 0, result.stderr.decode()
    output = diagnostic_sql.database_snapshot(lambda *args, **kwargs: result.stdout, "a" * 64)
    assert output["status"] == "OK"
    assert output["sections"]["pipeline_72h"]["rows"][0]["stage"] == "universe_snapshots"


def test_role_failure_exposes_sqlstate_and_section_only(readonly_role, diagnostic_sql):
    psql = shutil.which("psql")
    assert psql
    url = readonly_role.sql_engine.url
    # Only this disposable test query is replaced; production SQL stays fixed.
    sql = diagnostic_sql.SQL.replace(
        diagnostic_sql.QUERIES["history_72h"],
        "SELECT missing_diagnostic_test_column AS value",
    )
    # The disposable CI server requires SCRAM. Authenticate using its test-only
    # admin URL, then SET ROLE, as in the successful protocol test above. The
    # production helper continues connecting directly as tf_diagnostics only.
    result = subprocess.run(
        [
            psql,
            "-X",
            "-w",
            "-q",
            "-A",
            "-t",
            "-v",
            "ON_ERROR_STOP=1",
            "-v",
            "VERBOSITY=sqlstate",
            "--host=" + str(url.host),
            "--port=" + str(url.port),
            "--username=" + str(url.username),
            "--dbname=" + str(url.database),
            "--command",
            "SET ROLE tf_diagnostics",
            "--command",
            sql,
        ],
        capture_output=True,
        check=False,
        timeout=15,
        env={
            "PATH": "/usr/bin:/bin",
            "PGPASSFILE": "/dev/null",
            "PGSERVICEFILE": "/dev/null",
            **({"PGPASSWORD": url.password} if url.password else {}),
        },
    )
    assert result.returncode != 0
    failure = diagnostic_sql.database_failure(result.stdout, result.stderr)
    assert failure.failure == "DB_SCHEMA_MISMATCH"
    assert failure.failed_section == "history_72h"
    assert "missing_diagnostic_test_column" not in str(failure)


def test_isolated_reads_use_the_same_dedicated_role_and_fixed_counts(readonly_role, diagnostic_sql):
    psql = shutil.which("psql")
    assert psql
    url = readonly_role.sql_engine.url

    def runner(argv, **kwargs):
        result = subprocess.run(
            [
                psql,
                "-X",
                "-w",
                "-q",
                "-A",
                "-t",
                "-v",
                "ON_ERROR_STOP=1",
                "-v",
                "VERBOSITY=sqlstate",
                "--host=" + str(url.host),
                "--port=" + str(url.port),
                "--username=" + str(url.username),
                "--dbname=" + str(url.database),
                "--command",
                "SET ROLE tf_diagnostics",
                "--command",
                argv[-1],
            ],
            capture_output=True,
            timeout=kwargs["timeout"],
            check=False,
            env={
                "PATH": "/usr/bin:/bin",
                "PGPASSFILE": "/dev/null",
                "PGSERVICEFILE": "/dev/null",
                **({"PGPASSWORD": url.password} if url.password else {}),
            },
        )
        if result.returncode:
            raise diagnostic_sql.database_failure(result.stdout, result.stderr)
        return result.stdout

    result = diagnostic_sql.isolated_database_snapshot(runner, "a" * 64)
    assert result["status"] == "OK"
    assert result["read_mode"] == "independent_read_only_transactions"
    pipeline = result["sections"]["pipeline_72h"]["rows"]
    assert len(pipeline) == 9 and all(row["count"] == 0 for row in pipeline)
    assert result["sections"]["risk_current"]["status"] == "OK"
    assert result["sections"]["candidate_transitions_72h"] == {"status": "OK", "rows": []}


def test_history_interior_gap_and_72h_boundary_are_measured(seeded, diagnostic_sql):
    database, _, _ = seeded
    with database.sql_engine.begin() as connection:
        instrument = connection.execute(text("SELECT instrument_id FROM instruments")).scalar_one()
        connection.execute(
            text("""
            INSERT INTO closed_bars(instrument_id,timeframe,open_at,close_at,observed_at,
                                   open,high,low,close,volume,complete)
            SELECT :instrument,'5m',start_at,start_at+interval '5 minutes',
                   start_at+interval '5 minutes',100,100,100,100,1,true
            FROM (SELECT date_trunc('hour',now())-interval '1 hour'+gap AS start_at
                  FROM unnest(ARRAY[interval '0 minutes',interval '5 minutes',
                       interval '15 minutes',interval '-73 hours']) gap) starts
        """),
            {"instrument": instrument},
        )
    with database.sql_engine.begin() as connection:
        connection.execute(text("SET TRANSACTION READ ONLY"))
        row = connection.execute(text(diagnostic_sql.QUERIES["history_72h"])).scalar_one()
        assert row["timeframe"] == "5m"
        assert row["count"] == 3 and row["symbols"] == 1 and row["interior_gaps"] == 1


def test_candidate_transitions_are_separate_from_latest_states(migrated, diagnostic_sql):
    decision = str(uuid4())
    with migrated.sql_engine.begin() as connection:
        for sequence, state in enumerate(("DISCOVERED", "WATCHING")):
            connection.execute(
                text("""
                INSERT INTO decision_lifecycle(sequence,event_id,decision_id,version,state,at,
                                                previous_hash,record_hash,payload)
                VALUES (:sequence,gen_random_uuid(),CAST(:decision AS uuid),:sequence,:state,
                    now()-interval '1 hour',
                    coalesce((SELECT record_hash FROM decision_lifecycle
                              ORDER BY sequence DESC LIMIT 1),repeat('0',64)),
                    encode(sha256(convert_to('{}','UTF8')),'hex'),'{}')
            """),
                {"sequence": sequence, "decision": decision, "state": state},
            )
    with migrated.sql_engine.begin() as connection:
        connection.execute(text("SET TRANSACTION READ ONLY"))
        transitions = (
            connection.execute(text(diagnostic_sql.QUERIES["candidate_transitions_72h"]))
            .scalars()
            .all()
        )
        latest = (
            connection.execute(text(diagnostic_sql.QUERIES["candidate_latest"])).scalars().all()
        )
        assert [(row["state"], row["count"]) for row in transitions] == [
            ("DISCOVERED", 1),
            ("WATCHING", 1),
        ]
        assert [(row["state"], row["count"]) for row in latest] == [("WATCHING", 1)]


def test_revision_aggregate_reduces_duplicate_versions_before_distinct_sort(
    migrated, diagnostic_sql
):
    """Actual EXPLAIN ANALYZE runs only on this disposable database, never production."""
    from tests.market_fixtures import instrument
    from tradeforge.persistence.repositories import add_instrument

    with migrated.transaction() as c:
        for symbol, timeframe in (("BTCUSDT", "15m"), ("ETHUSDT", "5m")):
            identifier = add_instrument(c, instrument(symbol))
            c.execute(
                text("""
                INSERT INTO closed_bar_observations
                  (instrument_id,timeframe,open_at,close_at,source_at,observed_at,payload,input_hash)
                SELECT :id,:tf,opened,opened+interval '5 minutes',opened+interval '5 minutes',
                  now()-interval '2 hours'+revision*interval '1 millisecond','{}',
                  encode(sha256(convert_to('{}','UTF8')),'hex')
                FROM (SELECT now()-interval '1 day'+n*interval '15 minutes' AS opened
                      FROM generate_series(1,3) n) bars
                CROSS JOIN generate_series(1,1000) revision
            """),
                {"id": identifier, "tf": timeframe},
            )
        c.execute(text("ANALYZE closed_bar_observations"))
    with migrated.transaction() as c:
        c.execute(text("SET TRANSACTION READ ONLY"))
        c.execute(text("SET LOCAL work_mem='64kB'"))
        rows = c.execute(text(diagnostic_sql.QUERIES["history_revisions_72h"])).scalars().all()
        assert [(r["timeframe"], r["count"], r["distinct_bars"], r["symbols"]) for r in rows] == [
            ("15m", 3000, 3, 1),
            ("5m", 3000, 3, 1),
        ]
        plan = c.execute(
            text(
                "EXPLAIN (ANALYZE, FORMAT JSON) " + diagnostic_sql.QUERIES["history_revisions_72h"]
            )
        ).scalar_one()

    def sort_rows(node):
        return ([node["Actual Rows"]] if node["Node Type"] == "Sort" else []) + [
            rows for child in node.get("Plans", []) for rows in sort_rows(child)
        ]

    assert max(sort_rows(plan[0]["Plan"]), default=0) <= 6


def test_admission_refusal_is_aggregated_without_exposing_proof_payload(migrated, diagnostic_sql):
    with migrated.transaction() as c:
        c.execute(
            text("""
            INSERT INTO audit_log(event_type,payload) VALUES
              ('runtime.admission_rejected.v1',
               '{"reason_codes":["RISK_REJECTED","FACTOR_UNKNOWN"],"secret":"hidden"}')
        """)
        )
    with migrated.transaction() as c:
        c.execute(text("SET TRANSACTION READ ONLY"))
        rows = c.execute(text(diagnostic_sql.QUERIES["rejections_72h"])).scalars().all()
    assert {(r["source"], r["reason"], r["count"]) for r in rows} == {
        ("admission", "FACTOR_UNKNOWN", 1),
        ("admission", "RISK_REJECTED", 1),
    }
    projected = [diagnostic_sql.project_database_row("rejections_72h", row) for row in rows]
    assert all(row["source"] == "admission" for row in projected)
    assert {row["reason"] for row in projected} == {"FACTOR_UNKNOWN", "RISK_REJECTED"}
    assert "hidden" not in json.dumps(projected)
