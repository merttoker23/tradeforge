"""Database-enforced invariants and transactional migration behavior."""

from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError


def test_migration_up_down_up_and_transactional_failure(database_url):
    from tradeforge.persistence.db import Database
    from tradeforge.persistence.transactions import migrate

    db = Database(database_url)
    try:
        migrate(db, "head")
        with db.sql_engine.connect() as connection:
            names = set(inspect(connection).get_table_names())
            assert {
                "instruments",
                "universe_snapshots",
                "closed_bars",
                "feature_snapshots",
                "regime_snapshots",
                "opportunities",
                "decisions",
                "decision_reasons",
                "execution_intents",
                "outbox",
                "orders",
                "fills",
                "positions",
                "protective_orders",
                "account_snapshots",
                "risk_snapshots",
                "risk_state",
                "commands",
                "audit_log",
                "consumer_receipts",
                "schema_migrations",
            } <= names
            assert connection.scalar(text("SELECT version_num FROM schema_migrations")) == "0009"
        migrate(db, "base")
        with db.sql_engine.connect() as connection:
            assert "orders" not in inspect(connection).get_table_names()
        # A failed DDL transaction cannot leave half of an order schema behind.
        with pytest.raises(DBAPIError):
            with db.sql_engine.begin() as connection:
                connection.execute(text("CREATE TABLE migration_probe (id integer)"))
                connection.execute(text("SELECT 1/0"))
        with db.sql_engine.connect() as connection:
            assert "migration_probe" not in inspect(connection).get_table_names()
        migrate(db, "head")
        assert db.probe().db_writable
    finally:
        db.close()


def test_snapshot_hash_immutable_and_evidence_append_only(seeded):
    from tradeforge.persistence.db import PersistenceError

    db, decision, _ = seeded
    with db.transaction() as connection:
        row = connection.execute(text("SELECT snapshot, snapshot_hash FROM decisions")).one()
        import hashlib

        assert hashlib.sha256(row.snapshot.encode()).hexdigest() == row.snapshot_hash
        assert len(row.snapshot_hash) == 64
    for statement in [
        "UPDATE decisions SET snapshot='{}'",
        "DELETE FROM decisions",
        "TRUNCATE decisions CASCADE",
        "UPDATE decision_reasons SET reason_code='NO_TRADE'",
        "DELETE FROM decision_reasons",
        "TRUNCATE decision_reasons",
        "DELETE FROM audit_log",
        "TRUNCATE audit_log",
    ]:
        with pytest.raises(PersistenceError):
            with db.transaction() as connection:
                connection.execute(text(statement))
    with db.transaction() as connection:
        assert connection.scalar(text("SELECT count(*) FROM decisions")) == 1
        assert connection.scalar(text("SELECT count(*) FROM decision_reasons")) == 1


def test_foreign_keys_precision_utc_and_cas(seeded):
    from tradeforge.persistence.db import PersistenceError
    from tradeforge.persistence.repositories import advance_order, enqueue_intent

    db, decision, intent = seeded
    enqueue_intent(db, intent)
    with db.transaction() as connection:
        row = connection.execute(text("SELECT qty, price, created_at FROM execution_intents")).one()
        assert row.qty == Decimal("1.000000000000000123")
        assert row.price == Decimal("100.000000000000000001")
        assert row.created_at.utcoffset().total_seconds() == 0
        assert connection.scalar(text("SHOW timezone")) == "UTC"
        connection.execute(
            text(
                "INSERT INTO orders (intent_id, client_order_id, qty) VALUES (:id, :client, :qty)"
            ),
            {"id": intent.intent_id, "client": intent.client_order_id, "qty": intent.qty},
        )
    assert advance_order(db, intent.intent_id, 0, "ACK_UNKNOWN")
    assert not advance_order(db, intent.intent_id, 0, "ACCEPTED")
    with pytest.raises(PersistenceError):
        with db.transaction() as connection:
            connection.execute(
                text(
                    "INSERT INTO orders (intent_id, client_order_id, qty) VALUES (:id, 'orphan', 1)"
                ),
                {"id": uuid4()},
            )
    with pytest.raises(PersistenceError):
        with db.transaction() as connection:
            connection.execute(text("UPDATE orders SET version=8"))
    with db.transaction() as connection:
        assert connection.scalar(text("SELECT version FROM orders")) == 1


def test_snapshot_checksum_rejected_by_database(seeded):
    from tradeforge.persistence.db import PersistenceError

    db, decision, _ = seeded
    with pytest.raises(PersistenceError):
        with db.transaction() as connection:
            connection.execute(
                text(
                    "INSERT INTO decisions "
                    "(decision_id, instrument_id, side, state, expires_at, snapshot, "
                    "snapshot_hash) SELECT :id, instrument_id, side, state, expires_at, "
                    "snapshot, :hash FROM decisions"
                ),
                {"id": uuid4(), "hash": "0" * 64},
            )


def test_order_client_identity_cannot_reference_a_different_intent(seeded):
    from tradeforge.persistence.db import PersistenceError
    from tradeforge.persistence.repositories import enqueue_intent

    db, _, intent = seeded
    enqueue_intent(db, intent)
    changed = intent.model_dump()
    changed.update(intent_id=uuid4(), client_order_id="tf_" + uuid4().hex)
    other = type(intent).model_validate(changed)
    enqueue_intent(db, other)
    with pytest.raises(PersistenceError):
        with db.transaction() as connection:
            connection.execute(
                text(
                    "INSERT INTO orders (intent_id, client_order_id, qty) VALUES (:id, :client, 1)"
                ),
                {"id": intent.intent_id, "client": other.client_order_id},
            )


def test_failed_alembic_revision_leaves_no_partial_schema(database_url, tmp_path):
    import shutil
    from pathlib import Path

    from alembic import command
    from alembic.config import Config
    from sqlalchemy import create_engine

    root = Path(__file__).resolve().parents[2]
    shutil.copytree(root / "migrations", tmp_path / "migrations")
    sql = tmp_path / "migrations/versions/0001_durable_state.sql"
    sql.write_text(sql.read_text() + "\nSELECT 1/0;\n")
    config = Config()
    config.set_main_option("script_location", str(tmp_path / "migrations"))
    migration_engine = create_engine(database_url)
    try:
        with pytest.raises(DBAPIError):
            with migration_engine.begin() as connection:
                config.attributes["connection"] = connection
                command.upgrade(config, "head")
        with migration_engine.connect() as connection:
            assert inspect(connection).get_table_names() == []
    finally:
        migration_engine.dispose()
