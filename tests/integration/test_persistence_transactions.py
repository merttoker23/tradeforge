"""Atomic writes, competing writers, crashes and at-least-once transport."""

import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from sqlalchemy import text


def test_intent_and_outbox_atomic_and_unique(seeded):
    from tradeforge.persistence.db import PersistenceError
    from tradeforge.persistence.repositories import enqueue_intent

    db, _, intent = seeded
    enqueue_intent(db, intent)
    with pytest.raises(PersistenceError):
        enqueue_intent(db, intent)
    another_id = intent.model_dump()
    another_id["intent_id"] = uuid4()
    with pytest.raises(PersistenceError):
        enqueue_intent(db, type(intent).model_validate(another_id))
    with db.transaction() as connection:
        assert connection.scalar(text("SELECT count(*) FROM execution_intents")) == 1
        assert (
            connection.scalar(text("SELECT count(*) FROM outbox WHERE delivered_at IS NULL")) == 1
        )
        assert connection.scalar(text("SELECT count(*) FROM orders")) == 0


def test_failed_outbox_insert_rolls_back_intent(seeded):
    from tradeforge.persistence.db import PersistenceError
    from tradeforge.persistence.repositories import enqueue_intent

    db, _, intent = seeded
    with db.transaction() as connection:
        connection.execute(text("ALTER TABLE outbox ADD CONSTRAINT injected_failure CHECK (false)"))
    with pytest.raises(PersistenceError):
        enqueue_intent(db, intent)
    with db.transaction() as connection:
        assert connection.scalar(text("SELECT count(*) FROM execution_intents")) == 0
        assert connection.scalar(text("SELECT count(*) FROM outbox")) == 0


def test_parallel_intents_have_one_winner(seeded):
    from tradeforge.persistence.db import PersistenceError
    from tradeforge.persistence.repositories import enqueue_intent

    db, _, intent = seeded

    def attempt(_):
        try:
            enqueue_intent(db, intent)
            return True
        except PersistenceError:
            return False

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert list(pool.map(attempt, range(4))).count(True) == 1
    with db.transaction() as connection:
        assert connection.scalar(text("SELECT count(*) FROM outbox")) == 1


def test_crash_before_commit_rolls_back_and_committed_pending_survives_restart(
    seeded, postgres_service
):
    from tradeforge.persistence.db import Database
    from tradeforge.persistence.repositories import enqueue_intent

    db, _, intent = seeded
    # Abrupt child death closes an uncommitted server transaction, without Python cleanup.
    script = """
import os, sys
from sqlalchemy import create_engine, text
db = create_engine(sys.argv[1])
c = db.connect()
t = c.begin()
c.execute(text("INSERT INTO audit_log (event_type, payload) VALUES ('crash_probe', '{}')"))
os._exit(23)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, db.sql_engine.url.render_as_string(hide_password=False)],
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 23
    enqueue_intent(db, intent)
    url = db.sql_engine.url
    db.close()
    if "pg_ctl" in postgres_service:
        subprocess.run(
            [
                postgres_service["pg_ctl"],
                "-D",
                postgres_service["data"],
                "-m",
                "immediate",
                "-w",
                "stop",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=60,
        )
        subprocess.run(
            postgres_service["start"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=60,
        )
    reopened = Database(url)
    try:
        with reopened.transaction() as connection:
            assert (
                connection.scalar(text("SELECT count(*) FROM outbox WHERE delivered_at IS NULL"))
                == 1
            )
            assert (
                connection.scalar(
                    text("SELECT count(*) FROM audit_log WHERE event_type='crash_probe'")
                )
                == 0
            )
    finally:
        reopened.close()


def test_publish_ack_loss_redelivers_and_consumer_effect_once(seeded):
    from tradeforge.persistence.outbox import consume_once, dispatch_one
    from tradeforge.persistence.repositories import enqueue_intent

    db, _, intent = seeded
    enqueue_intent(db, intent)
    received = []

    def effect(connection, message):
        connection.execute(
            text("INSERT INTO audit_log (event_type, payload) VALUES ('consumer_effect', '{}')")
        )

    def transport(message):
        received.append(message.message_id)
        with db.transaction() as connection:
            consume_once(connection, "test_consumer", message, effect)
        if len(received) == 1:
            raise TimeoutError("accepted but acknowledgement lost")

    with pytest.raises(TimeoutError):
        dispatch_one(db, transport)
    assert dispatch_one(db, transport)
    assert not dispatch_one(db, transport)
    assert len(received) == 2 and received[0] == received[1]
    with db.transaction() as connection:
        assert connection.scalar(text("SELECT count(*) FROM consumer_receipts")) == 1
        assert (
            connection.scalar(
                text("SELECT count(*) FROM audit_log WHERE event_type='consumer_effect'")
            )
            == 1
        )


def test_failed_consumer_rolls_back_receipt_and_effect(seeded):
    from tradeforge.persistence.outbox import consume_once, dispatch_one
    from tradeforge.persistence.repositories import enqueue_intent

    db, _, intent = seeded
    enqueue_intent(db, intent)
    messages = []
    dispatch_one(db, messages.append)

    def broken(connection, message):
        connection.execute(
            text("INSERT INTO audit_log (event_type, payload) VALUES ('failed_consumer', '{}')")
        )
        raise ValueError("consumer crash")

    with pytest.raises(ValueError):
        with db.transaction() as connection:
            consume_once(connection, "test_consumer", messages[0], broken)
    with db.transaction() as connection:
        assert connection.scalar(text("SELECT count(*) FROM consumer_receipts")) == 0
        assert (
            connection.scalar(
                text("SELECT count(*) FROM audit_log WHERE event_type='failed_consumer'")
            )
            == 0
        )


def test_caught_consumer_failure_still_rolls_back_receipt_and_effect(seeded):
    from tradeforge.persistence.outbox import consume_once, dispatch_one
    from tradeforge.persistence.repositories import enqueue_intent

    db, _, intent = seeded
    enqueue_intent(db, intent)
    messages = []
    dispatch_one(db, messages.append)

    def broken(connection, message):
        connection.execute(
            text("INSERT INTO audit_log (event_type, payload) VALUES ('caught_failure', '{}')")
        )
        raise ValueError("consumer crash")

    with db.transaction() as connection:
        with pytest.raises(ValueError):
            consume_once(connection, "test_consumer", messages[0], broken)
    with db.transaction() as connection:
        assert connection.scalar(text("SELECT count(*) FROM consumer_receipts")) == 0
        assert (
            connection.scalar(
                text("SELECT count(*) FROM audit_log WHERE event_type='caught_failure'")
            )
            == 0
        )


def test_parallel_dispatchers_deliver_one_locked_message(seeded):
    from tradeforge.persistence.outbox import dispatch_one
    from tradeforge.persistence.repositories import enqueue_intent

    db, _, intent = seeded
    enqueue_intent(db, intent)
    messages = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: dispatch_one(db, messages.append), range(4)))
    assert results.count(True) == 1
    assert len(messages) == 1


def test_unavailable_db_and_wrong_schema_fail_closed(database_url, caplog):
    from tradeforge.persistence.db import Database, EntryHalt

    wrong_schema = Database(database_url)
    try:
        assert not wrong_schema.probe().db_writable
    finally:
        wrong_schema.close()
    db = Database("postgresql+psycopg://fake:DO_NOT_LOG_ME@127.0.0.1:1/test")
    try:
        with pytest.raises(EntryHalt, match="DB_UNAVAILABLE") as error:
            with db.transaction():
                pass
        assert not db.readiness.db_writable
        assert not db.readiness.can_open_entries
        assert db.readiness.can_reduce_risk
        assert "DO_NOT_LOG_ME" not in str(error.value) + caplog.text + repr(db)
    finally:
        db.close()


def test_mismatched_or_unapproved_decision_cannot_enqueue(seeded):
    from tradeforge.persistence.repositories import enqueue_intent

    db, _, intent = seeded
    changed = intent.model_dump()
    from tradeforge.domain.decision import Side

    changed.update(side=Side.SHORT, price=None)
    with pytest.raises(ValueError):
        enqueue_intent(db, type(intent).model_validate(changed))
    with db.transaction() as connection:
        assert connection.scalar(text("SELECT count(*) FROM execution_intents")) == 0
