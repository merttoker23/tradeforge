"""Safe database reason codes distinguish an unavailable writer from a timeout."""

from contextlib import ExitStack

import pytest
from sqlalchemy import text

from tradeforge.persistence.db import EntryHalt
from tradeforge.runtime.progress import safe_failure


def test_statement_timeout_has_safe_specific_diagnostic(migrated):
    with pytest.raises(EntryHalt) as failure:
        with migrated.transaction() as c:
            c.execute(text("SET LOCAL statement_timeout='1ms'"))
            c.execute(text("SELECT pg_sleep(0.05) /* private_marker */"))
    assert safe_failure(failure.value) == "DB_STATEMENT_TIMEOUT"
    assert "private_marker" not in str(failure.value)


def test_lock_timeout_has_safe_specific_diagnostic(migrated):
    with migrated.sql_engine.begin() as owner:
        owner.execute(text("SELECT pg_advisory_xact_lock(28467312)"))
        with pytest.raises(EntryHalt) as failure:
            with migrated.transaction() as c:
                c.execute(text("SET LOCAL lock_timeout='1ms'"))
                c.execute(text("SELECT pg_advisory_xact_lock(28467312)"))
    assert safe_failure(failure.value) == "DB_LOCK_TIMEOUT"


def test_permission_failure_is_safe_and_still_halts_entries(migrated, caplog):
    with pytest.raises(EntryHalt) as failure:
        with migrated.transaction() as c:
            c.execute(text("SET LOCAL ROLE pg_read_all_data"))
            c.execute(
                text("INSERT INTO audit_log(event_type,payload) VALUES('private_marker','{}')")
            )
    assert safe_failure(failure.value) == "DB_PERMISSION_DENIED"
    assert not migrated.readiness.can_open_entries
    assert migrated.readiness.can_reduce_risk
    assert "private_marker" not in str(failure.value) + caplog.text


def test_exhausted_pool_has_safe_specific_diagnostic(migrated):
    migrated.sql_engine.pool._timeout = 0.001
    with ExitStack() as stack:
        for _ in range(4):
            stack.enter_context(migrated.sql_engine.connect())
        with pytest.raises(EntryHalt) as failure:
            with migrated.transaction():
                pass
    assert safe_failure(failure.value) == "DB_POOL_TIMEOUT"
    assert not migrated.readiness.can_open_entries
