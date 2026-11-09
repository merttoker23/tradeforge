"""A slow schema backfill must not relax deadlines for ordinary DB operations."""

from contextlib import contextmanager

import pytest

from tradeforge.persistence import transactions


class Connection:
    def __init__(self):
        self.statements = []

    def execute(self, statement):
        self.statements.append(str(statement))


class Database:
    def __init__(self):
        self.connection = Connection()
        self.committed = False
        self.rolled_back = False

    @contextmanager
    def transaction(self):
        try:
            yield self.connection
        except Exception:
            self.rolled_back = True
            raise
        else:
            self.committed = True


@pytest.mark.parametrize("target,action", [("head", "upgrade"), ("base", "downgrade")])
def test_migration_timeout_is_local_to_transaction(monkeypatch, target, action):
    db = Database()
    observed = []

    def call(config, revision):
        observed.append((config.attributes["connection"], revision))

    monkeypatch.setattr(transactions.command, action, call)
    transactions.migrate(db, target)

    assert db.connection.statements == [
        "SELECT pg_advisory_xact_lock(74202002)",
        "SET LOCAL statement_timeout = '10min'",
    ]
    assert observed == [(db.connection, target)]
    assert db.committed and not db.rolled_back


def test_failed_migration_rolls_back_timeout_and_schema(monkeypatch):
    db = Database()

    def fail(_config, _target):
        raise RuntimeError("migration rejected")

    monkeypatch.setattr(transactions.command, "upgrade", fail)
    with pytest.raises(RuntimeError, match="migration rejected"):
        transactions.migrate(db, "head")
    assert db.rolled_back and not db.committed
    assert db.connection.statements[-1] == "SET LOCAL statement_timeout = '10min'"
