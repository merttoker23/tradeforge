"""Real Alembic upgrade SQL; PostgreSQL transactions preserve failed releases."""

import hashlib
from pathlib import Path

import pytest
from sqlalchemy import text

from deploy.automation.migrations import render
from deploy.automation.policy import migration_plan
from tradeforge.persistence.db import EntryHalt

ROOT = Path(__file__).resolve().parents[2]
BASE_REVISION = "0008"
NEXT_REVISION = "0009"


def sources():
    return {p.name: p.read_text() for p in (ROOT / "migrations/versions").glob("*.py")}


def extra(sql):
    return (
        f'from alembic import op\nrevision="{NEXT_REVISION}"\ndown_revision="{BASE_REVISION}"\n'
        "branch_labels=None\ndepends_on=None\n"
        "def upgrade() -> None:\n"
        + "".join(f"    op.execute({s!r})\n" for s in sql)
        + "def downgrade() -> None:\n    pass\n"
    )


def test_additive_alembic_upgrade_preserves_rows_and_advances_revision(migrated, tmp_path):
    with migrated.transaction() as c:
        c.exec_driver_sql("CREATE TABLE deployment_probe (id integer)")
        c.exec_driver_sql("INSERT INTO deployment_probe VALUES (42)")
    original = sources()
    hashes = {k: hashlib.sha256(v.encode()).hexdigest() for k, v in original.items()}
    candidate = {
        **original,
        "0008_extra.py": extra(["ALTER TABLE orders ADD COLUMN deploy_note text"]),
    }
    head, plan = migration_plan(candidate, hashes, BASE_REVISION)
    assert head == NEXT_REVISION and len(plan) == 1
    sql = render(candidate, BASE_REVISION, head, tmp_path)
    with migrated.transaction() as c:
        c.exec_driver_sql(sql)
        assert c.scalar(text("SELECT id FROM deployment_probe")) == 42
        assert c.scalar(text("SELECT version_num FROM schema_migrations")) == NEXT_REVISION
        assert (
            c.scalar(
                text(
                    "SELECT count(*) FROM information_schema.columns WHERE "
                    "table_name='orders' AND column_name='deploy_note'"
                )
            )
            == 1
        )


def test_failed_alembic_upgrade_rolls_back_all_steps_and_revision(migrated, tmp_path):
    candidate = {
        **sources(),
        "0008_extra.py": extra(
            [
                "ALTER TABLE orders ADD COLUMN deploy_note text",
                "ALTER TABLE orders ADD COLUMN deploy_note text",
            ]
        ),
    }
    sql = render(candidate, BASE_REVISION, NEXT_REVISION, tmp_path)
    with pytest.raises(EntryHalt, match="DB_UNAVAILABLE"):
        with migrated.transaction() as c:
            c.exec_driver_sql(sql)
    with migrated.transaction() as c:
        assert c.scalar(text("SELECT version_num FROM schema_migrations")) == BASE_REVISION
        assert (
            c.scalar(
                text(
                    "SELECT count(*) FROM information_schema.columns WHERE "
                    "table_name='orders' AND column_name='deploy_note'"
                )
            )
            == 0
        )


def test_destructive_upgrade_is_rejected_before_sql_execution(migrated):
    original = sources()
    hashes = {k: hashlib.sha256(v.encode()).hexdigest() for k, v in original.items()}
    with pytest.raises(ValueError, match="MANUAL_REVIEW"):
        migration_plan(
            {**original, "0008_extra.py": extra(["DROP TABLE orders"])},
            hashes,
            BASE_REVISION,
        )
    with migrated.transaction() as c:
        assert c.scalar(text("SELECT version_num FROM schema_migrations")) == BASE_REVISION
        assert c.scalar(text("SELECT to_regclass('orders')")) is not None
