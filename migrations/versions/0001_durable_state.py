"""PHASE_02: a frozen initial PostgreSQL schema, independent of future model changes."""

from pathlib import Path

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    sql = Path(__file__).with_suffix(".sql").read_text(encoding="utf-8")
    op.get_bind().exec_driver_sql(sql, execution_options={"no_parameters": True})


def downgrade() -> None:
    tables = (
        "consumer_receipts",
        "audit_log",
        "commands",
        "risk_state",
        "account_snapshots",
        "protective_orders",
        "positions",
        "fills",
        "orders",
        "outbox",
        "execution_intents",
        "risk_snapshots",
        "decision_reasons",
        "decisions",
        "opportunities",
        "regime_snapshots",
        "feature_snapshots",
        "closed_bars",
        "universe_snapshots",
        "instruments",
    )
    for table in tables:
        op.execute(f'DROP TABLE "{table}" CASCADE')
    for function in ("validate_intent", "immutable_outbox", "version_successor", "append_only"):
        op.execute(f"DROP FUNCTION {function}()")
    op.execute("DROP FUNCTION finite_numeric(numeric)")
