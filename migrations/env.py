"""Migrations use a caller-owned PostgreSQL connection; no embedded credentials."""

from alembic import context
from sqlalchemy.engine import Connection

connection = context.config.attributes.get("connection")
if not isinstance(connection, Connection):
    raise RuntimeError("Use python -m tradeforge.persistence.transactions with an explicit DB URL")
context.configure(
    connection=connection, version_table="schema_migrations", transaction_per_migration=True
)
with context.begin_transaction():
    context.run_migrations()
