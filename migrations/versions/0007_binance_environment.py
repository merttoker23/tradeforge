"""Encrypted per-environment Binance keys and durable active endpoint revision."""

from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    op.execute("""
        CREATE TABLE binance_runtime (
            id smallint PRIMARY KEY CHECK (id=1),
            environment text NOT NULL CHECK (environment IN ('testnet','live')),
            revision bigint NOT NULL CHECK (revision>0),
            changed_at timestamptz NOT NULL DEFAULT now()
        )
    """)
    # Existing installations collected their public market data from Production.
    # Do not relabel those persisted observations as Testnet on upgrade.
    op.execute("""
        INSERT INTO binance_runtime(id,environment,revision)
        SELECT 1, CASE WHEN
            EXISTS(SELECT 1 FROM universe_snapshots)
            OR EXISTS(SELECT 1 FROM closed_bars)
            OR EXISTS(SELECT 1 FROM closed_bar_observations)
            OR EXISTS(SELECT 1 FROM runtime_market)
            OR EXISTS(SELECT 1 FROM terminal_chart_cache)
            OR EXISTS(SELECT 1 FROM runtime_observations)
            THEN 'live' ELSE 'testnet' END, 1
    """)
    op.execute("""
        CREATE TABLE binance_credentials (
            environment text PRIMARY KEY CHECK (environment IN ('testnet','live')),
            api_key bytea NOT NULL,
            api_secret bytea NOT NULL,
            last_test_at timestamptz,
            last_test_ok boolean
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS binance_credentials")
    op.execute("DROP TABLE IF EXISTS binance_runtime")
