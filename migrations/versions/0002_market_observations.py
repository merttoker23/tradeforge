"""PHASE_05 append-only point-in-time market evidence and 12h bars."""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE closed_bars DROP CONSTRAINT closed_bars_timeframe_check")
    op.execute(
        "ALTER TABLE closed_bars ADD CONSTRAINT closed_bars_timeframe_check "
        "CHECK (timeframe IN ('1m','5m','15m','1h','4h','12h','1d'))"
    )
    op.execute("""
        CREATE TABLE closed_bar_observations (
            instrument_id uuid NOT NULL REFERENCES instruments,
            timeframe text NOT NULL CHECK (timeframe IN ('1m','5m','15m','1h','4h','12h','1d')),
            open_at timestamptz NOT NULL,
            close_at timestamptz NOT NULL CHECK (close_at > open_at),
            source_at timestamptz NOT NULL CHECK (source_at >= close_at),
            observed_at timestamptz NOT NULL CHECK (observed_at >= source_at),
            payload text NOT NULL,
            input_hash text NOT NULL CHECK (
                input_hash = encode(sha256(convert_to(payload, 'UTF8')), 'hex')),
            PRIMARY KEY (instrument_id, timeframe, open_at, observed_at)
        )
    """)
    op.execute(
        "CREATE INDEX bar_versions_asof ON closed_bar_observations "
        "(instrument_id, timeframe, observed_at)"
    )
    op.execute(
        "CREATE TRIGGER immutable_bar_versions BEFORE UPDATE OR DELETE OR TRUNCATE "
        "ON closed_bar_observations FOR EACH STATEMENT EXECUTE FUNCTION append_only()"
    )


def downgrade() -> None:
    op.execute("DROP TABLE closed_bar_observations")
    # Reject downgrade if 12h evidence exists; never delete user data to downgrade.
    op.execute("ALTER TABLE closed_bars DROP CONSTRAINT closed_bars_timeframe_check")
    op.execute(
        "ALTER TABLE closed_bars ADD CONSTRAINT closed_bars_timeframe_check "
        "CHECK (timeframe IN ('1m','5m','15m','1h','4h','1d'))"
    )
