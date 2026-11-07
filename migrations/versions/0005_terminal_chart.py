"""Replaceable display-only chart cache; never a quant/execution evidence source."""

from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE terminal_chart_cache (
          instrument_id uuid NOT NULL REFERENCES instruments,
          timeframe text NOT NULL CHECK (timeframe IN ('1m','5m','15m','1h','4h')),
          as_of timestamptz NOT NULL,
          payload jsonb NOT NULL CHECK (jsonb_typeof(payload)='object'
            AND jsonb_typeof(payload->'bars')='array'
            AND jsonb_array_length(payload->'bars') BETWEEN 1 AND 300),
          PRIMARY KEY(instrument_id,timeframe)
        )
    """)
    op.execute("""
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='tf_market') THEN
            GRANT SELECT,INSERT,UPDATE ON terminal_chart_cache TO tf_market;
          END IF;
          IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='tf_dashboard') THEN
            GRANT SELECT ON terminal_chart_cache TO tf_dashboard;
          END IF;
        END $$
    """)


def downgrade() -> None:
    op.execute("DROP TABLE terminal_chart_cache")
