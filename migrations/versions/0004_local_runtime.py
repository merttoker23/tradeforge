"""PHASE_16A timestamped runtime evidence and replaceable public market cache."""

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE runtime_observations (
            sample_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            service text NOT NULL CHECK (service IN ('market_gateway','quant_core')),
            observed_at timestamptz NOT NULL,
            expires_at timestamptz NOT NULL,
            payload jsonb NOT NULL CHECK (jsonb_typeof(payload)='object')
        )
    """)
    op.execute(
        "CREATE INDEX runtime_latest ON runtime_observations"
        "(service,observed_at DESC,sample_id DESC)"
    )
    op.execute("""
        CREATE TRIGGER runtime_immutable BEFORE UPDATE OR DELETE OR TRUNCATE
        ON runtime_observations FOR EACH STATEMENT EXECUTE FUNCTION append_only()
    """)
    op.execute("""
        CREATE TABLE runtime_market (
            instrument_id uuid PRIMARY KEY REFERENCES instruments,
            source_at timestamptz NOT NULL,
            observed_at timestamptz NOT NULL CHECK (source_at<=observed_at),
            payload jsonb NOT NULL CHECK (jsonb_typeof(payload)='object')
        )
    """)


def downgrade() -> None:
    op.execute("""
        DO $$ BEGIN IF EXISTS (SELECT 1 FROM runtime_observations) THEN
        RAISE EXCEPTION 'RUNTIME_HISTORY_PRESENT' USING ERRCODE='23514'; END IF; END $$
    """)
    op.execute("DROP TABLE runtime_market")
    op.execute("DROP TABLE runtime_observations")
