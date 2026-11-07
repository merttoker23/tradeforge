"""PHASE_10 append-only, hash-linked analyst lifecycle journal."""

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE decision_lifecycle (
            sequence bigint PRIMARY KEY CHECK (sequence >= 0),
            event_id uuid NOT NULL UNIQUE,
            decision_id uuid NOT NULL,
            version bigint NOT NULL CHECK (version >= 0),
            state text NOT NULL CHECK (state IN ('DISCOVERED','WATCHING','ARMED','VALIDATING',
                'APPROVED','REJECTED','EXPIRED','NO_TRADE','CANCELLED')),
            at timestamptz NOT NULL,
            previous_hash text NOT NULL CHECK (previous_hash ~ '^[a-f0-9]{64}$'),
            record_hash text NOT NULL CHECK (
                record_hash = encode(sha256(convert_to(payload, 'UTF8')), 'hex')),
            payload text NOT NULL CHECK (jsonb_typeof(payload::jsonb) = 'object'),
            UNIQUE (decision_id, version)
        )
    """)
    op.execute("CREATE INDEX lifecycle_asof ON decision_lifecycle (at, sequence)")
    op.execute("""
        CREATE FUNCTION validate_decision_append() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE previous decision_lifecycle; candidate_previous decision_lifecycle;
        BEGIN
            PERFORM pg_advisory_xact_lock(74202010);
            SELECT * INTO previous FROM decision_lifecycle ORDER BY sequence DESC LIMIT 1;
            IF NEW.sequence <> COALESCE(previous.sequence + 1, 0)
               OR NEW.previous_hash <> COALESCE(previous.record_hash, repeat('0',64))
               OR NEW.at < previous.at THEN
                RAISE EXCEPTION 'INVALID_JOURNAL_SUCCESSOR' USING ERRCODE = '23514';
            END IF;
            SELECT * INTO candidate_previous FROM decision_lifecycle
                WHERE decision_id = NEW.decision_id ORDER BY version DESC LIMIT 1;
            IF NEW.version <> COALESCE(candidate_previous.version + 1, 0)
               OR candidate_previous.state IN
                    ('APPROVED','REJECTED','EXPIRED','NO_TRADE','CANCELLED')
               OR (NEW.version = 0 AND NEW.state NOT IN ('DISCOVERED','NO_TRADE','EXPIRED')) THEN
                RAISE EXCEPTION 'INVALID_CANDIDATE_SUCCESSOR' USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END $$
    """)
    op.execute(
        "CREATE TRIGGER lifecycle_successor BEFORE INSERT ON decision_lifecycle "
        "FOR EACH ROW EXECUTE FUNCTION validate_decision_append()"
    )
    op.execute(
        "CREATE TRIGGER immutable_lifecycle BEFORE UPDATE OR DELETE OR TRUNCATE "
        "ON decision_lifecycle FOR EACH STATEMENT EXECUTE FUNCTION append_only()"
    )


def downgrade() -> None:
    # A downgrade may not silently remove actual decision evidence.
    op.execute(
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM decision_lifecycle) THEN "
        "RAISE EXCEPTION 'DECISION_HISTORY_PRESENT' USING ERRCODE = '23514'; "
        "END IF; END $$"
    )
    op.execute("DROP TABLE decision_lifecycle")
    op.execute("DROP FUNCTION validate_decision_append()")
