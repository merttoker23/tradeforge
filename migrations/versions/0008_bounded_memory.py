"""Semantic bar dedupe, latest-revision upsert, and bounded evidence retention."""

from alembic import op

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE closed_bar_observations ADD COLUMN content_hash text")
    # Legacy rows predate semantic identity. Their exact input hash is a safe
    # conservative backfill: one extra revision may be written after upgrade,
    # but no historical evidence is deleted or relabeled.
    op.execute(
        "UPDATE closed_bar_observations SET content_hash=input_hash WHERE content_hash IS NULL"
    )
    op.execute("ALTER TABLE closed_bar_observations ALTER COLUMN content_hash SET NOT NULL")
    op.execute(
        "ALTER TABLE closed_bar_observations ADD CONSTRAINT closed_bar_content_hash_check "
        "CHECK (content_hash ~ '^[a-f0-9]{64}$')"
    )
    op.execute(
        "CREATE INDEX bar_content_lookup ON closed_bar_observations"
        "(instrument_id,timeframe,open_at,content_hash)"
    )
    op.execute("""
        CREATE OR REPLACE FUNCTION retention_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF TG_OP='UPDATE' THEN
            IF TG_TABLE_NAME='closed_bars' AND NEW.observed_at > OLD.observed_at THEN
              RETURN NEW;
            END IF;
            RAISE EXCEPTION 'APPEND_ONLY' USING ERRCODE='23514';
          END IF;
          IF TG_OP='DELETE' AND current_setting('tradeforge.retention',true)='on'
             AND current_user=pg_get_userbyid(
                 (SELECT relowner FROM pg_class WHERE oid=TG_RELID)) THEN
            IF TG_TABLE_NAME='audit_log' THEN
              IF OLD.event_type IN ('protection.health.v1','demo.preflight.v1',
                                    'dashboard.chart.v1','protection.position.v1')
              THEN RETURN OLD; END IF;
            ELSIF TG_TABLE_NAME='account_snapshots' THEN
              IF OLD.payload->>'kind'='guardian.account.v1' THEN RETURN OLD; END IF;
            ELSE RETURN OLD;
            END IF;
          END IF;
          RAISE EXCEPTION 'APPEND_ONLY' USING ERRCODE='23514';
        END $$
    """)
    op.execute("""
        CREATE OR REPLACE FUNCTION retain_runtime_history() RETURNS void LANGUAGE plpgsql
        SECURITY DEFINER SET search_path=pg_catalog,public AS $$
        BEGIN
          PERFORM pg_advisory_xact_lock(74202060);
          PERFORM set_config('tradeforge.retention','on',true);
          DELETE FROM public.audit_log WHERE audit_id IN (
            SELECT audit_id FROM public.audit_log WHERE observed_at<now()-interval '30 days'
            AND event_type IN ('protection.health.v1','demo.preflight.v1',
                               'dashboard.chart.v1','protection.position.v1')
            ORDER BY audit_id LIMIT 5000);
          DELETE FROM public.account_snapshots WHERE snapshot_id IN (
            SELECT snapshot_id FROM public.account_snapshots
            WHERE as_of<now()-interval '30 days' AND payload->>'kind'='guardian.account.v1'
            ORDER BY as_of LIMIT 5000);
          DELETE FROM public.opportunities WHERE opportunity_id IN (
            SELECT opportunity_id FROM public.opportunities
            WHERE observed_at<now()-interval '7 days'
            ORDER BY observed_at LIMIT 5000);
          DELETE FROM public.feature_snapshots WHERE snapshot_id IN (
            SELECT f.snapshot_id FROM public.feature_snapshots f
            WHERE as_of<now()-interval '7 days'
            AND NOT EXISTS (SELECT 1 FROM public.opportunities o
                            WHERE o.feature_snapshot_id=f.snapshot_id)
            ORDER BY as_of LIMIT 5000);
          DELETE FROM public.closed_bar_observations WHERE ctid IN (
            SELECT ctid FROM public.closed_bar_observations WHERE close_at<now()-interval '30 days'
            ORDER BY close_at LIMIT 5000);
          DELETE FROM public.closed_bars WHERE ctid IN (
            SELECT ctid FROM public.closed_bars WHERE close_at<now()-interval '30 days'
            ORDER BY close_at LIMIT 5000);
          DELETE FROM public.regime_snapshots WHERE snapshot_id IN (
            SELECT r.snapshot_id FROM public.regime_snapshots r WHERE as_of<now()-interval '7 days'
            AND NOT EXISTS (SELECT 1 FROM public.opportunities o
                            WHERE o.regime_snapshot_id=r.snapshot_id)
            AND NOT EXISTS (SELECT 1 FROM public.decisions d
                            WHERE d.snapshot::jsonb->>'regime_version'=r.input_version)
            AND NOT EXISTS (SELECT 1 FROM public.decision_lifecycle l WHERE
                l.payload::jsonb#>>'{discovery,candidate,regime_version}'=r.input_version)
            ORDER BY as_of LIMIT 100);
          DELETE FROM public.universe_snapshots WHERE snapshot_id IN (
            SELECT u.snapshot_id FROM public.universe_snapshots u
            WHERE as_of<now()-interval '30 days'
            AND NOT EXISTS (SELECT 1 FROM public.decisions d
                            WHERE d.snapshot::jsonb->>'universe_version'=u.version_hash)
            AND NOT EXISTS (SELECT 1 FROM public.decision_lifecycle l WHERE
                l.payload::jsonb#>>'{discovery,candidate,universe_version}'=u.version_hash)
            ORDER BY as_of LIMIT 100);
          PERFORM set_config('tradeforge.retention','off',true);
        END $$
    """)
    op.execute("""
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='tf_market') THEN
            GRANT UPDATE ON closed_bars TO tf_market;
          END IF;
        END $$
    """)


def downgrade() -> None:
    op.execute("""
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='tf_market') THEN
            REVOKE UPDATE ON closed_bars FROM tf_market;
          END IF;
        END $$
    """)
    op.execute("""
        CREATE OR REPLACE FUNCTION retain_runtime_history() RETURNS void LANGUAGE plpgsql
        SECURITY DEFINER SET search_path=pg_catalog,public AS $$
        BEGIN
          PERFORM pg_advisory_xact_lock(74202060);
          PERFORM set_config('tradeforge.retention','on',true);
          DELETE FROM public.audit_log WHERE audit_id IN (
            SELECT audit_id FROM public.audit_log WHERE observed_at<now()-interval '30 days'
            AND event_type IN ('protection.health.v1','demo.preflight.v1',
                               'dashboard.chart.v1','protection.position.v1')
            ORDER BY audit_id LIMIT 5000);
          DELETE FROM public.account_snapshots WHERE snapshot_id IN (
            SELECT snapshot_id FROM public.account_snapshots
            WHERE as_of<now()-interval '30 days' AND payload->>'kind'='guardian.account.v1'
            ORDER BY as_of LIMIT 5000);
          DELETE FROM public.opportunities WHERE opportunity_id IN (
            SELECT opportunity_id FROM public.opportunities
            WHERE observed_at<now()-interval '30 days'
            ORDER BY observed_at LIMIT 5000);
          DELETE FROM public.feature_snapshots WHERE snapshot_id IN (
            SELECT f.snapshot_id FROM public.feature_snapshots f
            WHERE as_of<now()-interval '30 days'
            AND NOT EXISTS (SELECT 1 FROM public.opportunities o
                            WHERE o.feature_snapshot_id=f.snapshot_id)
            ORDER BY as_of LIMIT 5000);
          DELETE FROM public.closed_bar_observations WHERE ctid IN (
            SELECT ctid FROM public.closed_bar_observations WHERE close_at<now()-interval '30 days'
            ORDER BY close_at LIMIT 5000);
          DELETE FROM public.closed_bars WHERE ctid IN (
            SELECT ctid FROM public.closed_bars WHERE close_at<now()-interval '30 days'
            ORDER BY close_at LIMIT 5000);
          DELETE FROM public.regime_snapshots WHERE snapshot_id IN (
            SELECT r.snapshot_id FROM public.regime_snapshots r WHERE as_of<now()-interval '30 days'
            AND NOT EXISTS (SELECT 1 FROM public.opportunities o
                            WHERE o.regime_snapshot_id=r.snapshot_id)
            AND NOT EXISTS (SELECT 1 FROM public.decisions d
                            WHERE d.snapshot::jsonb->>'regime_version'=r.input_version)
            AND NOT EXISTS (SELECT 1 FROM public.decision_lifecycle l WHERE
                l.payload::jsonb#>>'{discovery,candidate,regime_version}'=r.input_version)
            ORDER BY as_of LIMIT 100);
          DELETE FROM public.universe_snapshots WHERE snapshot_id IN (
            SELECT u.snapshot_id FROM public.universe_snapshots u
            WHERE as_of<now()-interval '30 days'
            AND NOT EXISTS (SELECT 1 FROM public.decisions d
                            WHERE d.snapshot::jsonb->>'universe_version'=u.version_hash)
            AND NOT EXISTS (SELECT 1 FROM public.decision_lifecycle l WHERE
                l.payload::jsonb#>>'{discovery,candidate,universe_version}'=u.version_hash)
            ORDER BY as_of LIMIT 100);
          PERFORM set_config('tradeforge.retention','off',true);
        END $$
    """)
    op.execute("""
        CREATE OR REPLACE FUNCTION retention_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF TG_OP='DELETE' AND current_setting('tradeforge.retention',true)='on'
             AND current_user=pg_get_userbyid(
                 (SELECT relowner FROM pg_class WHERE oid=TG_RELID)) THEN
            IF TG_TABLE_NAME='audit_log' THEN
              IF OLD.event_type IN ('protection.health.v1','demo.preflight.v1',
                                    'dashboard.chart.v1','protection.position.v1')
              THEN RETURN OLD; END IF;
            ELSIF TG_TABLE_NAME='account_snapshots' THEN
              IF OLD.payload->>'kind'='guardian.account.v1' THEN RETURN OLD; END IF;
            ELSE RETURN OLD;
            END IF;
          END IF;
          RAISE EXCEPTION 'APPEND_ONLY' USING ERRCODE='23514';
        END $$
    """)
    op.execute("DROP INDEX bar_content_lookup")
    op.execute("ALTER TABLE closed_bar_observations DROP CONSTRAINT closed_bar_content_hash_check")
    op.execute("ALTER TABLE closed_bar_observations DROP COLUMN content_hash")
