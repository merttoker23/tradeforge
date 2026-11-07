"""Bound heartbeat gauges and expose a narrow owner-executed retention operation."""

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE runtime_observations DROP CONSTRAINT runtime_observations_service_check"
    )
    op.execute(
        "ALTER TABLE runtime_observations ADD CHECK "
        "(service IN ('market_gateway','quant_core','execution_guardian'))"
    )
    op.execute("DROP TRIGGER runtime_immutable ON runtime_observations")
    op.execute(
        "DELETE FROM runtime_observations a USING runtime_observations b "
        "WHERE a.service=b.service AND (a.observed_at,a.sample_id)<(b.observed_at,b.sample_id)"
    )
    op.execute("CREATE UNIQUE INDEX runtime_service ON runtime_observations(service)")
    for statement in (
        "CREATE INDEX bar_retention_age ON closed_bar_observations(close_at)",
        "CREATE INDEX closed_retention_age ON closed_bars(close_at)",
        "CREATE INDEX opportunity_retention_age ON opportunities(observed_at)",
        "CREATE INDEX opportunity_regime ON opportunities(regime_snapshot_id)",
        "CREATE INDEX account_retention_age ON account_snapshots(as_of)",
        "CREATE INDEX regime_latest ON regime_snapshots(as_of DESC)",
        "CREATE INDEX audit_type_latest ON audit_log(event_type,audit_id DESC)",
        "CREATE INDEX audit_intent_lookup ON audit_log(event_type,(payload->>'intent_id'))",
    ):
        op.execute(statement)
    op.execute("""
        CREATE FUNCTION retention_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
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
    for table in (
        "closed_bars",
        "feature_snapshots",
        "regime_snapshots",
        "universe_snapshots",
        "audit_log",
        "account_snapshots",
    ):
        op.execute(f"DROP TRIGGER append_only_rows ON {table}")
        op.execute(
            f"CREATE TRIGGER append_only_rows BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION retention_immutable()"
        )
    op.execute("DROP TRIGGER immutable_bar_versions ON closed_bar_observations")
    op.execute(
        "CREATE TRIGGER immutable_bar_truncate BEFORE TRUNCATE ON closed_bar_observations "
        "FOR EACH STATEMENT EXECUTE FUNCTION append_only()"
    )
    op.execute(
        "CREATE TRIGGER observation_immutable BEFORE UPDATE OR DELETE ON closed_bar_observations "
        "FOR EACH ROW EXECUTE FUNCTION retention_immutable()"
    )
    op.execute("""
        CREATE FUNCTION retain_runtime_history() RETURNS void LANGUAGE plpgsql
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
    op.execute("REVOKE ALL ON FUNCTION retain_runtime_history() FROM PUBLIC")


def downgrade() -> None:
    op.execute("DROP FUNCTION retain_runtime_history()")
    for table in (
        "closed_bars",
        "feature_snapshots",
        "regime_snapshots",
        "universe_snapshots",
        "audit_log",
        "account_snapshots",
    ):
        op.execute(f"DROP TRIGGER append_only_rows ON {table}")
        op.execute(
            f"CREATE TRIGGER append_only_rows BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION append_only()"
        )
    op.execute("DROP TRIGGER observation_immutable ON closed_bar_observations")
    op.execute("DROP TRIGGER immutable_bar_truncate ON closed_bar_observations")
    op.execute(
        "CREATE TRIGGER immutable_bar_versions BEFORE UPDATE OR DELETE OR TRUNCATE "
        "ON closed_bar_observations "
        "FOR EACH STATEMENT EXECUTE FUNCTION append_only()"
    )
    op.execute("DROP FUNCTION retention_immutable()")
    op.execute("DROP INDEX runtime_service")
    for name in (
        "bar_retention_age",
        "closed_retention_age",
        "opportunity_retention_age",
        "opportunity_regime",
        "account_retention_age",
        "regime_latest",
        "audit_type_latest",
        "audit_intent_lookup",
    ):
        op.execute(f"DROP INDEX {name}")
    op.execute("DELETE FROM runtime_observations WHERE service='execution_guardian'")
    op.execute(
        "ALTER TABLE runtime_observations DROP CONSTRAINT runtime_observations_service_check"
    )
    op.execute(
        "ALTER TABLE runtime_observations ADD CHECK (service IN ('market_gateway','quant_core'))"
    )
    op.execute(
        "CREATE TRIGGER runtime_immutable BEFORE UPDATE OR DELETE OR TRUNCATE "
        "ON runtime_observations FOR EACH STATEMENT EXECUTE FUNCTION append_only()"
    )
