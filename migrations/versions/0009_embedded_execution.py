"""Quant-owned execution telemetry and privileges; retain old immutable audit history."""
from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # The previous live service has been retired, not silently reclassified.
    op.execute("DELETE FROM runtime_observations WHERE service='execution_guardian'")
    op.execute("ALTER TABLE runtime_observations DROP CONSTRAINT runtime_observations_service_check")
    op.execute("ALTER TABLE runtime_observations ADD CONSTRAINT runtime_observations_service_check "
               "CHECK (service IN ('market_gateway','quant_core','execution_core'))")
    # Upgrade existing deployments before the separate bootstrap grant pass.
    # Fresh installs have no worker roles yet; initialize() will grant them.
    op.execute("""DO $$ BEGIN
       IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='tf_quant') THEN
         GRANT SELECT ON public.binance_credentials TO tf_quant;
         GRANT INSERT ON public.instruments,public.decisions,public.decision_reasons,
           public.execution_intents,public.risk_snapshots,public.outbox,public.orders,
           public.fills,public.positions,public.protective_orders,public.account_snapshots,
           public.risk_state,public.commands,public.audit_log,public.decision_lifecycle,
           public.runtime_observations TO tf_quant;
         GRANT UPDATE ON public.orders,public.outbox,public.positions,
           public.protective_orders,public.risk_state,public.commands,
           public.runtime_observations TO tf_quant;
         GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO tf_quant;
         GRANT EXECUTE ON FUNCTION pg_ls_waldir() TO tf_quant;
         GRANT EXECUTE ON FUNCTION retain_runtime_history() TO tf_quant;
       END IF;
    END $$""")
    # A clean install has no legacy role yet. Only revoke it when upgrading
    # a previously provisioned deployment.
    op.execute("""DO $$ BEGIN
       IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='tf_guardian') THEN
         REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM tf_guardian;
         REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM tf_guardian;
         EXECUTE format('REVOKE ALL ON DATABASE %I FROM tf_guardian', current_database());
         ALTER ROLE tf_guardian NOLOGIN;
       END IF;
    END $$""")



def downgrade() -> None:
    op.execute("DELETE FROM runtime_observations WHERE service='execution_core'")
    op.execute("ALTER TABLE runtime_observations DROP CONSTRAINT runtime_observations_service_check")
    op.execute("ALTER TABLE runtime_observations ADD CONSTRAINT runtime_observations_service_check "
               "CHECK (service IN ('market_gateway','quant_core','execution_guardian'))")
    # Historical role remains disabled on rollback; a deliberate migration is required.
