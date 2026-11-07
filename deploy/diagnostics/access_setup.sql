-- ADMINISTRATOR ACCESS CONTROL INSTALLATION ONLY; the helper NEVER executes this file.
-- Run once in the tradeforge database as its authorized administrator after review.
-- No password, application setting, trading permission, application data or schema change.
-- An existing tf_diagnostics must be inspected instead of silently changed or reused.
BEGIN;
CREATE ROLE tf_diagnostics LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 2;
GRANT CONNECT ON DATABASE tradeforge TO tf_diagnostics;
GRANT USAGE ON SCHEMA public TO tf_diagnostics;
GRANT SELECT ON public.runtime_observations, public.universe_snapshots,
    public.feature_snapshots, public.opportunities, public.decisions,
    public.execution_intents, public.outbox, public.orders, public.fills,
    public.decision_lifecycle, public.decision_reasons, public.regime_snapshots,
    public.audit_log, public.closed_bars, public.closed_bar_observations,
    public.risk_state TO tf_diagnostics;
COMMIT;
