-- Initial fixed READ ONLY diagnostic transaction.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SELECT 'TF_DIAGNOSTICS_SECTION:risk_current';
SELECT jsonb_build_object('section','risk_current','row',value) FROM (
        SELECT jsonb_build_object('kill_switch',kill_switch,'version',version,'as_of',as_of)
          AS value FROM public.risk_state ORDER BY as_of DESC LIMIT 1
    ) fixed_rows;
SELECT 'TF_DIAGNOSTICS_SECTION:runtime_current';
SELECT jsonb_build_object('section','runtime_current','row',value) FROM (
        SELECT jsonb_build_object('service',service,'observed_at',observed_at,
          'expires_at',expires_at,'age_seconds',extract(epoch FROM now()-observed_at),
          'fresh',observed_at<=now() AND expires_at>now(),'data',payload) AS value
        FROM public.runtime_observations
        WHERE service IN ('market_gateway','quant_core','execution_core') ORDER BY service
    ) fixed_rows;
SELECT 'TF_DIAGNOSTICS_SECTION:pipeline_72h';
SELECT jsonb_build_object('section','pipeline_72h','row',value) FROM (
        SELECT jsonb_build_object('stage',stage,'count',n,'first_at',first_at,'last_at',last_at)
          AS value FROM (
        SELECT 'universe_snapshots' AS stage,count(*) AS n,min(as_of) AS first_at,
          max(as_of) AS last_at FROM public.universe_snapshots
          WHERE as_of>=now()-interval '72 hours'
        UNION ALL SELECT 'feature_snapshots',count(*),min(as_of),max(as_of)
          FROM public.feature_snapshots WHERE as_of>=now()-interval '72 hours'
        UNION ALL SELECT 'opportunities',count(*),min(observed_at),max(observed_at)
          FROM public.opportunities WHERE observed_at>=now()-interval '72 hours'
        UNION ALL SELECT 'decisions',count(*),min(observed_at),max(observed_at)
          FROM public.decisions WHERE observed_at>=now()-interval '72 hours'
        UNION ALL SELECT 'execution_intents',count(*),min(created_at),max(created_at)
          FROM public.execution_intents WHERE created_at>=now()-interval '72 hours'
        UNION ALL SELECT 'outbox_created',count(*),min(created_at),max(created_at)
          FROM public.outbox WHERE created_at>=now()-interval '72 hours'
        UNION ALL SELECT 'outbox_delivered',count(*),min(delivered_at),max(delivered_at)
          FROM public.outbox WHERE delivered_at>=now()-interval '72 hours'
        UNION ALL SELECT 'orders_updated',count(*),min(updated_at),max(updated_at)
          FROM public.orders WHERE updated_at>=now()-interval '72 hours'
        UNION ALL SELECT 'fills_observed',count(*),min(observed_at),max(observed_at)
          FROM public.fills WHERE observed_at>=now()-interval '72 hours') aggregate_rows
    ) fixed_rows;
SELECT 'TF_DIAGNOSTICS_SECTION:candidate_transitions_72h';
SELECT jsonb_build_object('section','candidate_transitions_72h','row',value) FROM (
        SELECT jsonb_build_object('state',state,'count',count(*),
          'unique_decisions',count(DISTINCT decision_id),'first_at',min(at),'last_at',max(at))
          AS value FROM public.decision_lifecycle WHERE at>=now()-interval '72 hours'
        GROUP BY state ORDER BY state
    ) fixed_rows;
SELECT 'TF_DIAGNOSTICS_SECTION:candidate_latest';
SELECT jsonb_build_object('section','candidate_latest','row',value) FROM (
        WITH latest AS (SELECT DISTINCT ON(decision_id) state,at,decision_id
          FROM public.decision_lifecycle ORDER BY decision_id,version DESC)
        SELECT jsonb_build_object('state',state,'count',count(*),'first_at',min(at),
          'last_at',max(at)) AS value FROM latest GROUP BY state ORDER BY state
    ) fixed_rows;
SELECT 'TF_DIAGNOSTICS_SECTION:rejections_72h';
SELECT jsonb_build_object('section','rejections_72h','row',value) FROM (
        WITH reasons AS (
          SELECT 'lifecycle' AS source,r.reason,l.at AS at
          FROM public.decision_lifecycle l CROSS JOIN LATERAL jsonb_array_elements_text(
            CASE WHEN jsonb_typeof(l.payload::jsonb->'reason_codes')='array'
              THEN l.payload::jsonb->'reason_codes' ELSE '[]'::jsonb END) r(reason)
          WHERE l.at>=now()-interval '72 hours'
          UNION ALL SELECT 'decision',reason_code,observed_at FROM public.decision_reasons
            WHERE observed_at>=now()-interval '72 hours'
          UNION ALL SELECT CASE a.event_type WHEN 'demo.preflight.v1'
            THEN 'preflight' ELSE 'admission' END,r.reason,a.observed_at
          FROM public.audit_log a CROSS JOIN LATERAL jsonb_array_elements_text(
            CASE WHEN jsonb_typeof(a.payload->'reason_codes')='array'
              THEN a.payload->'reason_codes' ELSE '[]'::jsonb END) r(reason)
          WHERE a.event_type IN ('demo.preflight.v1','runtime.admission_rejected.v1')
            AND a.observed_at>=now()-interval '72 hours')
        SELECT jsonb_build_object('source',source,'reason',reason,'count',count(*),
          'first_at',min(at),'last_at',max(at)) AS value FROM reasons
        GROUP BY source,reason ORDER BY count(*) DESC,source,reason LIMIT 64
    ) fixed_rows;
SELECT 'TF_DIAGNOSTICS_SECTION:outbox_current';
SELECT jsonb_build_object('section','outbox_current','row',value) FROM (
        SELECT jsonb_build_object('delivered',delivered_at IS NOT NULL,'count',count(*),
          'first_at',min(created_at),'last_at',max(created_at)) AS value
        FROM public.outbox GROUP BY (delivered_at IS NOT NULL)
    ) fixed_rows;
SELECT 'TF_DIAGNOSTICS_SECTION:orders_current';
SELECT jsonb_build_object('section','orders_current','row',value) FROM (
        SELECT jsonb_build_object('state',status,'count',count(*),'first_at',min(updated_at),
          'last_at',max(updated_at)) AS value FROM public.orders GROUP BY status ORDER BY status
    ) fixed_rows;
SELECT 'TF_DIAGNOSTICS_SECTION:regimes_72h';
SELECT jsonb_build_object('section','regimes_72h','row',value) FROM (
        SELECT jsonb_build_object('state',payload->'state'->>'label','count',count(*),
          'distinct_closed_periods',count(DISTINCT payload->'frame'->>'source_at'),
          'first_at',min(as_of),'last_at',max(as_of)) AS value
        FROM public.regime_snapshots WHERE as_of>=now()-interval '72 hours'
        GROUP BY payload->'state'->>'label' ORDER BY count(*) DESC LIMIT 16
    ) fixed_rows;
SELECT 'TF_DIAGNOSTICS_SECTION:audit_72h';
SELECT jsonb_build_object('section','audit_72h','row',value) FROM (
        SELECT jsonb_build_object('event',event_type,'count',count(*),'first_at',min(observed_at),
          'last_at',max(observed_at)) AS value FROM public.audit_log
        WHERE observed_at>=now()-interval '72 hours' AND event_type IN (
          'runtime.candidate.v1','runtime.forecast.v1','runtime.closed_result.v1',
          'runtime.demo.activated.v1','edge.outcome.v1','execution.intent_expired.v1',
          'runtime.admission_rejected.v1')
        GROUP BY event_type ORDER BY event_type
    ) fixed_rows;
SELECT 'TF_DIAGNOSTICS_SECTION:history_72h';
SELECT jsonb_build_object('section','history_72h','row',value) FROM (
        WITH bars AS (
          SELECT instrument_id,timeframe,open_at,close_at,observed_at,
            lag(open_at) OVER (PARTITION BY instrument_id,timeframe ORDER BY open_at) AS previous
          FROM public.closed_bars WHERE close_at>=now()-interval '72 hours' AND close_at<=now()
            AND timeframe IN ('5m','15m','1h'))
        SELECT jsonb_build_object('timeframe',timeframe,'count',count(*),
          'symbols',count(DISTINCT instrument_id),
          'interior_gaps',count(*) FILTER (WHERE open_at-previous>
            CASE timeframe WHEN '5m' THEN interval '5 minutes'
              WHEN '15m' THEN interval '15 minutes' ELSE interval '1 hour' END),
          'first_at',min(close_at),'last_at',max(close_at),
          'observed_at',max(observed_at)) AS value FROM bars GROUP BY timeframe ORDER BY timeframe
    ) fixed_rows;
SELECT 'TF_DIAGNOSTICS_SECTION:history_revisions_72h';
SELECT jsonb_build_object('section','history_revisions_72h','row',value) FROM (
        WITH bars AS (
          SELECT timeframe,instrument_id,open_at,count(*) AS observations,
            min(observed_at) AS first_at,max(observed_at) AS last_at
          FROM public.closed_bar_observations
          WHERE observed_at>=now()-interval '72 hours' AND timeframe IN ('5m','15m','1h')
          GROUP BY timeframe,instrument_id,open_at),
        members AS (
          SELECT timeframe,instrument_id,sum(observations) AS observations,
            count(*) AS distinct_bars,min(first_at) AS first_at,max(last_at) AS last_at
          FROM bars GROUP BY timeframe,instrument_id)
        SELECT jsonb_build_object('timeframe',timeframe,'count',sum(observations),
          'distinct_bars',sum(distinct_bars),'symbols',count(*),
          'first_at',min(first_at),'last_at',max(last_at)) AS value
        FROM members GROUP BY timeframe ORDER BY timeframe
    ) fixed_rows;
ROLLBACK;

-- Isolated fixed read: risk_current
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SELECT 'TF_DIAGNOSTICS_SECTION:risk_current';
SELECT jsonb_build_object('section','risk_current','row',value) FROM (
        SELECT jsonb_build_object('kill_switch',kill_switch,'version',version,'as_of',as_of)
          AS value FROM public.risk_state ORDER BY as_of DESC LIMIT 1
    ) fixed_rows;
ROLLBACK;

-- Isolated fixed read: runtime_current
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SELECT 'TF_DIAGNOSTICS_SECTION:runtime_current';
SELECT jsonb_build_object('section','runtime_current','row',value) FROM (
        SELECT jsonb_build_object('service',service,'observed_at',observed_at,
          'expires_at',expires_at,'age_seconds',extract(epoch FROM now()-observed_at),
          'fresh',observed_at<=now() AND expires_at>now(),'data',payload) AS value
        FROM public.runtime_observations
        WHERE service IN ('market_gateway','quant_core','execution_core') ORDER BY service
    ) fixed_rows;
ROLLBACK;

-- Isolated fixed read: pipeline_72h.universe_snapshots
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SELECT 'TF_DIAGNOSTICS_SECTION:pipeline_72h';
SELECT jsonb_build_object('section','pipeline_72h','row',value) FROM (SELECT jsonb_build_object('stage','universe_snapshots','count',count(*),'first_at',min(as_of),'last_at',max(as_of)) AS value FROM public.universe_snapshots WHERE as_of>=now()-interval '72 hours') fixed_rows;
ROLLBACK;

-- Isolated fixed read: pipeline_72h.feature_snapshots
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SELECT 'TF_DIAGNOSTICS_SECTION:pipeline_72h';
SELECT jsonb_build_object('section','pipeline_72h','row',value) FROM (SELECT jsonb_build_object('stage','feature_snapshots','count',count(*),'first_at',min(as_of),'last_at',max(as_of)) AS value FROM public.feature_snapshots WHERE as_of>=now()-interval '72 hours') fixed_rows;
ROLLBACK;

-- Isolated fixed read: pipeline_72h.opportunities
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SET LOCAL statement_timeout='10000ms';
SELECT 'TF_DIAGNOSTICS_SECTION:pipeline_72h';
SELECT jsonb_build_object('section','pipeline_72h','row',value) FROM (SELECT jsonb_build_object('stage','opportunities','count',count(*),'first_at',min(observed_at),'last_at',max(observed_at)) AS value FROM public.opportunities WHERE observed_at>=now()-interval '72 hours') fixed_rows;
ROLLBACK;

-- Isolated fixed read: pipeline_72h.decisions
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SELECT 'TF_DIAGNOSTICS_SECTION:pipeline_72h';
SELECT jsonb_build_object('section','pipeline_72h','row',value) FROM (SELECT jsonb_build_object('stage','decisions','count',count(*),'first_at',min(observed_at),'last_at',max(observed_at)) AS value FROM public.decisions WHERE observed_at>=now()-interval '72 hours') fixed_rows;
ROLLBACK;

-- Isolated fixed read: pipeline_72h.execution_intents
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SELECT 'TF_DIAGNOSTICS_SECTION:pipeline_72h';
SELECT jsonb_build_object('section','pipeline_72h','row',value) FROM (SELECT jsonb_build_object('stage','execution_intents','count',count(*),'first_at',min(created_at),'last_at',max(created_at)) AS value FROM public.execution_intents WHERE created_at>=now()-interval '72 hours') fixed_rows;
ROLLBACK;

-- Isolated fixed read: pipeline_72h.outbox_created
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SELECT 'TF_DIAGNOSTICS_SECTION:pipeline_72h';
SELECT jsonb_build_object('section','pipeline_72h','row',value) FROM (SELECT jsonb_build_object('stage','outbox_created','count',count(*),'first_at',min(created_at),'last_at',max(created_at)) AS value FROM public.outbox WHERE created_at>=now()-interval '72 hours') fixed_rows;
ROLLBACK;

-- Isolated fixed read: pipeline_72h.outbox_delivered
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SELECT 'TF_DIAGNOSTICS_SECTION:pipeline_72h';
SELECT jsonb_build_object('section','pipeline_72h','row',value) FROM (SELECT jsonb_build_object('stage','outbox_delivered','count',count(*),'first_at',min(delivered_at),'last_at',max(delivered_at)) AS value FROM public.outbox WHERE delivered_at>=now()-interval '72 hours') fixed_rows;
ROLLBACK;

-- Isolated fixed read: pipeline_72h.orders_updated
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SELECT 'TF_DIAGNOSTICS_SECTION:pipeline_72h';
SELECT jsonb_build_object('section','pipeline_72h','row',value) FROM (SELECT jsonb_build_object('stage','orders_updated','count',count(*),'first_at',min(updated_at),'last_at',max(updated_at)) AS value FROM public.orders WHERE updated_at>=now()-interval '72 hours') fixed_rows;
ROLLBACK;

-- Isolated fixed read: pipeline_72h.fills_observed
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SELECT 'TF_DIAGNOSTICS_SECTION:pipeline_72h';
SELECT jsonb_build_object('section','pipeline_72h','row',value) FROM (SELECT jsonb_build_object('stage','fills_observed','count',count(*),'first_at',min(observed_at),'last_at',max(observed_at)) AS value FROM public.fills WHERE observed_at>=now()-interval '72 hours') fixed_rows;
ROLLBACK;

-- Isolated fixed read: candidate_transitions_72h
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SELECT 'TF_DIAGNOSTICS_SECTION:candidate_transitions_72h';
SELECT jsonb_build_object('section','candidate_transitions_72h','row',value) FROM (
        SELECT jsonb_build_object('state',state,'count',count(*),
          'unique_decisions',count(DISTINCT decision_id),'first_at',min(at),'last_at',max(at))
          AS value FROM public.decision_lifecycle WHERE at>=now()-interval '72 hours'
        GROUP BY state ORDER BY state
    ) fixed_rows;
ROLLBACK;

-- Isolated fixed read: candidate_latest
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SELECT 'TF_DIAGNOSTICS_SECTION:candidate_latest';
SELECT jsonb_build_object('section','candidate_latest','row',value) FROM (
        WITH latest AS (SELECT DISTINCT ON(decision_id) state,at,decision_id
          FROM public.decision_lifecycle ORDER BY decision_id,version DESC)
        SELECT jsonb_build_object('state',state,'count',count(*),'first_at',min(at),
          'last_at',max(at)) AS value FROM latest GROUP BY state ORDER BY state
    ) fixed_rows;
ROLLBACK;

-- Isolated fixed read: rejections_72h
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SELECT 'TF_DIAGNOSTICS_SECTION:rejections_72h';
SELECT jsonb_build_object('section','rejections_72h','row',value) FROM (
        WITH reasons AS (
          SELECT 'lifecycle' AS source,r.reason,l.at AS at
          FROM public.decision_lifecycle l CROSS JOIN LATERAL jsonb_array_elements_text(
            CASE WHEN jsonb_typeof(l.payload::jsonb->'reason_codes')='array'
              THEN l.payload::jsonb->'reason_codes' ELSE '[]'::jsonb END) r(reason)
          WHERE l.at>=now()-interval '72 hours'
          UNION ALL SELECT 'decision',reason_code,observed_at FROM public.decision_reasons
            WHERE observed_at>=now()-interval '72 hours'
          UNION ALL SELECT CASE a.event_type WHEN 'demo.preflight.v1'
            THEN 'preflight' ELSE 'admission' END,r.reason,a.observed_at
          FROM public.audit_log a CROSS JOIN LATERAL jsonb_array_elements_text(
            CASE WHEN jsonb_typeof(a.payload->'reason_codes')='array'
              THEN a.payload->'reason_codes' ELSE '[]'::jsonb END) r(reason)
          WHERE a.event_type IN ('demo.preflight.v1','runtime.admission_rejected.v1')
            AND a.observed_at>=now()-interval '72 hours')
        SELECT jsonb_build_object('source',source,'reason',reason,'count',count(*),
          'first_at',min(at),'last_at',max(at)) AS value FROM reasons
        GROUP BY source,reason ORDER BY count(*) DESC,source,reason LIMIT 64
    ) fixed_rows;
ROLLBACK;

-- Isolated fixed read: outbox_current
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SELECT 'TF_DIAGNOSTICS_SECTION:outbox_current';
SELECT jsonb_build_object('section','outbox_current','row',value) FROM (
        SELECT jsonb_build_object('delivered',delivered_at IS NOT NULL,'count',count(*),
          'first_at',min(created_at),'last_at',max(created_at)) AS value
        FROM public.outbox GROUP BY (delivered_at IS NOT NULL)
    ) fixed_rows;
ROLLBACK;

-- Isolated fixed read: orders_current
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SELECT 'TF_DIAGNOSTICS_SECTION:orders_current';
SELECT jsonb_build_object('section','orders_current','row',value) FROM (
        SELECT jsonb_build_object('state',status,'count',count(*),'first_at',min(updated_at),
          'last_at',max(updated_at)) AS value FROM public.orders GROUP BY status ORDER BY status
    ) fixed_rows;
ROLLBACK;

-- Isolated fixed read: regimes_72h
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SET LOCAL statement_timeout='10000ms';
SELECT 'TF_DIAGNOSTICS_SECTION:regimes_72h';
SELECT jsonb_build_object('section','regimes_72h','row',value) FROM (
        SELECT jsonb_build_object('state',payload->'state'->>'label','count',count(*),
          'distinct_closed_periods',count(DISTINCT payload->'frame'->>'source_at'),
          'first_at',min(as_of),'last_at',max(as_of)) AS value
        FROM public.regime_snapshots WHERE as_of>=now()-interval '72 hours'
        GROUP BY payload->'state'->>'label' ORDER BY count(*) DESC LIMIT 16
    ) fixed_rows;
ROLLBACK;

-- Isolated fixed read: audit_72h
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SELECT 'TF_DIAGNOSTICS_SECTION:audit_72h';
SELECT jsonb_build_object('section','audit_72h','row',value) FROM (
        SELECT jsonb_build_object('event',event_type,'count',count(*),'first_at',min(observed_at),
          'last_at',max(observed_at)) AS value FROM public.audit_log
        WHERE observed_at>=now()-interval '72 hours' AND event_type IN (
          'runtime.candidate.v1','runtime.forecast.v1','runtime.closed_result.v1',
          'runtime.demo.activated.v1','edge.outcome.v1','execution.intent_expired.v1',
          'runtime.admission_rejected.v1')
        GROUP BY event_type ORDER BY event_type
    ) fixed_rows;
ROLLBACK;

-- Isolated fixed read: history_72h
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SET LOCAL statement_timeout='10000ms';
SELECT 'TF_DIAGNOSTICS_SECTION:history_72h';
SELECT jsonb_build_object('section','history_72h','row',value) FROM (
        WITH bars AS (
          SELECT instrument_id,timeframe,open_at,close_at,observed_at,
            lag(open_at) OVER (PARTITION BY instrument_id,timeframe ORDER BY open_at) AS previous
          FROM public.closed_bars WHERE close_at>=now()-interval '72 hours' AND close_at<=now()
            AND timeframe IN ('5m','15m','1h'))
        SELECT jsonb_build_object('timeframe',timeframe,'count',count(*),
          'symbols',count(DISTINCT instrument_id),
          'interior_gaps',count(*) FILTER (WHERE open_at-previous>
            CASE timeframe WHEN '5m' THEN interval '5 minutes'
              WHEN '15m' THEN interval '15 minutes' ELSE interval '1 hour' END),
          'first_at',min(close_at),'last_at',max(close_at),
          'observed_at',max(observed_at)) AS value FROM bars GROUP BY timeframe ORDER BY timeframe
    ) fixed_rows;
ROLLBACK;

-- Isolated fixed read: history_revisions_72h
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';
SET LOCAL idle_in_transaction_session_timeout='3s';
SET LOCAL search_path=pg_catalog,public;
SELECT 'TF_DIAGNOSTICS_SECTION:role_check';

SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
 AND r.rolname='tf_diagnostics' AND r.rolcanlogin AND NOT r.rolsuper
 AND NOT r.rolinherit AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit BETWEEN 1 AND 2
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
 AND NOT pg_catalog.has_database_privilege(current_user,current_database(),'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND pg_catalog.has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
   AND c.relkind IN ('r','p','v','m','f')
   AND pg_catalog.has_table_privilege(current_user,c.oid,
       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind='S'
   AND pg_catalog.has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE'))
) THEN 1 ELSE 0 END
;
SET LOCAL statement_timeout='10000ms';
SELECT 'TF_DIAGNOSTICS_SECTION:history_revisions_72h';
SELECT jsonb_build_object('section','history_revisions_72h','row',value) FROM (
        WITH bars AS (
          SELECT timeframe,instrument_id,open_at,count(*) AS observations,
            min(observed_at) AS first_at,max(observed_at) AS last_at
          FROM public.closed_bar_observations
          WHERE observed_at>=now()-interval '72 hours' AND timeframe IN ('5m','15m','1h')
          GROUP BY timeframe,instrument_id,open_at),
        members AS (
          SELECT timeframe,instrument_id,sum(observations) AS observations,
            count(*) AS distinct_bars,min(first_at) AS first_at,max(last_at) AS last_at
          FROM bars GROUP BY timeframe,instrument_id)
        SELECT jsonb_build_object('timeframe',timeframe,'count',sum(observations),
          'distinct_bars',sum(distinct_bars),'symbols',count(*),
          'first_at',min(first_at),'last_at',max(last_at)) AS value
        FROM members GROUP BY timeframe ORDER BY timeframe
    ) fixed_rows;
ROLLBACK;
