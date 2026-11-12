#!/usr/bin/python3 -I
"""Argument-free root diagnostic snapshot; install this one file, never import app code.

The command construction, SQL, endpoints, filesystem paths and projections are fixed.
All Docker/container/application output is untrusted. No raw errors or log text escape.
"""

import json
import math
import os
import re
import selectors
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Never

ENV = {"PATH": "/usr/bin:/bin", "HOME": "/", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"}
SERVICES = (
    "market_gateway",
    "quant_core",
    "dashboard",
    "postgres",
    "caddy",
)
APPLICATIONS = ("market_gateway", "quant_core", "dashboard")
PORTS = {"market_gateway": 8101, "quant_core": 8102}
UNKNOWN = "UNKNOWN"
CAP = 96_000

# Exact code membership, never a token-shaped regex or raw exception/message field.
CODES = frozenset(
    """
UNKNOWN OK READY NOT_READY STARTING STARTUP DISPATCH_COMPLETE DISPATCH_STALE
VOL_COMPRESSION DISORDER
BROAD_ANALYSIS_NOT_READY REGIME_UNKNOWN REGIME_TRANSITION DEMO_ENTRIES_DISABLED
ACCOUNT_RECONCILIATION_STALE ACCOUNT_RECONCILIATION_UNAVAILABLE WAITING_CLOSED_BAR
ANALYSIS_STALE QUANT_TICK_STALE DISPATCH_NOT_OBSERVED CLOSED_BARS_MISSING
STALE_OR_INVALID_INPUT STALE_INPUT STALE_UNIVERSE CHANGE_POINT
LIQUIDITY_STRESS FUNDING_STRESS RETURN_SHOCK VOLATILITY_STRESS COMPRESSED_VOLATILITY
TREND_CONFIRMED RANGE_CONFIRMED HISTORY_STARTING HISTORY_SYNCED HISTORY_PARTIAL
HISTORY_CYCLE_CHANGED
KEY_PERMISSIONS_UNVERIFIED EXPLICIT_TESTNET_EXECUTION_REQUIRED
DIRECTIONAL_BREADTH RANGE_EVIDENCE HYSTERESIS OVERLAP_REVISION HISTORY_RESTORED DATA_GAP
ANALYZED QUANT_LOAD HISTORY_PARSE REGIME_ANALYSIS REGIME_STORAGE QUANT_STORAGE_OR_INPUT_UNAVAILABLE
MARKET_HANDOFF ALPHA_INPUTS ALPHA_EVALUATION RUNTIME_STORAGE HISTORY_SWEEP
UNIVERSE_DISCOVERY UNIVERSE_UNAVAILABLE UNIVERSE_STALE WAITING_UNIVERSE
WAITING_HISTORY_SWEEP HISTORY_SWEEP_COMPLETE HISTORY_UNAVAILABLE HISTORY_INCOMPLETE
CONTIGUOUS_BARS_MISSING INSUFFICIENT_HISTORY BTC_HISTORY_MISSING LOW_COVERAGE
STRESS_DATA_MISSING CLOCK_UNSAFE CLOCK_PROBE_SLOW DATA_UNAVAILABLE FUTURE_INPUT
MARKET_UNAVAILABLE MARKET_HANDOFF_UNAVAILABLE MARKET_HANDOFF_INVALID MARKET_HANDOFF_CAPACITY
BOOK_UNAVAILABLE FLOW_UNAVAILABLE PREVIOUS_FLOW_UNAVAILABLE FEATURE_BAR_UNAVAILABLE
BAR_RECOVERY_REQUIRED ALPHA_INPUT_INVALID CANONICAL_DATA_NOT_READY STORAGE_ADMISSION_HALTED
CANONICAL_STORAGE_UNAVAILABLE QUANT_ANALYSIS_UNAVAILABLE QUANT_STORAGE_UNAVAILABLE
CANONICAL_1M_MISSING CANONICAL_1M_STALE CANONICAL_1M_GAP CANONICAL_1M_UNKNOWN
CANONICAL_5M_MISSING CANONICAL_5M_STALE CANONICAL_5M_GAP CANONICAL_5M_UNKNOWN
CANONICAL_15M_MISSING CANONICAL_15M_STALE CANONICAL_15M_GAP CANONICAL_15M_UNKNOWN
CANONICAL_1H_MISSING CANONICAL_1H_STALE CANONICAL_1H_GAP CANONICAL_1H_UNKNOWN
CANONICAL_4H_MISSING CANONICAL_4H_STALE CANONICAL_4H_GAP CANONICAL_4H_UNKNOWN
CANONICAL_12H_MISSING CANONICAL_12H_STALE CANONICAL_12H_GAP CANONICAL_12H_UNKNOWN
CANONICAL_1D_MISSING CANONICAL_1D_STALE CANONICAL_1D_GAP CANONICAL_1D_UNKNOWN
CANONICAL_REPAIR_PENDING
ACCOUNT_INCOMPLETE ACCOUNT_STALE ACCOUNT_UNKNOWN BALANCE_LIMIT BETA_LIMIT CLUSTER_LIMIT
COOLDOWN CORRELATION_UNKNOWN DAILY_LOSS_LIMIT DB_UNAVAILABLE DELTA_LIMIT FACTOR_UNKNOWN
FUTURE_RISK_STATE GROSS_LIMIT HEAT_LIMIT KILL_SWITCH LEVERAGE_LIMIT LIQUIDITY_UNSAFE
LOSS_STREAK_LIMIT MAX_POSITIONS POLICY_MISMATCH POSITION_STALE PROTECTION_NOT_READY
QUANT_NOT_ELIGIBLE QUANT_SIZE_EXCEEDED REQUEST_STALE ROUNDING_OR_FILTER_LIMIT SIDE_LIMIT
SYMBOL_LIMIT UTC_DAY_MISMATCH
CONTEXT_CHECK_REQUIRED EXECUTION_HALTED EXPLICIT_DEMO_ENABLE_REQUIRED
EXPLICIT_RISK_AND_CONTEXT_REQUIRED EXPLICIT_RISK_AND_CONTEXT_REQUIRED
ACCOUNT_RECONCILIATION_REQUIRED ACCOUNT_EVIDENCE_STALE UNKNOWN_EXCHANGE_ORDER
PUBLIC_MARKET_RECOVERY_REQUIRED FLAT_ACCOUNT_REQUIRED INCOME_HISTORY_INCOMPLETE
INCOME_HISTORY_INVALID RISK_FACTOR_UNAVAILABLE DB_UNAVAILABLE DB_STATEMENT_TIMEOUT
DB_LOCK_TIMEOUT DB_PERMISSION_DENIED DB_DEADLOCK DB_POOL_TIMEOUT DB_CONSTRAINT_REJECTED
PRIVATE_STREAM_NOT_READY UNRECONCILED_EXCHANGE_ORDER TESTNET_TRIAL_LIMIT
ACK_UNKNOWN_RECONCILIATION_REQUIRED EXCHANGE_REJECTED ENTRY_HALTED STORAGE_UNAVAILABLE
TRANSPORT_UNAVAILABLE INPUT_INVALID EXCHANGE_TRANSPORT_UNKNOWN EXCHANGE_AUTH_FAILED
EXCHANGE_RATE_LIMITED EXCHANGE_TIME_INVALID EXCHANGE_SIGNATURE_INVALID
EXCHANGE_RESPONSE_INVALID EXCHANGE_RESPONSE_TOO_LARGE RATE_LIMITED MARGIN_INSUFFICIENT
API_KEY_INVALID API_KEY_PERMISSION_OR_IP_DENIED SIGNATURE_INVALID ORDER_REJECTED
UNPROVEN VALIDATED UNCALIBRATED NOT_EVALUATED TRIAL_CANNOT_MEET_SAMPLE_MINIMUM
PROBABILITY_FORECASTS_UNAVAILABLE INSUFFICIENT_TRAIN_SAMPLES INSUFFICIENT_OOS_SAMPLES
TESTNET_TRIAL_BUDGET_EXHAUSTED TESTNET_TRIAL_SETTLEMENT_REQUIRED TESTNET_TRIAL_LOSS_LIMIT
OOS_EMBARGO_OR_UNSETTLED_TRAIN
NO_ARMED_CANDIDATES CANDIDATE_EXPIRED INSUFFICIENT_STATISTICAL_EVIDENCE VOLUME_UNAVAILABLE
TRAIN_INSUFFICIENT_SAMPLES OOS_INSUFFICIENT_SAMPLES TRAIN_STATS_STALE OOS_STATS_STALE
TRAIN_PROBABILITY_UNCERTAIN OOS_PROBABILITY_UNCERTAIN OOS_NOT_PURGED OOS_UNPROVEN
OOS_STRESS_UNPROVEN MARKET_SHIFT EDGE_UNKNOWN EDGE_BELOW_POLICY LIVE_LOCKED DISABLED
EVIDENCE_ENVIRONMENT_MISMATCH FEE_UNKNOWN COST_UNCERTAINTY_UNKNOWN COST_STALE
DEMO_ADMISSION_HALTED DATA_NOT_READY MARKET_EVIDENCE_MISSING MICRO_EVIDENCE_MISSING
DATA_QUALITY_BLOCKED BOOK_NOT_READY MARKET_EVIDENCE_STALE TRIGGER_NOT_CROSSED
TESTNET_TRIAL_PUBLISHED APPROVED_PUBLISHED RISK_REJECTED CANDIDATE_INPUT_MISSING
CANDIDATE_INPUT_MISMATCH DEMO_INSTRUMENT_UNAVAILABLE
STALE_BARS STALE_REGIME REGIME_NOT_ALLOWED FEATURE_UNAVAILABLE RELATIVE_STRENGTH_UNKNOWN
TREND_MISALIGNED RELATIVE_STRENGTH_OPPOSED SPREAD_GATE LIQUIDITY_GATE FLOW_OPPOSED
INSTRUMENT_CAPACITY_EXHAUSTED BREAKOUT_INVALIDATED VOLATILITY_NOT_EXPANDING WEAK_PARTICIPATION
TRIGGER_ALREADY_PASSED PULLBACK_TOO_DEEP STOP_ALREADY_BREACHED DUPLICATE_SETUP
SETUP_CAPACITY_EXHAUSTED STRONG_TREND_FADE MICROSTRUCTURE_DESYNC FLOW_NOT_STABILIZED
NO_SWEEP_RECLAIM STRETCH_GATE WEAK_WICK_REJECTION RECLAIM_INVALIDATED INVALID_TARGET_GEOMETRY
RECONCILIATION_REQUIRED PROTECTION_REQUIRED PROTECTION_NOT_READY RISK_KILL_SWITCH
TREND_EXPANSION RANGE TRANSITION STRESS DISCOVERED WATCHING ARMED VALIDATING
APPROVED REJECTED EXPIRED NO_TRADE CANCELLED QUEUED EXECUTING MANAGING CLOSED
NEW ACK_UNKNOWN ACCEPTED PARTIALLY_FILLED FILLED CANCELED
MOMENTUM REVERSAL TREND_REQUIRED RANGE_REQUIRED QUALITY_LOW SPREAD_TOO_WIDE
LIQUIDITY_INSUFFICIENT FLOW_NOT_CONFIRMED TRIGGER_NOT_READY FUNDING_UNAVAILABLE
CONFIDENCE_LOW EDGE_UNAVAILABLE EDGE_INSUFFICIENT RISK_LIMIT EXPIRED_INPUT
RECONCILIATION DISPATCH PROTECTION ACCOUNT PRIVATE_STREAM PUBLIC_MARKET
""".split()
)
BOOLS = frozenset(
    """
alive healthy ready live orders_enabled data_ready analysis_ready execution_ready
execution_capability stream_ready clock_safe measured analysis_coverage_ready
history_complete entries_enabled trial_enabled protection_ready connected transport_ready
account_ready storage_ready admission_ready
trial_can_self_validate execution_authorized kill_switch
""".split()
)
NUMBERS = frozenset(
    """
eligible covered rest_market_covered analysis_market_covered analysis_volume_covered
quote_attempts quote_failures
subscriptions omitted watched deep protected processed tick_ms history_query_ms history_rows
loop_lag_ms cycle_ms cycle_count error_count history_total history_completed history_attempted
history_failed history_pending history_inflight history_current_members history_current_coverage
history_elapsed_ms history_last_member_ms selected_deep_count ready_deep_count analysis_symbols
evaluated_count candidate_count transport_age_seconds connection_attempts reconnects
reconciled_age_seconds account_age_seconds last_close_code
approvals train_effective_samples oos_effective_samples train_required oos_required
train_missing oos_missing minimum_total_effective_samples maximum_trial_entries version
""".split()
)
TIMES = frozenset(
    """
last_progress_at last_success_at last_error_at last_analysis_at last_analysis_source_at
last_evaluation_at last_source_at reconciled_at last_failure_at last_ping_at last_pong_at
history_last_failure_at as_of source_at observed_at expires_at first_at last_at oldest_at
newest_at sampled_at started_at
""".split()
)
CODE_FIELDS = frozenset(
    (
        "reason",
        "stage",
        "last_error",
        "last_error_stage",
        "last_failure",
        "history_reason",
        "history_last_failure_reason",
        "quote_last_reason",
        "regime_label",
        "last_reason",
        "proof_state",
    )
)
CODE_LISTS = (
    "reasons",
    "reason_codes",
    "blockers",
    "regime_reasons",
    "bootstrap_blockers",
    "edge_reasons",
)
DIAGNOSTIC_PATHS = frozenset(
    (
        "/fapi/v1/ping",
        "/fapi/v1/time",
        "/fapi/v3/account",
        "/fapi/v3/balance",
        "/fapi/v3/positionRisk",
        "/fapi/v1/accountConfig",
        "/fapi/v1/positionSide/dual",
        "/fapi/v1/openOrders",
        "/fapi/v1/openAlgoOrders",
        "/fapi/v1/income",
        "/fapi/v1/userTrades",
    )
)
PHASES = ("CONNECTION", "DNS", "CONNECT", "TLS", "SEND", "RESPONSE_HEADERS", "RESPONSE_BODY")
FAILURES = ("IO", "HTTP", "TIMEOUT", "WATCHDOG")


class ReadUnavailable(Exception):
    """Deliberately carries no process output or original exception text."""

    def __init__(self, failure: str = "DB_READ_UNAVAILABLE", failed_section: str | None = None):
        super().__init__()
        self.failure = failure
        self.failed_section = failed_section


def database_failure(stdout: bytes, stderr: bytes) -> ReadUnavailable:
    """Reduce bounded untrusted output to fixed SQLSTATE/stage enums only."""
    section = None
    for line in stdout.splitlines():
        for name in ("role_check", *QUERIES):
            if line == ("TF_DIAGNOSTICS_SECTION:" + name).encode("ascii"):
                section = name
    failure = "DB_READ_UNAVAILABLE"
    for line in stderr.splitlines():
        match = re.fullmatch(rb"(?:ERROR|FATAL):\s+([0-9A-Z]{5})\s*", line)
        if match:
            state = match[1].decode("ascii")
            failure = {
                "42501": "DB_PERMISSION_DENIED",
                "28000": "DB_AUTHENTICATION_FAILED",
                "28P01": "DB_AUTHENTICATION_FAILED",
                "3D000": "DB_DATABASE_MISSING",
                "42P01": "DB_RELATION_MISSING",
                "42703": "DB_SCHEMA_MISMATCH",
                "57014": "DB_QUERY_CANCELED",
                "55P03": "DB_LOCK_UNAVAILABLE",
                "53300": "DB_CONNECTION_LIMIT",
            }.get(state, "DB_QUERY_FAILED")
            if state == "22012" and section == "role_check":
                failure = "DB_ROLE_POLICY_REJECTED"
    if section is None:
        if b'role "tf_diagnostics" does not exist' in stderr:
            failure = "DB_ROLE_MISSING"
        elif any(
            marker in stderr
            for marker in (
                b'Peer authentication failed for user "tf_diagnostics"',
                b'password authentication failed for user "tf_diagnostics"',
                b"no password supplied",
                b"no pg_hba.conf entry",
            )
        ):
            failure = "DB_AUTHENTICATION_FAILED"
    return ReadUnavailable(failure, section)


class DeadlineExpired(BaseException):
    """Bypass all recoverable per-boundary Exception handlers at the global limit."""


def expire_deadline(*_: Any) -> Never:
    raise DeadlineExpired()


def run_bounded(argv: list[str], *, timeout: float, limit: int) -> bytes:
    """Private process boundary: fixed env/cwd, no shell, capped bytes and wall time."""
    started = time.monotonic()
    collect_log_stderr = argv[:2] == ["/usr/bin/docker", "logs"]
    process = subprocess.Popen(
        argv,
        cwd="/",
        env=ENV,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    assert process.stdout is not None and process.stderr is not None
    stdout = bytearray()
    stderr = bytearray()
    total = 0
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ, True)
            selector.register(process.stderr, selectors.EVENT_READ, False)
            while selector.get_map():
                remaining = timeout - (time.monotonic() - started)
                if remaining <= 0:
                    raise ReadUnavailable()
                for key, _ in selector.select(min(remaining, 0.1)):
                    chunk = os.read(key.fd, min(65536, limit + 1))
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    total += len(chunk)
                    if total > limit:
                        raise ReadUnavailable()
                    if key.data or collect_log_stderr:
                        stdout.extend(chunk)
                    elif "--username=tf_diagnostics" in argv:
                        stderr.extend(chunk)
            remaining = timeout - (time.monotonic() - started)
            if remaining <= 0:
                raise ReadUnavailable()
            if process.wait(timeout=remaining) != 0:
                if "--username=tf_diagnostics" in argv:
                    raise database_failure(bytes(stdout), bytes(stderr))
                raise ReadUnavailable()
        return bytes(stdout)
    except (OSError, subprocess.SubprocessError) as error:
        raise ReadUnavailable() from error
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()
        process.stdout.close()
        process.stderr.close()


def safe_number(value: Any) -> int | float | None:
    if type(value) in (int, float) and 0 <= value <= 10**15 and math.isfinite(value):
        return float(value) if type(value) is float else int(value)
    return None


def safe_time(value: Any) -> str | None:
    if isinstance(value, str) and re.fullmatch(
        r"20[0-9]{2}-[01][0-9]-[0-3][0-9][T ][0-2][0-9]:[0-5][0-9]:[0-5][0-9]"
        r"(?:\.[0-9]{1,9})?(?:Z|[+-][0-2][0-9]:[0-5][0-9])",
        value,
    ):
        return value
    return None


def safe_code(value: Any) -> str:
    return value if isinstance(value, str) and value in CODES else UNKNOWN


def project_requests(rows: Any) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        return []
    projected: list[dict[str, Any]] = []
    for row in rows[:32]:
        if not isinstance(row, dict) or row.get("path") not in DIAGNOSTIC_PATHS:
            continue
        item: dict[str, Any] = {
            "method": "GET" if row.get("method") == "GET" else UNKNOWN,
            "path": row["path"],
            "reason": safe_code(row.get("reason")),
            "duration_ms": safe_number(row.get("duration_ms")),
        }
        if "phase" in row:
            item["phase"] = row["phase"] if row["phase"] in PHASES else UNKNOWN
        if "failure" in row:
            item["failure"] = row["failure"] if row["failure"] in FAILURES else UNKNOWN
        projected.append(item)
    return projected


def project_telemetry(diagnostic_value: Any, depth: int = 0) -> dict[str, Any]:
    if not isinstance(diagnostic_value, dict) or depth >= 4:
        return {}
    result: dict[str, Any] = {}
    for key in BOOLS & diagnostic_value.keys():
        result[key] = diagnostic_value[key] if type(diagnostic_value[key]) is bool else None
    for key in NUMBERS & diagnostic_value.keys():
        result[key] = safe_number(diagnostic_value[key])
    for key in TIMES & diagnostic_value.keys():
        result[key] = safe_time(diagnostic_value[key])
    for key in CODE_FIELDS & diagnostic_value.keys():
        result[key] = safe_code(diagnostic_value[key])
    for key in CODE_LISTS:
        if isinstance(diagnostic_value.get(key), list):
            result[key] = sorted({safe_code(v) for v in diagnostic_value[key][:64]})
    if diagnostic_value.get("mode") in ("paper", "testnet", "live"):
        result["mode"] = diagnostic_value["mode"]
    for key, allowed in (
        ("evidence_environment", ("paper", "testnet", "live", "replay", "disabled")),
        ("calibration_label", ("calibrated", "uncalibrated")),
    ):
        if key in diagnostic_value:
            result[key] = diagnostic_value[key] if diagnostic_value[key] in allowed else UNKNOWN
    if "failed_stage" in diagnostic_value:
        value = diagnostic_value["failed_stage"]
        result["failed_stage"] = value if value in DIAGNOSTIC_PATHS else None
    if "account_requests" in diagnostic_value:
        result["account_requests"] = project_requests(diagnostic_value["account_requests"])
    counts = diagnostic_value.get("reason_counts")
    if isinstance(counts, dict):
        result["reason_counts"] = {
            code: safe_number(counts[code]) for code in sorted(CODES & counts.keys())
        }
    rejected = diagnostic_value.get("input_rejections")
    if isinstance(rejected, list):
        result["input_rejections"] = {}
        for row in rejected[:64]:
            code = safe_code(row.get("reason")) if isinstance(row, dict) else UNKNOWN
            result["input_rejections"][code] = result["input_rejections"].get(code, 0) + 1
    for key in ("protection", "private_stream", "autonomous_decisions", "statistical_evidence"):
        nested = diagnostic_value.get(key)
        if isinstance(nested, dict):
            result[key] = project_telemetry(nested, depth + 1)
    return result


PROBE_RULES = (
    "import math,re\n"
    + "CODES="
    + repr(sorted(CODES))
    + "\nBOOLS="
    + repr(sorted(BOOLS))
    + "\nNUMBERS="
    + repr(sorted(NUMBERS))
    + "\nTIMES="
    + repr(sorted(TIMES))
    + "\nCODE_FIELDS="
    + repr(sorted(CODE_FIELDS))
    + "\nCODE_LISTS="
    + repr(CODE_LISTS)
    + "\nPATHS="
    + repr(sorted(DIAGNOSTIC_PATHS))
    + "\nPHASES="
    + repr(PHASES)
    + "\nFAILURES="
    + repr(FAILURES)
    + "\n"
    + r"""
def sanitize(data,depth=0):
    if not isinstance(data,dict) or depth>=4: return {}
    out={}
    for key,value in data.items():
        if key in BOOLS: out[key]=value if type(value) is bool else None
        elif key in NUMBERS:
            out[key]=value if type(value) in (int,float) and math.isfinite(value) \
                and 0<=value<=10**15 else None
        elif key in TIMES:
            out[key]=value if isinstance(value,str) and re.fullmatch(
                r'20[0-9]{2}-[01][0-9]-[0-3][0-9][T ][0-2][0-9]:[0-5][0-9]:[0-5][0-9]'
                r'(?:\.[0-9]{1,9})?(?:Z|[+-][0-2][0-9]:[0-5][0-9])',value) else None
        elif key in CODE_FIELDS: out[key]=value if isinstance(value,str) \
            and value in CODES else 'UNKNOWN'
        elif key in CODE_LISTS and isinstance(value,list):
            out[key]=[v if isinstance(v,str) and v in CODES else 'UNKNOWN' for v in value[:64]]
        elif key in ('protection','private_stream','autonomous_decisions','statistical_evidence'):
            out[key]=sanitize(value,depth+1)
        elif key=='reason_counts' and isinstance(value,dict):
            out[key]={k:v for k,v in value.items() if k in CODES and type(v) is int
                      and 0<=v<=10**15}
        elif key in ('mode','evidence_environment'):
            out[key]=value if value in ('paper','testnet','live','replay','disabled') else 'UNKNOWN'
        elif key=='calibration_label':
            out[key]=value if value in ('calibrated','uncalibrated') else 'UNKNOWN'
        elif key=='failed_stage': out[key]=value if value in PATHS else None
        elif key=='account_requests' and isinstance(value,list):
            out[key]=[]
            for row in value[:32]:
                if not isinstance(row,dict) or row.get('path') not in PATHS: continue
                record={k:v for k,v in row.items() if k in ('method','path','reason',
                        'duration_ms','phase','failure')}
                for name,allowed in (('method',('GET',)),('reason',CODES),
                                     ('phase',PHASES),('failure',FAILURES)):
                    if name in record and record[name] not in allowed: record[name]='UNKNOWN'
                duration=record.get('duration_ms')
                record['duration_ms']=duration if type(duration) in (int,float) \
                    and math.isfinite(duration) and 0<=duration<=10**15 else None
                out[key].append(record)
    return out
"""
)
PROBE_COMMON = (
    PROBE_RULES
    + r"""
import http.client,json,os,signal,stat
signal.alarm(9)
def read(path,token=None):
    connection=http.client.HTTPConnection('127.0.0.1',PORT,timeout=3)
    try:
        headers={} if token is None else {'Authorization':'Bearer '+token}
        connection.request('GET',path,headers=headers)
        response=connection.getresponse()
        body=response.read(4_000_001)
        if len(body)>4_000_000: return {'status':'UNKNOWN'}
        data=json.loads(body)
        if not isinstance(data,dict): return {'status':'UNKNOWN'}
        data={'status':sanitize(data.get('status',{}))} if path=='/market-state' else sanitize(data)
        return {'http_status':response.status,'data':data}
    except Exception:
        return {'status':'UNKNOWN'}
    finally:
        connection.close()
result={'readiness':read('/readiness')}
"""
)
PROBE_ANALYSIS = r"""
try:
    if stat.S_ISLNK(os.lstat('/run/secrets').st_mode): raise ValueError()
    descriptor=os.open('/run/secrets/gateway_token',os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(descriptor,'rb') as stream:
        info=os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or not 43<=info.st_size<=4096: raise ValueError()
        token=stream.read(4097).decode('ascii').strip()
    if not 43<=len(token)<=4096 or any(ord(c)<33 or ord(c)>126 for c in token): raise ValueError()
    result['analysis']=read('/market-state',token)
except Exception:
    result['analysis']={'status':'UNKNOWN'}
"""
PROBE_GATEWAY = "PORT=8101\n" + PROBE_COMMON + PROBE_ANALYSIS + "\nprint(json.dumps(result))\n"
PROBES = {
    "market_gateway": PROBE_GATEWAY,
    "quant_core": "PORT=8102\n" + PROBE_COMMON + "\nprint(json.dumps(result))\n",
}

QUERIES = {
    "risk_current": """
        SELECT jsonb_build_object('kill_switch',kill_switch,'version',version,'as_of',as_of)
          AS value FROM public.risk_state ORDER BY as_of DESC LIMIT 1
    """,
    "runtime_current": """
        SELECT jsonb_build_object('service',service,'observed_at',observed_at,
          'expires_at',expires_at,'age_seconds',extract(epoch FROM now()-observed_at),
          'fresh',observed_at<=now() AND expires_at>now(),'data',payload) AS value
        FROM public.runtime_observations
        WHERE service IN ('market_gateway','quant_core','execution_core') ORDER BY service
    """,
    "pipeline_72h": """
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
    """,
    "candidate_transitions_72h": """
        SELECT jsonb_build_object('state',state,'count',count(*),
          'unique_decisions',count(DISTINCT decision_id),'first_at',min(at),'last_at',max(at))
          AS value FROM public.decision_lifecycle WHERE at>=now()-interval '72 hours'
        GROUP BY state ORDER BY state
    """,
    "candidate_latest": """
        WITH latest AS (SELECT DISTINCT ON(decision_id) state,at,decision_id
          FROM public.decision_lifecycle ORDER BY decision_id,version DESC)
        SELECT jsonb_build_object('state',state,'count',count(*),'first_at',min(at),
          'last_at',max(at)) AS value FROM latest GROUP BY state ORDER BY state
    """,
    "rejections_72h": """
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
    """,
    "outbox_current": """
        SELECT jsonb_build_object('delivered',delivered_at IS NOT NULL,'count',count(*),
          'first_at',min(created_at),'last_at',max(created_at)) AS value
        FROM public.outbox GROUP BY (delivered_at IS NOT NULL)
    """,
    "orders_current": """
        SELECT jsonb_build_object('state',status,'count',count(*),'first_at',min(updated_at),
          'last_at',max(updated_at)) AS value FROM public.orders GROUP BY status ORDER BY status
    """,
    "regimes_72h": """
        SELECT jsonb_build_object('state',payload->'state'->>'label','count',count(*),
          'distinct_closed_periods',count(DISTINCT payload->'frame'->>'source_at'),
          'first_at',min(as_of),'last_at',max(as_of)) AS value
        FROM public.regime_snapshots WHERE as_of>=now()-interval '72 hours'
        GROUP BY payload->'state'->>'label' ORDER BY count(*) DESC LIMIT 16
    """,
    "audit_72h": """
        SELECT jsonb_build_object('event',event_type,'count',count(*),'first_at',min(observed_at),
          'last_at',max(observed_at)) AS value FROM public.audit_log
        WHERE observed_at>=now()-interval '72 hours' AND event_type IN (
          'runtime.candidate.v1','runtime.forecast.v1','runtime.closed_result.v1',
          'runtime.demo.activated.v1','edge.outcome.v1','execution.intent_expired.v1',
          'runtime.admission_rejected.v1')
        GROUP BY event_type ORDER BY event_type
    """,
    "history_72h": """
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
    """,
    "history_revisions_72h": """
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
    """,
}

ROLE_CHECK = """
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
"""
SQL_PREFIX = (
    "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;\n"
    "SET LOCAL statement_timeout='1200ms'; SET LOCAL lock_timeout='300ms';\n"
    "SET LOCAL idle_in_transaction_session_timeout='3s';\n"
    "SET LOCAL search_path=pg_catalog,public;\n"
    "SELECT 'TF_DIAGNOSTICS_SECTION:role_check';\n" + ROLE_CHECK + ";\n"
)
SQL = (
    SQL_PREFIX
    + "\n".join(
        "SELECT 'TF_DIAGNOSTICS_SECTION:"
        + name
        + "';\nSELECT jsonb_build_object('section','"
        + name
        + "','row',value) FROM ("
        + query
        + ") fixed_rows;"
        for name, query in QUERIES.items()
    )
    + "\nROLLBACK;"
)

# Same approved table/time aggregates as pipeline_72h, each under its own limit.
PIPELINE_READS = (
    ("universe_snapshots", "universe_snapshots", "as_of"),
    ("feature_snapshots", "feature_snapshots", "as_of"),
    ("opportunities", "opportunities", "observed_at"),
    ("decisions", "decisions", "observed_at"),
    ("execution_intents", "execution_intents", "created_at"),
    ("outbox_created", "outbox", "created_at"),
    ("outbox_delivered", "outbox", "delivered_at"),
    ("orders_updated", "orders", "updated_at"),
    ("fills_observed", "fills", "observed_at"),
)
SLOW_READS = frozenset(
    {
        ("pipeline_72h", "opportunities"),
        ("regimes_72h", None),
        ("history_72h", None),
        ("history_revisions_72h", None),
    }
)


def isolated_reads() -> list[tuple[str, str | None, str]]:
    reads = []
    for section, query in QUERIES.items():
        fragments = [(None, query)]
        if section == "pipeline_72h":
            fragments = [
                (
                    stage,
                    "SELECT jsonb_build_object('stage','"
                    + stage
                    + "','count',count(*),'first_at',min("
                    + column
                    + "),'last_at',max("
                    + column
                    + ")) AS value FROM public."
                    + table
                    + " WHERE "
                    + column
                    + ">=now()-interval '72 hours'",
                )
                for stage, table, column in PIPELINE_READS
            ]
        for stage, fragment in fragments:
            # Only four measured production aggregates; the role gate keeps 1200ms.
            budget = (
                "SET LOCAL statement_timeout='10000ms';\n" if (section, stage) in SLOW_READS else ""
            )
            statement = (
                SQL_PREFIX
                + budget
                + "SELECT 'TF_DIAGNOSTICS_SECTION:"
                + section
                + "';\nSELECT jsonb_build_object('section','"
                + section
                + "','row',value) FROM ("
                + fragment
                + ") fixed_rows;\nROLLBACK;"
            )
            reads.append((section, stage, statement))
    return reads


def database_command(container_id: str, statement: str) -> list[str]:
    return [
        "/usr/bin/docker",
        "exec",
        "--user",
        "postgres",
        "--workdir",
        "/",
        container_id,
        "/usr/bin/env",
        "-i",
        "PATH=/usr/bin:/bin",
        "HOME=/",
        "LC_ALL=C.UTF-8",
        "PGCONNECT_TIMEOUT=2",
        "PGOPTIONS=-c default_transaction_read_only=on",
        "PGPASSFILE=/dev/null",
        "PGSERVICEFILE=/dev/null",
        "PSQLRC=/dev/null",
        "/usr/bin/psql",
        "-X",
        "-w",
        "-q",
        "-A",
        "-t",
        "-v",
        "ON_ERROR_STOP=1",
        "-v",
        "VERBOSITY=sqlstate",
        "--host=/var/run/postgresql",
        "--port=5432",
        "--username=tf_diagnostics",
        "--dbname=tradeforge",
        "--command",
        statement,
    ]


def isolated_database_snapshot(
    runner: Callable[..., bytes],
    container_id: str,
) -> dict[str, Any]:
    sections: dict[str, Any] = {name: {"status": "UNKNOWN", "rows": None} for name in QUERIES}
    pipeline_rows = []
    for section, stage, statement in isolated_reads():
        try:
            raw = runner(database_command(container_id, statement), timeout=12, limit=256_000)
            lines = raw.splitlines()
            if (
                len(raw) > 256_000
                or not 3 <= len(lines) <= 67
                or lines[:3]
                != [
                    b"TF_DIAGNOSTICS_SECTION:role_check",
                    b"1",
                    ("TF_DIAGNOSTICS_SECTION:" + section).encode("ascii"),
                ]
            ):
                raise ReadUnavailable("DB_PROTOCOL_INVALID", section)
            rows = []
            for line in lines[3:]:
                item = json.loads(line)
                if item.get("section") != section or not isinstance(item.get("row"), dict):
                    raise ReadUnavailable("DB_PROTOCOL_INVALID", section)
                rows.append(project_database_row(section, item["row"]))
            if stage is not None:
                if len(rows) != 1 or rows[0].get("stage") != stage:
                    raise ReadUnavailable("DB_PROTOCOL_INVALID", section)
                pipeline_rows.extend(rows)
            else:
                sections[section] = {"status": "OK", "rows": rows}
        except Exception as error:
            failure = error.failure if isinstance(error, ReadUnavailable) else "DB_PROTOCOL_INVALID"
            if stage is None:
                sections[section] = {"status": "UNKNOWN", "rows": None, "failure": failure}
            else:
                pipeline_rows.append(
                    {
                        "stage": stage,
                        "status": "UNKNOWN",
                        "count": None,
                        "first_at": None,
                        "last_at": None,
                        "failure": failure,
                    }
                )
            if isinstance(error, ReadUnavailable) and (
                error.failed_section == "role_check"
                or failure
                in {"DB_ROLE_MISSING", "DB_AUTHENTICATION_FAILED", "DB_ROLE_POLICY_REJECTED"}
            ):
                break
    if pipeline_rows:
        complete = len(pipeline_rows) == len(PIPELINE_READS) and all(
            row.get("status") != "UNKNOWN" for row in pipeline_rows
        )
        sections["pipeline_72h"] = {
            "status": "OK" if complete else "PARTIAL",
            "rows": pipeline_rows,
        }
    statuses = [section["status"] for section in sections.values()]
    status = (
        "OK"
        if all(v == "OK" for v in statuses)
        else ("PARTIAL" if "OK" in statuses or pipeline_rows else "UNKNOWN")
    )
    return {
        "status": status,
        "role": "tf_diagnostics",
        "window_hours": 72,
        "read_mode": "independent_read_only_transactions",
        "sections": sections,
    }


def project_database_row(section: str, row: Any) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {}
    if section == "runtime_current":
        return {
            "service": row.get("service") if row.get("service") in PORTS else UNKNOWN,
            "observed_at": safe_time(row.get("observed_at")),
            "expires_at": safe_time(row.get("expires_at")),
            "age_seconds": safe_number(row.get("age_seconds")),
            "fresh": row.get("fresh") if type(row.get("fresh")) is bool else None,
            "data": project_telemetry(row.get("data")),
        }
    result: dict[str, Any] = {k: safe_time(v) for k, v in row.items() if k in TIMES}
    if section == "risk_current":
        result.update(
            kill_switch=row.get("kill_switch") if type(row.get("kill_switch")) is bool else None,
            version=safe_number(row.get("version")),
        )
    for key in (
        "count",
        "unique_decisions",
        "distinct_closed_periods",
        "symbols",
        "interior_gaps",
        "distinct_bars",
    ):
        if key in row:
            result[key] = safe_number(row[key])
    for key in ("state", "reason"):
        if key in row:
            result[key] = safe_code(row[key])
    if "timeframe" in row:
        result["timeframe"] = (
            row["timeframe"] if row["timeframe"] in ("5m", "15m", "1h") else UNKNOWN
        )
    if "stage" in row:
        allowed: tuple[str, ...] = (
            "universe_snapshots",
            "feature_snapshots",
            "opportunities",
            "decisions",
            "execution_intents",
            "outbox_created",
            "outbox_delivered",
            "orders_updated",
            "fills_observed",
        )
        result["stage"] = row["stage"] if row["stage"] in allowed else UNKNOWN
    if "source" in row:
        result["source"] = (
            row["source"]
            if row["source"] in ("lifecycle", "decision", "preflight", "admission")
            else UNKNOWN
        )
    if "event" in row:
        allowed = (
            "runtime.candidate.v1",
            "runtime.forecast.v1",
            "runtime.closed_result.v1",
            "runtime.demo.activated.v1",
            "edge.outcome.v1",
            "execution.intent_expired.v1",
            "runtime.admission_rejected.v1",
        )
        result["event"] = row["event"] if row["event"] in allowed else UNKNOWN
    if "delivered" in row:
        result["delivered"] = row["delivered"] if type(row["delivered"]) is bool else None
    return result


def database_snapshot(runner: Callable[..., bytes], container_id: str) -> dict[str, Any]:
    unknown: dict[str, Any] = {
        "status": UNKNOWN,
        "role": "tf_diagnostics",
        "sections": {name: {"status": UNKNOWN, "rows": None} for name in QUERIES},
    }
    try:
        diagnostic_value = runner(
            database_command(container_id, SQL),
            timeout=12,
            limit=256_000,
        )
        if len(diagnostic_value) > 256_000:
            return unknown
        lines = diagnostic_value.splitlines()
        if (
            len(lines) < 2
            or lines[0] != b"TF_DIAGNOSTICS_SECTION:role_check"
            or lines[1].strip() != b"1"
            or len(lines) > 512
        ):
            return unknown
        sections: dict[str, Any] = {name: {"status": "OK", "rows": []} for name in QUERIES}
        names = list(QUERIES)
        seen = []
        current = None
        for line in lines[2:]:
            if line.startswith(b"TF_DIAGNOSTICS_SECTION:"):
                name = line.removeprefix(b"TF_DIAGNOSTICS_SECTION:").decode("ascii")
                if len(seen) >= len(names) or name != names[len(seen)]:
                    return unknown
                seen.append(name)
                current = name
                continue
            item = json.loads(line)
            section = item.get("section")
            if (
                section != current
                or section not in sections
                or len(sections[section]["rows"]) >= 64
            ):
                return unknown
            sections[section]["rows"].append(project_database_row(section, item.get("row")))
        if seen != names:
            return unknown
        return {"status": "OK", "role": "tf_diagnostics", "window_hours": 72, "sections": sections}
    except ReadUnavailable as error:
        unknown.update(failure=error.failure, failed_section=error.failed_section)
        if error.failure == "DB_QUERY_CANCELED" and error.failed_section in QUERIES:
            separated = isolated_database_snapshot(runner, container_id)
            if any(v["status"] == "OK" for v in separated["sections"].values()) or any(
                row.get("count") is not None
                for row in separated["sections"]["pipeline_72h"].get("rows") or []
            ):
                separated["initial_failure"] = "DB_QUERY_CANCELED"
                return separated
        return unknown
    except Exception:
        return unknown


def project_logs(raw: bytes) -> dict[str, Any]:
    records = []
    discarded = 0
    for line in raw.splitlines()[-100:]:
        try:
            if len(line) > 8192:
                raise ValueError()
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ValueError()
            record = project_telemetry(row)
            if row.get("level") in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
                record["level"] = row["level"]
            if not record:
                raise ValueError()
            records.append(record)
        except Exception:
            discarded += 1
    return {
        "status": "OK",
        "records": records,
        "discarded": discarded,
        "scope": "last_100_lines_last_15_minutes",
    }


def read_json(runner: Callable[..., bytes], argv: list[str], limit: int = 2_000_000) -> Any:
    raw = runner(argv, timeout=12, limit=limit)
    if len(raw) > limit:
        raise ReadUnavailable()
    return json.loads(raw)


def empty_service() -> dict[str, Any]:
    return {
        "status": UNKNOWN,
        "running": None,
        "started_at": None,
        "restart_count": None,
        "oom_killed": None,
        "health": UNKNOWN,
        "image_id": None,
        "image_digest": None,
        "revision": None,
        "readiness": {"status": UNKNOWN},
        "analysis": {"status": UNKNOWN},
        "logs": {"status": UNKNOWN},
    }


def project_probe(row: Any, analysis: bool = False) -> dict[str, Any]:
    if not isinstance(row, dict) or type(row.get("http_status")) is not int:
        return {"status": UNKNOWN}
    status = row["http_status"]
    if not 100 <= status <= 599 or not isinstance(row.get("data"), dict):
        return {"status": UNKNOWN}
    diagnostic_value = row["data"].get("status", {}) if analysis else row["data"]
    return {
        "status": "OK" if status in (200, 503) else UNKNOWN,
        "http_status": status,
        "data": project_telemetry(diagnostic_value),
    }


def snapshot(runner: Callable[..., bytes] = run_bounded) -> dict[str, Any]:
    result: dict[str, Any] = {
        "status": "PARTIAL",
        "sampled_at": datetime.now(UTC).isoformat(),
        "services": {s: empty_service() for s in SERVICES},
        "database": {
            "status": UNKNOWN,
            "role": "tf_diagnostics",
            "sections": {n: {"status": UNKNOWN, "rows": None} for n in QUERIES},
        },
    }
    try:
        raw = runner(
            [
                "/usr/bin/docker",
                "ps",
                "-aq",
                "--no-trunc",
                "--filter",
                "label=com.docker.compose.project=forgetrade",
            ],
            timeout=12,
            limit=4096,
        )
        ids = raw.decode("ascii").split()
        if not 0 < len(ids) <= 16 or any(re.fullmatch("[a-f0-9]{64}", i) is None for i in ids):
            return result
        items = read_json(runner, ["/usr/bin/docker", "inspect", *ids])
        if not isinstance(items, list) or len(items) != len(ids):
            return result
    except Exception:
        return result
    verified: dict[str, Any] = {}
    duplicated = set()
    for item in items:
        try:
            labels = item["Config"]["Labels"]
            role = labels.get("com.docker.compose.service")
            if (
                role not in SERVICES
                or item.get("Id") not in ids
                or labels.get("com.docker.compose.project") != "forgetrade"
                or labels.get("com.docker.compose.project.working_dir") != "/opt/forgetrade"
                or re.fullmatch(r"sha256:[a-f0-9]{64}", item.get("Image", "")) is None
            ):
                continue
            if role in verified:
                duplicated.add(role)
            verified[role] = item
        except (KeyError, TypeError, AttributeError):
            continue
    for role in duplicated:
        del verified[role]
    for role, item in verified.items():
        try:
            state = item["State"]
            out = result["services"][role]
            out.update(
                status=state.get("Status")
                if state.get("Status")
                in ("created", "running", "paused", "restarting", "removing", "exited", "dead")
                else UNKNOWN,
                running=state.get("Running") if type(state.get("Running")) is bool else None,
                started_at=safe_time(state.get("StartedAt")),
                restart_count=safe_number(item.get("RestartCount")),
                oom_killed=state.get("OOMKilled") if type(state.get("OOMKilled")) is bool else None,
                health=state.get("Health", {}).get("Status")
                if state.get("Health", {}).get("Status") in ("starting", "healthy", "unhealthy")
                else UNKNOWN,
                image_id=item["Image"],
            )
            image = read_json(
                runner, ["/usr/bin/docker", "image", "inspect", item["Image"]], 128_000
            )
            if isinstance(image, list) and len(image) == 1 and image[0].get("Id") == item["Image"]:
                digest = image[0].get("RepoDigests", [])
                if isinstance(digest, list):
                    out["image_digest"] = sorted(
                        {
                            d.rsplit("@", 1)[-1]
                            for d in digest[:16]
                            if isinstance(d, str)
                            and re.fullmatch(r"[a-z0-9./:_-]{1,180}@sha256:[a-f0-9]{64}", d)
                        }
                    )
                revision = (
                    image[0]
                    .get("Config", {})
                    .get("Labels", {})
                    .get("org.opencontainers.image.revision")
                )
                if isinstance(revision, str) and re.fullmatch("[a-f0-9]{40}", revision):
                    out["revision"] = revision
        except Exception:
            pass
        if role in APPLICATIONS:
            try:
                logs = runner(
                    ["/usr/bin/docker", "logs", "--since", "15m", "--tail", "100", item["Id"]],
                    timeout=12,
                    limit=128_000,
                )
                out["logs"] = project_logs(logs) if len(logs) <= 128_000 else {"status": UNKNOWN}
            except Exception:
                pass
        if state.get("Running") is not True:
            continue
        if role in PROBES:
            try:
                probe = read_json(
                    runner,
                    [
                        "/usr/bin/docker",
                        "exec",
                        "--user",
                        "10001:10001",
                        "--workdir",
                        "/",
                        item["Id"],
                        "/usr/local/bin/python3",
                        "-I",
                        "-S",
                        "-B",
                        "-c",
                        PROBES[role],
                    ],
                    128_000,
                )
                out["readiness"] = project_probe(probe.get("readiness"))
                if role == "market_gateway":
                    out["analysis"] = project_probe(probe.get("analysis"), analysis=True)
            except Exception:
                pass
        if role == "postgres":
            result["database"] = database_snapshot(runner, item["Id"])
    return result


def render(result: dict[str, Any]) -> str:
    output = json.dumps(result, ensure_ascii=True, allow_nan=False, separators=(",", ":"))
    if len(output.encode("ascii")) > CAP:
        return '{"status":"OUTPUT_CAPACITY","database":{"status":"UNKNOWN"}}'
    return output


def main() -> int:
    if len(sys.argv) != 1 or os.geteuid() != 0:
        print('{"status":"ARGUMENT_FREE_ROOT_REQUIRED"}')
        return 2
    os.chdir("/")
    os.environ.clear()
    os.environ.update(ENV)
    signal.signal(signal.SIGALRM, expire_deadline)
    signal.alarm(90)
    try:
        result = snapshot()
        print(render(result))
        return 0
    except (Exception, DeadlineExpired):
        print('{"status":"UNKNOWN","database":{"status":"UNKNOWN"}}')
        return 1
    finally:
        signal.alarm(0)


if __name__ == "__main__":
    raise SystemExit(main())
