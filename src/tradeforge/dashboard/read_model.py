"""Bounded allowlisted projections in enforced PostgreSQL read-only transactions.

Missing telemetry stays unknown. Historical rows retain individual timestamps;
neither a healthy dashboard nor a queued resume grants order permission.
"""

import json
import math
import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import Connection

from tradeforge.domain.common import utc
from tradeforge.persistence.db import Database

LIMIT = 100
# SQL selects individual display fields only. No raw credential-bearing JSON
# response, arbitrary audit payload, intent, configuration or account dump.
QUERIES = {
    "demo": """
        SELECT (payload->>'as_of')::timestamptz AS as_of,payload->>'expires_at' AS expires_at,
          payload->>'environment' AS environment,payload->>'connection' AS connection,
          payload->>'account' AS account,payload->>'balance' AS balance,
          payload->>'positions' AS positions,payload->>'clock' AS clock,
          payload->>'key_permissions' AS key_permissions,
          payload->'account_can_trade' AS account_can_trade,
          payload->>'equity' AS equity,payload->>'available_balance' AS available_balance,
          payload->'position_count' AS position_count,
          payload->'open_order_count' AS open_order_count,
          payload->>'guardian' AS guardian,payload->'reason_codes' AS reason_codes
          ,payload->'terminal' AS terminal
        FROM audit_log WHERE event_type='demo.preflight.v1'
          AND (payload->>'as_of')::timestamptz<=:at
        ORDER BY audit_id DESC LIMIT 1
    """,
    "universe": """
        SELECT as_of, version_hash, payload#>'{metadata,total}' AS total,
          jsonb_array_length(payload#>'{universe,eligible}') AS eligible,
          payload#>>'{expires_at}' AS expires_at, payload#>>'{reason}' AS reason,
          ARRAY(SELECT x->>'symbol' FROM jsonb_array_elements(
            coalesce(nullif(payload#>'{universe,watched}','null'), '[]')) x LIMIT 100) AS watched,
          ARRAY(SELECT x->>'symbol' FROM jsonb_array_elements(
            coalesce(nullif(payload#>'{universe,deep}','null'), '[]')) x LIMIT 100) AS deep
        FROM universe_snapshots WHERE as_of<=:at ORDER BY as_of DESC LIMIT 1
    """,
    "regime": """
        SELECT as_of, payload#>>'{state,source_at}' AS source_at,
          payload#>>'{state,label}' AS label,
          payload#>'{state,metrics}' AS metrics, payload->'reason_codes' AS reason_codes,
          payload#>>'{policy,timeframe}' AS timeframe,
          payload#>>'{policy,max_age_periods}' AS max_age_periods
        FROM regime_snapshots WHERE as_of<=:at ORDER BY as_of DESC LIMIT 1
    """,
    "streams": """
        SELECT observed_at AS as_of, expires_at::text AS expires_at,
          payload->'eligible' AS eligible, payload->'covered' AS covered,
          payload->'measured' AS measured,payload->'clock_safe' AS clock_safe,
          payload->'coverage_deadlines' AS coverage_deadlines,
          payload->>'universe_version' AS universe_version,payload->>'reason' AS reason
          ,payload->>'market_environment' AS market_environment,
          payload->'rest_market_covered' AS rest_market_covered,
          payload->>'history_reason' AS history_reason,payload->>'history_period' AS history_period,
          payload->'history_complete' AS history_complete,
          payload->'history_total' AS history_total,
          payload->'history_completed' AS history_completed,
          payload->'history_attempted' AS history_attempted,
          payload->'history_failed' AS history_failed,
          payload->'history_pending' AS history_pending,
          payload->'history_inflight' AS history_inflight,
          payload->>'history_last_completed_symbol' AS history_last_completed_symbol,
          payload->>'history_last_failure_symbol' AS history_last_failure_symbol,
          payload->'history_last_member_ms' AS history_last_member_ms,
          payload->>'history_last_failure_at' AS history_last_failure_at,
          payload->>'history_last_failure_reason' AS history_last_failure_reason,
          payload->'history_elapsed_ms' AS history_elapsed_ms,
          payload->'canonical_storage_ready' AS canonical_storage_ready,
          payload->>'canonical_storage_error' AS canonical_storage_error,
          payload->'storage_admission_ready' AS storage_admission_ready
          ,payload->'watched' AS watched,payload->'deep' AS deep,payload->'protected' AS protected
        FROM runtime_observations WHERE service='market_gateway' AND observed_at<=:at
        ORDER BY observed_at DESC,sample_id DESC LIMIT 1
    """,
    "workers": """
        SELECT observed_at AS as_of,service,
          (observed_at+interval '15 seconds')::text AS expires_at,
          payload->'data_ready' AS data_ready,payload->>'reason' AS reason,
          payload->>'last_source_at' AS last_source_at,
          payload->'healthy' AS healthy,payload->'analysis_ready' AS analysis_ready,
          payload->'execution_ready' AS execution_ready,payload->>'stage' AS stage,
          payload->>'last_progress_at' AS last_progress_at,
          payload->>'last_success_at' AS last_success_at,payload->>'last_error' AS last_error,
          payload->>'last_analysis_at' AS last_analysis_at,
          payload->>'last_analysis_source_at' AS last_analysis_source_at,
          payload->>'last_evaluation_at' AS last_evaluation_at,
          payload->'last_evaluation' AS last_evaluation,
          payload->'last_input_check' AS last_input_check,
          payload->>'last_error_at' AS last_error_at,
          payload->>'last_error_stage' AS last_error_stage,
          payload->'error_count' AS error_count,payload->'cycle_count' AS cycle_count,
          payload->'cycle_ms' AS cycle_ms,payload->'loop_lag_ms' AS loop_lag_ms,
          payload->'candidate_count' AS candidate_count,payload->'reason_counts' AS reason_counts,
          payload->'evaluated_count' AS evaluated_count,
          payload->'analysis_symbols' AS analysis_symbols,
          payload->'history_current_members' AS history_current_members,
          payload->'history_current_coverage' AS history_current_coverage,
          payload->'selected_deep_count' AS selected_deep_count,
          payload->'ready_deep_count' AS ready_deep_count,
          payload->'input_rejections' AS input_rejections,
          payload->'symbol_results' AS symbol_results,payload->>'regime_label' AS regime_label,
          payload->'regime_reasons' AS regime_reasons,
          payload->'entries_enabled' AS entries_enabled,payload->'trial_enabled' AS trial_enabled,
          payload->'stream_ready' AS stream_ready,payload->'protection_ready' AS protection_ready,
          payload->'blockers' AS blockers,
          payload->'autonomous_decisions' AS autonomous_decisions
          ,payload->'private_stream' AS private_stream,payload->'protection' AS protection
        FROM (SELECT DISTINCT ON (service) * FROM runtime_observations
              WHERE observed_at<=:at ORDER BY service,observed_at DESC,sample_id DESC) latest
    """,
    "opportunities": """
        SELECT o.observed_at AS as_of,i.symbol,o.payload->'score' AS score,
          o.payload->'reason_codes' AS reason_codes,
          o.payload->>'source_at' AS source_at,o.payload->>'expires_at' AS expires_at
        FROM opportunities o JOIN instruments i USING(instrument_id)
        WHERE o.regime_snapshot_id=(SELECT snapshot_id FROM regime_snapshots
            WHERE as_of<=:at ORDER BY as_of DESC LIMIT 1) AND o.observed_at<=:at
        ORDER BY (o.payload->>'score')::double precision DESC NULLS LAST,i.symbol LIMIT :limit
    """,
    "candidates": """
        SELECT at AS as_of,state,decision_id,
          payload::jsonb#>>'{discovery,candidate,instrument,symbol}' AS symbol,
          payload::jsonb#>>'{discovery,candidate,side}' AS side,
          payload::jsonb#>>'{discovery,candidate,alpha_family}' AS alpha_family,
          payload::jsonb#>>'{discovery,candidate,expires_at}' AS expires_at,
          payload::jsonb#>>'{discovery,candidate,trigger}' AS trigger,
          payload::jsonb#>>'{discovery,candidate,stop_proposal}' AS stop_proposal,
          payload::jsonb#>>'{discovery,candidate,take_profit_proposal}' AS take_profit_proposal,
          payload::jsonb#>'{reason_codes}' AS reason_codes,
          NULL::text AS "net_ev_low_R"
        FROM (SELECT DISTINCT ON (decision_id) * FROM decision_lifecycle
              WHERE at<=:at ORDER BY decision_id,version DESC) latest
        WHERE state IN ('DISCOVERED','WATCHING','ARMED','VALIDATING')
        ORDER BY at DESC,decision_id LIMIT :limit
    """,
    "signals": """
        SELECT * FROM (
          SELECT at AS as_of,state,decision_id,
            payload::jsonb#>>'{discovery,candidate,instrument,symbol}' AS symbol,
            payload::jsonb#>>'{discovery,candidate,side}' AS side,
            payload::jsonb#>>'{discovery,candidate,alpha_family}' AS alpha_family,
            payload::jsonb#>>'{discovery,candidate,expires_at}' AS expires_at,
            payload::jsonb#>>'{discovery,candidate,trigger}' AS trigger,
            payload::jsonb#>>'{discovery,candidate,stop_proposal}' AS stop_proposal,
            payload::jsonb#>>'{discovery,candidate,take_profit_proposal}' AS take_profit_proposal,
            payload::jsonb#>'{reason_codes}' AS reason_codes,
            payload::jsonb#>>'{command,evidence,proposal,net_ev_low_R}' AS "net_ev_low_R",
            (SELECT d.snapshot::jsonb->>'execution_authority' FROM decisions d
             WHERE d.decision_id=latest.decision_id) AS execution_authority,
            (SELECT d.snapshot::jsonb->>'proof_state' FROM decisions d
             WHERE d.decision_id=latest.decision_id) AS proof_state
          FROM (SELECT DISTINCT ON (decision_id) * FROM decision_lifecycle
                WHERE at<=:at ORDER BY decision_id,version DESC) latest
          UNION ALL
          SELECT d.observed_at,d.state,d.decision_id,i.symbol,d.side,
            d.snapshot::jsonb->>'alpha_family',d.expires_at::text,
            d.snapshot::jsonb->>'trigger',d.snapshot::jsonb->>'stop_proposal',
            d.snapshot::jsonb->>'take_profit_proposal',
            d.snapshot::jsonb->'reason_codes',d.snapshot::jsonb->>'net_ev_low_R',
            d.snapshot::jsonb->>'execution_authority',d.snapshot::jsonb->>'proof_state'
          FROM decisions d JOIN instruments i USING(instrument_id)
          WHERE d.observed_at<=:at AND NOT EXISTS
            (SELECT 1 FROM decision_lifecycle l WHERE l.decision_id=d.decision_id AND l.at<=:at)
        ) signals ORDER BY as_of DESC,decision_id LIMIT :limit
    """,
    "orders": """
        SELECT o.updated_at AS as_of,i.symbol,o.status,o.qty,o.filled_qty,o.avg_price,o.fee
        FROM orders o JOIN execution_intents e USING(intent_id)
          JOIN instruments i ON i.instrument_id=e.instrument_id
        WHERE o.updated_at<=:at ORDER BY o.updated_at DESC,o.intent_id LIMIT :limit
    """,
    "positions": """
        SELECT p.as_of,i.symbol,p.side,p.qty,p.entry_price,p.mark_price,p.protected_qty
        FROM positions p JOIN instruments i USING(instrument_id)
        WHERE p.as_of<=:at AND p.qty>0 ORDER BY p.as_of DESC,p.position_id LIMIT :limit
    """,
    "stops": """
        SELECT s.as_of,i.symbol,p.side,s.qty,s.stop_price,s.status,
          CASE WHEN left(s.client_order_id,3)='sl_' THEN 'STOP_MARKET'
               WHEN left(s.client_order_id,3)='tp_' THEN 'TAKE_PROFIT_MARKET'
               ELSE 'UNKNOWN' END AS kind
        FROM protective_orders s JOIN positions p USING(position_id)
          JOIN instruments i USING(instrument_id)
        WHERE s.as_of<=:at AND p.qty>0 ORDER BY s.as_of DESC,s.protective_id LIMIT :limit
    """,
    "account": """
        SELECT CASE WHEN kind='execution.context.v1' THEN (body->>'as_of')::timestamptz
               ELSE recorded_at END AS as_of,
          body->>'equity' AS equity,body->>'available_balance' AS available_balance,
          body->>'realized_pnl_today' AS realized_pnl_today,
          body->>'unrealized_pnl' AS unrealized_pnl
        FROM (SELECT as_of AS recorded_at,snapshot_id,payload->>'kind' AS kind,
          CASE WHEN payload->>'kind'='execution.context.v1' THEN payload#>'{body,account}'
          ELSE payload END AS body FROM account_snapshots WHERE as_of<=:at
          AND (payload->>'kind'='execution.context.v1' OR payload ? 'equity')) accounts
        ORDER BY recorded_at DESC,snapshot_id LIMIT 1
    """,
    "risk": """
        SELECT as_of,kill_switch,version FROM risk_state
        WHERE state_id='portfolio.v1' AND as_of<=:at
    """,
    "guardian": """
        SELECT observed_at AS as_of,payload->'ready' AS ready,
          payload->'reconciled_at' AS reconciled_at,payload->'reasons' AS reason_codes,
          jsonb_array_length(payload->'unprotected') AS unprotected_positions
        FROM audit_log WHERE event_type='protection.health.v1' AND observed_at<=:at
        ORDER BY observed_at DESC,audit_id DESC LIMIT 1
    """,
}


def _json(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (Decimal, UUID)):
        return str(value)
    if isinstance(value, (tuple, list)):
        return [_json(x) for x in value]
    if isinstance(value, dict):
        return {k: _json(v) for k, v in value.items()}
    return value


def _stream_status(row: dict[str, Any], now: datetime) -> str:
    deadlines = row.get("coverage_deadlines")
    if (
        row.get("measured") is not True
        or row.get("clock_safe") is not True
        or not isinstance(deadlines, list)
    ):
        return "UNKNOWN"
    count = row.get("eligible")
    if type(count) is not int or count <= 0 or len(deadlines) > count:
        return "UNKNOWN"
    try:
        parsed = [utc(datetime.fromisoformat(value)) for value in deadlines]
    except (ValueError, TypeError):
        return "UNKNOWN"
    row["covered"] = sum(deadline > now for deadline in parsed)
    return "OK" if row["covered"] == count and row.get("clock_safe") is True else "DEGRADED"


def _freshness(name: str, row: dict[str, Any], now: datetime) -> str:
    if row["as_of"] is None:
        return "UNKNOWN"
    if name in {"streams", "workers", "opportunities", "demo"}:
        expiry = row.get("expires_at")
        if not expiry or not row["as_of"] <= now < datetime.fromisoformat(expiry):
            return "STALE"
        if name == "streams":
            return _stream_status(row, now)
        if name == "opportunities" and row.get("score") is None:
            return "UNKNOWN"
        if name == "demo" and any(
            row.get(k) != "OK" for k in ("connection", "account", "balance", "positions", "clock")
        ):
            return "UNKNOWN"
        return "OK"
    age = (now - row["as_of"]).total_seconds()
    maximum = 15.0 if name in {"guardian", "positions", "stops"} else 30.0
    if name == "risk":
        # Durable controls/results do not expire merely because nobody changes them.
        maximum = math.inf
    if name == "universe":
        expiry = row.get("expires_at")
        if (
            row["reason"] is not None
            or expiry is None
            or utc(datetime.fromisoformat(expiry)) <= now
        ):
            return "STALE"
        maximum = math.inf
        if row.get("eligible") is None:
            return "UNKNOWN"
    if name == "regime":
        period = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}.get(
            str(row.get("timeframe"))
        )
        if period is None or row.get("max_age_periods") is None:
            return "UNKNOWN"
        maximum = period * float(row["max_age_periods"])
        if row.get("source_at"):
            age = (now - datetime.fromisoformat(row["source_at"])).total_seconds()
    if not 0 <= age <= maximum:
        return "STALE"
    if name == "regime" and row.get("label") in {None, "UNKNOWN"}:
        return "UNKNOWN"
    if name == "account" and any(row.get(k) is None for k in ("equity", "available_balance")):
        return "UNKNOWN"
    if name == "guardian" and row.get("ready") is not True:
        return "FAIL_CLOSED"
    expiry = row.get("expires_at")
    if name in {"signals", "candidates"} and expiry is not None:
        if datetime.fromisoformat(expiry) <= now:
            return "STALE"
    return "OK"


def _deadline(name: str, row: dict[str, Any]) -> datetime | None:
    if row["as_of"] is None or name == "risk":
        return None
    if name in {"streams", "workers", "opportunities", "demo"}:
        return datetime.fromisoformat(row["expires_at"]) if row.get("expires_at") else None
    if name == "universe":
        return datetime.fromisoformat(row["expires_at"]) if row.get("expires_at") else None
    seconds = 15.0 if name in {"guardian", "positions", "stops"} else 30.0
    if name == "regime":
        period = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}.get(
            str(row.get("timeframe"))
        )
        if period is None or row.get("max_age_periods") is None:
            return None
        seconds = period * float(row["max_age_periods"])
    source = row["as_of"]
    if name == "regime" and row.get("source_at"):
        source = datetime.fromisoformat(row["source_at"])
    if not isinstance(source, datetime):
        raise ValueError("SOURCE_TIMESTAMP_REQUIRED")
    deadline = source + timedelta(seconds=seconds)
    if name in {"signals", "candidates"} and row.get("expires_at"):
        deadline = min(deadline, datetime.fromisoformat(row["expires_at"]))
    return deadline


def refresh_freshness(snapshot: dict[str, Any], at: datetime) -> dict[str, Any]:
    """Age source deadlines after a query or cache hit without renewing evidence."""
    utc(at)
    result = deepcopy(snapshot)
    if result.get("status") == "STALE":
        return result
    sections = result.get("sections", {})
    priority = {"OK": 0, "UNKNOWN": 1, "DEGRADED": 2, "STALE": 3, "FAIL_CLOSED": 4}
    for name, section in sections.items():
        rows = section.get("data") or []
        for row in rows:
            if row.get("fresh_until") and datetime.fromisoformat(row["fresh_until"]) <= at:
                row["freshness"] = "STALE"
            elif name == "streams":
                row["freshness"] = _stream_status(row, at)
        if rows:
            section["status"] = max((r["freshness"] for r in rows), key=priority.__getitem__)
    universe = sections.get("universe", {}).get("data") or []
    streams = sections.get("streams", {})
    samples = streams.get("data") or []
    if (
        samples
        and universe
        and streams.get("status") == "OK"
        and samples[0].get("universe_version") != universe[0].get("version_hash")
    ):
        samples[0]["freshness"] = streams["status"] = "DEGRADED"
        samples[0]["reason"] = "STREAM_UNIVERSE_MISMATCH"
    measured = bool(
        universe
        and samples
        and streams.get("status") in {"OK", "DEGRADED"}
        and samples[0].get("universe_version") == universe[0].get("version_hash")
        and samples[0].get("eligible") == universe[0].get("eligible")
        and sections["universe"]["status"] == "OK"
    )
    for row in universe:
        row["stream_coverage"] = (
            samples[0]["covered"] / samples[0]["eligible"] if measured else None
        )
    covered = measured and streams.get("status") == "OK"
    result["service_health"] = {"dashboard": "OK", "postgresql": "OK"}
    for worker in sections.get("workers", {}).get("data") or []:
        result["service_health"][worker["service"]] = worker["freshness"]
    for name in ("market_gateway", "quant_core"):
        result["service_health"].setdefault(name, "UNKNOWN")
    workers = sections.get("workers", {}).get("data") or []
    execution = next((w for w in workers if w.get("service") == "execution_guardian"), None)
    execution_fresh = bool(execution and execution.get("freshness") == "OK")
    result["execution_readiness"] = {
        "ready": bool(execution_fresh and execution and execution.get("execution_ready") is True),
        "reason": (
            execution.get("reason", "DISPATCH_NOT_OBSERVED")
            if execution_fresh and execution
            else "DISPATCH_STALE"
            if execution
            else "DISPATCH_NOT_OBSERVED"
        ),
        "stage": execution.get("stage") if execution else None,
        "last_progress_at": execution.get("last_progress_at") if execution else None,
        "last_error": execution.get("last_error") if execution else None,
        "entries_enabled": bool(execution_fresh and execution and execution.get("entries_enabled")),
        "trial_enabled": bool(execution_fresh and execution and execution.get("trial_enabled")),
        "blockers": execution.get("blockers", [])
        if execution_fresh and execution
        else ["DISPATCH_STALE"],
        "stream_ready": bool(execution_fresh and execution and execution.get("stream_ready")),
        "protection_ready": bool(
            execution_fresh and execution and execution.get("protection_ready")
        ),
    }
    quant = next((w for w in workers if w.get("service") == "quant_core"), None)
    result["analysis_readiness"] = {
        "ready": bool(
            quant and quant.get("freshness") == "OK" and quant.get("analysis_ready") is True
        ),
        "reason": quant.get("reason") if quant else "ANALYSIS_NOT_OBSERVED",
    }
    result["data_readiness"] = {
        name: sections.get(name, {}).get("status", "UNKNOWN")
        for name in ("universe", "streams", "regime", "opportunities", "risk", "guardian")
    }
    result["trading_authority"] = {
        "status": "NOT_READY",
        "live": False,
        "orders_enabled": False,
        "reason": "LOCAL_OBSERVATION_ONLY",
    }
    demo = sections.get("demo", {})
    demo_rows = demo.get("data") or []
    result["market_environment"] = (
        result.get("sections", {}).get("streams", {}).get("data", [{}])[0].get("market_environment")
        if result.get("sections", {}).get("streams", {}).get("data")
        else "UNKNOWN"
    )
    result["demo_readiness"] = {
        "status": demo.get("status", "UNKNOWN"),
        "environment": "BINANCE_FUTURES_DEMO",
        "orders_enabled": False,
        "live": False,
        "reason_codes": (demo_rows[0].get("reason_codes") or [])
        if demo_rows
        else ["DEMO_PREFLIGHT_NOT_RUN", "GUARDIAN_NOT_RUNNING"],
    }
    if demo_rows and demo.get("status") == "STALE":
        result["demo_readiness"]["reason_codes"] = ["DEMO_PREFLIGHT_STALE"]
    result["service_health"]["execution_guardian"] = (
        "OK"
        if demo_rows and demo.get("status") != "STALE" and demo_rows[0].get("guardian") == "RUNNING"
        else "NOT_READY"
    )
    result["status"] = (
        "OK"
        if covered
        and all(
            sections.get(k, {}).get("status") == "OK"
            for k in ("universe", "regime", "account", "guardian", "risk")
        )
        else "DEGRADED"
    )
    result["alarms"] = [
        {"code": f"{key.upper()}_{value['status']}", "severity": "WARN"}
        for key, value in sections.items()
        if value["status"] != "OK"
    ]
    if not covered:
        result["alarms"].append({"code": "STREAM_COVERAGE_UNKNOWN", "severity": "WARN"})
    guardian = sections.get("guardian", {}).get("data") or []
    if guardian and (
        guardian[0].get("ready") is not True
        or guardian[0].get("unprotected_positions")
        or sections["guardian"]["status"] != "OK"
    ):
        result["alarms"].append({"code": "GUARDIAN_NOT_READY", "severity": "CRITICAL"})
    result["evaluated_at"] = at.isoformat()
    return result


def _safe_fields(row: dict[str, Any]) -> None:
    if "terminal" in row:
        from tradeforge.dashboard.terminal_projection import sanitize_account

        row["terminal"] = sanitize_account(row["terminal"])
    if "input_rejections" in row:
        entries = row["input_rejections"]
        row["input_rejections"] = (
            None
            if not isinstance(entries, list)
            else [
                {"symbol": entry["symbol"], "reason": entry["reason"]}
                for entry in entries[:128]
                if isinstance(entry, dict)
                and isinstance(entry.get("symbol"), str)
                and re.fullmatch(r"[A-Z0-9_]{2,40}", entry["symbol"])
                and isinstance(entry.get("reason"), str)
                and re.fullmatch(r"[A-Z][A-Z0-9_]{0,95}", entry["reason"])
            ]
        )
    if "reason_codes" in row:
        row["reason_codes"] = [
            x
            for x in (row["reason_codes"] or [])[:32]
            if isinstance(x, str) and re.fullmatch(r"[A-Z][A-Z0-9_]{0,95}", x)
        ]
    if "metrics" in row:
        row["metrics"] = [
            x
            for x in (row["metrics"] or [])[:32]
            if isinstance(x, list)
            and len(x) == 2
            and isinstance(x[0], str)
            and re.fullmatch(r"[a-z_]{1,48}", x[0])
            and type(x[1]) in (int, float)
            and math.isfinite(x[1])
        ]
    for key in (
        "equity",
        "available_balance",
        "realized_pnl_today",
        "unrealized_pnl",
        "net_ev_low_R",
    ):
        if row.get(key) is not None:
            try:
                if len(str(row[key])) > 256 or not Decimal(str(row[key])).is_finite():
                    raise ValueError
            except (ValueError, ArithmeticError):
                row[key] = None


class ReadModel:
    def __init__(self, db: Database, *, clock: Callable[[], datetime] | None = None) -> None:
        self.db = db
        self.clock = clock or (lambda: datetime.now(UTC))

    @contextmanager
    def transaction(self) -> Iterator[Connection]:
        with self.db.transaction() as c:
            c.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            yield c

    def terminal_symbols(self, *, search: str = "", offset: int = 0) -> dict[str, Any]:
        if (
            re.fullmatch(r"[A-Za-z0-9_]{0,40}", search) is None
            or type(offset) is not int
            or not 0 <= offset <= 10000
        ):
            raise ValueError("MARKET_QUERY_INVALID")
        now = utc(self.clock())
        with self.transaction() as c:
            rows = (
                c.execute(
                    text("""
                WITH members AS (
                  SELECT x->>'symbol' AS symbol FROM jsonb_array_elements(
                    (SELECT payload#>'{universe,eligible}' FROM universe_snapshots
                     WHERE as_of<=:at ORDER BY as_of DESC LIMIT 1)) x
                )
                SELECT i.symbol,m.source_at,m.payload->>'mark_price' AS price,
                  m.payload->>'volume' AS volume,m.payload->>'source' AS source,
                  m.payload->>'environment' AS environment,
                  o.payload->'score' AS score
                FROM instruments i JOIN members USING(symbol)
                LEFT JOIN runtime_market m USING(instrument_id)
                LEFT JOIN (SELECT DISTINCT ON (instrument_id) instrument_id,payload
                  FROM opportunities WHERE observed_at<=:at
                    AND regime_snapshot_id=(SELECT snapshot_id FROM regime_snapshots
                      WHERE as_of<=:at ORDER BY as_of DESC LIMIT 1)
                  ORDER BY instrument_id,observed_at DESC,opportunity_id DESC) o
                    USING(instrument_id)
                WHERE i.symbol LIKE :search
                ORDER BY (o.payload->>'score')::double precision DESC NULLS LAST,i.symbol
                LIMIT 51 OFFSET :offset
            """),
                    {"at": now, "search": search.upper() + "%", "offset": offset},
                )
                .mappings()
                .all()
            )
        items = []
        for row in rows[:50]:
            item = dict(row)
            fresh = (
                row["source_at"] is not None and 0 <= (now - row["source_at"]).total_seconds() < 15
            )
            item["freshness"] = "OK" if fresh else "STALE"
            if not fresh:
                item["price"], item["volume"] = None, None
            item.pop("environment", None)
            items.append(_json(item))
        return {
            "items": items,
            "has_more": len(rows) > 50,
            "offset": offset,
            "as_of": now.isoformat(),
            "environment": "UNKNOWN",
        }

    def terminal_chart(self, symbol: str, timeframe: str) -> dict[str, Any]:
        from tradeforge.domain.market import ClosedBar

        if re.fullmatch(r"[A-Z0-9_]{2,40}", symbol) is None or timeframe not in {
            "1m",
            "5m",
            "15m",
            "1h",
            "4h",
        }:
            raise ValueError("CHART_QUERY_INVALID")
        now = utc(self.clock())
        with self.transaction() as c:
            display = c.scalar(
                text("""
              SELECT t.payload->'bars' FROM terminal_chart_cache t
              JOIN instruments i USING(instrument_id)
              WHERE i.symbol=:symbol AND t.timeframe=:tf AND t.as_of<=:at LIMIT 1
            """),
                {"symbol": symbol, "tf": timeframe, "at": now},
            )
            rows = (
                c.execute(
                    text("""
                SELECT payload FROM (
                  SELECT DISTINCT ON (b.open_at) b.open_at,b.payload
                  FROM closed_bar_observations b JOIN instruments i USING(instrument_id)
                  WHERE i.symbol=:symbol AND b.timeframe=:tf AND b.observed_at<=:at
                    AND b.close_at<=:at
                  ORDER BY b.open_at DESC,b.observed_at DESC LIMIT 300
                ) bars ORDER BY open_at
            """),
                    {"symbol": symbol, "tf": timeframe, "at": now},
                )
                .scalars()
                .all()
            )
            if isinstance(display, list) and 0 < len(display) <= 300:
                rows = [json.dumps(row) for row in display]
            features = (
                c.execute(
                    text("""
                SELECT x->>'volatility' AS volatility,x->>'relative_return' AS relative_strength,
                  r.as_of AS as_of
                FROM (SELECT as_of,payload FROM regime_snapshots
                  WHERE as_of<=:at ORDER BY as_of DESC LIMIT 1) r,
                  LATERAL jsonb_array_elements(r.payload->'features') x
                WHERE x#>>'{instrument,symbol}'=:symbol LIMIT 1
            """),
                    {"at": now, "symbol": symbol},
                )
                .mappings()
                .one_or_none()
            )
        bars = []
        for raw in rows:
            bar = ClosedBar.model_validate_json(raw, context={"now": now})
            if bar.symbol != symbol or bar.timeframe.value != timeframe:
                raise ValueError("CHART_SOURCE_INVALID")
            bars.append(
                {
                    "time": int(bar.open_at.timestamp()),
                    **{
                        key: str(getattr(bar, key))
                        for key in ("open", "high", "low", "close", "volume")
                    },
                }
            )
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "bars": bars,
            "as_of": now.isoformat(),
            "environment": "UNKNOWN",
            "execution_evidence": False,
            "metrics": _json(dict(features)) if features else None,
            "reason": "STORED_CLOSED_BARS" if bars else "CHART_HISTORY_MISSING",
        }

    def snapshot(self) -> dict[str, Any]:
        now = utc(self.clock())
        result: dict[str, Any] = {
            "as_of": now.isoformat(),
            "live": False,
            "orders_enabled": False,
            "poll_seconds": 5,
            "row_limit": LIMIT,
        }
        try:
            sections: dict[str, Any] = {}
            with self.transaction() as c:
                c.execute(text("SET LOCAL transaction_timeout='10s'"))
                for name, sql in QUERIES.items():
                    rows = [
                        dict(x)
                        for x in c.execute(text(sql), {"at": now, "limit": LIMIT}).mappings()
                    ]
                    status = "UNKNOWN"
                    for row in rows:
                        _safe_fields(row)
                        if name == "account" and (row["as_of"] is None or row["as_of"] > now):
                            row.update(
                                as_of=None,
                                equity=None,
                                available_balance=None,
                                realized_pnl_today=None,
                                unrealized_pnl=None,
                            )
                        row["freshness"] = _freshness(name, row, now)
                        row["fresh_until"] = _json(_deadline(name, row))
                    if rows:
                        priority = {
                            "OK": 0,
                            "UNKNOWN": 1,
                            "DEGRADED": 2,
                            "STALE": 3,
                            "FAIL_CLOSED": 4,
                        }
                        status = max((r["freshness"] for r in rows), key=priority.__getitem__)
                    sections[name] = {
                        "status": status,
                        "as_of": _json(rows[0]["as_of"]) if rows else None,
                        "data": _json(rows) if rows else None,
                    }
            result["sections"] = sections
            result = refresh_freshness(result, utc(self.clock()))
        except Exception:
            # SQL, credentials, schema details and driver errors never cross HTTP.
            result.update(
                status="STALE",
                reason="READ_MODEL_UNAVAILABLE",
                sections={},
                alarms=[{"code": "READ_MODEL_UNAVAILABLE", "severity": "CRITICAL"}],
                service_health={
                    "dashboard": "OK",
                    "postgresql": "NOT_READY",
                    "market_gateway": "UNKNOWN",
                    "quant_core": "UNKNOWN",
                },
                data_readiness={"status": "UNKNOWN"},
                trading_authority={
                    "status": "NOT_READY",
                    "live": False,
                    "orders_enabled": False,
                    "reason": "READ_MODEL_UNAVAILABLE",
                },
            )
        return result
