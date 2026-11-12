"""Durable command inbox. Only server-side execution consumption may alter portfolio controls."""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal

from pydantic import Field
from sqlalchemy import text

from tradeforge.dashboard.auth import Principal
from tradeforge.domain.common import Count, StrictModel, utc
from tradeforge.market.bars import canonical
from tradeforge.persistence.db import Database, EntryHalt
from tradeforge.risk.storage import RiskService, _audit, _save

COMMAND_LOCK = 84120516


class Request(StrictModel):
    action: Literal[
        "pause", "resume", "kill_on", "kill_off", "testnet_start", "testnet_stop", "testnet_cleanup"
    ]
    expected_version: Count
    idempotency_key: Annotated[str, Field(pattern=r"^[a-f0-9-]{36}$")]


class CommandQueue:
    def __init__(self, db: Database, *, clock: Callable[[], datetime] | None = None) -> None:
        self.db = db
        self.clock = clock or (lambda: datetime.now(UTC))

    def audit_auth(self, status: int) -> None:
        with self.db.transaction() as c:
            _audit(c, "dashboard.auth.v1", {"status": status})

    def request_chart(self, principal: Principal, body: object) -> None:
        import re

        principal.require("read")
        if not isinstance(body, dict) or set(body) != {"symbol", "timeframe"}:
            raise ValueError("CHART_QUERY_INVALID")
        if (
            not isinstance(body["symbol"], str)
            or re.fullmatch(r"[A-Z0-9_]{2,40}", body["symbol"]) is None
            or body["timeframe"] not in {"1m", "5m", "15m", "1h", "4h"}
        ):
            raise ValueError("CHART_QUERY_INVALID")
        at = utc(self.clock())
        with self.db.transaction() as c:
            c.execute(text("SELECT pg_advisory_xact_lock(84121603)"))
            rows = (
                c.execute(
                    text(
                        "SELECT payload FROM audit_log WHERE event_type='dashboard.chart.v1' "
                        "AND observed_at>:since"
                    ),
                    {"since": at - timedelta(seconds=30)},
                )
                .scalars()
                .all()
            )
            if body in rows:
                return
            if len(rows) >= 4:
                raise ValueError("CHART_RATE_LIMITED")
            _audit(c, "dashboard.chart.v1", body)


class ExecutionCommands:
    def __init__(self, db: Database, *, clock: Callable[[], datetime] | None = None) -> None:
        self.db = db
        self.clock = clock or (lambda: datetime.now(UTC))

    def testnet_authority(self, default: tuple[bool, bool] = (False, False)) -> tuple[bool, bool]:
        with self.db.transaction() as c:
            action = c.scalar(
                text(
                    "SELECT payload->>'action' FROM audit_log "
                    "WHERE event_type='dashboard.command_result.v1' "
                    "AND payload->>'action' IN ('testnet_start','testnet_stop') "
                    "AND payload->>'status'='APPLIED' "
                    "ORDER BY audit_id DESC LIMIT 1"
                )
            )
        return default if action is None else (action == "testnet_start", action == "testnet_start")

    def run_once(
        self,
        *,
        ready: Callable[[], bool],
        testnet: bool = False,
        cleanup: Callable[[], object] | None = None,
    ) -> int:
        with self.db.transaction() as c:
            c.execute(text("SELECT pg_advisory_xact_lock(:id)"), {"id": COMMAND_LOCK})
            row = (
                c.execute(
                    text(
                        "SELECT command_id,payload,version FROM commands "
                        "WHERE command_type LIKE 'dashboard.%' AND payload->>'status'='PENDING' "
                        "ORDER BY observed_at,command_id LIMIT 1 FOR UPDATE"
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return 0
            old, at = RiskService(self.db, clock=self.clock)._locked_state(c)
            payload = dict(row["payload"])
            try:
                request = Request.model_validate({k: payload[k] for k in Request.model_fields})
                expiry = utc(datetime.fromisoformat(payload["expires_at"]))
                if expiry <= at:
                    result = "COMMAND_EXPIRED"
                elif request.expected_version != old.version:
                    result = "RISK_VERSION_CONFLICT"
                elif request.action.startswith("testnet_") and not testnet:
                    result = "TESTNET_EXECUTION_REQUIRED"
                elif request.action == "testnet_cleanup" and not old.kill_switch:
                    result = "PAUSE_REQUIRED"
                elif request.action == "testnet_cleanup" and cleanup is None:
                    result = "TESTNET_EXECUTION_REQUIRED"
                elif request.action in {"resume", "kill_off", "testnet_start"} and not ready():
                    result = "EXECUTION_CORE_NOT_READY"
                else:
                    if request.action == "testnet_cleanup":
                        assert cleanup is not None
                        cleanup()
                    halted = request.action in {
                        "pause",
                        "kill_on",
                        "testnet_stop",
                        "testnet_cleanup",
                    }
                    if request.action == "testnet_start":
                        halted = old.kill_switch
                    state = old.evolve(at=at, kill_switch=halted)
                    _save(c, state)
                    result = "APPLIED"
            except EntryHalt:
                result = "ACCOUNT_RECONCILIATION_REQUIRED"
            except (ValueError, TypeError, KeyError):
                result = "COMMAND_INVALID"
            payload.update(
                status="APPLIED" if result == "APPLIED" else "REJECTED",
                result=result,
                processed_at=at.isoformat(),
            )
            c.execute(
                text(
                    "UPDATE commands SET payload=CAST(:body AS jsonb),version=version+1 "
                    "WHERE command_id=:id AND version=:version"
                ),
                {"body": canonical(payload), "id": row["command_id"], "version": row["version"]},
            )
            _audit(
                c, "dashboard.command_result.v1", {"command_id": str(row["command_id"]), **payload}
            )
        return 1
