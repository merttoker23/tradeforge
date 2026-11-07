"""Serialized PostgreSQL controls and append-only, replayable risk assessments.

This service records analysis only. The execution transaction must reserve all
pending exposure and repeat guardian checks before any future order dispatch.
"""

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.engine import Connection

from tradeforge.domain.common import utc
from tradeforge.intelligence.models import digest
from tradeforge.market.bars import canonical
from tradeforge.persistence.db import Database, EntryHalt
from tradeforge.risk.governor import RiskAssessment, RiskGovernor
from tradeforge.risk.kill_switch import RiskState
from tradeforge.risk.limits import RiskLimits
from tradeforge.risk.models import AccountSnapshot, EntryRequest

LOCK_ID = 84120512
STATE_ID = "portfolio.v1"


def _save(c: Connection, state: RiskState, *, initial: bool = False) -> None:
    snapshot_id = uuid4()
    body = state.model_dump(mode="json")
    c.execute(
        text(
            "INSERT INTO risk_snapshots(snapshot_id, as_of, payload) "
            "VALUES (:id,:at,CAST(:payload AS jsonb))"
        ),
        {
            "id": snapshot_id,
            "at": state.as_of,
            "payload": canonical({"kind": STATE_ID, "body": body, "hash": digest(body)}),
        },
    )
    params = {
        "id": STATE_ID,
        "snapshot": snapshot_id,
        "kill": state.kill_switch,
        "version": state.version,
        "at": state.as_of,
    }
    if initial:
        c.execute(
            text(
                "INSERT INTO risk_state(state_id,risk_snapshot_id,kill_switch,version,as_of) "
                "VALUES (:id,:snapshot,:kill,:version,:at)"
            ),
            params,
        )
    else:
        c.execute(
            text(
                "UPDATE risk_state SET risk_snapshot_id=:snapshot,kill_switch=:kill,"
                "version=:version,as_of=:at WHERE state_id=:id"
            ),
            params,
        )


def _load(c: Connection, *, at: datetime) -> RiskState:
    row = (
        c.execute(
            text(
                "SELECT s.version,s.kill_switch,s.as_of,r.payload,r.as_of AS snapshot_at "
                "FROM risk_state s JOIN risk_snapshots r ON r.snapshot_id=s.risk_snapshot_id "
                "WHERE s.state_id=:id FOR UPDATE OF s"
            ),
            {"id": STATE_ID},
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        state = RiskState(as_of=at)
        _save(c, state, initial=True)
        return state
    payload = row["payload"]
    try:
        state = RiskState.model_validate_json(canonical(payload["body"]))
        if (
            payload["kind"] != STATE_ID
            or payload["hash"] != digest(payload["body"])
            or state.version != row["version"]
            or state.kill_switch != row["kill_switch"]
            or state.as_of != row["as_of"]
            or state.as_of != row["snapshot_at"]
        ):
            raise ValueError
    except (KeyError, TypeError, ValueError, ValidationError):
        raise EntryHalt("CORRUPT_RISK_STATE") from None
    if state.as_of > at:
        raise ValueError("OUT_OF_ORDER_RISK_STATE")
    return state


def _audit(c: Connection, kind: str, payload: object) -> None:
    c.execute(
        text("INSERT INTO audit_log(event_type,payload) VALUES (:kind,CAST(:body AS jsonb))"),
        {"kind": kind, "body": canonical(payload)},
    )


class RiskService:
    def __init__(
        self,
        db: Database,
        policy: RiskLimits | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.db = db
        self.governor = RiskGovernor(policy)
        self.clock = clock or (lambda: datetime.now(UTC))

    def _locked_state(self, c: Connection) -> tuple[RiskState, datetime]:
        c.execute(text("SELECT pg_advisory_xact_lock(:id)"), {"id": LOCK_ID})
        at = utc(self.clock())  # Waiting for the lock cannot freeze the evidence clock.
        return _load(c, at=at), at

    def state(self) -> RiskState:
        with self.db.transaction() as c:
            state, _ = self._locked_state(c)
        return state

    def set_kill_switch(self, active: bool, *, reason: str) -> RiskState:
        if type(active) is not bool or not reason.strip() or len(reason) > 256:
            raise ValueError("EXPLICIT_SWITCH_COMMAND_REQUIRED")
        with self.db.transaction() as c:
            old, at = self._locked_state(c)
            state = old.evolve(at=at, kill_switch=active)
            _save(c, state)
            _audit(
                c,
                "risk.switch.v1",
                {"active": active, "reason": reason, "state": state.model_dump(mode="json")},
            )
        return state

    def record_result(self, event_id: UUID, pnl: Decimal) -> RiskState:
        if not isinstance(event_id, UUID) or not isinstance(pnl, Decimal) or not pnl.is_finite():
            raise ValueError("INVALID_RISK_RESULT")
        with self.db.transaction() as c:
            old, at = self._locked_state(c)
            existing = c.scalar(
                text(
                    "SELECT payload FROM audit_log "
                    "WHERE event_type='risk.result.v1' AND payload->>'id'=:id"
                ),
                {"id": str(event_id)},
            )
            if existing is not None:
                if Decimal(existing["pnl"]) != pnl:
                    raise ValueError("RISK_RESULT_CONFLICT")
                return old
            state = old.after_result(pnl, at=at, policy=self.governor.policy)
            _save(c, state)
            _audit(
                c,
                "risk.result.v1",
                {
                    "id": str(event_id),
                    "pnl": str(pnl),
                    "policy": self.governor.policy.model_dump(mode="json"),
                    "state": state.model_dump(mode="json"),
                },
            )
        return state

    def assess(self, request: EntryRequest, account: AccountSnapshot) -> RiskAssessment:
        return self._assess(request, account)

    def guardian_recheck(self, quant: RiskAssessment, account: AccountSnapshot) -> RiskAssessment:
        return self._assess(quant.request, account, quant=quant)

    def _assess(
        self,
        request: EntryRequest,
        account: AccountSnapshot,
        *,
        quant: RiskAssessment | None = None,
    ) -> RiskAssessment:
        with self.db.transaction() as c:
            state, at = self._locked_state(c)
            result = self.governor.assess(request, account, state, at=at, db_writable=True)
            latch = state.daily_loss_day
            if (
                "DAILY_LOSS_LIMIT" in result.reason_codes
                and state.daily_loss_day != at.date()
                and account.trading_day == at.date()
                and 0
                <= (at - account.as_of).total_seconds()
                <= self.governor.policy.account_age_seconds
            ):
                latch = at.date()
            state = state.evolve(at=at, daily_loss_day=latch)
            _save(c, state)
            result = self.governor.assess(request, account, state, at=at, db_writable=True)
            if quant is not None:
                result = self.governor.guardian_recheck(
                    quant, account, state, at=at, db_writable=True
                )
            body = result.model_dump(mode="json")
            _audit(c, "risk.assessment.v1", {"id": result.digest, "body": body})
        # A write or COMMIT error exits through Database's halt path, never through here.
        return result
