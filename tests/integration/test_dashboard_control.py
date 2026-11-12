"""Only the guardian consumes operator commands and changes durable risk."""

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import text


def setup(db):
    from tradeforge.dashboard.auth import Principal
    from tradeforge.dashboard.control import ExecutionCommands
    from tradeforge.risk.storage import RiskService

    at = [datetime.now(UTC)]
    operator = Principal("operator", frozenset({"read", "control"}))
    risk = RiskService(db, clock=lambda: at[0])
    risk.state()
    return at, operator, risk, ExecutionCommands(db, clock=lambda: at[0])


def request(action="resume", version=0, key=None):
    return {"action": action, "expected_version": version, "idempotency_key": str(key or uuid4())}


def seed(db, body, at):
    """Queue a durable dashboard command the way the retired submission API did."""
    payload = {
        **body,
        "subject": "operator",
        "status": "PENDING",
        "result": "AWAITING_GUARDIAN",
        "expires_at": (at + timedelta(seconds=30)).isoformat(),
    }
    with db.transaction() as c:
        c.execute(
            text(
                "INSERT INTO commands"
                "(command_id,idempotency_key,command_type,payload,observed_at) "
                "VALUES (:id,:key,:type,CAST(:body AS jsonb),:at)"
            ),
            {
                "id": uuid4(),
                "key": body["idempotency_key"],
                "type": "dashboard." + body["action"],
                "body": json.dumps(payload),
                "at": at,
            },
        )


def outcome(db, key):
    with db.transaction() as c:
        return (
            c.execute(
                text(
                    "SELECT payload->>'status' AS status,payload->>'result' AS result "
                    "FROM commands WHERE idempotency_key=:key"
                ),
                {"key": key},
            )
            .mappings()
            .one()
        )


def test_guardian_consumes_exactly_once_and_risk_stays_locked_before_apply(migrated):
    at, operator, risk, guardian = setup(migrated)
    body = request()
    seed(migrated, body, at[0])
    assert outcome(migrated, body["idempotency_key"])["result"] == "AWAITING_GUARDIAN"
    assert risk.state().kill_switch
    with migrated.transaction() as c:
        assert c.scalar(text("SELECT count(*) FROM commands")) == 1
    assert guardian.run_once(ready=lambda: True) == 1
    assert not risk.state().kill_switch
    assert outcome(migrated, body["idempotency_key"])["status"] == "APPLIED"
    assert guardian.run_once(ready=lambda: True) == 0


def test_guardian_rejects_unsafe_rearm_expiry_and_cas(migrated):
    at, _, risk, guardian = setup(migrated)
    body = request()
    seed(migrated, body, at[0])
    guardian.run_once(ready=lambda: False)
    assert outcome(migrated, body["idempotency_key"])["result"] == "EXECUTION_CORE_NOT_READY"
    assert risk.state().kill_switch
    late = request()
    seed(migrated, late, at[0])
    at[0] += timedelta(seconds=31)
    guardian.run_once(ready=lambda: True)
    assert outcome(migrated, late["idempotency_key"])["result"] == "COMMAND_EXPIRED"
    race = request()
    seed(migrated, race, at[0])
    risk.set_kill_switch(True, reason="INCIDENT")
    guardian.run_once(ready=lambda: True)
    assert outcome(migrated, race["idempotency_key"])["result"] == "RISK_VERSION_CONFLICT"
    assert risk.state().kill_switch


def test_stop_commands_apply_even_when_guardian_unsafe_and_survive_restart(migrated):
    from tradeforge.dashboard.control import ExecutionCommands
    from tradeforge.risk.storage import RiskService

    at, operator, risk, guardian = setup(migrated)
    risk.set_kill_switch(False, reason="TEST_SETUP")
    body = request("pause", version=1)
    seed(migrated, body, at[0])
    restarted = ExecutionCommands(migrated, clock=lambda: at[0])
    restarted.run_once(ready=lambda: False)
    assert RiskService(migrated).state().kill_switch
    assert outcome(migrated, body["idempotency_key"])["status"] == "APPLIED"


def test_concurrent_consumers_apply_exactly_once(migrated):
    from concurrent.futures import ThreadPoolExecutor

    at, _, risk, guardian = setup(migrated)
    seed(migrated, request(), at[0])
    with ThreadPoolExecutor(max_workers=2) as workers:
        counts = list(workers.map(lambda _: guardian.run_once(ready=lambda: True), range(2)))
    assert sum(counts) == 1
    assert risk.state().version == 1


def test_result_audit_failure_rolls_back_risk_and_command_atomically(migrated):
    from tradeforge.persistence.db import PersistenceError

    at, _, risk, guardian = setup(migrated)
    body = request()
    seed(migrated, body, at[0])
    with migrated.transaction() as c:
        c.execute(
            text(
                "CREATE FUNCTION reject_result() RETURNS trigger LANGUAGE plpgsql "
                "AS $$ BEGIN IF NEW.event_type='dashboard.command_result.v1' THEN "
                "RAISE EXCEPTION 'audit unavailable'; END IF; RETURN NEW; END $$"
            )
        )
        c.execute(
            text(
                "CREATE TRIGGER reject_result BEFORE INSERT ON audit_log "
                "FOR EACH ROW EXECUTE FUNCTION reject_result()"
            )
        )
    with pytest.raises(PersistenceError):
        guardian.run_once(ready=lambda: True)
    assert risk.state().kill_switch and risk.state().version == 0
    assert outcome(migrated, body["idempotency_key"])["status"] == "PENDING"


def test_testnet_trial_requires_testnet_guardian_and_preserves_halt(migrated):
    from tradeforge.dashboard.control import ExecutionCommands

    at, _, risk, guardian = setup(migrated)
    denied = request("testnet_start")
    seed(migrated, denied, at[0])
    guardian.run_once(ready=lambda: True)
    assert outcome(migrated, denied["idempotency_key"])["result"] == "TESTNET_EXECUTION_REQUIRED"
    allowed = request("testnet_start")
    seed(migrated, allowed, at[0])
    guardian.run_once(ready=lambda: True, testnet=True)
    assert outcome(migrated, allowed["idempotency_key"])["status"] == "APPLIED"
    assert risk.state().kill_switch
    restarted = ExecutionCommands(migrated, clock=lambda: at[0])
    assert restarted.testnet_authority() == (True, True)
    stop = request("testnet_stop", version=risk.state().version)
    seed(migrated, stop, at[0])
    restarted.run_once(ready=lambda: False, testnet=True)
    assert restarted.testnet_authority() == (False, False)
    assert risk.state().kill_switch


def test_orphan_cleanup_requires_halt_and_never_rearms(migrated):
    at, _, risk, guardian = setup(migrated)
    cleaned = []
    body = request("testnet_cleanup")
    seed(migrated, body, at[0])
    guardian.run_once(ready=lambda: False, testnet=True, cleanup=lambda: cleaned.append(True))
    assert cleaned == [True]
    assert risk.state().kill_switch
    assert outcome(migrated, body["idempotency_key"])["status"] == "APPLIED"
    risk.set_kill_switch(False, reason="TEST_SETUP")
    body = request("testnet_cleanup", version=risk.state().version)
    seed(migrated, body, at[0])
    guardian.run_once(ready=lambda: True, testnet=True, cleanup=lambda: cleaned.append(True))
    assert outcome(migrated, body["idempotency_key"])["result"] == "PAUSE_REQUIRED"
    assert cleaned == [True]
