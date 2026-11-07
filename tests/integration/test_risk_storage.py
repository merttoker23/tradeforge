"""Real PostgreSQL failure/restart/UTC invariants for portfolio controls."""

from datetime import timedelta
from decimal import Decimal as D
from uuid import uuid4

import pytest
from sqlalchemy import text

from tests.risk_fixtures import NOW, account, change, request


def test_boot_is_locked_manual_clear_and_restart_restore_kill_switch(migrated):
    from tradeforge.risk.storage import RiskService

    service = RiskService(migrated, clock=lambda: NOW)
    assert "KILL_SWITCH" in service.assess(request(), account()).reason_codes
    service.set_kill_switch(False, reason="operator paper authorization")
    assert service.assess(request(), account()).allowed
    restored = RiskService(migrated, clock=lambda: NOW)
    restored.set_kill_switch(True, reason="operator halt")
    result = RiskService(migrated, clock=lambda: NOW).assess(request(), account())
    assert not result.allowed and result.can_reduce_risk
    with migrated.transaction() as c:
        assert (
            c.scalar(text("SELECT count(*) FROM audit_log WHERE event_type='risk.assessment.v1'"))
            == 3
        )


def test_loss_results_are_idempotent_streak_and_cooldown_survive_restart(migrated):
    from tradeforge.risk.storage import RiskService

    service = RiskService(migrated, clock=lambda: NOW)
    service.set_kill_switch(False, reason="operator")
    event = uuid4()
    service.record_result(event, D("-10"))
    service.record_result(event, D("-10"))
    assert service.state().consecutive_losses == 1
    with pytest.raises(ValueError, match="CONFLICT"):
        service.record_result(event, D("10"))
    assert "COOLDOWN" in service.assess(request(), account()).reason_codes
    for _ in range(2):
        service.record_result(uuid4(), D("-1"))
    later = NOW + timedelta(hours=1)
    restored = RiskService(migrated, clock=lambda: later)
    result = restored.assess(
        change(request(), observed_at=later, expires_at=later + timedelta(seconds=30)),
        change(account(), as_of=later, factors=()),
    )
    assert "LOSS_STREAK_LIMIT" in result.reason_codes
    assert "COOLDOWN" not in result.reason_codes
    assert restored.state().consecutive_losses == 3


def test_daily_loss_latch_resets_at_utc_midnight_without_rearming_other_controls(migrated):
    from tradeforge.risk.storage import RiskService

    at = NOW.replace(hour=23, minute=59, second=59)
    service = RiskService(migrated, clock=lambda: at)
    service.set_kill_switch(False, reason="operator")
    from tests.risk_fixtures import factor

    req = change(request(), observed_at=at, expires_at=at + timedelta(seconds=30))
    acc = change(account(), as_of=at, factors=(factor(as_of=at),), realized_pnl_today=D("-200"))
    assert "DAILY_LOSS_LIMIT" in service.assess(req, acc).reason_codes
    assert (
        "DAILY_LOSS_LIMIT"
        in service.assess(req, change(acc, realized_pnl_today=D("0"))).reason_codes
    )
    service.record_result(uuid4(), D("-1"))
    service.set_kill_switch(True, reason="halt overnight")
    tomorrow = at + timedelta(seconds=1)
    restored = RiskService(migrated, clock=lambda: tomorrow)
    acc = change(
        acc,
        trading_day=tomorrow.date(),
        as_of=tomorrow,
        realized_pnl_today=D("0"),
        factors=(factor(as_of=tomorrow),),
    )
    result = restored.assess(req, acc)
    assert "DAILY_LOSS_LIMIT" not in result.reason_codes
    assert "KILL_SWITCH" in result.reason_codes and "COOLDOWN" in result.reason_codes
    assert restored.state().consecutive_losses == 1
    # Overnight unrealized losses still count in the new UTC session.
    assert (
        "DAILY_LOSS_LIMIT"
        in restored.assess(req, change(acc, unrealized_pnl=D("-200"))).reason_codes
    )


def test_db_read_and_write_failures_cannot_return_approval(migrated):
    from tradeforge.persistence.db import EntryHalt
    from tradeforge.risk.storage import RiskService

    service = RiskService(migrated, clock=lambda: NOW)
    service.set_kill_switch(False, reason="operator")
    with migrated.transaction() as c:
        c.execute(
            text(
                "CREATE FUNCTION reject_risk_audit() RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN RAISE EXCEPTION 'injected write failure'; END $$"
            )
        )
        c.execute(
            text(
                "CREATE TRIGGER reject_risk_audit BEFORE INSERT ON audit_log "
                "FOR EACH ROW EXECUTE FUNCTION reject_risk_audit()"
            )
        )
    with pytest.raises(EntryHalt):
        service.assess(request(), account())
    assert not migrated.readiness.can_open_entries
    assert migrated.readiness.can_reduce_risk
    with migrated.transaction() as c:
        c.execute(text("DROP TRIGGER reject_risk_audit ON audit_log"))
        c.execute(text("DROP TABLE risk_state"))
    with pytest.raises(EntryHalt):
        service.assess(request(), account())


def test_corrupt_state_and_future_clock_cannot_silently_unlock(migrated):
    from tradeforge.persistence.db import EntryHalt
    from tradeforge.risk.storage import RiskService

    service = RiskService(migrated, clock=lambda: NOW)
    service.set_kill_switch(False, reason="operator")
    with pytest.raises(ValueError, match="OUT_OF_ORDER"):
        RiskService(migrated, clock=lambda: NOW - timedelta(seconds=1)).set_kill_switch(
            False, reason="bad clock"
        )
    with migrated.transaction() as c:
        # A mutable state row pointing at an unrelated immutable snapshot is corruption.
        c.execute(text("UPDATE risk_state SET kill_switch=true, version=version+1"))
    with pytest.raises(EntryHalt, match="CORRUPT_RISK_STATE"):
        service.assess(request(), account())


def test_guardian_uses_its_own_persistent_state_and_transaction(migrated):
    from tradeforge.risk.storage import RiskService

    service = RiskService(migrated, clock=lambda: NOW)
    service.set_kill_switch(False, reason="operator")
    quant = service.assess(request(), account())
    assert quant.allowed
    service.set_kill_switch(True, reason="operator")
    result = RiskService(migrated, clock=lambda: NOW).guardian_recheck(quant, account())
    assert result.stage == "guardian" and "KILL_SWITCH" in result.reason_codes


def test_simultaneous_results_are_serialized_without_lost_streak(migrated):
    from concurrent.futures import ThreadPoolExecutor

    from tradeforge.risk.storage import RiskService

    def lose(_):
        return RiskService(migrated, clock=lambda: NOW).record_result(uuid4(), D("-1"))

    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(lose, range(3)))
    assert RiskService(migrated, clock=lambda: NOW).state().consecutive_losses == 3


def test_assessment_advances_clock_watermark_and_samples_time_after_lock(migrated):
    from tradeforge.risk.storage import LOCK_ID, RiskService

    service = RiskService(migrated, clock=lambda: NOW)
    service.set_kill_switch(False, reason="operator")
    service = RiskService(migrated, clock=lambda: NOW + timedelta(seconds=1))
    service.assess(request(), account())
    with pytest.raises(ValueError, match="OUT_OF_ORDER"):
        RiskService(migrated, clock=lambda: NOW).assess(request(), account())

    # A clock callback probes the lock on an independent DB connection. It must
    # already be held by the service when the callback is invoked.
    def clock():
        with migrated.transaction() as c:
            assert not c.scalar(text("SELECT pg_try_advisory_xact_lock(:id)"), {"id": LOCK_ID})
        return NOW + timedelta(seconds=7)

    result = RiskService(migrated, clock=clock).assess(request(), account())
    assert "ACCOUNT_STALE" in result.reason_codes
