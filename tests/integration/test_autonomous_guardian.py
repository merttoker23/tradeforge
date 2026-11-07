"""Explicit Demo enablement cannot override durable operator halts or reservations."""

from sqlalchemy import text

from tests.execution_fixtures import setup_execution


def test_transport_gap_is_transient_when_runtime_owns_separate_readiness(seeded):
    from tradeforge.execution.private_stream import PrivateStream
    from tradeforge.risk.storage import RiskService

    db, _, _, at = setup_execution(seeded)
    risk = RiskService(db, clock=lambda: at)
    risk.set_kill_switch(False, reason="OFFLINE_EXPLICIT_ENABLE")
    stream = PrivateStream(db, lambda _: None, clock=lambda: at, latch_gap=False)
    stream.ready = True
    stream.gap()
    assert not stream.ready and not stream.connected
    assert risk.state().kill_switch is False
    stream.mark_recovered(True)
    assert not stream.ready


def test_initial_demo_activation_never_clears_existing_manual_kill(migrated):
    from tradeforge.risk.storage import RiskService
    from tradeforge.runtime.trading import initial_demo_activation

    risk = RiskService(migrated)
    risk.set_kill_switch(True, reason="OPERATOR_KILL")
    assert initial_demo_activation(migrated, first_start=False, ready=True) is False
    assert risk.state().kill_switch
    with migrated.transaction() as c:
        assert c.scalar(text("SELECT count(*) FROM orders")) == 0


def test_initial_demo_activation_waits_for_account_and_stream(migrated):
    from tradeforge.risk.storage import RiskService
    from tradeforge.runtime.trading import initial_demo_activation

    assert not initial_demo_activation(migrated, first_start=True, ready=False)
    assert initial_demo_activation(migrated, first_start=True, ready=True)
    assert not RiskService(migrated).state().kill_switch
