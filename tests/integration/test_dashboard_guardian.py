"""Dashboard lifecycle is independent of protection; commands run before dispatch."""

from tests.integration.test_dashboard_control import request, seed, setup
from tests.integration.test_protection_recovery import prepared


def test_dashboard_stopped_does_not_stop_embedded_protection(seeded):
    from tradeforge.dashboard.api import create_app

    db, _, client, recovery, _ = prepared(seeded)
    unused_dashboard = create_app()
    assert unused_dashboard is not None
    # No dashboard listener or task is running. Embedded protection still repairs the stop.
    recovery.reconcile()
    assert recovery.ready
    assert any(method == "POST" and "algoOrder" in path for method, path, _ in client.calls)
    from tradeforge.dashboard.read_model import ReadModel

    protectors = ReadModel(db).snapshot()["sections"]["stops"]["data"]
    assert {row["kind"] for row in protectors} == {"STOP_MARKET", "TAKE_PROFIT_MARKET"}


def test_execution_cycle_processes_pause_before_entry_and_preserves_recovery(migrated):
    from tradeforge.runtime.auto_execution import execution_cycle

    at, _, risk, commands = setup(migrated)
    risk.set_kill_switch(False, reason="TEST_SETUP")
    seed(migrated, request("pause", version=1), at[0])
    calls = []

    class Recovery:
        ready = True

        def reconcile(self):
            calls.append("protection")

    class Stream:
        ready = True

        def mark_recovered(self, value):
            calls.append("stream")

    class Worker:
        def run_once(self):
            assert risk.state().kill_switch
            calls.append("worker")

    execution_cycle(Recovery(), Stream(), Worker(), commands)
    assert calls == ["protection", "stream", "worker"]
