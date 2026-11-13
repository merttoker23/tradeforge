"""Heartbeats are bounded gauges; retention never touches trading evidence."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from tradeforge.runtime.maintenance import StorageBudget, observation_admission, observe_storage
from tradeforge.runtime.progress import RuntimeProgress
from tradeforge.runtime.storage import save_runtime


def set_storage_ready(monkeypatch, ready):
    budget = StorageBudget(2 * 1024**3 if ready else 0, 0, 0)
    monkeypatch.setattr("tradeforge.runtime.maintenance.storage_budget", lambda db: budget)


def test_guardian_progress_preserves_independent_storage_measurement(migrated, monkeypatch):
    clock = [datetime.now(UTC) - timedelta(seconds=2)]

    class ProbeClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0]

    monkeypatch.setattr("tradeforge.runtime.maintenance.datetime", ProbeClock)
    set_storage_ready(monkeypatch, True)
    assert observe_storage(migrated) is True
    clock[0] += timedelta(seconds=1)
    progress = RuntimeProgress(clock=lambda: clock[0])
    progress.advance("RECONCILIATION")
    at = clock[0]
    save_runtime(migrated, "execution_core", at, at + timedelta(seconds=15), progress.snapshot())
    assert observation_admission(migrated) is True
    with migrated.transaction() as c:
        payload = c.scalar(text("SELECT payload FROM runtime_observations"))
    assert payload["stage"] == "RECONCILIATION"
    assert payload["storage_ready"] is True


def test_guardian_heartbeat_cannot_renew_expired_storage_evidence(migrated):
    source = datetime.now(UTC) - timedelta(seconds=31)
    save_runtime(
        migrated,
        "execution_core",
        source,
        source + timedelta(seconds=30),
        {
            "storage_ready": True,
            "storage_checked_at": source.isoformat(),
        },
    )
    at = datetime.now(UTC)
    save_runtime(
        migrated, "execution_core", at, at + timedelta(seconds=15), {"stage": "RECONCILIATION"}
    )
    assert observation_admission(migrated) is False
    with migrated.transaction() as c:
        payload = c.scalar(text("SELECT payload FROM runtime_observations"))
    assert payload["storage_ready"] is True
    assert payload["storage_checked_at"] == source.isoformat()


@pytest.mark.parametrize("probe_delay", [timedelta(0), timedelta(seconds=1)])
def test_failed_storage_probe_revokes_admission_after_heartbeat(migrated, monkeypatch, probe_delay):
    source = datetime.now(UTC) - timedelta(seconds=3)
    clock = [source]

    class ProbeClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0]

    monkeypatch.setattr("tradeforge.runtime.maintenance.datetime", ProbeClock)
    set_storage_ready(monkeypatch, True)
    assert observe_storage(migrated) is True
    clock[0] += probe_delay
    set_storage_ready(monkeypatch, False)
    assert observe_storage(migrated) is False
    at = clock[0] = source + timedelta(seconds=2)
    save_runtime(
        migrated, "execution_core", at, at + timedelta(seconds=15), {"stage": "RECONCILIATION"}
    )
    assert observation_admission(migrated) is False
    with migrated.transaction() as c:
        payload = c.scalar(text("SELECT payload FROM runtime_observations"))
    assert payload["storage_ready"] is False
    assert payload["storage_checked_at"] == (source + probe_delay).isoformat()


def test_equal_time_positive_probe_cannot_resurrect_failed_storage(migrated, monkeypatch):
    source = datetime.now(UTC) - timedelta(seconds=1)

    class ProbeClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return source

    monkeypatch.setattr("tradeforge.runtime.maintenance.datetime", ProbeClock)
    set_storage_ready(monkeypatch, False)
    assert observe_storage(migrated) is False
    set_storage_ready(monkeypatch, True)
    assert observe_storage(migrated) is True
    assert observation_admission(migrated) is False
    with migrated.transaction() as c:
        payload = c.scalar(text("SELECT payload FROM runtime_observations"))
    assert payload["storage_ready"] is False
    assert payload["storage_checked_at"] == source.isoformat()


def test_equal_time_heartbeat_preserves_original_storage_expiry(migrated):
    at = datetime.now(UTC)
    source = at - timedelta(seconds=31)
    save_runtime(
        migrated,
        "execution_core",
        at,
        at + timedelta(seconds=15),
        {
            "storage_ready": True,
            "storage_checked_at": source.isoformat(),
            "stage": "BEFORE",
        },
    )
    save_runtime(migrated, "execution_core", at, at + timedelta(seconds=15), {"stage": "AFTER"})
    assert observation_admission(migrated) is False
    with migrated.transaction() as c:
        payload = c.scalar(text("SELECT payload FROM runtime_observations"))
    assert payload["storage_checked_at"] == source.isoformat()
    assert payload["stage"] == "BEFORE"


@pytest.mark.parametrize(
    "source", [None, "invalid", "2099-01-01T00:00:00+00:00", "2026-01-01T00:00:00"]
)
def test_storage_admission_requires_original_safe_measurement_time(migrated, source):
    at = datetime.now(UTC)
    save_runtime(
        migrated,
        "execution_core",
        at,
        at + timedelta(seconds=15),
        {
            "storage_ready": True,
            "storage_checked_at": source,
        },
    )
    assert observation_admission(migrated) is False


def test_heartbeat_count_is_bounded_and_late_sample_cannot_resurrect(migrated):
    at = datetime.now(UTC) - timedelta(seconds=40)
    for i in range(20):
        stamp = at + timedelta(seconds=i)
        save_runtime(
            migrated, "quant_core", stamp, stamp + timedelta(seconds=15), {"reason": "NO_TRADE"}
        )
    save_runtime(migrated, "quant_core", at, at + timedelta(seconds=15), {"reason": "OLD"})
    with migrated.transaction() as c:
        assert c.scalar(text("SELECT count(*) FROM runtime_observations")) == 1
        assert c.scalar(text("SELECT payload->>'reason' FROM runtime_observations")) == "NO_TRADE"


def test_retention_preserves_trading_and_rejects_non_owner_delete(seeded, tmp_path):
    from tradeforge.runtime.maintenance import retain

    db, _, _ = seeded
    retain(db, volume_path=tmp_path)
    with db.transaction() as c:
        assert c.scalar(text("SELECT count(*) FROM decisions")) == 1
        assert c.scalar(text("SELECT count(*) FROM risk_snapshots")) == 1


def test_retention_prunes_redundant_guardian_snapshots_but_keeps_context(migrated):
    with migrated.transaction() as c:
        c.execute(
            text(
                "INSERT INTO account_snapshots(snapshot_id,as_of,payload) VALUES "
                "(gen_random_uuid(),now()-interval '40 days','{\"kind\":\"guardian.account.v1\"}'),"
                "(gen_random_uuid(),now()-interval '40 days','{\"kind\":\"execution.context.v1\"}')"
            )
        )
        c.execute(text("SELECT retain_runtime_history()"))
        assert c.scalar(text("SELECT count(*) FROM account_snapshots")) == 1
        assert (
            c.scalar(text("SELECT payload->>'kind' FROM account_snapshots"))
            == "execution.context.v1"
        )
