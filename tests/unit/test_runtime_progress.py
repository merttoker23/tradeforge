"""Dispatch progress is measured separately from process/diagnostic heartbeats."""

from datetime import UTC, datetime, timedelta

from tradeforge.dashboard.read_model import refresh_freshness


def test_demo_diagnostic_running_does_not_claim_execution_progress():
    now = datetime.now(UTC)
    snapshot = {
        "status": "DEGRADED",
        "sections": {
            "demo": {"status": "OK", "data": [{"execution_engine": "RUNNING", "freshness": "OK"}]}
        },
    }
    state = refresh_freshness(snapshot, now)
    assert state["execution_readiness"]["ready"] is False
    assert state["execution_readiness"]["reason"] == "DISPATCH_NOT_OBSERVED"


def test_dispatch_readiness_expires_independently_of_diagnostics():
    now = datetime.now(UTC)
    snapshot = {
        "status": "DEGRADED",
        "sections": {
            "workers": {
                "status": "OK",
                "data": [
                    {
                        "service": "execution_core",
                        "freshness": "OK",
                        "fresh_until": (now + timedelta(seconds=15)).isoformat(),
                        "execution_ready": True,
                        "stage": "DISPATCH_COMPLETE",
                        "last_progress_at": now.isoformat(),
                        "reason": "READY",
                    }
                ],
            }
        },
    }
    assert refresh_freshness(snapshot, now)["execution_readiness"]["ready"] is True
    expired = refresh_freshness(snapshot, now + timedelta(seconds=16))
    assert expired["execution_readiness"]["ready"] is False
    assert expired["execution_readiness"]["reason"] == "DISPATCH_STALE"


def test_progress_never_records_exception_payload_and_stall_revokes_readiness():
    from tradeforge.runtime.progress import RuntimeProgress

    now = [datetime.now(UTC)]
    mono = [1.0]
    progress = RuntimeProgress(clock=lambda: now[0], monotonic=lambda: mono[0])
    progress.advance("RECONCILIATION")
    progress.fail(ValueError("SECRET_API_KEY_DO_NOT_OUTPUT"))
    failed = progress.snapshot()
    assert failed["last_error"] == "INPUT_INVALID"
    assert failed["error_count"] == 1
    assert "SECRET" not in str(failed)
    progress.complete(data_ready=True, analysis_ready=True, execution_ready=True, reason="READY")
    assert progress.snapshot()["execution_ready"] is True
    now[0] += timedelta(seconds=16)
    mono[0] += 16
    stalled = progress.snapshot()
    assert stalled["healthy"] is True
    assert stalled["execution_ready"] is False
    assert stalled["reason"] == "DISPATCH_STALE"
