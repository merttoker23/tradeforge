"""Successful dispatch is not a substitute for current broad quant evidence."""

from datetime import UTC, datetime, timedelta

from tradeforge.runtime.storage import save_runtime


def test_embedded_execution_requires_current_measured_quant_analysis(migrated):
    from tradeforge.runtime.auto_execution import analysis_ready

    at = datetime.now(UTC)
    assert not analysis_ready(migrated, at)
    save_runtime(
        migrated,
        "quant_core",
        at,
        at + timedelta(seconds=15),
        {
            "alive": True,
            "data_ready": True,
            "analysis_ready": False,
        },
    )
    assert not analysis_ready(migrated, at)
    current = at + timedelta(seconds=1)
    save_runtime(
        migrated,
        "quant_core",
        current,
        current + timedelta(seconds=15),
        {
            "alive": True,
            "data_ready": True,
            "analysis_ready": True,
        },
    )
    assert analysis_ready(migrated, current)
    assert not analysis_ready(migrated, current + timedelta(seconds=16))
    assert not analysis_ready(migrated, at)
