import json
from datetime import timedelta
from decimal import Decimal as D

import pytest
from sqlalchemy import text

from tests.decision_fixtures import NOW, change
from tests.edge_fixtures import analyze, trades


def test_outcome_costs_forecast_error_and_versioned_analysis_survive_restart(migrated):
    from tradeforge.decision.edge import EdgeStore

    row = trades(1, 0)[0]
    store = EdgeStore(migrated)
    store.save_outcome(row)
    store.save_outcome(row)  # same immutable identity is idempotent
    report = analyze()
    store.save_analysis(report)
    restored = EdgeStore(migrated)
    loaded = restored.load_outcomes(at=NOW)
    assert len(loaded) == 1
    assert loaded[0].net_R == row.net_R
    assert loaded[0].forecast_error_R == row.forecast_error_R
    assert restored.load_outcomes(at=row.closed_at) == ()
    assert restored.load_analysis(report.digest, at=NOW) == report
    assert restored.load_analysis(report.digest, at=NOW - timedelta(seconds=1)) is None
    with pytest.raises(ValueError, match="CONFLICT"):
        store.save_outcome(change(row, gross_R=D("99")))


def test_corrupt_evidence_and_db_failure_never_return_success(migrated):
    from tradeforge.decision.edge import EdgeStore
    from tradeforge.persistence.db import EntryHalt

    store = EdgeStore(migrated)
    with migrated.transaction() as c:
        c.execute(
            text(
                "INSERT INTO audit_log(event_type,payload) VALUES "
                "('edge.outcome.v1', CAST(:payload AS jsonb))"
            ),
            {"payload": json.dumps({"id": "bad", "hash": "0" * 64, "body": {}})},
        )
    with pytest.raises(ValueError):
        store.load_outcomes(at=NOW)
    with migrated.transaction() as c:
        c.execute(text("DROP TABLE audit_log"))
    with pytest.raises(EntryHalt):
        store.save_outcome(trades(1, 0)[0])
