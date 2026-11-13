"""Real alpha evidence becomes a durable candidate without acquiring order authority."""

from sqlalchemy import text

from tests.unit.test_momentum_alpha import change, evidence
from tradeforge.domain.common import DataQuality


def test_alpha_cycle_records_armed_setup_once_across_restart(migrated):
    from tradeforge.runtime.analysis import AutonomousAnalysis

    sample = evidence()
    worker = AutonomousAnalysis(migrated, clock=lambda: sample.as_of)
    assert worker.evaluate((sample,)) == 1
    assert AutonomousAnalysis(migrated, clock=lambda: sample.as_of).evaluate((sample,)) == 0
    with migrated.transaction() as c:
        assert c.scalar(text("SELECT count(*) FROM decision_lifecycle")) == 3
        assert c.scalar(text("SELECT count(*) FROM execution_intents")) == 0
        assert c.scalar(text("SELECT count(*) FROM orders")) == 0
        assert (
            c.scalar(text("SELECT count(*) FROM audit_log WHERE event_type='runtime.candidate.v1'"))
            == 1
        )


def test_invalid_market_evidence_does_not_force_trade(migrated):
    from tradeforge.runtime.analysis import AutonomousAnalysis

    sample = evidence()
    worker = AutonomousAnalysis(migrated, clock=lambda: sample.as_of)
    assert worker.evaluate((change(sample, quality=DataQuality.GAP),)) == 0
    with migrated.transaction() as c:
        assert c.scalar(text("SELECT count(*) FROM decision_lifecycle")) == 0
        assert c.scalar(text("SELECT count(*) FROM execution_intents")) == 0


def test_no_trade_reports_bounded_safe_reason_counts_without_order_authority(migrated):
    from tradeforge.runtime.analysis import AutonomousAnalysis

    sample = evidence()
    worker = AutonomousAnalysis(migrated, clock=lambda: sample.as_of)
    assert worker.evaluate((change(sample, quality=DataQuality.GAP),)) == 0
    assert worker.state["candidate_count"] == 0
    assert worker.state["reason_counts"] == {"DATA_UNAVAILABLE": 2}
    assert worker.state["evaluated_count"] == 2
    assert worker.state["execution_ready"] is False
    assert "input" not in worker.state
    with migrated.transaction() as connection:
        assert connection.scalar(text("SELECT count(*) FROM execution_intents")) == 0


def test_analysis_explains_each_symbol_strategy_and_side_without_raw_inputs(migrated):
    from tradeforge.runtime.analysis import AutonomousAnalysis

    sample = evidence()
    worker = AutonomousAnalysis(migrated, clock=lambda: sample.as_of)
    worker.evaluate((change(sample, quality=DataQuality.GAP),))
    assert worker.state["analysis_symbols"] == 1
    rows = worker.state["symbol_results"]
    assert len(rows) == 2
    assert {r["side"] for r in rows} == {"LONG", "SHORT"}
    assert all(r["symbol"] == sample.instrument.symbol for r in rows)
    assert all(r["strategy"] == "MOMENTUM" for r in rows)
    assert all(r["state"] == "NO_TRADE" and r["reasons"] == ["DATA_UNAVAILABLE"] for r in rows)
    assert all(
        set(r)
        <= {
            "symbol",
            "side",
            "strategy",
            "state",
            "reasons",
            "as_of",
            "trigger",
            "stop",
            "target",
            "expires_at",
        }
        for r in rows
    )


def test_analysis_reports_latest_outcome_per_symbol_and_real_totals(migrated):
    from tradeforge.runtime.analysis import AutonomousAnalysis

    sample = evidence()
    worker = AutonomousAnalysis(migrated, clock=lambda: sample.as_of)
    assert worker.evaluate((change(sample, quality=DataQuality.GAP),)) == 0
    summaries = worker.state["symbol_summaries"]
    assert len(summaries) == 1
    summary = summaries[0]
    assert summary["symbol"] == "ETHUSDT"
    assert summary["decision"] == "NO_TRADE"
    assert summary["analyzed_at"] == sample.as_of.isoformat()
    assert "DATA_UNAVAILABLE" in summary["reasons"]
    assert worker.state["analyzed_symbols"] == 1
    assert worker.state["evaluation_total"] == 2
    assert worker.state["analysis_cycle_total"] == 1

    worker.evaluate((change(sample, quality=DataQuality.GAP),), symbols=frozenset({"ETHUSDT"}))
    assert {row["symbol"] for row in worker.state["symbol_summaries"]} == {"ETHUSDT"}
    assert worker.state["evaluation_total"] == 4
    assert worker.state["analysis_cycle_total"] == 2
