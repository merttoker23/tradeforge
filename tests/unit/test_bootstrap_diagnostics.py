"""A finite transport trial cannot silently become statistical qualification."""

from datetime import timedelta

from tests.decision_fixtures import NOW, change
from tests.edge_fixtures import analyze, trades
from tradeforge.decision.trial import TestnetTrialPolicy as TrialPolicy
from tradeforge.domain.decision import ProofState


def test_twenty_trial_outcomes_cannot_meet_two_hundred_effective_sample_requirement():
    from tradeforge.decision.bootstrap import bootstrap_diagnostics

    rows = tuple(change(r, forecast_p_win=None) for r in trades(5, 5))
    rows += tuple(
        change(r, forecast_p_win=None)
        for r in trades(5, 5, start=NOW - timedelta(hours=1), split="OOS", offset=100)
    )
    edge = analyze(rows)
    report = bootstrap_diagnostics(edge, TrialPolicy(enabled=True))
    assert report["minimum_total_effective_samples"] == 200
    assert report["maximum_trial_entries"] == 20
    assert report["train_missing"] == 90 and report["oos_missing"] == 90
    assert report["trial_can_self_validate"] is False
    assert "TRIAL_CANNOT_MEET_SAMPLE_MINIMUM" in report["bootstrap_blockers"]
    assert "PROBABILITY_FORECASTS_UNAVAILABLE" in report["bootstrap_blockers"]
    assert "UNCALIBRATED" in report["edge_reasons"]
    assert edge.proof_state is ProofState.UNPROVEN
    assert report["execution_authorized"] is False


def test_more_positive_unforecast_outcomes_do_not_fix_calibration():
    from tradeforge.decision.bootstrap import bootstrap_diagnostics

    edge = analyze(
        tuple(
            change(r, forecast_p_win=None)
            for r in (
                trades()
                + trades(180, 220, start=NOW - timedelta(hours=1), split="OOS", offset=2000)
            )
        )
    )
    report = bootstrap_diagnostics(edge, TrialPolicy())
    assert report["train_missing"] == 0 and report["oos_missing"] == 0
    assert "PROBABILITY_FORECASTS_UNAVAILABLE" in report["bootstrap_blockers"]
    assert report["calibration_label"] == "uncalibrated"
    assert report["trial_enabled"] is False
    assert report["execution_authorized"] is False


def test_valid_paper_evidence_remains_paper_and_diagnostics_never_grants_authority():
    from tradeforge.decision.bootstrap import bootstrap_diagnostics

    edge = analyze()
    report = bootstrap_diagnostics(edge, TrialPolicy())
    assert report["evidence_environment"] == "paper"
    assert report["proof_state"] == "VALIDATED"
    assert report["calibration_label"] == "calibrated"
    assert report["execution_authorized"] is False
    assert "PROBABILITY_FORECASTS_UNAVAILABLE" not in report["bootstrap_blockers"]
    assert edge.confidence is None  # A bucket analysis is not a per-entry forecast.


def test_replay_outcomes_cannot_qualify_testnet_even_when_sample_statistics_match():
    from tradeforge.config.policy import RuntimeMode
    from tradeforge.decision.bootstrap import bootstrap_diagnostics

    original = analyze()
    edge = change(
        original, bucket=change(original.bucket, environment="replay"), mode=RuntimeMode.TESTNET
    )
    report = bootstrap_diagnostics(edge, TrialPolicy(enabled=True))
    assert report["proof_state"] == "UNPROVEN"
    assert "EVIDENCE_ENVIRONMENT_MISMATCH" in report["edge_reasons"]
    assert report["execution_authorized"] is False
