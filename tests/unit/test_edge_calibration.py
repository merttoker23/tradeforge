from datetime import timedelta
from decimal import Decimal as D

import pytest

from tests.decision_fixtures import NOW, change
from tests.edge_fixtures import analyze, inputs, trades
from tradeforge.config.policy import RuntimeMode
from tradeforge.domain.decision import ProofState, Side


def test_forty_five_percent_wins_can_have_positive_conservative_expectation():
    result = analyze()
    assert result.proof_state is ProofState.VALIDATED
    assert result.net_ev_low_R > D("0.1")
    assert result.side is Side.LONG
    assert result.calibration_label == "calibrated"
    assert not result.live_approval
    assert result.confidence is None  # no individual forecast from a bucket win rate


def test_seventy_percent_wins_can_still_lose_money():
    rows = trades(700, 300, payoff="0.2", predicted_p=0.7)
    rows += trades(
        140,
        60,
        payoff="0.2",
        predicted_p=0.7,
        start=NOW - timedelta(hours=1),
        split="OOS",
        offset=2000,
    )
    result = analyze(rows)
    assert result.net_ev_low_R < 0
    assert result.side is Side.NO_TRADE
    assert "EDGE_BELOW_POLICY" in result.reason_codes


def test_fee_inversion_and_strict_policy_minimum():
    from tradeforge.decision.calibration import EdgePolicy

    result = analyze()
    assert analyze(policy=EdgePolicy(min_net_R=result.net_ev_low_R)).side is Side.NO_TRADE
    assert analyze(cost=inputs(taker_fee_bps=D("100"))).side is Side.NO_TRADE


@pytest.mark.parametrize(
    "kind",
    [
        "small",
        "stale",
        "missing_oos",
        "bad_oos",
        "shift",
        "unknown_probability",
        "miscalibrated",
        "leak",
    ],
)
def test_unproven_or_shifted_evidence_is_uncalibrated_and_closed(kind):
    rows = trades()
    oos = trades(180, 220, start=NOW - timedelta(hours=1), split="OOS", offset=2000)
    patch = {}
    if kind == "small":
        rows = rows[:5]
    elif kind == "stale":
        from tradeforge.decision.calibration import EdgePolicy

        patch["policy"] = EdgePolicy(max_age_seconds=60)
    elif kind == "missing_oos":
        oos = ()
    elif kind == "bad_oos":
        oos = tuple(change(r, gross_R=-D("1")) for r in oos)
    elif kind == "shift":
        oos = trades(10, 190, start=NOW - timedelta(hours=1), split="OOS", offset=2000)
    elif kind == "unknown_probability":
        oos = tuple(change(r, forecast_p_win=None) for r in oos)
    elif kind == "miscalibrated":
        oos = tuple(change(r, forecast_p_win=0.95) for r in oos)
    elif kind == "leak":
        oos = trades(90, 110, start=NOW - timedelta(days=2), split="OOS", offset=2000)
    result = analyze(rows + oos, **patch)
    assert result.proof_state is ProofState.UNPROVEN
    assert result.side is Side.NO_TRADE
    assert result.calibration_label == "uncalibrated"
    assert result.confidence is None
    assert not result.live_approval


@pytest.mark.parametrize("patch", [dict(maker_fee_bps=None), dict(stress=None)])
def test_live_unknown_cost_or_probability_is_no_trade(patch):
    result = analyze(cost=inputs(**patch), mode=RuntimeMode.LIVE)
    assert result.side is Side.NO_TRADE
    assert not result.live_approval
    assert result.net_ev_low_R is None
    assert analyze((), mode=RuntimeMode.LIVE).side is Side.NO_TRADE


def test_analysis_is_versioned_replayable_and_does_not_mutate_policy():
    from tradeforge.decision.edge import EdgeAnalysis

    result = analyze()
    replay = EdgeAnalysis.model_validate_json(result.model_dump_json())
    assert result.digest == replay.digest
    assert result.reason_codes == replay.reason_codes
    assert analyze(policy=change(result.policy, min_net_R=D("0.1"))).digest != result.digest


def test_probability_prediction_must_exist_before_entry():
    row = trades(1, 0)[0]
    with pytest.raises(ValueError, match="FORECAST"):
        change(row, forecast_at=row.closed_at)


def test_training_results_must_be_available_before_oos_begins():
    # Close time alone cannot prove the training result was known at the OOS cutoff.
    rows = tuple(change(r, observed_at=NOW - timedelta(minutes=10)) for r in trades())
    oos = trades(180, 220, start=NOW - timedelta(hours=1), split="OOS", offset=2000)
    assert analyze(rows + oos).side is Side.NO_TRADE


def test_oos_payoff_shift_cannot_pass_on_historical_cheap_costs():
    rows = trades(payoff="10")
    oos = trades(180, 220, payoff="1.6", start=NOW - timedelta(hours=1), split="OOS", offset=2000)
    result = analyze(rows + oos)
    assert result.net_ev_low_R < 0
    assert result.side is Side.NO_TRADE


def test_oos_forecasts_cannot_predate_the_training_evidence():
    rows = trades()
    oos = trades(180, 220, start=NOW - timedelta(hours=1), split="OOS", offset=2000)
    oos = tuple(change(r, forecast_at=NOW - timedelta(days=2)) for r in oos)
    assert analyze(rows + oos).side is Side.NO_TRADE
