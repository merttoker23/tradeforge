from datetime import timedelta
from decimal import Decimal as D
from uuid import UUID

import pytest

from tests.decision_fixtures import NOW, change
from tests.edge_fixtures import bucket, trades


def test_realized_r_and_forecast_error_are_net_of_all_costs():
    row = trades(1, 0)[0]
    assert row.net_R == D("2.98")
    assert row.forecast_error_R == D("2.78")


def test_beta_posterior_conservative_bound_not_raw_win_rate():
    from tradeforge.decision.stats import summarize

    result = summarize(trades(), at=NOW, bucket=bucket())
    assert result.count == result.effective_samples == 1000
    assert 0.37 < result.p_lower < 0.45
    assert 0.45 < result.p_upper < 0.53
    assert result.avg_win_low_R == D("3")
    assert result.avg_loss_high_R == D("1")
    assert result.net_mean_low_R > 0


def test_overlap_clusters_and_duplicate_outcomes_cannot_inflate_evidence():
    from tradeforge.decision.stats import summarize

    rows = trades(20, 0)
    overlapping = tuple(
        change(r, opened_at=rows[0].opened_at, forecast_at=rows[0].opened_at) for r in rows
    )
    result = summarize(overlapping, at=NOW, bucket=bucket())
    assert result.count == 20
    assert result.effective_samples == 1
    with pytest.raises(ValueError, match="DUPLICATE"):
        summarize((rows[0], rows[0]), at=NOW, bucket=bucket())


def test_bucket_and_point_in_time_filtering_and_order_invariance():
    from tradeforge.decision.stats import summarize

    rows = trades(20, 20)
    wrong_bucket = change(rows[0], trade_id=UUID(int=999), bucket=bucket(symbol="ETHUSDT"))
    future = change(rows[0], trade_id=UUID(int=998), observed_at=NOW + timedelta(seconds=1))
    a = summarize(rows, at=NOW, bucket=bucket())
    assert summarize(tuple(reversed(rows)) + (wrong_bucket, future), at=NOW, bucket=bucket()) == a


def test_bootstrap_payoffs_and_negative_tail_are_not_hidden():
    from tradeforge.decision.stats import summarize

    rows = trades(45, 55)
    tail = change(rows[0], gross_R=D("-100"))
    result = summarize((tail,) + rows[1:], at=NOW, bucket=bucket())
    assert result.avg_loss_high_R > D("1")
    assert result.net_mean_low_R < 0


def test_empty_sample_has_no_invented_probability_or_payoff():
    from tradeforge.decision.stats import summarize

    s = summarize((), at=NOW, bucket=bucket())
    assert s.p_lower is s.avg_win_low_R is s.avg_loss_high_R is s.net_mean_low_R is None


@pytest.mark.parametrize(
    "patch",
    [
        dict(initial_risk=D("0")),
        dict(gross_R=D("NaN")),
        dict(closed_at=NOW - timedelta(days=2)),
        dict(forecast_p_win=float("nan")),
    ],
)
def test_invalid_outcome_is_rejected(patch):
    with pytest.raises(ValueError):
        change(trades(1, 0)[0], **patch)
