from datetime import timedelta
from decimal import Decimal as D

import pytest

from tests.decision_fixtures import NOW, change
from tests.edge_fixtures import inputs


def test_round_trip_fees_and_normal_stress_attribution():
    from tradeforge.decision.cost_model import estimate_cost

    quote = estimate_cost(inputs(), at=NOW)
    assert quote.normal_R == D("0.14")
    assert quote.stress_R == D("0.21")
    assert quote.fees_R == D("0.07")
    assert quote.reason_codes == ()
    assert estimate_cost(inputs(entry_role="taker"), at=NOW).stress_R == D("0.24")


def test_unknown_fee_or_uncertainty_is_never_zero_cost():
    from tradeforge.decision.cost_model import estimate_cost

    for patch in ({"maker_fee_bps": None}, {"stress": None}, {"normal": None}):
        quote = estimate_cost(inputs(**patch), at=NOW)
        assert quote.stress_R is None
        assert quote.reason_codes
    # A missing unused maker fee does not invalidate an all-taker quote.
    assert estimate_cost(inputs(maker_fee_bps=None, entry_role="taker"), at=NOW).stress_R


def test_rebate_is_observed_but_never_forecast_as_free_edge():
    from tradeforge.decision.cost_model import RealizedCost, estimate_cost

    assert estimate_cost(inputs(maker_fee_bps=D("-2")), at=NOW).fees_R == D("0.05")
    actual = RealizedCost(
        maker_fees=D("-0.1"),
        taker_fees=D("0.5"),
        spread=D("0.2"),
        slippage=D("0.3"),
        funding=D("-0.1"),
        latency=D("0.1"),
    )
    assert actual.total == D("0.9")


def test_expired_future_or_inverted_stress_costs_fail_closed():
    from tradeforge.decision.cost_model import estimate_cost

    assert estimate_cost(inputs(), at=NOW + timedelta(minutes=1)).stress_R is None
    assert estimate_cost(inputs(observed_at=NOW + timedelta(seconds=1)), at=NOW).stress_R is None
    c = inputs()
    with pytest.raises(ValueError, match="STRESS"):
        inputs(stress=change(c.stress, slippage_bps=D("0")))


@pytest.mark.parametrize("bad", [D("NaN"), D("Infinity"), 0.1, True])
def test_monetary_inputs_cannot_be_nonfinite_or_float(bad):
    with pytest.raises(ValueError):
        inputs(notional=bad)
