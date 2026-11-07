from decimal import Decimal as D

import pytest
from pydantic import ValidationError

from tests.risk_fixtures import account, assess, change, factor, request
from tradeforge.domain.decision import Side


def test_stop_risk_includes_cost_and_ignores_leverage():
    for leverage in (D("1"), D("10")):
        result = assess(request(requested_qty=D("100"), leverage=leverage))
        assert result.allowed
        assert result.sized.qty == D("4.16")  # 0.25% * 10k / (5 + 1), floored
        assert result.sized.risk == D("24.96")
        assert result.live_approval is False


def test_tick_rounding_recalculates_budget_before_lot_rounding():
    result = assess(request(requested_qty=D("100"), entry_price=D("100.01"), stop_price=D("95.09")))
    assert result.allowed
    assert result.sized.entry_price == D("100.1")
    assert result.sized.stop_price == D("95.0")
    assert result.sized.qty == D("4.09")
    assert result.sized.risk == D("24.949")
    assert result.sized.risk <= D("25")


def test_short_rounding_is_adverse_and_rounding_over_cap_is_rejected():
    short = assess(request(side=Side.SHORT, entry_price=D("100.09"), stop_price=D("104.91")))
    assert short.allowed
    assert short.sized.entry_price == D("100.0")
    assert short.sized.stop_price == D("105.0")
    req = request(requested_qty=D("100"))
    # Minimum lot risk is 60, well beyond the 25 currency-unit risk budget.
    req = change(req, filters=change(req.filters, min_qty=D("10"), step_size=D("10")))
    assert "ROUNDING_OR_FILTER_LIMIT" in assess(req).reason_codes


def test_high_vol_and_bad_liquidity_only_reduce_size():
    base = assess(request(requested_qty=D("100")))
    high = assess(request(requested_qty=D("100")), account(factors=(factor(realized_vol=D("1")),)))
    assert high.sized.qty == D("2.08")
    thin = assess(request(requested_qty=D("100"), liquidity_scale=D("0.5")))
    assert thin.sized.qty == high.sized.qty < base.sized.qty
    assert "LIQUIDITY_UNSAFE" in assess(request(spread_bps=D("100"))).reason_codes
    assert not assess(request(liquidity_scale=D("0"))).allowed


@pytest.mark.parametrize(
    "patch",
    [
        {"entry_price": D("NaN")},
        {"requested_qty": 1.0},
        {"cost_per_unit": D("-1")},
        {"leverage": D("0")},
        {"liquidity_scale": D("1.1")},
        {"side": Side.NO_TRADE},
        {"stop_price": D("101")},
    ],
)
def test_invalid_sizing_inputs_never_enter_math(patch):
    with pytest.raises((ValidationError, ValueError)):
        request(**patch)
