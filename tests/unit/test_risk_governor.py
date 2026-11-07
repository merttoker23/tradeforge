from datetime import timedelta
from decimal import Decimal as D

import pytest

from tests.risk_fixtures import (
    NOW,
    account,
    assess,
    change,
    correlation,
    factor,
    position,
    state,
)
from tradeforge.domain.decision import Side


def test_default_unknown_state_is_closed_and_high_alpha_cannot_bypass():
    from tradeforge.risk.kill_switch import RiskState

    assert not assess(risk_state=RiskState(as_of=NOW), alpha_score=100.0).allowed
    assert not assess(acc=account(realized_pnl_today=D("-200")), alpha_score=100.0).allowed
    assert assess().allowed


@pytest.mark.parametrize(
    "patch,reason",
    [
        ({"equity": None}, "ACCOUNT_UNKNOWN"),
        ({"available_balance": None}, "ACCOUNT_UNKNOWN"),
        ({"day_start_equity": None}, "ACCOUNT_UNKNOWN"),
        ({"unrealized_pnl": None}, "ACCOUNT_UNKNOWN"),
        ({"realized_pnl_today": None}, "ACCOUNT_UNKNOWN"),
        ({"as_of": NOW - timedelta(seconds=6)}, "ACCOUNT_STALE"),
        ({"as_of": NOW + timedelta(seconds=1)}, "ACCOUNT_STALE"),
        ({"protection_ready": False}, "PROTECTION_NOT_READY"),
        ({"positions_complete": False}, "ACCOUNT_INCOMPLETE"),
        ({"available_balance": D("0")}, "BALANCE_LIMIT"),
        ({"trading_day": (NOW - timedelta(days=1)).date()}, "UTC_DAY_MISMATCH"),
        ({"realized_pnl_today": D("-100"), "unrealized_pnl": D("-100")}, "DAILY_LOSS_LIMIT"),
    ],
)
def test_account_hard_gates(patch, reason):
    assert reason in assess(acc=account(**patch)).reason_codes


def test_db_failure_does_not_revoke_emergency_repair_or_exit():
    result = assess(db_writable=False, risk_state=state(kill_switch=True))
    assert not result.allowed
    assert "DB_UNAVAILABLE" in result.reason_codes
    assert result.can_reduce_risk


def test_max_positions_heat_symbol_side_and_delta_use_pending_exposure():
    from tradeforge.risk.limits import RiskLimits

    acc = account(pending=(position("ETHUSDT"),))
    assert "MAX_POSITIONS" in assess(acc=acc, policy=RiskLimits(max_positions=1)).reason_codes
    assert "HEAT_LIMIT" in assess(acc=acc, policy=RiskLimits(max_heat=D("0.001"))).reason_codes
    assert (
        "SYMBOL_LIMIT"
        in assess(acc=acc, policy=RiskLimits(max_symbol_notional=D("0.015"))).reason_codes
    )
    assert (
        "SIDE_LIMIT"
        in assess(acc=acc, policy=RiskLimits(max_side_notional=D("0.015"))).reason_codes
    )
    assert "DELTA_LIMIT" in assess(acc=acc, policy=RiskLimits(max_delta=D("0.015"))).reason_codes


def test_unknown_beta_vol_and_correlation_block_without_assumed_defaults():
    for patch in ({"beta": None}, {"realized_vol": None}, {"as_of": NOW - timedelta(minutes=5)}):
        assert "FACTOR_UNKNOWN" in assess(acc=account(factors=(factor(**patch),))).reason_codes
    acc = account(positions=(position(),), factors=(factor(), factor("BTCUSDT")))
    assert "CORRELATION_UNKNOWN" in assess(acc=acc).reason_codes
    for pair in (
        correlation(rho=None),
        correlation(samples=2),
        correlation(as_of=NOW + timedelta(seconds=1)),
    ):
        assert "CORRELATION_UNKNOWN" in assess(acc=change(acc, correlations=(pair,))).reason_codes


def test_correlated_positions_and_opposite_sides_cannot_hide_gross_cluster_risk():
    from tradeforge.risk.limits import RiskLimits

    policy = RiskLimits(max_cluster_notional=D("0.015"))
    for side, rho in ((Side.LONG, D("0.9")), (Side.SHORT, D("-0.9"))):
        acc = account(
            positions=(position(side=side),),
            factors=(factor(), factor("BTCUSDT")),
            correlations=(correlation(rho=rho),),
        )
        assert "CLUSTER_LIMIT" in assess(acc=acc, policy=policy).reason_codes
    acc = change(acc, correlations=(correlation(rho=D("0.1")),))
    assert assess(acc=acc, policy=policy).allowed


def test_beta_limit_and_mark_to_stop_risk_are_conservative():
    from tradeforge.risk.limits import RiskLimits

    assert (
        "BETA_LIMIT"
        in assess(
            acc=account(factors=(factor(beta=D("20")),)), policy=RiskLimits(max_beta_delta=D("0.1"))
        ).reason_codes
    )
    acc = account(positions=(position("ETHUSDT", mark_price=D("200")),))
    assert "HEAT_LIMIT" in assess(acc=acc, policy=RiskLimits(max_heat=D("0.01"))).reason_codes


@pytest.mark.parametrize(
    "pos",
    [
        position(protected=False),
        position(stop_price=None),
        position(as_of=NOW - timedelta(seconds=6)),
    ],
)
def test_unprotected_or_stale_positions_block(pos):
    assert not assess(
        acc=account(
            positions=(pos,), factors=(factor(), factor("BTCUSDT")), correlations=(correlation(),)
        )
    ).allowed


def test_guardian_recomputes_current_account_even_after_quant_approval():
    from tradeforge.risk.governor import RiskGovernor
    from tradeforge.risk.limits import RiskLimits

    quant = assess()
    governor = RiskGovernor()
    fresh = account(realized_pnl_today=D("-200"))
    result = governor.guardian_recheck(quant, fresh, state(), at=NOW, db_writable=True)
    assert not result.allowed and "DAILY_LOSS_LIMIT" in result.reason_codes
    assert governor.guardian_recheck(quant, account(), state(), at=NOW, db_writable=True).allowed
    other = RiskGovernor(RiskLimits(max_positions=1))
    assert (
        "POLICY_MISMATCH"
        in other.guardian_recheck(quant, account(), state(), at=NOW, db_writable=True).reason_codes
    )


def test_expired_requests_loss_streak_cooldown_and_daily_latch_block():
    assert "REQUEST_STALE" in assess(at=NOW + timedelta(seconds=31)).reason_codes
    assert "LOSS_STREAK_LIMIT" in assess(risk_state=state(consecutive_losses=3)).reason_codes
    assert (
        "COOLDOWN"
        in assess(risk_state=state(cooldown_until=NOW + timedelta(seconds=1))).reason_codes
    )
    assert "DAILY_LOSS_LIMIT" in assess(risk_state=state(daily_loss_day=NOW.date())).reason_codes


def test_an_already_overweight_other_symbol_or_side_also_blocks_new_entries():
    from tradeforge.risk.limits import RiskLimits

    acc = account(
        positions=(position(qty=D("20"), side=Side.SHORT, stop_price=D("105")),),
        factors=(factor(), factor("BTCUSDT")),
        correlations=(correlation(rho=D("0.1")),),
    )
    result = assess(
        acc=acc, policy=RiskLimits(max_symbol_notional=D("0.15"), max_side_notional=D("0.15"))
    )
    assert "SYMBOL_LIMIT" in result.reason_codes
    assert "SIDE_LIMIT" in result.reason_codes


def test_a_crossed_stop_does_not_count_as_healthy_protection():
    acc = account(positions=(position("ETHUSDT", mark_price=D("94")),))
    assert "PROTECTION_NOT_READY" in assess(acc=acc).reason_codes
