"""Decision proposals remain distinct from execution authority."""

from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from pydantic import ValidationError

from tradeforge.domain.common import DataQuality, ReasonCode
from tradeforge.domain.decision import AlphaFamily, Decision, DecisionState, ProofState, Side
from tradeforge.domain.execution import (
    ExecutionIntent,
    OrderState,
    OrderStatus,
    Position,
    PositionMode,
    ProtectionStatus,
    StopPlan,
)
from tradeforge.domain.risk import TradingReadiness

from .conftest import NOW


@pytest.fixture
def decision(event: dict[str, object], identity: dict[str, object]) -> dict[str, object]:
    return {
        **event,
        **identity,
        "decision_id": UUID("00000000-0000-4000-8000-000000000002"),
        "regime_version": "r1",
        "universe_version": "u1",
        "feature_hash": "a" * 64,
        "alpha_family": AlphaFamily.MOMENTUM,
        "side": Side.LONG,
        "state": DecisionState.DISCOVERED,
        "expires_at": NOW + timedelta(minutes=1),
        "trigger": Decimal("100"),
        "stop_proposal": Decimal("90"),
        "take_profit_proposal": Decimal("120"),
        "invalidations": ("below_support",),
        "spread_bps": Decimal("1"),
        "slippage_bps": Decimal("2"),
        "estimated_fees": Decimal("0.1"),
        "funding_cost": Decimal("0"),
        "net_ev_low_R": Decimal("-0.2"),
        "proof_state": ProofState.UNPROVEN,
        "calibrated": False,
        "confidence": None,
        "quality": None,
        "reason_codes": (ReasonCode.OOS_UNPROVEN,),
        "risk_policy_version": "risk1",
        "decision_policy_version": "decision1",
    }


def test_unproven_candidate_has_no_invented_confidence(decision: dict[str, object]) -> None:
    assert Decision.model_validate(decision).confidence is None
    for patch in [
        {"confidence": 0.9},
        {"quality": 0.9},
        {"state": DecisionState.APPROVED},
        {"reason_codes": ()},
        {"reason_codes": (ReasonCode.NO_TRADE, ReasonCode.NO_TRADE)},
        {"reason_codes": ("UNKNOWN_REASON",)},
        {"expires_at": NOW},
        {"stop_proposal": Decimal("110")},
        {"take_profit_proposal": Decimal("95")},
        {"side": Side.SHORT},
        {"confidence": float("nan")},
        {"spread_bps": Decimal("-0.1")},
    ]:
        with pytest.raises(ValidationError):
            Decision.model_validate({**decision, **patch})


def test_no_trade_is_explicit_non_executable_result(decision: dict[str, object]) -> None:
    data = {
        **decision,
        "side": Side.NO_TRADE,
        "state": DecisionState.NO_TRADE,
        "trigger": None,
        "stop_proposal": None,
        "take_profit_proposal": None,
        "reason_codes": (ReasonCode.INSUFFICIENT_EVIDENCE,),
    }
    assert Decision.model_validate(data).side is Side.NO_TRADE
    for patch in [{"trigger": Decimal("100")}, {"state": DecisionState.APPROVED}]:
        with pytest.raises(ValidationError):
            Decision.model_validate({**data, **patch})


def test_approved_is_calibrated_candidate_only(decision: dict[str, object]) -> None:
    data = {
        **decision,
        "state": DecisionState.APPROVED,
        "proof_state": ProofState.VALIDATED,
        "calibrated": True,
        "confidence": 0.65,
        "quality": 0.8,
        "net_ev_low_R": Decimal("0.2"),
        "reason_codes": (ReasonCode.CANDIDATE_VALIDATED,),
    }
    record = Decision.model_validate(data)
    assert record.state is DecisionState.APPROVED
    assert not hasattr(record, "orders_enabled")
    with pytest.raises(ValidationError):
        Decision.model_validate({**data, "net_ev_low_R": Decimal("0")})


@pytest.fixture
def intent(event: dict[str, object], identity: dict[str, object]) -> dict[str, object]:
    return {
        **event,
        **identity,
        "intent_id": UUID("00000000-0000-4000-8000-000000000003"),
        "decision_id": UUID("00000000-0000-4000-8000-000000000002"),
        "client_order_id": "tf-unique-1",
        "side": Side.LONG,
        "qty": Decimal("2"),
        "price": Decimal("100"),
        "position_mode": PositionMode.ONE_WAY,
        "reduce_only": False,
        "stop_plan": StopPlan(stop_price=Decimal("90"), qty=Decimal("2")),
        "max_slippage_bps": Decimal("5"),
        "expires_at": NOW + timedelta(seconds=10),
        "created_at": NOW,
        "policy_version": "risk1",
        "risk_snapshot_id": UUID("00000000-0000-4000-8000-000000000004"),
    }


def test_intent_is_immutable_and_rejects_unsafe_shape(intent: dict[str, object]) -> None:
    record = ExecutionIntent.model_validate(intent)
    assert ExecutionIntent.model_validate_json(record.model_dump_json()) == record
    with pytest.raises(ValidationError):
        record.qty = Decimal("3")
    for patch in [
        {"side": Side.NO_TRADE},
        {"qty": Decimal("-1")},
        {"qty": "2"},
        {"stop_plan": None},
        {"stop_plan": StopPlan(stop_price=Decimal("90"), qty=Decimal("1"))},
        {"expires_at": NOW},
        {"created_at": NOW + timedelta(seconds=1)},
        {"client_order_id": "a" * 37},
        {"reduce_only": "false"},
        {"price": Decimal("80")},
    ]:
        with pytest.raises(ValidationError):
            ExecutionIntent.model_validate({**intent, **patch})


def test_order_fill_and_position_contracts(
    event: dict[str, object], identity: dict[str, object]
) -> None:
    order = {
        **event,
        **identity,
        "client_order_id": "tf-1",
        "exchange_order_id": "123",
        "intent_id": UUID("00000000-0000-4000-8000-000000000003"),
        "status": OrderStatus.PARTIALLY_FILLED,
        "qty": Decimal("2"),
        "filled_qty": Decimal("1"),
        "fill_ids": ("f1",),
        "avg_executed_price": Decimal("100"),
        "fee": Decimal("0.1"),
        "exchange_at": NOW - timedelta(seconds=1),
        "last_reconciliation": NOW,
    }
    assert OrderState.model_validate(order).filled_qty == Decimal("1")
    for patch in [
        {"filled_qty": Decimal("3")},
        {"fill_ids": ("f1", "f1")},
        {"status": OrderStatus.FILLED},
        {"avg_executed_price": None},
        {"last_reconciliation": NOW + timedelta(seconds=1)},
    ]:
        with pytest.raises(ValidationError):
            OrderState.model_validate({**order, **patch})
    position = {
        **event,
        **identity,
        "side": Side.LONG,
        "qty": Decimal("1"),
        "entry_price": Decimal("100"),
        "mark_price": Decimal("101"),
        "margin_usage": Decimal("10"),
        "accrued_funding": Decimal("-0.01"),
        "protected_qty": Decimal("1"),
        "stop_status": ProtectionStatus.READY,
    }
    assert Position.model_validate(position).stop_status is ProtectionStatus.READY
    for patch in [
        {"protected_qty": Decimal("2")},
        {"protected_qty": Decimal("0.5")},
        {"side": Side.NO_TRADE},
    ]:
        with pytest.raises(ValidationError):
            Position.model_validate({**position, **patch})
    assert Position.model_validate(
        {**position, "protected_qty": Decimal("0"), "stop_status": ProtectionStatus.MISSING}
    )


def test_trading_readiness_fails_closed() -> None:
    blocked = TradingReadiness()
    assert blocked.can_open_entries is False
    assert blocked.can_reduce_risk is True
    ready = {
        "data_quality": DataQuality.OK,
        "db_writable": True,
        "outbox_ready": True,
        "account_ready": True,
        "risk_ready": True,
        "protection_ready": True,
        "edge_validated": True,
        "mode_authorized": True,
        "kill_switch": False,
        "reason_codes": (ReasonCode.CANDIDATE_VALIDATED,),
    }
    assert TradingReadiness.model_validate(ready).can_open_entries
    for field in [
        "db_writable",
        "outbox_ready",
        "account_ready",
        "risk_ready",
        "protection_ready",
        "edge_validated",
        "mode_authorized",
    ]:
        assert not TradingReadiness.model_validate({**ready, field: False}).can_open_entries
    assert not TradingReadiness.model_validate({**ready, "kill_switch": True}).can_open_entries
    assert not TradingReadiness.model_validate(
        {**ready, "data_quality": DataQuality.STALE}
    ).can_open_entries
    assert TradingReadiness.model_validate({**ready, "kill_switch": True}).can_reduce_risk


def test_transition_log_has_optimistic_version_evidence(event: dict[str, object]) -> None:
    from tradeforge.domain.decision import DecisionTransition

    data = {
        **event,
        "decision_id": UUID("00000000-0000-4000-8000-000000000002"),
        "expected_previous_state": DecisionState.DISCOVERED,
        "expected_previous_version": 0,
        "state": DecisionState.WATCHING,
        "version": 1,
        "reason_codes": (ReasonCode.INSUFFICIENT_EVIDENCE,),
    }
    assert DecisionTransition.model_validate(data).version == 1
    for patch in [
        {"version": 0},
        {"version": 2},
        {"expected_previous_version": -1},
        {"reason_codes": ()},
    ]:
        with pytest.raises(ValidationError):
            DecisionTransition.model_validate({**data, **patch})
