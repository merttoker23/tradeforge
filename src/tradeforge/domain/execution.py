"""Immutable execution shapes; no broker, persistence or authority in this phase."""

from enum import StrEnum
from typing import Annotated, Self
from uuid import UUID

from pydantic import Field, model_validator

from tradeforge.domain.common import (
    Money,
    NonNegative,
    Positive,
    StrictModel,
    Text,
    Timestamp,
)
from tradeforge.domain.decision import Side
from tradeforge.domain.instrument import InstrumentEvent

ClientOrderId = Annotated[str, Field(min_length=1, max_length=36, pattern=r"^[A-Za-z0-9._:/-]+$")]


class PositionMode(StrEnum):
    ONE_WAY = "ONE_WAY"
    HEDGE = "HEDGE"


class StopPlan(StrictModel):
    stop_price: Positive
    qty: Positive


class ExecutionIntent(InstrumentEvent):
    intent_id: UUID
    client_order_id: ClientOrderId
    decision_id: UUID
    side: Side
    qty: Positive
    price: Positive | None
    position_mode: PositionMode
    reduce_only: bool
    stop_plan: StopPlan | None
    max_slippage_bps: NonNegative
    expires_at: Timestamp
    created_at: Timestamp
    policy_version: Text
    risk_snapshot_id: UUID

    @model_validator(mode="after")
    def safe_shape(self) -> Self:
        if self.side is Side.NO_TRADE:
            raise ValueError("EXECUTABLE_SIDE_REQUIRED")
        if not self.observed_at <= self.created_at <= self.as_of < self.expires_at:
            raise ValueError("INVALID_INTENT_TIMES")
        if not self.reduce_only:
            if self.stop_plan is None or self.stop_plan.qty != self.qty:
                raise ValueError("FULL_STOP_PLAN_REQUIRED")
            if self.price is not None:
                valid = (
                    self.stop_plan.stop_price < self.price
                    if self.side is Side.LONG
                    else self.stop_plan.stop_price > self.price
                )
                if not valid:
                    raise ValueError("INVALID_STOP_DIRECTION")
        return self


class OrderStatus(StrEnum):
    NEW = "NEW"
    ACK_UNKNOWN = "ACK_UNKNOWN"
    ACCEPTED = "ACCEPTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"


class OrderState(InstrumentEvent):
    intent_id: UUID
    client_order_id: ClientOrderId
    exchange_order_id: Text | None
    status: OrderStatus
    qty: Positive
    filled_qty: NonNegative
    fill_ids: tuple[Text, ...]
    avg_executed_price: Positive | None
    fee: Money
    exchange_at: Timestamp | None
    last_reconciliation: Timestamp | None

    @model_validator(mode="after")
    def coherent_fill(self) -> Self:
        if self.filled_qty > self.qty or len(set(self.fill_ids)) != len(self.fill_ids):
            raise ValueError("INVALID_FILL_QUANTITY_OR_IDS")
        if self.status is OrderStatus.FILLED and self.filled_qty != self.qty:
            raise ValueError("INCOMPLETE_FILLED_ORDER")
        if self.status is OrderStatus.PARTIALLY_FILLED and not 0 < self.filled_qty < self.qty:
            raise ValueError("INVALID_PARTIAL_FILL")
        if self.status in {OrderStatus.NEW, OrderStatus.REJECTED} and self.filled_qty != 0:
            raise ValueError("UNEXPECTED_FILL")
        if self.filled_qty > 0:
            if self.avg_executed_price is None or not self.fill_ids:
                raise ValueError("FILL_EVIDENCE_REQUIRED")
        elif self.avg_executed_price is not None or self.fill_ids:
            raise ValueError("UNEXPECTED_FILL_EVIDENCE")
        if self.exchange_at is not None and self.exchange_at > self.observed_at:
            raise ValueError("FUTURE_EXCHANGE_TIME")
        if self.last_reconciliation is not None and self.last_reconciliation > self.as_of:
            raise ValueError("FUTURE_RECONCILIATION")
        return self


class ProtectionStatus(StrEnum):
    UNKNOWN = "UNKNOWN"
    MISSING = "MISSING"
    PARTIAL = "PARTIAL"
    READY = "READY"
    FAILED = "FAILED"


class Position(InstrumentEvent):
    side: Side
    qty: NonNegative
    entry_price: Positive
    mark_price: Positive
    margin_usage: NonNegative
    accrued_funding: Money
    protected_qty: NonNegative
    stop_status: ProtectionStatus

    @model_validator(mode="after")
    def coherent_protection(self) -> Self:
        if self.side is Side.NO_TRADE or self.protected_qty > self.qty:
            raise ValueError("INVALID_POSITION")
        if self.stop_status is ProtectionStatus.READY and self.protected_qty != self.qty:
            raise ValueError("INCOMPLETE_POSITION_PROTECTION")
        return self
