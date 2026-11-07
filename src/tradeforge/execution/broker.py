"""Guardian boundary contracts. No credentials or implicit execution authority."""

from decimal import Decimal
from typing import Literal, Protocol, Self

from pydantic import Field, model_validator

from tradeforge.domain.common import NonNegative, Positive, StrictModel, Text, Timestamp
from tradeforge.domain.execution import ClientOrderId, ExecutionIntent
from tradeforge.market.exchange_info import ContractMetadata
from tradeforge.risk.models import AccountSnapshot


class AckUnknown(RuntimeError):
    """The request may have been accepted. Only a lookup is safe."""


class ExchangeRejected(RuntimeError):
    """A definitive rejection, expressed only as a credential-free reason code."""


class ExecutionContext(StrictModel):
    environment: Literal["unspecified", "mainnet", "testnet"] = Field(
        default="unspecified", exclude_if=lambda v: v == "unspecified"
    )
    as_of: Timestamp
    metadata: ContractMetadata
    account: AccountSnapshot
    mark_price: Positive
    bid: Positive
    ask: Positive
    leverage: Positive
    max_notional: Positive
    one_way: bool = False
    isolated: bool = False
    clock_ready: bool = False
    stream_ready: bool = False

    @model_validator(mode="after")
    def uncrossed(self) -> Self:
        if self.bid > self.ask:
            raise ValueError("CROSSED_EXECUTION_QUOTE")
        return self


class ExchangeFill(StrictModel):
    trade_id: Text
    qty: Positive
    price: Positive
    fee: NonNegative
    exchange_at: Timestamp


class ExchangeReport(StrictModel):
    client_order_id: ClientOrderId
    symbol: str
    order_id: Text
    side: Literal["BUY", "SELL"]
    qty: Positive
    filled_qty: NonNegative
    status: Literal["NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "EXPIRED", "REJECTED"]
    avg_price: Positive | None
    exchange_at: Timestamp
    fills: tuple[ExchangeFill, ...] = ()

    @model_validator(mode="after")
    def complete_trade_evidence(self) -> Self:
        if (
            self.filled_qty > self.qty
            or sum((x.qty for x in self.fills), Decimal(0)) != self.filled_qty
        ):
            raise ValueError("INCOMPLETE_EXCHANGE_FILLS")
        if len({x.trade_id for x in self.fills}) != len(self.fills):
            raise ValueError("DUPLICATE_EXCHANGE_FILL")
        if self.status == "FILLED" and self.filled_qty != self.qty:
            raise ValueError("INCOMPLETE_FILLED_ORDER")
        if self.status == "PARTIALLY_FILLED" and not 0 < self.filled_qty < self.qty:
            raise ValueError("INVALID_PARTIAL_FILL")
        if self.status in {"NEW", "REJECTED"} and self.filled_qty:
            raise ValueError("UNEXPECTED_FILL")
        if bool(self.filled_qty) != (self.avg_price is not None):
            raise ValueError("MISSING_AVERAGE_PRICE")
        if any(x.exchange_at > self.exchange_at for x in self.fills):
            raise ValueError("FUTURE_FILL")
        return self


class Broker(Protocol):
    def place(self, intent: ExecutionIntent) -> ExchangeReport: ...
    def query(self, intent: ExecutionIntent) -> ExchangeReport | None: ...
    def protect(
        self, intent: ExecutionIntent, client_id: str, *, query_only: bool = False
    ) -> bool: ...
    def cancel(self, intent: ExecutionIntent) -> ExchangeReport: ...
