"""Supported contract identity and exchange metadata; no symbol allowlist."""

from enum import StrEnum
from typing import Literal, Self

from pydantic import model_validator

from tradeforge.domain.common import Positive, StrictModel, Symbol, Timestamp
from tradeforge.domain.events import Event


class InstrumentKey(StrictModel):
    symbol: Symbol
    exchange: Literal["BINANCE"]
    market_type: Literal["USD_M"]
    quote: Literal["USDT"]
    margin_asset: Literal["USDT"]
    contract_type: Literal["PERPETUAL"]

    @property
    def key(self) -> tuple[str, ...]:
        return (self.exchange, self.market_type, self.symbol, self.contract_type)


class InstrumentEvent(Event, InstrumentKey):
    """An observation bound to one complete supported contract identity."""


class ContractStatus(StrEnum):
    PENDING_TRADING = "PENDING_TRADING"
    TRADING = "TRADING"
    PRE_DELIVERING = "PRE_DELIVERING"
    DELIVERING = "DELIVERING"
    DELIVERED = "DELIVERED"
    PRE_SETTLE = "PRE_SETTLE"
    SETTLING = "SETTLING"
    CLOSE = "CLOSE"


class InstrumentFilters(StrictModel):
    tick_size: Positive
    step_size: Positive
    min_price: Positive
    max_price: Positive
    min_qty: Positive
    max_qty: Positive
    min_notional: Positive

    @model_validator(mode="after")
    def coherent_limits(self) -> Self:
        if self.min_price > self.max_price or self.min_qty > self.max_qty:
            raise ValueError("INVERTED_INSTRUMENT_LIMITS")
        if self.tick_size > self.max_price or self.step_size > self.max_qty:
            raise ValueError("UNUSABLE_INSTRUMENT_FILTERS")
        return self


class Instrument(InstrumentEvent):
    contract_status: ContractStatus
    filters: InstrumentFilters
    onboard_at: Timestamp
    delist_at: Timestamp | None

    @model_validator(mode="after")
    def lifecycle(self) -> Self:
        if self.delist_at is not None and self.delist_at <= self.onboard_at:
            raise ValueError("INVALID_INSTRUMENT_LIFECYCLE")
        return self
