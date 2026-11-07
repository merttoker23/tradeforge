"""Strict, point-in-time portfolio facts, including all pending entry exposure."""

from datetime import date
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, model_validator

from tradeforge.domain.common import (
    Count,
    Money,
    NonNegative,
    Positive,
    StrictModel,
    Symbol,
    Timestamp,
)
from tradeforge.domain.decision import Side
from tradeforge.domain.instrument import InstrumentFilters, InstrumentKey
from tradeforge.intelligence.models import digest


class EntryRequest(StrictModel):
    environment: Literal["unspecified", "mainnet", "testnet"] = Field(
        default="unspecified", exclude_if=lambda v: v == "unspecified"
    )
    instrument: InstrumentKey
    filters: InstrumentFilters
    side: Side
    requested_qty: Positive
    entry_price: Positive
    stop_price: Positive
    cost_per_unit: NonNegative
    leverage: Positive
    observed_at: Timestamp
    expires_at: Timestamp
    liquidity_scale: Annotated[NonNegative, Field(le=1)]
    spread_bps: NonNegative

    @model_validator(mode="after")
    def coherent(self) -> Self:
        if self.side is Side.NO_TRADE or self.expires_at <= self.observed_at:
            raise ValueError("INVALID_ENTRY_REQUEST")
        if not (
            self.stop_price < self.entry_price
            if self.side is Side.LONG
            else self.stop_price > self.entry_price
        ):
            raise ValueError("INVALID_STOP_DIRECTION")
        return self

    @property
    def digest(self) -> str:
        return digest(self.model_dump(mode="json"))


class ExposurePosition(StrictModel):
    symbol: Symbol
    side: Side
    qty: Positive
    entry_price: Positive
    mark_price: Positive
    stop_price: Positive | None
    cost_per_unit: NonNegative
    protected: bool
    as_of: Timestamp

    @model_validator(mode="after")
    def directional(self) -> Self:
        if self.side is Side.NO_TRADE:
            raise ValueError("DIRECTION_REQUIRED")
        return self


class RiskFactor(StrictModel):
    symbol: Symbol
    beta: Money | None
    realized_vol: Positive | None
    as_of: Timestamp


class Correlation(StrictModel):
    left: Symbol
    right: Symbol
    rho: Annotated[Money, Field(ge=-1, le=1)] | None
    samples: Count
    as_of: Timestamp

    @model_validator(mode="after")
    def distinct(self) -> Self:
        if self.left == self.right:
            raise ValueError("DISTINCT_SYMBOLS_REQUIRED")
        return self


class AccountSnapshot(StrictModel):
    environment: Literal["unspecified", "mainnet", "testnet"] = Field(
        default="unspecified", exclude_if=lambda v: v == "unspecified"
    )
    snapshot_id: UUID
    as_of: Timestamp
    trading_day: date
    equity: Positive | None
    available_balance: NonNegative | None
    day_start_equity: Positive | None
    realized_pnl_today: Money | None
    unrealized_pnl: Money | None
    positions_complete: bool = False
    protection_ready: bool = False
    positions: Annotated[tuple[ExposurePosition, ...], Field(max_length=10000)] = ()
    pending: Annotated[tuple[ExposurePosition, ...], Field(max_length=10000)] = ()
    factors: Annotated[tuple[RiskFactor, ...], Field(max_length=10000)] = ()
    correlations: Annotated[tuple[Correlation, ...], Field(max_length=100000)] = ()

    @model_validator(mode="after")
    def unique_evidence(self) -> Self:
        if len({x.symbol for x in self.factors}) != len(self.factors):
            raise ValueError("DUPLICATE_RISK_FACTOR")
        if len({tuple(sorted((x.left, x.right))) for x in self.correlations}) != len(
            self.correlations
        ):
            raise ValueError("DUPLICATE_CORRELATION")
        return self

    @property
    def digest(self) -> str:
        return digest(self.model_dump(mode="json"))
