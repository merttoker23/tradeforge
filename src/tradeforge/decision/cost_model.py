"""Round-trip quote costs in initial-risk R; rebates never subsidize a forecast."""

from datetime import datetime
from decimal import Decimal
from typing import Literal, Self

from pydantic import Field, model_validator

from tradeforge.domain.common import (
    Digest,
    Money,
    NonNegative,
    Positive,
    StrictModel,
    Timestamp,
    utc,
)
from tradeforge.intelligence.models import digest

ZERO = Decimal(0)
BPS = Decimal(10_000)


class RealizedCost(StrictModel):
    # Quote-currency attribution relative to the gross mid-price PnL benchmark.
    # Slippage excludes spread; latency excludes both. Fees/funding are signed debits.
    maker_fees: Money
    taker_fees: Money
    spread: NonNegative
    slippage: NonNegative
    funding: Money
    latency: NonNegative
    pnl_basis: Literal["mid_price", "executed_price"] = Field(
        default="mid_price", exclude_if=lambda value: value == "mid_price"
    )

    @model_validator(mode="after")
    def embedded_execution_costs(self) -> Self:
        # With exchange realized PnL, execution friction is embedded in gross_R.
        # Zero here means no *additional* debit, never a measured zero spread.
        if self.pnl_basis == "executed_price" and any((self.spread, self.slippage, self.latency)):
            raise ValueError("EXECUTION_FRICTION_ALREADY_IN_GROSS")
        return self

    @property
    def total(self) -> Decimal:
        return sum(
            (
                getattr(self, field)
                for field in (
                    "maker_fees",
                    "taker_fees",
                    "spread",
                    "slippage",
                    "funding",
                    "latency",
                )
            ),
            ZERO,
        )


class Friction(StrictModel):
    # Each field is the complete round-trip cost, not a per-leg cost.
    spread_bps: NonNegative
    slippage_bps: NonNegative
    funding_bps: NonNegative
    latency_bps: NonNegative

    @property
    def total_bps(self) -> Decimal:
        return sum(self.model_dump().values(), ZERO)


class CostInput(StrictModel):
    notional: Positive
    initial_risk: Positive
    entry_role: Literal["maker", "taker"]
    exit_role: Literal["maker", "taker"]
    maker_fee_bps: Money | None
    taker_fee_bps: Money | None
    normal: Friction | None
    stress: Friction | None
    observed_at: Timestamp
    expires_at: Timestamp
    source_version: Digest

    @model_validator(mode="after")
    def coherent(self) -> Self:
        if self.expires_at <= self.observed_at:
            raise ValueError("INVALID_COST_EXPIRY")
        if (
            self.normal is not None
            and self.stress is not None
            and any(
                getattr(self.stress, field) < getattr(self.normal, field)
                for field in Friction.model_fields
            )
        ):
            raise ValueError("STRESS_BELOW_NORMAL")
        return self

    @property
    def digest(self) -> str:
        return digest(self.model_dump(mode="json"))


class CostEstimate(StrictModel):
    input_version: Digest
    normal_R: NonNegative | None
    stress_R: NonNegative | None
    fees_R: NonNegative | None
    reason_codes: tuple[str, ...]


def estimate_cost(cost: CostInput, *, at: datetime) -> CostEstimate:
    utc(at)
    reasons = []
    fees = [getattr(cost, role + "_fee_bps") for role in (cost.entry_role, cost.exit_role)]
    if any(fee is None for fee in fees):
        reasons.append("FEE_UNKNOWN")
    if cost.normal is None or cost.stress is None:
        reasons.append("COST_UNCERTAINTY_UNKNOWN")
    if not cost.observed_at <= at < cost.expires_at:
        reasons.append("COST_STALE")
    if reasons:
        return CostEstimate(
            input_version=cost.digest,
            normal_R=None,
            stress_R=None,
            fees_R=None,
            reason_codes=tuple(reasons),
        )
    fee_bps = sum((max(ZERO, f) for f in fees if f is not None), ZERO)
    scale = cost.notional / BPS / cost.initial_risk
    assert cost.normal is not None and cost.stress is not None
    return CostEstimate(
        input_version=cost.digest,
        normal_R=(cost.normal.total_bps + fee_bps) * scale,
        stress_R=(cost.stress.total_bps + fee_bps) * scale,
        fees_R=fee_bps * scale,
        reason_codes=(),
    )
