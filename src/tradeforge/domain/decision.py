"""Quant proposals are evidence records, never exchange authorization."""

from enum import StrEnum
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, model_validator

from tradeforge.domain.common import (
    Count,
    Digest,
    Money,
    NonNegative,
    Positive,
    ReasonCode,
    Reasons,
    Text,
    Timestamp,
    UnitFloat,
)
from tradeforge.domain.events import Event
from tradeforge.domain.instrument import InstrumentEvent


class Side(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"
    NO_TRADE = "NO_TRADE"


class AlphaFamily(StrEnum):
    MOMENTUM = "MOMENTUM"
    REVERSAL = "REVERSAL"


class ProofState(StrEnum):
    UNPROVEN = "UNPROVEN"
    VALIDATED = "VALIDATED"
    STALE = "STALE"


class DecisionState(StrEnum):
    DISCOVERED = "DISCOVERED"
    WATCHING = "WATCHING"
    ARMED = "ARMED"
    VALIDATING = "VALIDATING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    NO_TRADE = "NO_TRADE"
    CANCELLED = "CANCELLED"
    QUEUED = "QUEUED"
    EXECUTING = "EXECUTING"
    MANAGING = "MANAGING"
    CLOSED = "CLOSED"


class Decision(InstrumentEvent):
    decision_id: UUID
    regime_version: Text
    universe_version: Text
    feature_hash: Digest
    alpha_family: AlphaFamily
    side: Side
    state: DecisionState
    expires_at: Timestamp
    trigger: Positive | None
    stop_proposal: Positive | None
    take_profit_proposal: Positive | None
    invalidations: Annotated[tuple[Text, ...], Field(min_length=1)]
    spread_bps: NonNegative
    slippage_bps: NonNegative
    estimated_fees: NonNegative
    funding_cost: Money
    net_ev_low_R: Money | None
    execution_authority: Literal["STATISTICAL_EDGE", "TESTNET_TRIAL"] = Field(
        default="STATISTICAL_EDGE", exclude_if=lambda value: value == "STATISTICAL_EDGE"
    )
    proof_state: ProofState
    calibrated: bool
    confidence: UnitFloat | None
    quality: UnitFloat | None
    reason_codes: Reasons
    risk_policy_version: Text
    decision_policy_version: Text

    @model_validator(mode="after")
    def proposal_semantics(self) -> Self:
        if self.expires_at <= self.observed_at:
            raise ValueError("INVALID_DECISION_EXPIRY")
        active = {
            DecisionState.DISCOVERED,
            DecisionState.WATCHING,
            DecisionState.ARMED,
            DecisionState.VALIDATING,
            DecisionState.APPROVED,
            DecisionState.QUEUED,
        }
        if self.state in active and self.expires_at <= self.as_of:
            raise ValueError("EXPIRED_CANDIDATE")
        calibrated = self.calibrated and self.proof_state is ProofState.VALIDATED
        if not calibrated and (self.confidence is not None or self.quality is not None):
            raise ValueError("UNCALIBRATED_CONFIDENCE")
        if self.state in {DecisionState.APPROVED, DecisionState.QUEUED}:
            trial = self.execution_authority == "TESTNET_TRIAL"
            if trial and (
                self.proof_state is not ProofState.UNPROVEN
                or self.calibrated
                or self.confidence is not None
                or self.quality is not None
                or ReasonCode.TESTNET_TRIAL_AUTHORIZED not in self.reason_codes
                or ReasonCode.CANDIDATE_VALIDATED in self.reason_codes
            ):
                raise ValueError("TRIAL_CANNOT_CLAIM_VALIDATION")
            if not trial and (
                not calibrated or self.net_ev_low_R is None or self.net_ev_low_R <= 0
            ):
                raise ValueError("UNPROVEN_OR_NONPOSITIVE_EDGE")
        if self.side is Side.NO_TRADE:
            if self.state is not DecisionState.NO_TRADE or any(
                p is not None for p in (self.trigger, self.stop_proposal, self.take_profit_proposal)
            ):
                raise ValueError("NO_TRADE_HAS_NO_EXECUTABLE_PROPOSAL")
        else:
            if self.state is DecisionState.NO_TRADE:
                raise ValueError("NO_TRADE_SIDE_REQUIRED")
            if (
                self.trigger is None
                or self.stop_proposal is None
                or self.take_profit_proposal is None
            ):
                raise ValueError("DIRECTIONAL_PROPOSAL_REQUIRED")
            long = self.stop_proposal < self.trigger < self.take_profit_proposal
            short = self.take_profit_proposal < self.trigger < self.stop_proposal
            if not (long if self.side is Side.LONG else short):
                raise ValueError("INVALID_PROPOSAL_DIRECTION")
        return self


class DecisionTransition(Event):
    """Append-only CAS evidence shape; transition enforcement/persistence is PHASE_10."""

    decision_id: UUID
    expected_previous_state: DecisionState
    expected_previous_version: Count
    state: DecisionState
    version: Count
    reason_codes: Reasons

    @model_validator(mode="after")
    def successor_version(self) -> Self:
        if self.version != self.expected_previous_version + 1:
            raise ValueError("INVALID_SUCCESSOR_VERSION")
        return self
