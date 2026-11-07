"""Immutable source identity and point-in-time availability for analyst evidence."""

from typing import Annotated, Self
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BeforeValidator, Field, model_validator

from tradeforge.alpha.base import AlphaCandidate
from tradeforge.decision.edge import EdgeAnalysis
from tradeforge.decision.trial import TestnetTrialPolicy
from tradeforge.domain.common import DataQuality, Digest, StrictModel, Text, Timestamp, exact_bool
from tradeforge.domain.decision import Decision
from tradeforge.domain.market import StreamState
from tradeforge.intelligence.models import digest
from tradeforge.risk.governor import RiskAssessment


class Discovery(StrictModel):
    candidate: AlphaCandidate
    cluster: Text
    source_data_version: Digest
    as_of: Timestamp
    available_at: Timestamp

    @model_validator(mode="after")
    def linked(self) -> Self:
        if self.source_data_version != self.candidate.input_hash:
            raise ValueError("SOURCE_VERSION_MISMATCH")
        if not self.candidate.observed_at <= self.as_of <= self.available_at:
            raise ValueError("INVALID_DISCOVERY_TIME_ORDER")
        return self

    @property
    def decision_id(self) -> UUID:
        c = self.candidate
        identity = (*c.instrument.key, c.alpha_family.value, c.setup_id)
        return uuid5(NAMESPACE_URL, "tradeforge/decision/" + "/".join(identity))

    @property
    def digest(self) -> str:
        return digest(self.model_dump(mode="json"))


class ValidationEvidence(StrictModel):
    source_data_version: Digest
    as_of: Timestamp
    available_at: Timestamp
    triggered: Annotated[bool, BeforeValidator(exact_bool)] = False
    data_quality: DataQuality = DataQuality.UNKNOWN
    book_state: StreamState = StreamState.UNSYNCED
    cost_version: Digest | None = None
    risk_version: Digest | None = None
    proposal: Decision | None = None
    edge: EdgeAnalysis | None = None
    risk: RiskAssessment | None = None
    trial: TestnetTrialPolicy | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="after")
    def available(self) -> Self:
        if self.as_of > self.available_at:
            raise ValueError("INVALID_EVIDENCE_TIME_ORDER")
        if self.proposal is not None and self.proposal.as_of > self.as_of:
            raise ValueError("FUTURE_PROPOSAL")
        return self
