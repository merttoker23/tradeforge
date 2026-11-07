"""Versioned fail-closed analyst gates; cost and risk engines remain separate."""

from datetime import datetime
from typing import Annotated, Literal

from pydantic import Field

from tradeforge.decision.provenance import Discovery, ValidationEvidence
from tradeforge.decision.reasons import DecisionReason as R
from tradeforge.domain.common import DataQuality, StrictModel
from tradeforge.domain.decision import DecisionState, ProofState
from tradeforge.domain.market import StreamState
from tradeforge.intelligence.models import digest
from tradeforge.risk.governor import RiskGovernor


class DecisionPolicy(StrictModel):
    version: Literal[1] = 1
    max_ttl_seconds: Annotated[int, Field(ge=1, le=3600)] = 900
    evidence_age_seconds: Annotated[int, Field(ge=1, le=60)] = 10
    cooldown_seconds: Annotated[int, Field(ge=0, le=3600)] = 30
    max_records: Annotated[int, Field(ge=1, le=1_000_000)] = 100_000

    @property
    def digest(self) -> str:
        return digest(self.model_dump(mode="json"))


def approval_reasons(
    discovery: Discovery, evidence: ValidationEvidence | None, at: datetime, policy: DecisionPolicy
) -> tuple[R, ...]:
    if evidence is None:
        return (
            R.STALE,
            R.NO_TRIGGER,
            R.COST_UNKNOWN,
            R.ORDER_BOOK_GAP,
            R.RISK_EVIDENCE_MISSING,
            R.INSUFFICIENT_EVIDENCE,
        )
    reasons: list[R] = []
    if evidence.source_data_version != discovery.source_data_version:
        reasons.append(R.PROVENANCE_MISMATCH)
    if (
        evidence.available_at > at
        or evidence.as_of < discovery.as_of
        or (at - evidence.as_of).total_seconds() > policy.evidence_age_seconds
        or evidence.data_quality is not DataQuality.OK
    ):
        reasons.append(R.STALE)
    if not evidence.triggered:
        reasons.append(R.NO_TRIGGER)
    if evidence.cost_version is None:
        reasons.append(R.COST_UNKNOWN)
    if evidence.book_state is not StreamState.READY:
        reasons.append(R.ORDER_BOOK_GAP)
    risk = evidence.risk
    if evidence.risk_version is None or risk is None:
        reasons.append(R.RISK_EVIDENCE_MISSING)
    if risk is not None:
        rebuilt = RiskGovernor(risk.policy).assess(
            risk.request, risk.account, risk.state, at=risk.at, db_writable=True
        )
        if not risk.allowed or risk != rebuilt or risk.stage != "quant":
            reasons.append(R.RISK_REJECTED)
        c = discovery.candidate
        if (
            risk.policy.digest != evidence.risk_version
            or risk.at > at
            or (at - risk.at).total_seconds() > risk.policy.account_age_seconds
            or risk.request.instrument.key != c.instrument.key
            or risk.request.side is not c.side
            or risk.request.entry_price != c.trigger
            or risk.request.stop_price != c.stop_proposal
            or risk.request.spread_bps < c.spread_bps
        ):
            reasons.append(R.PROVENANCE_MISMATCH)
    edge = evidence.edge
    trial = evidence.trial
    trial_allowed = (
        trial is not None
        and trial.enabled
        and edge is not None
        and edge.mode.value == "testnet"
        and edge.bucket.environment == "testnet"
        and edge.proof_state is ProofState.UNPROVEN
        and not edge.costs.reason_codes
        and risk is not None
        and risk.request.environment == "testnet"
        and risk.account.environment == "testnet"
        and risk.sized is not None
        and risk.sized.notional <= trial.max_notional
        and risk.sized.qty * abs(risk.sized.entry_price - risk.sized.stop_price)
        <= trial.max_initial_risk
    )
    if edge is None or (edge.proof_state is not ProofState.VALIDATED and not trial_allowed):
        reasons.append(R.INSUFFICIENT_EVIDENCE)
    if trial is not None and not trial_allowed:
        reasons.append(R.INSUFFICIENT_EVIDENCE)
    if edge is not None:
        candidate = discovery.candidate
        if (
            edge.as_of > at
            or (at - edge.as_of).total_seconds() > policy.evidence_age_seconds
            or not edge.cost_input.observed_at <= at < edge.cost_input.expires_at
        ):
            reasons.append(R.STALE)
        if (
            edge.cost_input.digest != evidence.cost_version
            or edge.bucket.symbol != candidate.instrument.symbol
            or edge.bucket.side is not candidate.side
            or edge.bucket.alpha_family is not candidate.alpha_family
            or edge.feature_hash != candidate.source_feature_hash
            or edge.regime_version != candidate.regime_version
        ):
            reasons.append(R.PROVENANCE_MISMATCH)
        if risk is not None and risk.sized is not None:
            if (
                risk.sized.notional != edge.cost_input.notional
                or edge.costs.stress_R is None
                or risk.request.cost_per_unit
                < edge.costs.stress_R * abs(risk.sized.entry_price - risk.sized.stop_price)
            ):
                reasons.append(R.PROVENANCE_MISMATCH)
    p, c = evidence.proposal, discovery.candidate
    if p is None:
        reasons.append(R.INSUFFICIENT_EVIDENCE)
    else:
        if edge is not None:
            cost = edge.cost_input
            if (
                p.net_ev_low_R != edge.net_ev_low_R
                or cost.stress is None
                or edge.costs.fees_R is None
                or p.estimated_fees != edge.costs.fees_R * cost.initial_risk
                or p.slippage_bps != cost.stress.slippage_bps
                or p.funding_cost != cost.stress.funding_bps * cost.notional / 10_000
                or cost.normal is None
                or cost.normal.spread_bps < c.spread_bps
                or cost.initial_risk * c.trigger != cost.notional * abs(c.trigger - c.stop_proposal)
            ):
                reasons.append(R.PROVENANCE_MISMATCH)
        statistical = (
            p.execution_authority == "STATISTICAL_EDGE"
            and p.proof_state is ProofState.VALIDATED
            and p.calibrated
            and p.net_ev_low_R is not None
            and p.net_ev_low_R > 0
        )
        trial_proposal = (
            trial_allowed
            and p.execution_authority == "TESTNET_TRIAL"
            and p.proof_state is ProofState.UNPROVEN
            and not p.calibrated
            and p.confidence is None
            and p.quality is None
        )
        if p.state is not DecisionState.APPROVED or not (statistical or trial_proposal):
            reasons.append(R.INSUFFICIENT_EVIDENCE)
        if (
            p.decision_id != discovery.decision_id
            or p.key != c.instrument.key
            or p.side is not c.side
            or p.alpha_family is not c.alpha_family
            or p.feature_hash != c.source_feature_hash
            or p.regime_version != c.regime_version
            or p.universe_version != c.universe_version
            or p.expires_at != c.expires_at
            or p.trigger != c.trigger
            or p.stop_proposal != c.stop_proposal
            or p.take_profit_proposal != c.take_profit_proposal
            or p.invalidations != c.invalidations
            or p.spread_bps != c.spread_bps
            or p.decision_policy_version != policy.digest
            or p.risk_policy_version != evidence.risk_version
            or p.as_of < discovery.as_of
            or p.expires_at <= at
            or (at - p.as_of).total_seconds() > policy.evidence_age_seconds
        ):
            reasons.append(R.PROVENANCE_MISMATCH)
    return tuple(dict.fromkeys(reasons))
