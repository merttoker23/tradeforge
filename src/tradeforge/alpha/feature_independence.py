"""Pre-decision evidence audit: correlated price features share a capped group."""

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BeforeValidator

from tradeforge.alpha.base import AlphaResult
from tradeforge.domain.common import StrictModel, Text, UnitFloat, exact_bool, utc
from tradeforge.domain.decision import AlphaFamily, Side

WEIGHTS = {"price_structure": 0.6, "participation": 0.2, "aggressive_flow": 0.2}


class EvidenceAudit(StrictModel):
    version: Literal[1] = 1
    side: Side
    evidence_groups: tuple[tuple[Text, UnitFloat], ...]
    ordinal_quality: UnitFloat
    reason_codes: tuple[Text, ...]
    confidence: None = None
    trading_allowed: Annotated[Literal[False], BeforeValidator(exact_bool)] = False


def combine_evidence(
    momentum: AlphaResult, reversal: AlphaResult, *, now: datetime
) -> EvidenceAudit:
    """Never arbitrates a decision lifecycle or confers independent order authority."""
    utc(now)

    def reject(reason: str) -> EvidenceAudit:
        return EvidenceAudit(
            side=Side.NO_TRADE, evidence_groups=(), ordinal_quality=0.0, reason_codes=(reason,)
        )

    candidates = []
    for result, family in ((momentum, AlphaFamily.MOMENTUM), (reversal, AlphaFamily.REVERSAL)):
        c = result.candidate
        if c is None:
            continue
        if c.alpha_family is not family:
            raise ValueError("ALPHA_FAMILY_MISMATCH")
        if not c.observed_at <= now < c.expires_at:
            return reject("STALE_OR_FUTURE_ALPHA")
        candidates.append(c)
    if not candidates:
        return reject("NO_ALPHA_EVIDENCE")
    first = candidates[0]
    if any(
        c.instrument != first.instrument
        or c.observed_at != first.observed_at
        or c.source_feature_hash != first.source_feature_hash
        or c.regime_version != first.regime_version
        or c.universe_version != first.universe_version
        for c in candidates
    ):
        return reject("INCOMPARABLE_ALPHA_EVIDENCE")
    if len({c.side for c in candidates}) > 1:
        return reject("ALPHA_CONFLICT")
    groups = dict.fromkeys(WEIGHTS, 0.0)
    for c in candidates:
        names = [name for name, _ in c.evidence_groups]
        if len(names) != len(set(names)) or set(names) != set(WEIGHTS):
            raise ValueError("UNVERSIONED_OR_DUPLICATE_EVIDENCE_GROUP")
        for name, score in c.evidence_groups:
            groups[name] = max(groups[name], score)
    return EvidenceAudit(
        side=first.side,
        evidence_groups=tuple(groups.items()),
        ordinal_quality=sum(groups[k] * v for k, v in WEIGHTS.items()),
        reason_codes=("DEPENDENT_FEATURES_GROUPED",),
    )
