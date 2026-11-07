"""Stable candidate arbitration, including batch-wide opposing-alpha vetoes."""

from datetime import datetime

from tradeforge.decision.policy import DecisionPolicy
from tradeforge.decision.provenance import Discovery
from tradeforge.decision.reasons import DecisionReason as R
from tradeforge.domain.common import StrictModel, utc


class Selection(StrictModel):
    discovery: Discovery
    reason_codes: tuple[R, ...]


def overlap(left: Discovery, right: Discovery) -> R | None:
    a, b = left.candidate, right.candidate
    if a.instrument.key == b.instrument.key:
        if a.side is not b.side and a.alpha_family is not b.alpha_family:
            return R.CONFLICTING_ALPHA
        return R.SYMBOL_OVERLAP
    if left.cluster == right.cluster:
        return R.CLUSTER_OVERLAP
    return None


def select_candidates(
    candidates: tuple[Discovery, ...], *, at: datetime, policy: DecisionPolicy
) -> tuple[Selection, ...]:
    utc(at)
    if len(candidates) > 128 or len({d.decision_id for d in candidates}) != len(candidates):
        raise ValueError("INVALID_SELECTION_BATCH")
    ordered = sorted(candidates, key=lambda d: (-d.candidate.ordinal_quality, str(d.decision_id)))
    selected: list[Discovery] = []
    results = []
    for d in ordered:
        reason: R | None = None
        if d.available_at > at:
            raise ValueError("FUTURE_DISCOVERY")
        if d.candidate.expires_at <= at:
            reason = R.EXPIRED
        elif (at - d.candidate.observed_at).total_seconds() > policy.evidence_age_seconds:
            reason = R.STALE
        elif any(
            overlap(d, other) is R.CONFLICTING_ALPHA
            for other in ordered
            if other != d
            and other.candidate.expires_at > at
            and (at - other.candidate.observed_at).total_seconds() <= policy.evidence_age_seconds
        ):
            reason = R.CONFLICTING_ALPHA
        else:
            reason = next((r for other in selected if (r := overlap(d, other))), None)
        if reason is None:
            selected.append(d)
        results.append(Selection(discovery=d, reason_codes=() if reason is None else (reason,)))
    return tuple(results)
