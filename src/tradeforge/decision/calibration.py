"""Explicit, versioned evidence requirements; no automatic threshold relaxation."""

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import Field

from tradeforge.decision.stats import Summary
from tradeforge.domain.common import NonNegative, StrictModel
from tradeforge.intelligence.models import digest


class EdgePolicy(StrictModel):
    version: Literal[1] = 1
    min_net_R: NonNegative = Decimal("0")
    min_train: Annotated[int, Field(ge=100, le=10_000)] = 100
    min_oos: Annotated[int, Field(ge=50, le=10_000)] = 100
    max_age_seconds: Annotated[int, Field(ge=1, le=604800)] = 604800
    embargo_seconds: Annotated[int, Field(ge=1, le=86400)] = 60
    max_probability_width: Annotated[float, Field(gt=0, le=0.3)] = 0.3
    max_calibration_error: Annotated[float, Field(gt=0, le=0.1)] = 0.1
    max_brier_score: Annotated[float, Field(gt=0, le=0.25)] = 0.25
    max_win_rate_shift: Annotated[float, Field(gt=0, le=0.15)] = 0.15

    @property
    def digest(self) -> str:
        return digest(self.model_dump(mode="json"))


def conservative_ev(stats: Summary, cost_R: Decimal | None) -> Decimal | None:
    if (
        stats.p_lower is None
        or stats.avg_win_low_R is None
        or stats.avg_loss_high_R is None
        or cost_R is None
    ):
        return None
    p = Decimal(str(stats.p_lower))
    return p * stats.avg_win_low_R - (1 - p) * stats.avg_loss_high_R - cost_R


def calibration_reasons(
    train: Summary,
    oos: Summary,
    *,
    at: datetime,
    policy: EdgePolicy,
    stress_cost_R: Decimal | None,
) -> tuple[str, ...]:
    reasons = []
    for name, stats, minimum in (("TRAIN", train, policy.min_train), ("OOS", oos, policy.min_oos)):
        if stats.effective_samples < minimum:
            reasons.append(name + "_INSUFFICIENT_SAMPLES")
        if stats.last_observed_at is None or not (
            0 <= (at - stats.last_observed_at).total_seconds() <= policy.max_age_seconds
        ):
            reasons.append(name + "_STATS_STALE")
        if (
            stats.p_lower is None
            or stats.p_upper is None
            or (stats.p_upper - stats.p_lower > policy.max_probability_width)
        ):
            reasons.append(name + "_PROBABILITY_UNCERTAIN")
    if (
        train.last_closed_at is None
        or oos.first_opened_at is None
        or train.last_observed_at is None
        or oos.first_forecast_at is None
        or (oos.first_opened_at - train.last_closed_at).total_seconds() < policy.embargo_seconds
        or (oos.first_opened_at - train.last_observed_at).total_seconds() < policy.embargo_seconds
        or (oos.first_forecast_at - train.last_observed_at).total_seconds() < policy.embargo_seconds
    ):
        reasons.append("OOS_NOT_PURGED")
    if oos.net_mean_low_R is None or oos.net_mean_low_R <= policy.min_net_R:
        reasons.append("OOS_UNPROVEN")
    oos_ev = conservative_ev(oos, stress_cost_R)
    if oos_ev is None or oos_ev <= policy.min_net_R:
        reasons.append("OOS_STRESS_UNPROVEN")
    if (
        train.effective_samples
        and oos.effective_samples
        and abs(train.wins / train.effective_samples - oos.wins / oos.effective_samples)
        > policy.max_win_rate_shift
    ):
        reasons.append("MARKET_SHIFT")
    if (
        oos.calibration_error is None
        or oos.brier_score is None
        or (
            oos.calibration_error > policy.max_calibration_error
            or oos.brier_score > policy.max_brier_score
        )
    ):
        reasons.append("UNCALIBRATED")
    return tuple(reasons)
