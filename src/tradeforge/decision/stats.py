"""Bounded closed-trade evidence; no fitted scores or independent-count inflation."""

from collections import Counter
from datetime import datetime
from decimal import Decimal
from random import Random
from typing import Literal, Self
from uuid import UUID

from pydantic import Field, model_validator

from tradeforge.decision.cost_model import RealizedCost
from tradeforge.domain.common import (
    Count,
    Digest,
    Money,
    Positive,
    StrictModel,
    Symbol,
    Text,
    Timestamp,
    UnitFloat,
    utc,
)
from tradeforge.domain.decision import AlphaFamily, Side
from tradeforge.intelligence.models import digest

ZERO = Decimal(0)
MAX_SAMPLES = 10_000


class Bucket(StrictModel):
    symbol: Symbol
    regime: Text
    alpha_family: AlphaFamily
    side: Side
    environment: Literal["replay", "paper", "testnet", "live"]

    @model_validator(mode="after")
    def directional(self) -> Self:
        if self.side is Side.NO_TRADE:
            raise ValueError("DIRECTIONAL_BUCKET_REQUIRED")
        return self


class SettlementProvenance(StrictModel):
    environment: Literal["testnet"]
    exchange: Literal["BINANCE"] = "BINANCE"
    entry_order_id: Text
    exit_order_ids: tuple[Text, ...]
    trade_ids: tuple[Text, ...]
    funding_ids: tuple[Text, ...]
    qty: Positive
    entry_price: Positive
    exit_price: Positive
    stop_price: Positive
    fills_hash: Digest
    income_hash: Digest


class TradeOutcome(StrictModel):
    trade_id: UUID
    bucket: Bucket
    split: Literal["TRAIN", "OOS"]
    opened_at: Timestamp
    closed_at: Timestamp
    observed_at: Timestamp
    initial_risk: Positive
    forecast_at: Timestamp
    gross_R: Money
    costs: RealizedCost
    forecast_net_R: Money | None
    forecast_p_win: UnitFloat | None
    analysis_version: Digest
    settlement: SettlementProvenance | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @model_validator(mode="after")
    def closed(self) -> Self:
        if self.forecast_at > self.opened_at:
            raise ValueError("FORECAST_AFTER_ENTRY")
        if not self.opened_at < self.closed_at <= self.observed_at:
            raise ValueError("INVALID_CLOSED_TRADE_TIME_ORDER")
        return self

    @property
    def net_R(self) -> Decimal:
        return self.gross_R - self.costs.total / self.initial_risk

    @property
    def forecast_error_R(self) -> Decimal | None:
        return None if self.forecast_net_R is None else self.net_R - self.forecast_net_R

    @property
    def digest(self) -> str:
        return digest(self.model_dump(mode="json"))


class Summary(StrictModel):
    dataset_version: Digest
    count: Count
    effective_samples: Count
    wins: Count
    last_observed_at: Timestamp | None
    first_opened_at: Timestamp | None
    first_forecast_at: Timestamp | None
    last_closed_at: Timestamp | None
    p_lower: UnitFloat | None
    p_upper: UnitFloat | None
    avg_win_low_R: Money | None
    avg_loss_high_R: Money | None
    net_mean_low_R: Money | None
    brier_score: UnitFloat | None
    calibration_error: UnitFloat | None


def bootstrap_bounds(values: tuple[Decimal, ...]) -> tuple[Decimal, Decimal] | None:
    """Fixed-seed percentile bootstrap, 256 draws; empirical, never a profit guarantee."""
    if not values:
        return None
    if len(set(values)) == 1:
        return values[0], values[0]
    rng = Random(74202011)
    means = []
    for _ in range(256):
        counts = Counter(rng.choices(values, k=len(values)))
        means.append(sum((v * n for v, n in counts.items()), ZERO) / len(values))
    means.sort()
    return means[6], means[249]  # approximately 2.5% and 97.5%, conservative index rounding


def independent_rows(rows: tuple[TradeOutcome, ...]) -> tuple[TradeOutcome, ...]:
    # Connected overlapping holding intervals count once. Keep the worst net outcome,
    # with stable tie-breaking; this is intentionally more cautious than Kish weights.
    result: list[TradeOutcome] = []
    end: datetime | None = None
    for row in sorted(rows, key=lambda r: (r.opened_at, r.closed_at, str(r.trade_id))):
        if end is None or row.opened_at > end:
            result.append(row)
            end = row.closed_at
        else:
            if (row.net_R, str(row.trade_id)) < (result[-1].net_R, str(result[-1].trade_id)):
                result[-1] = row
            end = max(end, row.closed_at)
    return tuple(result)


def summarize(rows: tuple[TradeOutcome, ...], *, at: datetime, bucket: Bucket) -> Summary:
    utc(at)
    if len(rows) > MAX_SAMPLES:
        raise ValueError("SAMPLE_CAPACITY")
    if len({r.trade_id for r in rows}) != len(rows):
        raise ValueError("DUPLICATE_OUTCOME")
    selected = tuple(
        sorted(
            (r for r in rows if r.bucket == bucket and r.observed_at <= at),
            key=lambda r: str(r.trade_id),
        )
    )
    independent = independent_rows(selected)
    n = len(independent)
    wins = sum(r.gross_R > 0 for r in independent)
    lower = upper = None
    if n:
        # Beta(1+wins, 1+losses) posterior with a uniform prior. Cantelli's inequality
        # bounds each posterior tail by 5% WITHOUT a normal approximation/false precision.
        # These deliberately wide bounds are conditional on exchangeable independent groups.
        a, b = Decimal(1 + wins), Decimal(1 + n - wins)
        mean = a / (a + b)
        radius = (Decimal(19) * a * b / ((a + b) ** 2 * (a + b + 1))).sqrt()
        lower, upper = float(max(ZERO, mean - radius)), float(min(Decimal(1), mean + radius))
    win_bounds = bootstrap_bounds(tuple(r.gross_R for r in independent if r.gross_R > 0))
    loss_bounds = bootstrap_bounds(tuple(-r.gross_R for r in independent if r.gross_R <= 0))
    net_bounds = bootstrap_bounds(tuple(r.net_R for r in independent))
    brier = calibration = None
    if n and all(r.forecast_p_win is not None for r in independent):
        predictions = [(r.forecast_p_win, float(r.gross_R > 0)) for r in independent]
        brier = sum((p - y) ** 2 for p, y in predictions if p is not None) / n
        # Reliability error per fixed probability bin, rather than a cancelling global mean.
        bins: dict[int, list[tuple[float, float]]] = {}
        for p, y in predictions:
            assert p is not None
            bins.setdefault(min(9, int(p * 10)), []).append((p, y))
        calibration = sum(abs(sum(p - y for p, y in values)) for values in bins.values()) / n
    return Summary(
        dataset_version=digest(tuple(r.digest for r in selected)),
        count=len(selected),
        effective_samples=n,
        wins=wins,
        last_observed_at=max((r.observed_at for r in selected), default=None),
        first_opened_at=min((r.opened_at for r in selected), default=None),
        first_forecast_at=min((r.forecast_at for r in selected), default=None),
        last_closed_at=max((r.closed_at for r in selected), default=None),
        p_lower=lower,
        p_upper=upper,
        avg_win_low_R=None if win_bounds is None else win_bounds[0],
        avg_loss_high_R=None if loss_bounds is None else loss_bounds[1],
        net_mean_low_R=None if net_bounds is None else net_bounds[0],
        brier_score=brier,
        calibration_error=calibration,
    )
