"""Visible, immutable conservative limits. Fractions apply to account equity."""

from decimal import Decimal
from typing import Annotated, Literal

from pydantic import Field

from tradeforge.domain.common import Positive, StrictModel
from tradeforge.intelligence.models import digest

Fraction = Annotated[Positive, Field(le=1)]


class RiskLimits(StrictModel):
    version: Literal[1] = 1
    max_initial_risk: Fraction = Decimal("0.0025")
    daily_loss_cap: Fraction = Decimal("0.02")
    max_heat: Fraction = Decimal("0.02")
    max_positions: Annotated[int, Field(ge=1, le=100)] = 5
    max_symbol_notional: Fraction = Decimal("0.20")
    max_side_notional: Positive = Decimal("0.60")
    max_gross_notional: Positive = Decimal("1.00")
    max_delta: Positive = Decimal("0.50")
    max_beta_delta: Positive = Decimal("0.50")
    max_cluster_notional: Fraction = Decimal("0.30")
    target_vol: Positive = Decimal("0.50")
    max_spread_bps: Positive = Decimal("20")
    correlation_threshold: Fraction = Decimal("0.70")
    min_correlation_samples: Annotated[int, Field(ge=2, le=100000)] = 30
    account_age_seconds: Annotated[int, Field(ge=1, le=60)] = 5
    factor_age_seconds: Annotated[int, Field(ge=1, le=3600)] = 60
    loss_streak_limit: Annotated[int, Field(ge=1, le=100)] = 3
    cooldown_seconds: Annotated[int, Field(ge=0, le=86400)] = 900
    max_leverage: Positive = Decimal("10")

    @property
    def digest(self) -> str:
        return digest(self.model_dump(mode="json"))
