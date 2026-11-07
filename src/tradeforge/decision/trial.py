"""Explicit finite demo sampling budget, independent of statistical validation."""

from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BeforeValidator, Field

from tradeforge.domain.common import StrictModel, exact_bool
from tradeforge.intelligence.models import digest


class TestnetTrialPolicy(StrictModel):
    version: Literal[1] = 1
    enabled: Annotated[bool, BeforeValidator(exact_bool)] = False
    environment: Literal["testnet"] = "testnet"
    max_entries: Annotated[int, Field(ge=1, le=20)] = 20
    max_notional: Annotated[Decimal, Field(gt=0, le=100)] = Decimal("100")
    max_initial_risk: Annotated[Decimal, Field(gt=0, le=1)] = Decimal("1")
    max_loss: Annotated[Decimal, Field(gt=0, le=5)] = Decimal("5")
    train_entries: Annotated[int, Field(ge=1, le=100)] = 10
    embargo_seconds: Annotated[int, Field(ge=60, le=86400)] = 60

    @property
    def digest(self) -> str:
        return digest(self.model_dump(mode="json"))
