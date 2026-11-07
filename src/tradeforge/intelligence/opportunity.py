"""Versioned surveillance evidence; ranking carries no trading authority."""

from typing import Annotated, Self

from pydantic import Field, model_validator

from tradeforge.domain.common import Digest, StrictModel, Text, UnitFloat
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.intelligence.models import digest

Budget = Annotated[int, Field(ge=0, le=100_000)]
Seconds = Annotated[int, Field(ge=0, le=86400)]


class SurveillancePolicy(StrictModel):
    version: Annotated[int, Field(ge=1, le=1)] = 1
    watch: Budget = 20
    deep: Budget = 4
    max_positions: Annotated[int, Field(ge=1, le=100_000)] = 1024
    rebalance_seconds: Annotated[Seconds, Field(gt=0)] = 30
    dwell_seconds: Seconds = 120
    cooldown_seconds: Seconds = 60
    hysteresis: UnitFloat = 0.1
    rotation_limit: Annotated[int, Field(ge=1, le=1000)] = 2
    pressure_threshold: Annotated[UnitFloat, Field(gt=0)] = 0.8
    max_sockets: Annotated[int, Field(ge=1, le=32)] = 8
    streams_per_socket: Annotated[int, Field(ge=2, le=1024)] = 1024
    max_age_seconds: Annotated[int, Field(ge=1, le=86400)] = 1800
    minimum_volume_ratio: Annotated[UnitFloat, Field(gt=0)] = 0.1
    max_spread_bps: Annotated[float, Field(gt=0, le=1000)] = 30.0
    max_funding: Annotated[float, Field(gt=0, le=1)] = 0.003
    min_volatility: Annotated[float, Field(gt=0, le=1)] = 0.00001
    target_volatility: Annotated[float, Field(gt=0, le=1)] = 0.01
    max_volatility: Annotated[float, Field(gt=0, le=1)] = 0.05
    shock_return: Annotated[float, Field(gt=0, le=1)] = 0.08
    pump_return: Annotated[float, Field(gt=0, le=1)] = 0.03
    pump_volume_ratio: Annotated[float, Field(gt=1, le=1000)] = 8.0

    @model_validator(mode="after")
    def coherent(self) -> Self:
        if self.deep > self.watch:
            raise ValueError("DEEP_EXCEEDS_WATCH")
        if not self.min_volatility < self.target_volatility < self.max_volatility:
            raise ValueError("INVALID_VOLATILITY_RANGE")
        if self.pump_return >= self.shock_return:
            raise ValueError("INVALID_ANOMALY_THRESHOLDS")
        return self

    @property
    def policy_hash(self) -> str:
        return digest(self.model_dump(mode="json"))


class RankedOpportunity(StrictModel):
    instrument: InstrumentKey
    score: UnitFloat | None
    components: tuple[tuple[Text, UnitFloat], ...] = ()
    reason_codes: Annotated[tuple[Text, ...], Field(min_length=1)]
    source_feature_hash: Digest

    @model_validator(mode="after")
    def evidence(self) -> Self:
        if len({name for name, _ in self.components}) != len(self.components):
            raise ValueError("DUPLICATE_SCORE_COMPONENT")
        if (self.score is None) != (not self.components):
            raise ValueError("SCORE_EVIDENCE_MISMATCH")
        return self
