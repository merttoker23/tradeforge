"""Immutable policy and feature evidence. Every output stays non-executable."""

import hashlib
from typing import Annotated, Literal, Self

from pydantic import BeforeValidator, Field, model_validator

from tradeforge.domain.common import (
    Count,
    Digest,
    StatFloat,
    StrictModel,
    Text,
    UnitFloat,
    exact_bool,
    exact_int,
)
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.domain.market import MarketRegimeState, Timeframe
from tradeforge.intelligence.inputs import MarketFrame
from tradeforge.market.bars import canonical

REGIME_ALGORITHM_VERSION: Literal[2] = 2


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def feature_input_version(
    frame: MarketFrame,
    policy_hash: str,
    previous_hash: str,
    warmup_hash: str | None = None,
    *,
    algorithm_version: Literal[1, 2] = 1,
) -> str:
    inputs: dict[str, object] = {
        "frame": frame.model_dump(mode="json"),
        "policy": policy_hash,
        "previous": previous_hash,
        "algorithm": algorithm_version,
    }
    if warmup_hash is not None:
        inputs["warmup"] = warmup_hash
    return digest(inputs)


class RegimePolicy(StrictModel):
    version: Literal[1] = 1
    timeframe: Timeframe = Timeframe.M15
    window: Annotated[int, Field(ge=2, le=4096)] = 32
    minimum_history: Annotated[int, Field(ge=2)] = 8
    max_symbols: Annotated[int, Field(ge=1, le=100_000)] = 4096
    minimum_coverage: Annotated[UnitFloat, Field(gt=0)] = 0.8
    alpha: Annotated[UnitFloat, Field(gt=0)] = 0.2
    return_clip: Annotated[StatFloat, Field(gt=0, le=1)] = 0.05
    neutral_return: Annotated[StatFloat, Field(ge=0)] = 0.0001
    compression_vol: Annotated[StatFloat, Field(gt=0)] = 0.0005
    trend_efficiency: Annotated[UnitFloat, Field(gt=0)] = 0.65
    trend_breadth: Annotated[UnitFloat, Field(gt=0.5)] = 0.65
    stress_return: Annotated[StatFloat, Field(gt=0)] = 0.05
    stress_vol: Annotated[StatFloat, Field(gt=0)] = 0.03
    stress_dispersion: Annotated[StatFloat, Field(gt=0)] = 0.025
    stress_spread_bps: Annotated[StatFloat, Field(gt=0)] = 50.0
    stress_funding: Annotated[StatFloat, Field(gt=0)] = 0.003
    minimum_stress_breadth: Annotated[UnitFloat, Field(gt=0)] = Field(
        default=0.2, exclude_if=lambda value: value == 0.2
    )
    cusum_drift: Annotated[StatFloat, Field(ge=0)] = 0.001
    cusum_threshold: Annotated[StatFloat, Field(gt=0)] = 0.03
    confirmation: Annotated[int, Field(ge=2, le=100)] = 3
    cooldown: Annotated[int, Field(ge=0, le=1000)] = 3
    max_age_periods: Annotated[StatFloat, Field(gt=0, le=10)] = 1.5
    universe_ttl_seconds: Annotated[int, Field(ge=1, le=604800)] = 86400

    @model_validator(mode="after")
    def coherent(self) -> Self:
        if self.minimum_history > self.window or self.cusum_drift >= self.cusum_threshold:
            raise ValueError("INCOHERENT_REGIME_POLICY")
        if not self.compression_vol < self.stress_vol <= self.return_clip:
            raise ValueError("INCOHERENT_VOLATILITY_POLICY")
        return self

    @property
    def policy_hash(self) -> str:
        return digest(self.model_dump(mode="json"))


class InstrumentFeatures(StrictModel):
    instrument: InstrumentKey
    log_return: StatFloat
    sample_count: Count
    volatility: Annotated[StatFloat, Field(ge=0)]
    return_q90: Annotated[StatFloat, Field(ge=0)]
    efficiency: UnitFloat
    direction: Literal[-1, 0, 1]
    beta: StatFloat | None
    correlation: Annotated[StatFloat, Field(ge=-1, le=1)] | None
    relative_return: StatFloat | None
    spread_bps: Annotated[StatFloat, Field(ge=0)] | None
    funding: StatFloat | None
    change_point: bool


class FeatureSnapshot(StrictModel):
    schema_version: Annotated[Literal[1], BeforeValidator(exact_int)] = 1
    algorithm_version: Literal[1, 2] = 1
    policy: RegimePolicy
    policy_hash: Digest
    previous_hash: Digest
    warmup_hash: Digest | None = Field(default=None, exclude_if=lambda v: v is None)
    frame: MarketFrame
    features: tuple[InstrumentFeatures, ...]
    state: MarketRegimeState
    reason_codes: Annotated[tuple[Text, ...], Field(min_length=1)]
    trading_allowed: Annotated[Literal[False], BeforeValidator(exact_bool)] = False

    @model_validator(mode="after")
    def provenance(self) -> Self:
        if self.policy_hash != self.policy.policy_hash:
            raise ValueError("POLICY_HASH_MISMATCH")
        if self.state.as_of != self.frame.as_of or self.state.input_version != self.input_version:
            raise ValueError("SNAPSHOT_INPUT_MISMATCH")
        return self

    @property
    def input_version(self) -> str:
        return feature_input_version(
            self.frame,
            self.policy_hash,
            self.previous_hash,
            self.warmup_hash,
            algorithm_version=self.algorithm_version,
        )

    @property
    def digest(self) -> str:
        return digest(self.model_dump(mode="json"))
