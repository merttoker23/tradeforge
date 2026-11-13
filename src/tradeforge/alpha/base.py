"""Strict pre-decision contracts; an ordinal setup is never a calibrated forecast."""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Annotated, Literal, Self

from pydantic import BeforeValidator, Field, model_validator

from tradeforge.domain.common import (
    DataQuality,
    Digest,
    NonNegative,
    Positive,
    StrictModel,
    Text,
    Timestamp,
    UnitFloat,
    exact_bool,
)
from tradeforge.domain.decision import AlphaFamily, DecisionState, ProofState, Side
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.domain.market import (
    ClosedBar,
    MarketSnapshot,
    OrderBookSnapshot,
    Timeframe,
    TradeFlowWindow,
)
from tradeforge.intelligence.models import FeatureSnapshot, digest


class MomentumPolicy(StrictModel):
    version: Literal[1] = 1
    minimum_history: Annotated[int, Field(ge=4, le=64)] = 8
    ttl_seconds: Annotated[int, Field(ge=1, le=3600)] = 900
    micro_age_seconds: Annotated[int, Field(ge=1, le=60)] = 5
    spread_bps: Positive = Decimal("5")
    minimum_depth_quote: Positive = Decimal("10000")
    expansion_ratio: Positive = Decimal("1.5")
    volume_ratio: Positive = Decimal("1.5")
    minimum_pullback: Annotated[UnitFloat, Field(gt=0)] = 0.1
    maximum_pullback: Annotated[UnitFloat, Field(gt=0, lt=1)] = 0.6
    minimum_flow: Annotated[UnitFloat, Field(gt=0)] = 0.2
    trend_efficiency: Annotated[UnitFloat, Field(gt=0)] = 0.6
    max_setups: Annotated[int, Field(ge=1, le=100_000)] = 4096
    max_instruments: Annotated[int, Field(ge=1, le=100_000)] = 4096

    @model_validator(mode="after")
    def coherent(self) -> Self:
        if self.minimum_pullback >= self.maximum_pullback:
            raise ValueError("INCOHERENT_PULLBACK_POLICY")
        return self

    @property
    def digest(self) -> str:
        return digest(self.model_dump(mode="json"))


class MomentumInput(StrictModel):
    instrument: InstrumentKey
    as_of: Timestamp
    features: FeatureSnapshot
    setup_bars: Annotated[tuple[ClosedBar, ...], Field(max_length=64)]
    medium_bars: Annotated[tuple[ClosedBar, ...], Field(max_length=64)]
    higher_bars: Annotated[tuple[ClosedBar, ...], Field(max_length=64)]
    market: MarketSnapshot
    book: OrderBookSnapshot
    flow: TradeFlowWindow
    quality: DataQuality

    @model_validator(mode="after")
    def aligned(self) -> Self:
        records = [self.market, self.book, self.flow]
        for bars, tf in (
            (self.setup_bars, Timeframe.M5),
            (self.medium_bars, Timeframe.M15),
            (self.higher_bars, Timeframe.H1),
        ):
            for bar in bars:
                if bar.timeframe is not tf or bar.key[:-1] != self.instrument.key:
                    raise ValueError("BAR_IDENTITY_OR_TIMEFRAME_MISMATCH")
            if any(
                b.open_at - a.open_at != timedelta(seconds=tf.seconds)
                for a, b in zip(bars, bars[1:], strict=False)
            ):
                raise ValueError("BAR_GAP_OR_REORDER")
            records.extend(bars)
        if any(r.key != self.instrument.key for r in (self.market, self.book, self.flow)):
            raise ValueError("MICROSTRUCTURE_IDENTITY_MISMATCH")
        if any(r.as_of > self.as_of for r in records) or self.features.frame.as_of > self.as_of:
            raise ValueError("FUTURE_INPUT")
        rows = [r for r in self.features.frame.observations if r.instrument == self.instrument]
        if self.medium_bars and (len(rows) != 1 or rows[0].current != self.medium_bars[-1]):
            raise ValueError("FEATURE_BAR_MISMATCH")
        return self

    @property
    def digest(self) -> str:
        return digest(self.model_dump(mode="json"))


class AlphaCandidate(StrictModel):
    setup_id: Digest
    instrument: InstrumentKey
    alpha_family: Literal[AlphaFamily.MOMENTUM, AlphaFamily.REVERSAL] = AlphaFamily.MOMENTUM
    side: Side
    state: DecisionState
    observed_at: Timestamp
    expires_at: Timestamp
    trigger: Positive
    stop_proposal: Positive
    take_profit_proposal: Positive
    invalidations: Annotated[tuple[Text, ...], Field(min_length=1)]
    source_feature_hash: Digest
    input_hash: Digest
    policy_hash: Digest
    regime_version: Text
    universe_version: Digest
    reason_codes: Annotated[tuple[Text, ...], Field(min_length=1)]
    ordinal_quality: UnitFloat
    evidence_groups: tuple[tuple[Text, UnitFloat], ...]
    spread_bps: NonNegative
    proof_state: Literal[ProofState.UNPROVEN] = ProofState.UNPROVEN
    confidence: None = None
    trading_allowed: Annotated[Literal[False], BeforeValidator(exact_bool)] = False

    @model_validator(mode="after")
    def proposal(self) -> Self:
        if self.state not in (
            DecisionState.DISCOVERED,
            DecisionState.WATCHING,
            DecisionState.ARMED,
        ):
            raise ValueError("ALPHA_IS_NOT_APPROVAL")
        if self.expires_at <= self.observed_at or self.side is Side.NO_TRADE:
            raise ValueError("INVALID_ALPHA_DIRECTION_OR_EXPIRY")
        long = self.stop_proposal < self.trigger < self.take_profit_proposal
        short = self.take_profit_proposal < self.trigger < self.stop_proposal
        if not (long if self.side is Side.LONG else short):
            raise ValueError("INVALID_ALPHA_PRICES")
        return self


class AlphaResult(StrictModel):
    candidate: AlphaCandidate | None
    reason_codes: Annotated[tuple[Text, ...], Field(min_length=1)]
    failed_gates: tuple[Text, ...] = ()


def evidence_blockers(d: MomentumInput, now: datetime, minimum_history: int) -> list[str]:
    blockers = []
    if d.quality is not DataQuality.OK:
        blockers.append("DATA_UNAVAILABLE")
    for bars in (d.setup_bars, d.medium_bars, d.higher_bars):
        if len(bars) < minimum_history:
            blockers.append("INSUFFICIENT_HISTORY")
        elif (now - bars[-1].close_at).total_seconds() >= bars[-1].timeframe.seconds:
            blockers.append("STALE_BARS")
    f = d.features
    if (
        now - f.state.source_at
    ).total_seconds() > f.policy.max_age_periods * f.frame.timeframe.seconds:
        blockers.append("STALE_REGIME")
    universe = f.frame.universe
    if (
        d.instrument not in universe.eligible
        or (now - universe.observed_at).total_seconds() > f.policy.universe_ttl_seconds
    ):
        blockers.append("UNIVERSE_UNAVAILABLE")
    return blockers


def instrument_stress(sample: MomentumInput) -> str | None:
    policy = sample.features.policy
    feature = next(
        (row for row in sample.features.features if row.instrument == sample.instrument), None
    )
    if feature is None:
        return "FEATURE_UNAVAILABLE"
    if abs(feature.log_return) >= policy.stress_return:
        return "RETURN_SHOCK"
    if abs(sample.market.funding) >= Decimal(str(policy.stress_funding)) or (
        feature.funding is not None and abs(feature.funding) >= policy.stress_funding
    ):
        return "FUNDING_STRESS"
    spread = (
        20000
        * (sample.market.best_ask - sample.market.best_bid)
        / (sample.market.best_ask + sample.market.best_bid)
    )
    if spread >= Decimal(str(policy.stress_spread_bps)) or (
        feature.spread_bps is not None and feature.spread_bps >= policy.stress_spread_bps
    ):
        return "SPREAD_GATE"
    return None
