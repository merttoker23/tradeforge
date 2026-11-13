"""Canonical market record schemas. Stream processing belongs to later phases."""

from datetime import datetime, timedelta
from enum import StrEnum
from typing import Annotated, Literal, Self

from pydantic import BeforeValidator, Field, model_validator

from tradeforge.domain.common import (
    Count,
    DataQuality,
    Digest,
    Money,
    NonNegative,
    Positive,
    Reasons,
    StatFloat,
    Text,
    Timestamp,
    exact_bool,
)
from tradeforge.domain.events import Event
from tradeforge.domain.instrument import InstrumentEvent, InstrumentKey


class Timeframe(StrEnum):
    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    H1 = "1h"
    H4 = "4h"
    H12 = "12h"
    D1 = "1d"

    @property
    def seconds(self) -> int:
        # This property is on the readiness hot path for every retained bar
        # series; constructing the mapping on each access dominated sampling.
        if self is Timeframe.M1:
            return 60
        if self is Timeframe.M5:
            return 300
        if self is Timeframe.M15:
            return 900
        if self is Timeframe.H1:
            return 3600
        if self is Timeframe.H4:
            return 14400
        if self is Timeframe.H12:
            return 43200
        return 86400


class MarketQuote(InstrumentEvent):
    """Independent display mark; never composite execution or analysis evidence."""

    mark_price: Positive
    volume: NonNegative | None
    volume_source_at: Timestamp | None
    source: Text
    environment: Literal["mainnet", "testnet"]

    @model_validator(mode="after")
    def volume_provenance(self) -> Self:
        if (self.volume is None) != (self.volume_source_at is None):
            raise ValueError("QUOTE_VOLUME_PROVENANCE_REQUIRED")
        if self.volume_source_at is not None and self.volume_source_at > self.observed_at:
            raise ValueError("FUTURE_QUOTE_VOLUME")
        return self


class MarketSnapshot(InstrumentEvent):
    environment: Literal["unspecified", "mainnet", "testnet"] = Field(
        default="unspecified", exclude_if=lambda v: v == "unspecified"
    )
    source: Text = Field(default="unspecified", exclude_if=lambda v: v == "unspecified")
    best_bid: Positive
    best_ask: Positive
    mark_price: Positive
    index_price: Positive
    # Book and premium-index evidence are independent of the 24h ticker.
    # Legacy complete snapshots omit all three component times and retain their
    # conservative composite source_at. New producers always supply provenance.
    volume: NonNegative | None
    funding: Money
    quality: DataQuality
    book_source_at: Timestamp | None = Field(default=None, exclude_if=lambda v: v is None)
    mark_source_at: Timestamp | None = Field(default=None, exclude_if=lambda v: v is None)
    volume_source_at: Timestamp | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def component_provenance(self) -> Self:
        fields = {"book_source_at", "mark_source_at", "volume_source_at"}
        if not fields.intersection(self.model_fields_set):
            if self.volume is None:
                raise ValueError("MARKET_COMPONENT_PROVENANCE_REQUIRED")
            return self
        if self.book_source_at is None or self.mark_source_at is None:
            raise ValueError("MARKET_COMPONENT_PROVENANCE_REQUIRED")
        if (self.volume is None) != (self.volume_source_at is None):
            raise ValueError("MARKET_VOLUME_PROVENANCE_REQUIRED")
        times = (self.book_source_at, self.mark_source_at)
        if self.source_at != min(times):
            raise ValueError("MARKET_COST_SOURCE_MISMATCH")
        if any(
            t > self.observed_at
            for t in (*times, *((self.volume_source_at,) if self.volume_source_at else ()))
        ):
            raise ValueError("FUTURE_MARKET_COMPONENT")
        return self

    def costs_fresh(self, at: datetime, max_age_seconds: float) -> bool:
        return (
            self.quality is DataQuality.OK
            and self.observed_at <= self.as_of <= at
            and all(
                0 <= (at - source).total_seconds() <= max_age_seconds
                for source in (
                    self.book_source_at or self.source_at,
                    self.mark_source_at or self.source_at,
                )
            )
        )

    def volume_fresh(self, at: datetime, max_age_seconds: float) -> bool:
        source = self.volume_source_at if self.book_source_at is not None else self.source_at
        return (
            self.volume is not None
            and source is not None
            and self.quality is DataQuality.OK
            and self.observed_at <= self.as_of <= at
            and 0 <= (at - source).total_seconds() <= max_age_seconds
        )

    @model_validator(mode="after")
    def spread(self) -> Self:
        if self.best_bid >= self.best_ask:
            raise ValueError("CROSSED_MARKET")
        return self


class ClosedBar(InstrumentEvent):
    timeframe: Timeframe
    open_at: Timestamp
    close_at: Timestamp
    open: Positive
    high: Positive
    low: Positive
    close: Positive
    volume: NonNegative
    source: Text
    complete: Annotated[Literal[True], BeforeValidator(exact_bool)]

    @property
    def key(self) -> tuple[str, ...]:
        return (*super().key, self.timeframe.value)

    @model_validator(mode="after")
    def closed_ohlc(self) -> Self:
        if not self.open_at < self.close_at <= self.source_at:
            raise ValueError("BAR_NOT_AVAILABLE_AT_SOURCE")
        duration = timedelta(seconds=self.timeframe.seconds)
        if self.close_at != self.open_at + duration - timedelta(milliseconds=1):
            raise ValueError("INVALID_BAR_INTERVAL")
        if self.open_at.timestamp() % self.timeframe.seconds != 0:
            raise ValueError("UNALIGNED_BAR")
        if not self.low <= min(self.open, self.close) <= max(self.open, self.close) <= self.high:
            raise ValueError("INVALID_OHLC")
        return self


class StreamState(StrEnum):
    UNSYNCED = "UNSYNCED"
    READY = "READY"
    STALE = "STALE"
    GAP = "GAP"


Level = tuple[Positive, Positive]


class OrderBookSnapshot(InstrumentEvent):
    initial_update_id: Count
    last_update_id: Count
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    captured_at: Timestamp
    stream_state: StreamState

    @model_validator(mode="after")
    def coherent_book(self) -> Self:
        if self.initial_update_id > self.last_update_id or self.captured_at > self.observed_at:
            raise ValueError("INVALID_BOOK_SEQUENCE_OR_TIME")
        bids, asks = [p for p, _ in self.bids], [p for p, _ in self.asks]
        if any(a <= b for a, b in zip(bids, bids[1:], strict=False)):
            raise ValueError("UNSORTED_OR_DUPLICATE_BIDS")
        if any(a >= b for a, b in zip(asks, asks[1:], strict=False)):
            raise ValueError("UNSORTED_OR_DUPLICATE_ASKS")
        if bids and asks and bids[0] >= asks[0]:
            raise ValueError("CROSSED_BOOK")
        if self.stream_state is StreamState.READY and (not bids or not asks):
            raise ValueError("EMPTY_READY_BOOK")
        return self


class TradeFlowWindow(InstrumentEvent):
    # Stay within Python's representable duration domain before building a timedelta.
    period_seconds: Annotated[
        int, Field(gt=0, le=timedelta.max.days * 86400 + timedelta.max.seconds)
    ]
    signed_taker_flow: Money
    agg_volume: NonNegative
    count: Count
    source_start: Timestamp
    source_end: Timestamp
    validity: DataQuality

    @model_validator(mode="after")
    def coherent_window(self) -> Self:
        if not self.source_start <= self.source_end <= self.source_at:
            raise ValueError("INVALID_FLOW_TIME")
        if self.source_end - self.source_start > timedelta(seconds=self.period_seconds):
            raise ValueError("FLOW_EXCEEDS_WINDOW")
        if self.signed_taker_flow.copy_abs() > self.agg_volume:
            raise ValueError("FLOW_EXCEEDS_VOLUME")
        if (self.count == 0) != (self.agg_volume == 0):
            raise ValueError("INCONSISTENT_FLOW_COUNT")
        return self


class RegimeLabel(StrEnum):
    TREND_EXPANSION = "TREND_EXPANSION"
    VOL_COMPRESSION = "VOL_COMPRESSION"
    RANGE = "RANGE"
    DISORDER = "DISORDER"
    TRANSITION = "TRANSITION"
    UNKNOWN = "UNKNOWN"


class MarketRegimeState(Event):
    label: RegimeLabel
    metrics: tuple[tuple[Text, StatFloat], ...]
    reason_codes: Reasons
    input_version: Text

    @model_validator(mode="after")
    def unique_metrics(self) -> Self:
        if len({name for name, _ in self.metrics}) != len(self.metrics):
            raise ValueError("DUPLICATE_METRICS")
        return self


class UniverseSnapshot(Event):
    eligible: tuple[InstrumentKey, ...]
    watched: tuple[InstrumentKey, ...]
    deep: tuple[InstrumentKey, ...]
    filtering_rules: Annotated[tuple[Text, ...], Field(min_length=1)]
    version_hash: Digest

    @model_validator(mode="after")
    def allocations(self) -> Self:
        sets = [set(items) for items in (self.eligible, self.watched, self.deep)]
        if any(
            len(s) != len(items)
            for s, items in zip(sets, (self.eligible, self.watched, self.deep), strict=True)
        ):
            raise ValueError("DUPLICATE_UNIVERSE_IDENTITY")
        if not sets[2] <= sets[1] <= sets[0]:
            raise ValueError("INVALID_UNIVERSE_ALLOCATION")
        return self
