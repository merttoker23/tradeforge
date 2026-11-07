"""Closed, synchronized point-in-time inputs; invalid wire numerics fail validation."""

from datetime import timedelta
from typing import Self

from pydantic import Field, model_validator

from tradeforge.domain.common import DataQuality, StrictModel
from tradeforge.domain.events import Event
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.domain.market import ClosedBar, MarketSnapshot, Timeframe, UniverseSnapshot
from tradeforge.intelligence.volatility import finite


class MarketObservation(StrictModel):
    previous: ClosedBar
    current: ClosedBar
    market: MarketSnapshot | None
    quality: DataQuality

    @model_validator(mode="after")
    def aligned(self) -> Self:
        a, b = self.previous, self.current
        if a.key != b.key or b.open_at - a.open_at != timedelta(seconds=b.timeframe.seconds):
            raise ValueError("MISALIGNED_RETURN_BARS")
        if self.market is not None and self.market.key != b.key[:-1]:
            raise ValueError("MARKET_IDENTITY_MISMATCH")
        # Validate every Decimal -> statistical-float boundary before any engine
        # state can be advanced. Finite Decimal alone does not ensure finite float.
        _ = self.log_return, self.spread_bps
        if self.market is not None:
            finite(float(self.market.funding))
        return self

    @property
    def instrument(self) -> InstrumentKey:
        return InstrumentKey(**{k: getattr(self.current, k) for k in InstrumentKey.model_fields})

    @property
    def log_return(self) -> float:
        # Decimal ln avoids overflowing float(price) or rounding tiny relative moves.
        return finite(float(self.current.close.ln() - self.previous.close.ln())) + 0.0

    @property
    def spread_bps(self) -> float | None:
        if self.market is None:
            return None
        m = self.market
        return finite(float(20000 * (m.best_ask - m.best_bid) / (m.best_ask + m.best_bid)))


class MarketFrame(Event):
    timeframe: Timeframe
    universe: UniverseSnapshot
    observations: tuple[MarketObservation, ...] = Field(max_length=100_000)

    @model_validator(mode="after")
    def point_in_time(self) -> Self:
        if self.universe.as_of > self.as_of:
            raise ValueError("FUTURE_UNIVERSE")
        identities = [row.instrument for row in self.observations]
        if len(set(identities)) != len(identities):
            raise ValueError("DUPLICATE_OBSERVATION")
        if not set(identities) <= set(self.universe.eligible):
            raise ValueError("INELIGIBLE_OBSERVATION")
        for row in self.observations:
            if (
                row.current.timeframe is not self.timeframe
                or row.current.close_at != self.source_at
            ):
                raise ValueError("MISALIGNED_FRAME_PERIOD")
            inputs = (row.previous, row.current, *((row.market,) if row.market else ()))
            if any(
                item.as_of > self.as_of or item.observed_at > self.observed_at for item in inputs
            ):
                raise ValueError("FUTURE_FRAME_INPUT")
        object.__setattr__(
            self, "observations", tuple(sorted(self.observations, key=lambda r: r.current.key))
        )
        return self
