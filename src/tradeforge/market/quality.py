"""Series-local integrity and freshness; an absent interval is never zero-filled."""

from datetime import datetime, timedelta

from tradeforge.domain.common import DataQuality, utc
from tradeforge.domain.market import ClosedBar, Timeframe


def bar_quality(
    bars: tuple[ClosedBar, ...],
    timeframe: Timeframe,
    as_of: datetime,
    *,
    grace: timedelta = timedelta(seconds=5),
) -> DataQuality:
    utc(as_of)
    if not bars:
        return DataQuality.MISSING
    # Readiness re-evaluates every retained series. Comparing complete bar
    # identities per bar rebuilt Pydantic key tuples hundreds of thousands of
    # times per second. Every identity component except the symbol is a
    # single-valued literal, so symbol plus timeframe is the same proof at a
    # fraction of the cost.
    first = bars[0]
    if any(
        b.timeframe is not timeframe or b.symbol != first.symbol or b.as_of > as_of for b in bars
    ):
        return DataQuality.UNKNOWN
    step = timedelta(seconds=timeframe.seconds)
    if any(b.open_at - a.open_at != step for a, b in zip(bars, bars[1:], strict=False)):
        return DataQuality.GAP
    if as_of - bars[-1].close_at > timedelta(seconds=timeframe.seconds) + grace:
        return DataQuality.STALE
    return DataQuality.OK
