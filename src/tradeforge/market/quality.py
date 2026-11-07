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
    if any(b.timeframe is not timeframe or b.key != bars[0].key or b.as_of > as_of for b in bars):
        return DataQuality.UNKNOWN
    if any(
        b.open_at - a.open_at != timedelta(seconds=timeframe.seconds)
        for a, b in zip(bars, bars[1:], strict=False)
    ):
        return DataQuality.GAP
    if as_of - bars[-1].close_at > timedelta(seconds=timeframe.seconds) + grace:
        return DataQuality.STALE
    return DataQuality.OK
