"""Rebuild bounded return statistics from closed evidence known at this frame.

No historical regime, quotes, universe membership or profitability is invented.
Only the contiguous suffix ending exactly at the frame's previous bar is usable.
"""

from collections import defaultdict
from datetime import timedelta

from tradeforge.domain.market import ClosedBar
from tradeforge.intelligence.inputs import MarketFrame
from tradeforge.intelligence.models import digest
from tradeforge.intelligence.regime import RegimeEngine


def seed_history(detector: RegimeEngine, bars: list[ClosedBar], frame: MarketFrame) -> None:
    series: dict[tuple[str, ...], dict[object, ClosedBar]] = defaultdict(dict)
    for bar in bars:
        if (
            bar.timeframe is frame.timeframe
            and bar.observed_at <= frame.observed_at
            and bar.as_of <= frame.as_of
            and bar.close_at < frame.source_at
        ):
            series[bar.key][bar.open_at] = bar
    detector._series.clear()
    detector._seed_source = frame.source_at
    step = timedelta(seconds=frame.timeframe.seconds)
    tails = {}
    for row in frame.observations:
        known = series[row.current.key]
        previous = known.get(row.previous.open_at)
        if previous is None or previous.close != row.previous.close:
            continue
        tail = [previous]
        for _ in range(detector.policy.window):
            previous = known.get(tail[-1].open_at - step)
            if previous is None:
                break
            tail.append(previous)
        tails[row.instrument.key] = list(reversed(tail))
    # Bind exactly the actually used contiguous observations to the feature
    # version. A revision outside the latest pair must never share its input id.
    detector._warmup_hash = digest(
        [
            [list(key), [bar.model_dump(mode="json") for bar in tail]]
            for key, tail in sorted(tails.items())
        ]
    )
    benchmark = next((v for k, v in tails.items() if k[2] == "BTCUSDT"), [])
    btc_returns = {
        b.open_at: float(b.close.ln() - a.close.ln())
        for a, b in zip(benchmark, benchmark[1:], strict=False)
    }
    for key, tail in tails.items():
        stats = detector._new_series()
        for a, b in zip(tail, tail[1:], strict=False):
            value = float(b.close.ln() - a.close.ln())
            stats.vol.add(value)
            stats.cusum.add(value)
            if b.open_at in btc_returns:
                stats.pair.add(btc_returns[b.open_at], value)
            else:
                stats.pair = detector._new_pair()
        stats.last_close = tail[-1].close
        detector._series[key] = stats
