"""Synthetic closed observations, independent of public network or account state."""

import hashlib
import math
from datetime import timedelta
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from tests.market_fixtures import START, instrument
from tradeforge.domain.common import DataQuality
from tradeforge.domain.market import MarketSnapshot, Timeframe, UniverseSnapshot
from tradeforge.market.bars import canonical, make_bar


def universe(symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT"), at=START):
    return UniverseSnapshot(
        schema_version=1,
        event_id=uuid5(NAMESPACE_URL, str((symbols, at))),
        source_at=at,
        observed_at=at,
        as_of=at,
        eligible=tuple(instrument(s) for s in symbols),
        watched=(),
        deep=(),
        filtering_rules=("fixture-eligible",),
        version_hash=hashlib.sha256(canonical((symbols, at.isoformat())).encode()).hexdigest(),
    )


def observation(
    index, symbol="BTCUSDT", move=0.005, spread=2.0, funding=0.0001, previous_close=None
):
    from tradeforge.intelligence.inputs import MarketObservation

    key = instrument(symbol)
    bars = []
    before = previous_close or Decimal(str(100 * math.exp((index - 1) * move)))
    after = (
        before * Decimal(str(math.exp(move)))
        if previous_close is not None
        else Decimal(str(100 * math.exp(index * move)))
    )
    for n, price in ((index - 1, before), (index, after)):
        opened = START + timedelta(seconds=n * 900)
        closed = opened + timedelta(seconds=900) - timedelta(milliseconds=1)
        price = Decimal(str(price))
        bars.append(
            make_bar(
                key,
                Timeframe.M15,
                opened,
                closed,
                {
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": Decimal("10"),
                },
                closed,
                closed + timedelta(milliseconds=1),
                "intelligence-fixture",
            )
        )
    current = bars[-1]
    mid = current.close
    market = MarketSnapshot(
        **key.model_dump(),
        schema_version=1,
        event_id=uuid5(NAMESPACE_URL, f"market/{index}/{symbol}/{spread}/{funding}"),
        source_at=current.source_at,
        observed_at=current.observed_at,
        as_of=current.as_of,
        best_bid=mid * (1 - Decimal(str(spread)) / 20000),
        best_ask=mid * (1 + Decimal(str(spread)) / 20000),
        mark_price=mid,
        index_price=mid,
        volume=Decimal("10"),
        funding=Decimal(str(funding)),
        quality=DataQuality.OK,
    )
    return MarketObservation(
        previous=bars[0], current=current, market=market, quality=DataQuality.OK
    )


def frame(
    index, moves=(0.005, 0.006, 0.004), symbols=None, spread=2.0, funding=0.0001, previous=None
):
    from tradeforge.intelligence.inputs import MarketFrame

    symbols = symbols or ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    prices = (
        {r.instrument.symbol: r.current.close for r in previous.observations} if previous else {}
    )
    rows = tuple(
        observation(index, s, r, spread, funding, prices.get(s))
        for s, r in zip(symbols, moves, strict=True)
    )
    closed = START + timedelta(seconds=(index + 1) * 900) - timedelta(milliseconds=1)
    at = closed + timedelta(milliseconds=1)
    return MarketFrame(
        schema_version=1,
        event_id=uuid5(NAMESPACE_URL, f"frame/{index}"),
        source_at=closed,
        observed_at=at,
        as_of=at,
        timeframe=Timeframe.M15,
        universe=universe(),
        observations=rows,
    )


def engine(**kwargs):
    from tradeforge.intelligence.models import RegimePolicy
    from tradeforge.intelligence.regime import RegimeEngine

    return RegimeEngine(RegimePolicy(window=8, minimum_history=4, confirmation=2, **kwargs))
