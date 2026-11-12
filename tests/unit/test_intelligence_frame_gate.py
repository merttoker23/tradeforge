"""A published market scan frame must be a complete, benchmark-anchored period."""

from datetime import timedelta
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

import pytest

from tests.intelligence_fixtures import frame, observation, universe
from tests.market_fixtures import START, instrument
from tradeforge.domain.common import DataQuality
from tradeforge.domain.market import Timeframe
from tradeforge.intelligence.inputs import MarketFrame, MarketObservation
from tradeforge.intelligence.models import RegimePolicy
from tradeforge.intelligence.runtime import frame_coverage, publish_block
from tradeforge.intelligence.warmup import seed_history
from tradeforge.market.bars import make_bar

FIVE = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT")
THREE = ("ETHUSDT", "SOLUSDT", "XRPUSDT")


def custom_frame(index, symbols, moves):
    rows = tuple(observation(index, s, r) for s, r in zip(symbols, moves, strict=True))
    closed = START + timedelta(seconds=(index + 1) * 900) - timedelta(milliseconds=1)
    at = closed + timedelta(milliseconds=1)
    return MarketFrame(
        schema_version=1,
        event_id=uuid5(NAMESPACE_URL, f"frame/{index}/{len(symbols)}"),
        source_at=closed,
        observed_at=at,
        as_of=at,
        timeframe=Timeframe.M15,
        universe=universe(symbols),
        observations=rows,
    )


def test_complete_period_is_publishable():
    rows = frame(1).observations
    eligible = universe().eligible
    assert frame_coverage(rows, eligible) == pytest.approx(1.0)
    assert publish_block(rows, eligible, RegimePolicy()) is None


def test_partial_period_is_not_published():
    """One member of three is missing: the closed period is not complete."""
    rows = frame(1).observations
    eligible = universe().eligible
    partial = (rows[0],)
    assert frame_coverage(partial, eligible) == pytest.approx(1 / 3)
    assert publish_block(partial, eligible, RegimePolicy()) == "WAITING_HISTORY_SWEEP"


def test_missing_benchmark_is_not_published():
    """Coverage may be met while the benchmark is still missing."""
    row = custom_frame(1, FIVE, (0.005, 0.006, 0.004, 0.003, 0.002))
    eligible = row.universe.eligible
    assert publish_block(row.observations, eligible, RegimePolicy()) is None
    without_benchmark = tuple(r for r in row.observations if r.instrument.symbol != "BTCUSDT")
    assert frame_coverage(without_benchmark, eligible) == pytest.approx(0.8)
    assert publish_block(without_benchmark, eligible, RegimePolicy()) == "FRAME_BENCHMARK_MISSING"


def test_universe_without_a_benchmark_is_not_blocked():
    row = custom_frame(1, THREE, (0.005, 0.006, 0.004))
    assert publish_block(row.observations, row.universe.eligible, RegimePolicy()) is None


def bar(symbol: str, index: int, close: str):
    key = instrument(symbol)
    opened = START + timedelta(seconds=index * 900)
    closed = opened + timedelta(seconds=900) - timedelta(milliseconds=1)
    price = Decimal(close)
    return make_bar(
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
        "frame-gate-fixture",
    )


def test_member_longer_than_the_benchmark_keeps_its_beta_estimate():
    """The benchmark map covers the benchmark's whole retained history.

    The benchmark lost two middle periods, so its contiguous tail is short.
    A member with an unbroken history must still be paired with the
    benchmark periods that both of them observed.
    """
    from tests.intelligence_fixtures import engine

    btc = [bar("BTCUSDT", index, str(100 + index)) for index in (2, 3, 4, 5, 8, 9, 10, 11, 12)]
    eth = [bar("ETHUSDT", index, str(50 + index)) for index in range(2, 13)]
    previous_btc, current_btc = btc[-2], btc[-1]
    previous_eth, current_eth = eth[-2], eth[-1]
    at = current_btc.close_at + timedelta(milliseconds=1)
    row = MarketFrame(
        schema_version=1,
        event_id=uuid5(NAMESPACE_URL, "frame-gate/1"),
        source_at=current_btc.close_at,
        observed_at=at,
        as_of=at,
        timeframe=Timeframe.M15,
        universe=universe(("BTCUSDT", "ETHUSDT")),
        observations=(
            MarketObservation(
                previous=previous_btc,
                current=current_btc,
                market=None,
                quality=DataQuality.OK,
            ),
            MarketObservation(
                previous=previous_eth,
                current=current_eth,
                market=None,
                quality=DataQuality.OK,
            ),
        ),
    )
    detector = engine()
    seed_history(detector, [*btc, *eth], row)
    pair = detector._series[row.observations[1].instrument.key].pair
    assert pair.count >= detector.policy.minimum_history
    assert detector._series[row.observations[1].instrument.key].pair.beta is not None
