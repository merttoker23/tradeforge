"""300 instruments through real regime, surveillance and routed market plans."""

import math
from datetime import timedelta
from decimal import Decimal

from tests.intelligence_fixtures import frame, observation, universe
from tests.market_fixtures import instrument
from tradeforge.intelligence.inputs import MarketFrame
from tradeforge.intelligence.models import RegimePolicy
from tradeforge.intelligence.opportunity import SurveillancePolicy
from tradeforge.intelligence.regime import RegimeEngine
from tradeforge.intelligence.subscription_allocator import SubscriptionAllocator, stream_names

SYMBOLS = ("BTCUSDT", *(f"COIN{n:04}USDT" for n in range(299)))


def market(n, previous=None, symbols=SYMBOLS):
    moves = tuple(0.003 * math.sin(n * 0.9 + k % 7) + 0.001 for k in range(len(symbols)))
    source = frame(n)
    payload = source.model_dump()
    payload["universe"] = universe(symbols, at=source.as_of)
    prices = (
        {r.instrument.symbol: r.current.close for r in previous.observations} if previous else {}
    )
    rows = []
    for k, (symbol, move) in enumerate(zip(symbols, moves, strict=True)):
        row = observation(n, symbol, move, previous_close=prices.get(symbol))
        data = row.model_dump()
        data["current"]["volume"] = Decimal(100 + ((k + n * 20) % 300) * 10)
        data["previous"]["volume"] = Decimal(1000)
        rows.append(data)
    payload["observations"] = tuple(rows)
    return MarketFrame.model_validate(payload)


def test_300_symbols_changing_candidates_full_coverage_bounded_and_replayable():
    policy = SurveillancePolicy(hysteresis=0.0)
    workers = (SubscriptionAllocator(policy), SubscriptionAllocator(policy))
    regimes = (
        RegimeEngine(RegimePolicy(minimum_history=4)),
        RegimeEngine(RegimePolicy(minimum_history=4)),
    )
    previous = None
    allocations = []
    for n in range(1, 12):
        current = market(n, previous)
        outputs = [
            worker.update(
                regime.update(current), now=current.as_of, positions=(instrument("COIN0000USDT"),)
            )
            for worker, regime in zip(workers, regimes, strict=True)
        ]
        a, b = outputs
        assert a == b
        assert len(a.ranked) == len(a.universe.eligible) == 300
        assert len(a.universe.watched) <= policy.watch + 1
        assert len(a.universe.deep) <= policy.deep + 1
        assert len(a.plan.groups) <= policy.max_sockets
        assert not a.plan.omitted
        assert all(len(g) <= policy.streams_per_socket for g in a.plan.groups)
        assert {"!ticker@arr", "!markPrice@arr@1s"} <= stream_names(a.plan)
        assert a.plan.protected == {"COIN0000USDT"}
        a.verify_monitoring(stream_names(a.plan))
        assert workers[0].retained_count <= 2 * 300 + policy.watch + policy.deep
        allocations.append(a)
        previous = current
    assert any(a.status == "WATCHING" for a in allocations)
    assert allocations[-1].universe.watched != allocations[4].universe.watched
    final = allocations[-1]
    # 120 high-frequency scheduling calls cannot create a subscribe/unsubscribe storm.
    for seconds in range(1, 121):
        result = workers[0].update(
            regimes[0].latest,
            now=final.universe.as_of + timedelta(seconds=seconds),
            positions=(instrument("COIN0000USDT"),),
        )
        if seconds % policy.rebalance_seconds:
            assert not result.subscribe and not result.unsubscribe


def test_position_removed_from_tradable_universe_remains_monitored():
    worker = SubscriptionAllocator()
    regime = RegimeEngine(RegimePolicy(minimum_history=4))
    previous = None
    for n in range(1, 6):
        previous = market(n, previous)
        worker.update(
            regime.update(previous), now=previous.as_of, positions=(instrument("COIN0000USDT"),)
        )
    remaining = tuple(s for s in SYMBOLS if s != "COIN0000USDT")
    current = market(6, previous, remaining)
    result = worker.update(
        regime.update(current),
        now=current.as_of,
        positions=(instrument("COIN0000USDT"),),
        queue_pressure=1.0,
    )
    assert instrument("COIN0000USDT") not in result.universe.eligible
    assert result.plan.protected == {"COIN0000USDT"}
    assert "coin0000usdt@depth@100ms" not in result.unsubscribe
    result.verify_monitoring(stream_names(result.plan))
