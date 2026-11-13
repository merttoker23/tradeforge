"""Actual persisted candle revisions retain feature/alpha point-in-time identity."""

import asyncio
from datetime import datetime, timedelta

import pytest
from sqlalchemy import text

from tests.unit.test_alpha_handoff import feed_for
from tests.unit.test_momentum_alpha import change, evidence
from tests.unit.test_reversal_alpha import sample as reversal_sample
from tests.unit.test_reversal_alpha import with_frame
from tradeforge.domain.common import DataQuality
from tradeforge.domain.decision import Side
from tradeforge.domain.market import Timeframe
from tradeforge.market.market_state import BarView
from tradeforge.market.storage import save_view
from tradeforge.runtime.market_link import alpha_inputs


def test_later_persisted_candle_revision_cannot_poison_current_feature_input(migrated):
    sample = evidence()
    key = sample.instrument
    save_view(
        migrated,
        BarView(key, Timeframe.M15, sample.as_of, sample.medium_bars, DataQuality.OK),
    )
    save_view(
        migrated,
        BarView(key, Timeframe.M5, sample.as_of, sample.setup_bars, DataQuality.OK),
    )
    later = sample.as_of + timedelta(seconds=1)
    revision = change(sample.medium_bars[-1], observed_at=later, as_of=later)
    save_view(
        migrated,
        BarView(key, Timeframe.M15, later, (revision,), DataQuality.OK),
    )
    feed = feed_for(sample)
    feed["as_of"] = later.isoformat()
    diagnostic = {}
    rows = alpha_inputs(migrated, sample.features, feed, diagnostics=diagnostic)
    assert len(rows) == 1
    assert rows[0].medium_bars[-1] == sample.medium_bars[-1]
    assert rows[0].market.source_at == sample.market.source_at
    assert diagnostic["input_rejections"] == []
    with migrated.transaction() as c:
        assert c.scalar(text("SELECT count(*) FROM execution_intents")) == 0
        assert c.scalar(text("SELECT count(*) FROM orders")) == 0


@pytest.mark.parametrize("quarter", [0, 1, 2, 3])
@pytest.mark.parametrize("missing_child", [False, True])
def test_bounded_medium_history_keeps_complete_hourly_confirmation(
    migrated, quarter, missing_child
):
    from tests.intelligence_fixtures import engine, frame, observation

    sample = evidence()
    index = 127 + quarter
    features = engine().update(frame(index))
    at = features.frame.as_of
    medium = tuple(observation(i, "ETHUSDT", 0.006).current for i in range(index - 127, index + 1))
    if missing_child:
        # Lose a real child inside the retained history, before the last 64
        # medium bars. It must still invalidate the hourly confirmation.
        medium = (*medium[:20], *medium[21:])
    save_view(
        migrated,
        BarView(sample.instrument, Timeframe.M15, at, medium, DataQuality.OK),
    )
    save_view(
        migrated,
        BarView(sample.instrument, Timeframe.M5, at, sample.setup_bars, DataQuality.OK),
    )
    feed = feed_for(sample)
    feed["as_of"] = at.isoformat()
    # This test moves the retained history window by a day. Supply independent
    # current microstructure so its intended hourly-gap assertion is reachable.
    delay = at - sample.as_of
    for data in [
        feed["markets"][0],
        *(feed["deep"][0][kind] for kind in ("book", "flow", "previous_flow")),
    ]:
        for name in (
            "source_at",
            "observed_at",
            "as_of",
            "captured_at",
            "source_start",
            "source_end",
        ):
            if name in data:
                data[name] = (datetime.fromisoformat(data[name]) + delay).isoformat()
    diagnostics = {}
    rows = alpha_inputs(migrated, features, feed, diagnostics=diagnostics)
    if missing_child:
        assert rows == ()
        assert diagnostics["input_rejections"] == [
            {"symbol": "ETHUSDT", "reason": "ALPHA_INPUT_INVALID"}
        ]
        return
    assert len(rows) == 1
    assert rows[0].quality is DataQuality.OK
    assert len(rows[0].higher_bars) >= 8
    assert rows[0].higher_bars[-1].close_at == at.replace(minute=0) - timedelta(milliseconds=1)
    assert all(bar.close_at <= at for bar in rows[0].higher_bars)


@pytest.mark.parametrize("side", [Side.LONG, Side.SHORT])
@pytest.mark.parametrize("family", ["MOMENTUM", "REVERSAL"])
def test_hourly_window_reaches_durable_armed_candidate_with_current_evidence(
    migrated, monkeypatch, side, family
):
    from decimal import Decimal

    from apps.quant_core import runtime
    from tests.intelligence_fixtures import engine, frame, observation
    from tradeforge.domain.market import RegimeLabel
    from tradeforge.intelligence.runtime import QuantWorker
    from tradeforge.runtime.analysis import AutonomousAnalysis

    detector, previous = engine(), None
    sign = 1 if side is Side.LONG else -1
    medium = [observation(i, "ETHUSDT", sign * 0.004).current for i in range(-94, 1)]
    for index in range(1, 34):
        btc = 0.001 + (index % 7) * 0.0002
        previous = frame(
            index,
            moves=tuple(sign * value for value in (btc, btc + 0.003, btc + 0.001)),
            previous=previous,
        )
        features = detector.update(previous)
        medium.append(
            next(r.current for r in previous.observations if r.instrument.symbol == "ETHUSDT")
        )
    sample = evidence(side) if family == "MOMENTUM" else reversal_sample(side)
    if family == "REVERSAL":
        medium = [
            change(bar, open=Decimal(100), high=Decimal(101), low=Decimal(99), close=Decimal(100))
            for bar in medium
        ]
        observations = tuple(
            change(row, previous=medium[-2], current=medium[-1])
            if row.instrument == sample.instrument
            else row
            for row in features.frame.observations
        )
        features = with_frame(features, change(features.frame, observations=observations))
        rows = tuple(
            change(row, direction=0, efficiency=0.0) if row.instrument == sample.instrument else row
            for row in features.features
        )
        features = change(
            features, features=rows, state=change(features.state, label=RegimeLabel.RANGE)
        )
    at = features.frame.as_of
    delay = at - sample.as_of
    scale = medium[-1].close / sample.setup_bars[-1].close
    setup = tuple(
        change(
            bar,
            **{
                name: getattr(bar, name) + delay
                for name in ("open_at", "close_at", "source_at", "observed_at", "as_of")
            },
            **{name: getattr(bar, name) * scale for name in ("open", "high", "low", "close")},
        )
        for bar in sample.setup_bars
    )
    market = change(
        sample.market,
        source_at=at,
        observed_at=at,
        as_of=at,
        **{
            name: getattr(sample.market, name) * scale
            for name in ("best_bid", "best_ask", "mark_price", "index_price")
        },
    )
    book = change(
        sample.book,
        source_at=at,
        observed_at=at,
        as_of=at,
        captured_at=at,
        bids=((market.best_bid, Decimal("1000")),),
        asks=((market.best_ask, Decimal("1000")),),
    )
    flow = change(
        sample.flow,
        **{
            name: getattr(sample.flow, name) + delay
            for name in ("source_at", "observed_at", "as_of", "source_start", "source_end")
        },
    )
    feed = feed_for(change(sample, market=market, book=book, flow=flow, as_of=at))
    if family == "REVERSAL":
        flow_times = ("source_at", "observed_at", "as_of", "source_start", "source_end")
        old = change(
            sample.previous_flow,
            **{name: getattr(sample.previous_flow, name) + delay for name in flow_times},
        )
        feed["deep"][0]["previous_flow"] = old.model_dump(mode="json")
    for timeframe, bars in ((Timeframe.M15, tuple(medium)), (Timeframe.M5, setup)):
        save_view(
            migrated,
            BarView(sample.instrument, timeframe, at, bars, DataQuality.OK),
        )
    worker = QuantWorker(migrated, clock=lambda: at)
    worker.detector.latest = features
    worker.state = worker._state("ANALYZED", True, 1)
    analysis = AutonomousAnalysis(migrated, clock=lambda: at)

    async def handoff(*args, **kwargs):
        return feed

    monkeypatch.setattr(runtime, "fetch_memory", handoff)
    asyncio.run(runtime.run_cycle(worker, analysis, object(), None, refresh_features=False))
    assert analysis.state["candidate_count"] == 1
    assert any(
        r["state"] == "ARMED" and r["strategy"] == family and r["side"] == side.value
        for r in analysis.state["symbol_results"]
    )
    with migrated.transaction() as connection:
        assert connection.scalar(text("SELECT count(*) FROM decision_lifecycle")) == 3
        assert connection.scalar(text("SELECT count(*) FROM execution_intents")) == 0
