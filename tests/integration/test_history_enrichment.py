"""Full history arriving after a cold first analysis must repair that same period."""

from datetime import timedelta

from tests.intelligence_fixtures import frame
from tests.universe_fixtures import payload, symbol
from tradeforge.intelligence.runtime import QuantWorker
from tradeforge.market.instrument_registry import InstrumentRegistry
from tradeforge.market.market_state import BarView
from tradeforge.market.storage import save_view


def test_delayed_history_enriches_same_closed_period_once(migrated):
    frames = []
    for i in range(1, 13):
        move = 0.001 * (1 + i % 4)
        frames.append(
            frame(i, moves=(move, move * 1.5, -move * 0.5), previous=frames[-1] if frames else None)
        )
    latest = frames[-1]
    now = latest.as_of
    InstrumentRegistry(migrated).record(
        payload(*(symbol(k.symbol) for k in latest.universe.eligible)), now, timedelta(hours=1)
    )
    for row in latest.observations:
        save_view(
            migrated,
            BarView(
                row.instrument, latest.timeframe, now, (row.previous, row.current), row.quality
            ),
        )
    worker = QuantWorker(migrated, clock=lambda: now)
    assert worker.tick()["processed"] == 1
    assert all(f.sample_count == 1 for f in worker.detector.latest.features)
    for symbol_index, row in enumerate(latest.observations):
        bars = [frames[0].observations[symbol_index].previous]
        bars.extend(f.observations[symbol_index].current for f in frames)
        save_view(
            migrated, BarView(row.instrument, latest.timeframe, now, tuple(bars), row.quality)
        )
    now += timedelta(seconds=1)
    state = worker.tick()
    assert state["processed"] == 1
    assert all(f.sample_count == 12 for f in worker.detector.latest.features)
    assert all(f.relative_return is not None for f in worker.detector.latest.features)
    assert worker.tick()["processed"] == 0
    assert QuantWorker(migrated, clock=lambda: now).tick()["processed"] == 0


def test_stale_history_never_generates_repeated_analysis_snapshots(migrated):
    from sqlalchemy import text

    latest = frame(1)
    now = latest.as_of + timedelta(hours=2)
    InstrumentRegistry(migrated).record(
        payload(*(symbol(k.symbol) for k in latest.universe.eligible)), now, timedelta(hours=1)
    )
    for row in latest.observations:
        save_view(
            migrated,
            BarView(
                row.instrument, latest.timeframe, now, (row.previous, row.current), row.quality
            ),
        )
    worker = QuantWorker(migrated, clock=lambda: now)
    assert worker.tick()["processed"] == 0
    now += timedelta(seconds=1)
    assert worker.tick()["processed"] == 0
    with migrated.transaction() as c:
        assert c.scalar(text("SELECT count(*) FROM regime_snapshots")) == 0
