from datetime import timedelta

import pytest

from tests.market_fixtures import START, bar, instrument
from tradeforge.domain.common import DataQuality
from tradeforge.domain.market import Timeframe
from tradeforge.market.bars import resample
from tradeforge.market.market_state import MarketState
from tradeforge.market.rolling_window import BarWindow


def test_timeframe_collision_and_symbol_freshness():
    state = MarketState(capacity=64)
    one, fifteen = bar(timeframe=Timeframe.M1), bar()
    state.ingest_bar(one)
    state.ingest_bar(fifteen)
    at = fifteen.observed_at
    assert state.view(instrument(), Timeframe.M1, at).bars == (one,)
    assert state.view(instrument(), Timeframe.M15, at).bars == (fifteen,)
    assert state.view(instrument(), Timeframe.M1, at).quality is DataQuality.STALE
    assert state.view(instrument(), Timeframe.M15, at).quality is DataQuality.OK
    assert state.view(instrument("ETHUSDT"), Timeframe.M15, at).quality is DataQuality.MISSING


def test_revisions_dedup_and_no_future_data_leak():
    window = BarWindow(8)
    original = bar()
    revision = bar(delay=300, close="105")
    assert window.add(original)
    assert not window.add(original)
    assert window.add(revision)
    # Latest revision wins in bounded memory; a past frame is never rewritten
    # with a revision that was not observed at that time.
    assert window.at(original.observed_at) == ()
    assert window.at(revision.observed_at) == (revision,)
    assert window.at(original.observed_at - timedelta(milliseconds=1)) == ()
    with pytest.raises(ValueError, match="CONFLICT"):
        window.add(bar(delay=300, close="106"))


def test_out_of_order_and_bounded_retention_fail_closed():
    window = BarWindow(2)
    for i in (2, 0, 1):
        window.add(bar(i))
    assert len(window) == 2
    with pytest.raises(ValueError, match="RETAINED"):
        window.at(bar(0).observed_at)
    assert tuple(b.open_at for b in window.at(bar(2).observed_at)) == (
        bar(1).open_at,
        bar(2).open_at,
    )


@pytest.mark.parametrize(
    "target,count", [(Timeframe.H1, 4), (Timeframe.H4, 16), (Timeframe.H12, 48)]
)
def test_complete_resample_only_and_availability(target, count):
    children = tuple(bar(i) for i in range(count))
    at = children[-1].observed_at
    result = resample(children, target, START, at)
    assert result is not None and result.volume == count * 2
    assert result.observed_at == at
    assert resample(children[:-1], target, START, at) is None
    assert resample(children, target, START, at - timedelta(seconds=1)) is None
    assert resample((*children, bar(timeframe=Timeframe.M1)), target, START, at) is None


def test_snapshot_hash_and_replay_determinism_and_gap():
    left, right = MarketState(64), MarketState(64)
    events = [bar(0), bar(2), bar(0, delay=2000, close="105")]
    for event in events:
        left.ingest_bar(event)
    for event in reversed(events):
        right.ingest_bar(event)
    at = max(b.observed_at for b in events)
    a, b = left.view(instrument(), Timeframe.M15, at), right.view(instrument(), Timeframe.M15, at)
    assert a.digest == b.digest
    assert a.quality is DataQuality.GAP
    assert a.bars[0].close == 105


def test_resampled_revision_updates_latest_and_never_leaks_backwards():
    state = MarketState(64)
    children = [bar(i) for i in range(4)]
    for child in children:
        state.ingest_bar(child)
    at = children[-1].observed_at
    before = state.view(instrument(), Timeframe.H1, at)
    revised = bar(3, delay=4000, close="109")
    state.ingest_bar(revised)
    after = state.view(instrument(), Timeframe.H1, revised.observed_at)
    assert after.bars[0].close == 109
    assert before.bars[0].source.startswith("resample:")
    # The earlier frame cannot be reconstructed from a revision observed later.
    assert state.view(instrument(), Timeframe.H1, at).bars == ()


def test_retention_edge_does_not_poison_complete_recent_resample():
    state = MarketState(7)
    for n in range(8):
        state.ingest_bar(bar(n))
    view = state.view(instrument(), Timeframe.H1, bar(7).observed_at)
    assert len(view.bars) == 1
    assert view.quality is DataQuality.OK
