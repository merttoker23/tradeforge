from datetime import timedelta

import pytest
from sqlalchemy import text

from tests.market_fixtures import bar, instrument
from tradeforge.domain.market import Timeframe
from tradeforge.market.market_state import MarketState
from tradeforge.market.storage import load_bars, save_view
from tradeforge.persistence.db import PersistenceError


def test_selected_closed_data_and_revision_provenance_round_trip(migrated):
    state = MarketState(64)
    original, revision = bar(), bar(delay=60, close="109")
    state.ingest_bar(original)
    first = state.view(instrument(), Timeframe.M15, original.observed_at)
    save_view(migrated, first)
    save_view(migrated, first)
    state.ingest_bar(revision)
    second = state.view(instrument(), Timeframe.M15, revision.observed_at)
    save_view(migrated, second)
    assert load_bars(migrated, instrument(), Timeframe.M15, original.observed_at) == (original,)
    assert load_bars(migrated, instrument(), Timeframe.M15, revision.observed_at) == (revision,)
    with migrated.transaction() as connection:
        assert connection.scalar(text("SELECT count(*) FROM closed_bar_observations")) == 2
        assert connection.scalar(text("SELECT count(*) FROM closed_bars")) == 1
        assert connection.scalar(text("SELECT count(*) FROM feature_snapshots")) == 0
        observed = connection.scalar(
            text("SELECT observed_at FROM closed_bars WHERE timeframe='15m'")
        )
        assert observed == revision.observed_at
    for sql in (
        "DELETE FROM closed_bar_observations",
        "TRUNCATE closed_bar_observations",
        "UPDATE closed_bar_observations SET payload='{}'",
    ):
        with pytest.raises(PersistenceError):
            with migrated.transaction() as connection:
                connection.execute(text(sql))


def test_h12_migration_and_conflicting_revision_is_atomic(migrated):
    state = MarketState(64)
    for n in range(48):
        state.ingest_bar(bar(n))
    at = bar(47).observed_at
    view = state.view(instrument(), Timeframe.H12, at)
    save_view(migrated, view)
    assert load_bars(migrated, instrument(), Timeframe.H12, at) == view.bars
    state = MarketState(8)
    state.ingest_bar(bar())
    save_view(migrated, state.view(instrument(), Timeframe.M15, bar().observed_at))
    other = MarketState(8)
    other.ingest_bar(bar(close="106"))
    with pytest.raises(ValueError, match="CONFLICT"):
        save_view(migrated, other.view(instrument(), Timeframe.M15, bar().observed_at))
    assert load_bars(migrated, instrument(), Timeframe.M15, at) == tuple(bar(n) for n in range(48))
    assert (
        load_bars(migrated, instrument(), Timeframe.M15, bar().observed_at - timedelta(seconds=1))
        == ()
    )


def test_worker_snapshot_keeps_latest_revision_and_derived_children(migrated):
    state = MarketState(64)
    original, revision = bar(), bar(delay=60, close="109")
    state.ingest_bar(original)
    state.ingest_bar(revision)
    save_view(migrated, state.view(instrument(), Timeframe.M15, revision.observed_at))
    # Bounded memory keeps the latest revision; the superseded revision was
    # never observed by a save worker and is not reconstructed from nothing.
    assert load_bars(migrated, instrument(), Timeframe.M15, original.observed_at) == ()
    for n in range(1, 4):
        state.ingest_bar(bar(n))
    save_view(migrated, state.view(instrument(), Timeframe.H1, bar(3).observed_at))
    retained = load_bars(migrated, instrument(), Timeframe.M15, bar(3).observed_at)
    assert len(retained) == 4
    assert retained[0] == revision
