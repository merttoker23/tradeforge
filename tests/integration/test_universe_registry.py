from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from sqlalchemy import text

from tests.universe_fixtures import NOW, payload, symbol
from tradeforge.market.instrument_registry import InstrumentRegistry
from tradeforge.persistence.db import Database


def test_point_in_time_listing_delisting_and_metadata_survive_restart(migrated):
    registry = InstrumentRegistry(migrated)
    old = registry.record(payload(symbol(), symbol("OLDUSDT")), NOW, timedelta(hours=1), '"v1"')
    later = NOW + timedelta(minutes=15)
    changed = symbol(filters=symbol()["filters"])
    changed["filters"][0]["tickSize"] = "0.020"
    new = registry.record(
        payload(changed, symbol("NEWUSDT"), symbol("OLDUSDT", status="SETTLING")),
        later,
        timedelta(hours=1),
    )
    registry = InstrumentRegistry(migrated)
    assert registry.view(NOW - timedelta(microseconds=1)).observation is None
    before = registry.view(later - timedelta(microseconds=1))
    assert before.ready and before.observation.version_hash == old.version_hash
    assert [k.symbol for k in before.observation.universe.eligible] == ["BTCUSDT", "OLDUSDT"]
    after = registry.view(later)
    assert after.ready and after.observation.version_hash == new.version_hash
    assert [k.symbol for k in after.observation.universe.eligible] == ["BTCUSDT", "NEWUSDT"]
    assert str(before.observation.metadata.instruments[0].filters.tick_size) == "0.010"
    assert str(after.observation.metadata.instruments[0].filters.tick_size) == "0.020"
    assert after.total == 3 and after.eligible == 2 and after.excluded == 1
    assert after.coverage == 1.0
    assert not registry.view(later + timedelta(hours=1)).ready
    with migrated.transaction() as connection:
        assert connection.scalar(text("SELECT count(*) FROM instruments")) == 3


def test_failure_observation_blocks_restart_without_destroying_history(migrated):
    registry = InstrumentRegistry(migrated)
    registry.record(payload(), NOW, timedelta(hours=1))
    failed_at = NOW + timedelta(minutes=1)
    registry.record_failure(failed_at, "UNIVERSE_INVALID_METADATA")
    view = InstrumentRegistry(migrated).view(failed_at)
    assert not view.ready and view.reason == "UNIVERSE_INVALID_METADATA"
    assert registry.view(NOW).ready
    registry.record(
        payload(symbol("NEWUSDT")), failed_at + timedelta(seconds=1), timedelta(hours=1)
    )
    assert registry.view(failed_at + timedelta(seconds=1)).ready


def test_identical_observation_replay_is_idempotent_and_conflicts_reject(migrated):
    registry = InstrumentRegistry(migrated)
    first = registry.record(payload(), NOW, timedelta(hours=1))
    assert registry.record(payload(), NOW, timedelta(hours=1)) == first
    with pytest.raises(ValueError):
        registry.record(payload(symbol("OTHERUSDT")), NOW, timedelta(hours=1))
    with pytest.raises(ValueError):
        registry.record(payload(), NOW - timedelta(seconds=1), timedelta(hours=1))
    with migrated.transaction() as connection:
        assert connection.scalar(text("SELECT count(*) FROM universe_snapshots")) == 1


def test_concurrent_identical_writers_commit_one_complete_observation(migrated):
    def write(_):
        return InstrumentRegistry(migrated).record(payload(), NOW, timedelta(hours=1)).version_hash

    with ThreadPoolExecutor(max_workers=2) as pool:
        values = list(pool.map(write, range(2)))
    assert values[0] == values[1]
    with migrated.transaction() as connection:
        assert connection.scalar(text("SELECT count(*) FROM universe_snapshots")) == 1


def test_observation_and_instrument_insert_rollback_together(migrated):
    with migrated.transaction() as connection:
        connection.execute(
            text("""CREATE FUNCTION reject_universe() RETURNS trigger LANGUAGE plpgsql
          AS $$ BEGIN RAISE EXCEPTION 'fixture fault'; END $$;
          CREATE TRIGGER fixture_reject BEFORE INSERT ON universe_snapshots
          FOR EACH ROW EXECUTE FUNCTION reject_universe()""")
        )
    registry = InstrumentRegistry(migrated)
    with pytest.raises(RuntimeError):
        registry.record(payload(), NOW, timedelta(hours=1))
    with migrated.transaction() as connection:
        assert connection.scalar(text("SELECT count(*) FROM instruments")) == 0
        assert connection.scalar(text("SELECT count(*) FROM universe_snapshots")) == 0
    assert not registry.view(NOW).ready


def test_unavailable_database_is_fail_closed_and_redacted():
    db = Database("postgresql+psycopg://postgres:secret_not_logged@127.0.0.1:1/unavailable")
    try:
        view = InstrumentRegistry(db).view(NOW)
        assert not view.ready and view.reason == "DB_UNAVAILABLE"
        assert "secret" not in str(view)
    finally:
        db.close()


def test_contract_delivery_closes_gate_even_before_metadata_ttl(migrated):
    delivery = NOW + timedelta(seconds=10)
    registry = InstrumentRegistry(migrated)
    registry.record(
        payload(symbol(deliveryDate=int(delivery.timestamp() * 1000))), NOW, timedelta(hours=1)
    )
    assert registry.view(NOW + timedelta(seconds=9)).ready
    assert not registry.view(delivery).ready


@pytest.mark.parametrize("ttl", [timedelta(0), timedelta(days=2)])
def test_invalid_ttl_never_persists_metadata(migrated, ttl):
    with pytest.raises(ValueError):
        InstrumentRegistry(migrated).record(payload(), NOW, ttl)
