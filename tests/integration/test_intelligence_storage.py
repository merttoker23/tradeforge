import pytest
from sqlalchemy import text

from tests.intelligence_fixtures import engine, frame
from tradeforge.persistence.db import PersistenceError


def test_global_snapshot_roundtrip_append_only_and_point_in_time(migrated):
    from tradeforge.intelligence.storage import load_snapshot, save_snapshot

    detector = engine()
    first = detector.update(frame(1))
    second = detector.update(frame(2))
    save_snapshot(migrated, first)
    save_snapshot(migrated, first)
    save_snapshot(migrated, second)
    assert load_snapshot(migrated, first.input_version, first.frame.as_of) == first
    assert load_snapshot(migrated, second.input_version, first.frame.as_of) is None
    assert load_snapshot(migrated, second.input_version, second.frame.as_of) == second
    with migrated.transaction() as connection:
        assert connection.scalar(text("SELECT count(*) FROM regime_snapshots")) == 2
        assert connection.scalar(text("SELECT count(*) FROM execution_intents")) == 0
        payload = connection.scalar(
            text("SELECT payload FROM regime_snapshots WHERE input_version=:v"),
            {"v": first.input_version},
        )
        assert payload["policy_hash"] == first.policy_hash
        assert payload["frame"]["universe"]["version_hash"] == first.frame.universe.version_hash
        assert payload["reason_codes"] == list(first.reason_codes)
    for sql in (
        "DELETE FROM regime_snapshots",
        "TRUNCATE regime_snapshots",
        "UPDATE regime_snapshots SET payload='{}'",
    ):
        with pytest.raises(PersistenceError):
            with migrated.transaction() as connection:
                connection.execute(text(sql))


def test_conflicting_snapshot_is_rejected_without_overwrite(migrated):
    from tradeforge.intelligence.models import FeatureSnapshot
    from tradeforge.intelligence.storage import load_snapshot, save_snapshot

    original = engine().update(frame(1))
    save_snapshot(migrated, original)
    tampered = FeatureSnapshot.model_validate(
        {**original.model_dump(), "reason_codes": ("OTHER_REASON",)}
    )
    with pytest.raises(ValueError, match="CONFLICTING"):
        save_snapshot(migrated, tampered)
    assert load_snapshot(migrated, original.input_version, original.frame.as_of) == original


def test_database_failure_is_fail_closed():
    from tradeforge.intelligence.storage import save_snapshot
    from tradeforge.persistence.db import Database

    db = Database("postgresql+psycopg://postgres@127.0.0.1:1/unavailable")
    try:
        with pytest.raises(PersistenceError):
            save_snapshot(db, engine().update(frame(1)))
        assert not db.probe().db_writable
    finally:
        db.close()
