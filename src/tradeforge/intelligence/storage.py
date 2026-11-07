"""Explicit durable worker boundary; never perform DB I/O inside regime.update."""

from datetime import datetime
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import text

from tradeforge.domain.common import utc
from tradeforge.intelligence.models import FeatureSnapshot
from tradeforge.market.bars import canonical
from tradeforge.persistence.db import Database


def save_snapshot(db: Database, snapshot: FeatureSnapshot) -> None:
    payload = snapshot.model_dump(mode="json")
    params: dict[str, object] = {
        "id": uuid5(NAMESPACE_URL, snapshot.input_version),
        "version": snapshot.input_version,
        "at": snapshot.state.as_of,
        "payload": canonical(payload),
    }
    with db.transaction() as connection:
        connection.execute(
            text("""
            INSERT INTO regime_snapshots (snapshot_id,input_version,as_of,payload)
            VALUES (:id,:version,:at,CAST(:payload AS jsonb)) ON CONFLICT DO NOTHING
        """),
            params,
        )
        stored = connection.scalar(
            text("""
            SELECT payload FROM regime_snapshots WHERE input_version=:version AND as_of=:at
        """),
            params,
        )
        if stored != payload:
            raise ValueError("CONFLICTING_REGIME_SNAPSHOT")


def load_snapshot(db: Database, input_version: str, as_of: datetime) -> FeatureSnapshot | None:
    utc(as_of)
    with db.transaction() as connection:
        payload = connection.scalar(
            text("""
            SELECT payload FROM regime_snapshots WHERE input_version=:version AND as_of<=:at
            ORDER BY as_of DESC LIMIT 1
        """),
            {"version": input_version, "at": as_of},
        )
    if payload is None:
        return None
    return FeatureSnapshot.model_validate_json(canonical(payload), context={"now": as_of})
