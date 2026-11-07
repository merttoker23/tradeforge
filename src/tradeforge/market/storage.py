"""Explicit selected market persistence; call on a worker, never the ingest loop."""

from datetime import datetime
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import text

from tradeforge.domain.common import utc
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.domain.market import ClosedBar, Timeframe
from tradeforge.market.bars import bar_hash, canonical
from tradeforge.market.market_state import BarView
from tradeforge.persistence.db import Database
from tradeforge.persistence.repositories import add_instrument, instrument_id

STORED_OBSERVATIONS = text("""
    SELECT stored.input_hash
    FROM jsonb_to_recordset(CAST(:rows AS jsonb)) AS r(
      timeframe text,open_at timestamptz,observed_at timestamptz,input_hash text)
    CROSS JOIN LATERAL (
      SELECT observed_at,input_hash FROM closed_bar_observations
      WHERE instrument_id=:id AND timeframe=r.timeframe
        AND open_at=r.open_at AND observed_at<=r.observed_at
      ORDER BY observed_at DESC
      LIMIT 1
    ) stored
    WHERE stored.observed_at=r.observed_at AND stored.input_hash=r.input_hash
""")


def save_view(db: Database, view: BarView, *, persist_features: bool = True) -> None:
    """Atomic append-only observation + feature provenance, idempotent exact replay."""
    utc(view.as_of)
    with db.transaction() as connection:
        identifier = add_instrument(connection, view.instrument)
        bars = tuple(dict.fromkeys((*view.evidence, *view.bars)))
        hashed = [(bar, bar_hash(bar)) for bar in bars]
        if any(bar.as_of > view.as_of or bar.key[:-1] != view.instrument.key for bar in bars):
            raise ValueError("SNAPSHOT_INPUT_MISMATCH")
        stored_hashes: set[str] = set()
        for offset in range(0, len(hashed), 512):
            keys = [
                {
                    "timeframe": bar.timeframe.value,
                    "open_at": bar.open_at.isoformat(),
                    "observed_at": bar.observed_at.isoformat(),
                    "input_hash": digest,
                }
                for bar, digest in hashed[offset : offset + 512]
            ]
            stored_hashes.update(
                connection.scalars(STORED_OBSERVATIONS, {"id": identifier, "rows": canonical(keys)})
            )
        pending = [
            {
                **bar.model_dump(mode="json"),
                "payload": canonical(bar.model_dump(mode="json")),
                "input_hash": digest,
            }
            for bar, digest in hashed
            if digest not in stored_hashes
        ]
        # Bounded recordsets preserve the append-only revision checks without
        # three network round trips per candle on a throttled database.
        for offset in range(0, len(pending), 512):
            batch = pending[offset : offset + 512]
            params = {
                "id": identifier,
                "rows": canonical(batch),
            }
            connection.execute(
                text("""
                INSERT INTO closed_bar_observations
                  (instrument_id,timeframe,open_at,close_at,source_at,observed_at,payload,input_hash)
                SELECT :id,timeframe,open_at,close_at,source_at,observed_at,payload,input_hash
                FROM jsonb_to_recordset(CAST(:rows AS jsonb)) AS r(
                  timeframe text,open_at timestamptz,close_at timestamptz,
                  source_at timestamptz,observed_at timestamptz,payload text,input_hash text)
                ON CONFLICT DO NOTHING
            """),
                params,
            )
            confirmed = set(connection.scalars(STORED_OBSERVATIONS, params))
            if confirmed != {row["input_hash"] for row in batch}:
                raise ValueError("CONFLICTING_PERSISTED_BAR_REVISION")
            connection.execute(
                text("""
                INSERT INTO closed_bars
                  (instrument_id,timeframe,open_at,close_at,observed_at,
                   open,high,low,close,volume,complete)
                SELECT :id,timeframe,open_at,close_at,observed_at,
                       open,high,low,close,volume,true
                FROM jsonb_to_recordset(CAST(:rows AS jsonb)) AS r(
                  timeframe text,open_at timestamptz,close_at timestamptz,
                  observed_at timestamptz,open numeric,high numeric,low numeric,
                  close numeric,volume numeric)
                ORDER BY open_at,observed_at
                ON CONFLICT DO NOTHING
            """),
                params,
            )
        if not persist_features:
            return
        connection.execute(
            text("""
            INSERT INTO feature_snapshots (snapshot_id,instrument_id,feature_hash,as_of,payload)
            VALUES (:snapshot,:id,:hash,:as_of,CAST(:payload AS jsonb))
            ON CONFLICT DO NOTHING
        """),
            {
                "snapshot": uuid5(NAMESPACE_URL, view.digest),
                "id": identifier,
                "hash": view.digest,
                "as_of": view.as_of,
                "payload": canonical(view.payload),
            },
        )


def load_bars(
    db: Database, key: InstrumentKey, timeframe: Timeframe, as_of: datetime, *, limit: int = 2048
) -> tuple[ClosedBar, ...]:
    utc(as_of)
    if type(limit) is not int or not 1 <= limit <= 100_000:
        raise ValueError("INVALID_HISTORY_LIMIT")
    with db.transaction() as connection:
        rows = (
            connection.execute(
                text("""
            SELECT payload FROM (
                SELECT DISTINCT ON (open_at) open_at,payload
                FROM closed_bar_observations
                WHERE instrument_id=:id AND timeframe=:tf AND observed_at<=:at
                ORDER BY open_at DESC, observed_at DESC
                LIMIT :limit
            ) selected ORDER BY open_at
        """),
                {"id": instrument_id(key), "tf": timeframe.value, "at": as_of, "limit": limit},
            )
            .scalars()
            .all()
        )
    return tuple(ClosedBar.model_validate_json(row, context={"now": as_of}) for row in rows)
