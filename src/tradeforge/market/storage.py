"""Explicit selected market persistence; call on a worker, never the ingest loop."""

from datetime import datetime

from sqlalchemy import text

from tradeforge.domain.common import utc
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.domain.market import ClosedBar, Timeframe
from tradeforge.market.bars import bar_content_hash, bar_hash, canonical
from tradeforge.market.market_state import BarView
from tradeforge.persistence.db import Database
from tradeforge.persistence.repositories import add_instrument, instrument_id

# A semantic content hash removes observation time and transport identity from
# revision identity. Re-observing identical OHLCV is not a new revision.
STORED_CONTENT = text("""
    SELECT r.content_hash
    FROM jsonb_to_recordset(CAST(:rows AS jsonb)) AS r(
      timeframe text,open_at timestamptz,content_hash text)
    WHERE EXISTS (
      SELECT 1 FROM closed_bar_observations o
      WHERE o.instrument_id=:id AND o.timeframe=r.timeframe
        AND o.open_at=r.open_at AND o.content_hash=r.content_hash
    )
""")


def save_view(db: Database, view: BarView) -> None:
    """Persist changed bar revisions atomically; identical re-observations are free."""
    utc(view.as_of)
    with db.transaction() as connection:
        identifier = add_instrument(connection, view.instrument)
        bars = tuple(dict.fromkeys((*view.evidence, *view.bars)))
        hashed = [(bar, bar_hash(bar), bar_content_hash(bar)) for bar in bars]
        if any(bar.as_of > view.as_of or bar.key[:-1] != view.instrument.key for bar in bars):
            raise ValueError("SNAPSHOT_INPUT_MISMATCH")
        for offset in range(0, len(hashed), 512):
            batch = hashed[offset : offset + 512]
            keys = [
                {
                    "timeframe": bar.timeframe.value,
                    "open_at": bar.open_at.isoformat(),
                    "content_hash": content,
                }
                for bar, _, content in batch
            ]
            params = {"id": identifier, "rows": canonical(keys)}
            stored = set(connection.scalars(STORED_CONTENT, params))
            pending = [
                {
                    **bar.model_dump(mode="json"),
                    "payload": canonical(bar.model_dump(mode="json")),
                    "input_hash": digest,
                    "content_hash": content,
                }
                for bar, digest, content in batch
                if content not in stored
            ]
            if not pending:
                continue
            # Bounded recordsets preserve the append-only revision checks without
            # three network round trips per candle on a throttled database.
            params = {"id": identifier, "rows": canonical(pending)}
            connection.execute(
                text("""
                INSERT INTO closed_bar_observations
                  (instrument_id,timeframe,open_at,close_at,source_at,observed_at,
                   payload,input_hash,content_hash)
                SELECT :id,timeframe,open_at,close_at,source_at,observed_at,
                       payload,input_hash,content_hash
                FROM jsonb_to_recordset(CAST(:rows AS jsonb)) AS r(
                  timeframe text,open_at timestamptz,close_at timestamptz,
                  source_at timestamptz,observed_at timestamptz,payload text,
                  input_hash text,content_hash text)
                ON CONFLICT DO NOTHING
            """),
                params,
            )
            confirmed = set(connection.scalars(STORED_CONTENT, params))
            if confirmed != {row["content_hash"] for row in pending}:
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
                ON CONFLICT (instrument_id,timeframe,open_at) DO UPDATE SET
                  close_at=EXCLUDED.close_at,observed_at=EXCLUDED.observed_at,
                  open=EXCLUDED.open,high=EXCLUDED.high,low=EXCLUDED.low,
                  close=EXCLUDED.close,volume=EXCLUDED.volume,complete=EXCLUDED.complete
                WHERE EXCLUDED.observed_at > closed_bars.observed_at
            """),
                params,
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
