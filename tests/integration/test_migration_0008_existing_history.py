"""Run only against a disposable PostgreSQL test database, never production."""

from uuid import uuid4

import pytest
from sqlalchemy import text

from tradeforge.persistence.db import Database, PersistenceError
from tradeforge.persistence.transactions import migrate


def test_0007_existing_bar_backfill_keeps_evidence_immutable_after_0008(database_url):
    db = Database(database_url)
    identifier = uuid4()
    try:
        migrate(db, "0007")
        with db.transaction() as connection:
            connection.execute(
                text(
                    "INSERT INTO instruments "
                    "(instrument_id, exchange, market_type, symbol, contract_type, observed_at) "
                    "VALUES (:id, 'BINANCE', 'USD_M', 'BTCUSDT', 'PERPETUAL', now())"
                ),
                {"id": identifier},
            )
            connection.execute(
                text("""
                    INSERT INTO closed_bar_observations
                        (instrument_id, timeframe, open_at, close_at, source_at,
                         observed_at, payload, input_hash)
                    VALUES (
                        :id, '15m', '2026-10-01 12:00:00+00', '2026-10-01 12:15:00+00',
                        '2026-10-01 12:15:00+00', '2026-10-01 12:15:01+00',
                        :payload, encode(sha256(convert_to(:payload, 'UTF8')), 'hex')
                    )
                """),
                {"id": identifier, "payload": '{"fixture":true}'},
            )

        migrate(db, "head")
        with db.transaction() as connection:
            assert connection.scalar(text("SELECT version_num FROM schema_migrations")) == "0009"
            assert (
                connection.scalar(
                    text(
                        "SELECT count(*) FROM closed_bar_observations "
                        "WHERE instrument_id=:id AND content_hash=input_hash"
                    ),
                    {"id": identifier},
                )
                == 1
            )
            assert (
                connection.scalar(
                    text(
                        "SELECT tgenabled FROM pg_trigger "
                        "WHERE tgrelid='closed_bar_observations'::regclass "
                        "AND tgname='observation_immutable'"
                    )
                )
                == "O"
            )

        with pytest.raises(PersistenceError):
            with db.transaction() as connection:
                connection.execute(
                    text("UPDATE closed_bar_observations SET payload='{}' WHERE instrument_id=:id"),
                    {"id": identifier},
                )
    finally:
        db.close()
