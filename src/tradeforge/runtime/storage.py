"""Durable sampled health; heartbeats never renew source evidence."""

from datetime import datetime

from sqlalchemy import text

from tradeforge.domain.common import utc
from tradeforge.domain.market import MarketSnapshot
from tradeforge.market.bars import canonical
from tradeforge.persistence.db import Database
from tradeforge.persistence.repositories import add_instrument


def save_runtime(
    db: Database, service: str, at: datetime, deadline: datetime, payload: dict[str, object]
) -> None:
    utc(at)
    utc(deadline)
    if service not in {"market_gateway", "quant_core", "execution_guardian"}:
        raise ValueError("RUNTIME_SERVICE_INVALID")
    with db.transaction() as c:
        c.execute(
            text("""
            INSERT INTO runtime_observations(service,observed_at,expires_at,payload)
            VALUES (:service,:at,:deadline,CAST(:payload AS jsonb))
            ON CONFLICT(service) DO UPDATE SET observed_at=EXCLUDED.observed_at,
              expires_at=EXCLUDED.expires_at,payload=(
                CASE WHEN EXCLUDED.service='execution_guardian'
                  AND NOT (EXCLUDED.payload ? 'storage_ready')
                THEN jsonb_strip_nulls(jsonb_build_object(
                  'storage_ready',runtime_observations.payload->'storage_ready',
                  'storage_checked_at',runtime_observations.payload->'storage_checked_at'
                )) ELSE '{}'::jsonb END
              ) || EXCLUDED.payload
            WHERE runtime_observations.observed_at<EXCLUDED.observed_at
              OR (EXCLUDED.service='execution_guardian'
                AND runtime_observations.observed_at=EXCLUDED.observed_at
                AND EXCLUDED.payload ? 'storage_ready'
                AND EXCLUDED.payload->'storage_ready' IS DISTINCT FROM 'true'::jsonb)
        """),
            {"service": service, "at": at, "deadline": deadline, "payload": canonical(payload)},
        )


def save_markets(db: Database, markets: tuple[MarketSnapshot, ...]) -> None:
    with db.transaction() as c:
        for market in markets:
            identifier = add_instrument(c, market)
            c.execute(
                text("""
                INSERT INTO runtime_market(instrument_id,source_at,observed_at,payload)
                VALUES (:id,:source,:at,CAST(:payload AS jsonb))
                ON CONFLICT(instrument_id) DO UPDATE SET source_at=EXCLUDED.source_at,
                  observed_at=EXCLUDED.observed_at,payload=EXCLUDED.payload
                WHERE runtime_market.observed_at<EXCLUDED.observed_at
            """),
                {
                    "id": identifier,
                    "source": market.source_at,
                    "at": market.observed_at,
                    "payload": canonical(market.model_dump(mode="json")),
                },
            )
