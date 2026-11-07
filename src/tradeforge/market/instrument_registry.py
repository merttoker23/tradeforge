"""Append-only point-in-time metadata, with durable refresh failures and a TTL gate."""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import Column, DateTime, MetaData, String, Table, Uuid, select, text
from sqlalchemy.dialects.postgresql import JSONB, insert

from tradeforge.domain.common import Digest, StrictModel, Timestamp, utc
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.domain.market import UniverseSnapshot
from tradeforge.market.exchange_info import FILTERING_RULES, ParsedUniverse, parse_exchange_info
from tradeforge.persistence.db import Database, PersistenceError
from tradeforge.persistence.repositories import add_instrument

universe_table = Table(
    "universe_snapshots",
    MetaData(),
    Column("snapshot_id", Uuid, primary_key=True),
    Column("version_hash", String),
    Column("as_of", DateTime(timezone=True)),
    Column("payload", JSONB),
)


class UniverseObservation(StrictModel):
    version_hash: Digest
    observed_at: Timestamp
    expires_at: Timestamp
    universe: UniverseSnapshot | None
    metadata: ParsedUniverse | None
    raw_json: str | None
    etag: str | None
    reason: str | None


@dataclass(frozen=True)
class RegistryView:
    observation: UniverseObservation | None
    ready: bool
    reason: str

    @property
    def total(self) -> int:
        meta = self.observation.metadata if self.observation else None
        return meta.total if meta else 0

    @property
    def eligible(self) -> int:
        meta = self.observation.metadata if self.observation else None
        return len(meta.instruments) if meta else 0

    @property
    def excluded(self) -> int:
        return self.total - self.eligible

    @property
    def coverage(self) -> float:
        """Discovery coverage, not streaming coverage (which belongs to PHASE_04)."""
        return 1.0 if self.total and self.ready else 0.0


class InstrumentRegistry:
    def __init__(self, db: Database) -> None:
        self.db = db
        self.storage_failed = False

    def record(
        self,
        payload: object,
        observed_at: datetime,
        ttl: timedelta,
        etag: str | None = None,
    ) -> UniverseObservation:
        utc(observed_at)
        if not timedelta(0) < ttl <= timedelta(days=1):
            raise ValueError("INVALID_METADATA_TTL")
        metadata = parse_exchange_info(payload, observed_at)
        expiry = min(
            [observed_at + ttl]
            + [i.delist_at for i in metadata.instruments if i.delist_at is not None]
        )
        return self._record(
            observed_at,
            expiry,
            metadata,
            json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False),
            etag,
            None if metadata.instruments else "UNIVERSE_NO_ELIGIBLE_CONTRACTS",
        )

    def record_failure(
        self,
        observed_at: datetime,
        reason: str,
        retry_at: datetime | None = None,
    ) -> UniverseObservation:
        if reason not in {
            "UNIVERSE_NETWORK_ERROR",
            "UNIVERSE_RATE_LIMITED",
            "UNIVERSE_HTTP_UNAVAILABLE",
            "UNIVERSE_HTTP_REJECTED",
            "UNIVERSE_RESPONSE_TOO_LARGE",
            "UNIVERSE_INVALID_JSON",
            "UNIVERSE_INVALID_METADATA",
            "UNIVERSE_NOT_MODIFIED_WITHOUT_CACHE",
        }:
            raise ValueError("INVALID_UNIVERSE_FAILURE_REASON")
        expiry = utc(retry_at) if retry_at is not None else observed_at
        if expiry < observed_at:
            raise ValueError("INVALID_RETRY_TIME")
        return self._record(utc(observed_at), expiry, None, None, None, reason)

    def _record(
        self,
        observed_at: datetime,
        expires_at: datetime,
        metadata: ParsedUniverse | None,
        raw_json: str | None,
        etag: str | None,
        reason: str | None,
    ) -> UniverseObservation:
        content = {
            "observed_at": observed_at.isoformat(),
            "expires_at": expires_at.isoformat(),
            "metadata": metadata.model_dump(mode="json") if metadata else None,
            "raw_json": raw_json,
            "etag": etag,
            "reason": reason,
            "filtering_rules": FILTERING_RULES,
        }
        digest = hashlib.sha256(
            json.dumps(content, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest()
        identifier = uuid5(NAMESPACE_URL, "tradeforge/universe/" + digest)
        universe = None
        if metadata is not None:
            keys = tuple(
                InstrumentKey(
                    symbol=i.symbol,
                    exchange="BINANCE",
                    market_type="USD_M",
                    quote="USDT",
                    margin_asset="USDT",
                    contract_type="PERPETUAL",
                )
                for i in metadata.instruments
            )
            universe = UniverseSnapshot.model_validate(
                {
                    "schema_version": 1,
                    "event_id": identifier,
                    "source_at": observed_at,
                    "observed_at": observed_at,
                    "as_of": observed_at,
                    "eligible": keys,
                    "watched": (),
                    "deep": (),
                    "filtering_rules": FILTERING_RULES,
                    "version_hash": digest,
                },
                context={"now": observed_at},
            )
        observation = UniverseObservation(
            version_hash=digest,
            observed_at=observed_at,
            expires_at=expires_at,
            universe=universe,
            metadata=metadata,
            raw_json=raw_json,
            etag=etag,
            reason=reason,
        )
        try:
            with self.db.transaction() as connection:
                # All discovery writers share this lock, including failure observations.
                connection.execute(text("SELECT pg_advisory_xact_lock(7349003)"))
                last = connection.execute(
                    select(universe_table.c.as_of, universe_table.c.version_hash)
                    .order_by(universe_table.c.as_of.desc())
                    .limit(1)
                ).first()
                if last and (
                    observed_at < last.as_of
                    or (observed_at == last.as_of and digest != last.version_hash)
                ):
                    raise ValueError("UNIVERSE_OBSERVATION_CONFLICT")
                if metadata:
                    for instrument in metadata.instruments:
                        add_instrument(connection, instrument)
                connection.execute(
                    insert(universe_table)
                    .values(
                        snapshot_id=identifier,
                        version_hash=digest,
                        as_of=observed_at,
                        payload=observation.model_dump(mode="json"),
                    )
                    .on_conflict_do_nothing(index_elements=[universe_table.c.version_hash])
                )
        except PersistenceError:
            self.storage_failed = True
            raise
        self.storage_failed = False
        return observation

    def view(self, as_of: datetime) -> RegistryView:
        utc(as_of)
        if self.storage_failed:
            return RegistryView(None, False, "DB_UNAVAILABLE")
        try:
            with self.db.transaction() as connection:
                data = connection.scalar(
                    select(universe_table.c.payload)
                    .where(universe_table.c.as_of <= as_of)
                    .order_by(universe_table.c.as_of.desc())
                    .limit(1)
                )
            if data is None:
                return RegistryView(None, False, "UNIVERSE_MISSING")
            observation = UniverseObservation.model_validate_json(
                json.dumps(data), context={"now": as_of}
            )
        except PersistenceError:
            return RegistryView(None, False, "DB_UNAVAILABLE")
        except ValueError:
            return RegistryView(None, False, "UNIVERSE_INVALID_METADATA")
        if observation.reason:
            return RegistryView(observation, False, observation.reason)
        if as_of >= observation.expires_at:
            return RegistryView(observation, False, "UNIVERSE_STALE")
        return RegistryView(observation, True, "UNIVERSE_READY")
