"""Periodic metadata refresh. This gate never grants account or order authority."""

import json
import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from threading import Event

from tradeforge.domain.common import utc
from tradeforge.market.exchange_info import ExchangeInfoClient, FetchError, decode_payload
from tradeforge.market.instrument_registry import InstrumentRegistry, RegistryView
from tradeforge.persistence.db import PersistenceError

logger = logging.getLogger(__name__)


class UniverseRefresher:
    def __init__(
        self,
        registry: InstrumentRegistry,
        client: ExchangeInfoClient,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        interval: timedelta = timedelta(minutes=15),
        ttl: timedelta = timedelta(hours=1),
    ) -> None:
        if not timedelta(0) < interval <= timedelta(days=1):
            raise ValueError("INVALID_REFRESH_INTERVAL")
        if not timedelta(0) < ttl <= timedelta(days=1):
            raise ValueError("INVALID_METADATA_TTL")
        self.registry, self.client, self.clock = registry, client, clock
        self.interval, self.ttl = interval, ttl
        self.next_refresh: datetime | None = None

    def tick(self) -> RegistryView:
        now = utc(self.clock())
        previous = self.registry.view(now)
        observation = previous.observation
        if (
            observation is not None
            and observation.reason == "UNIVERSE_RATE_LIMITED"
            and now < observation.expires_at
        ):
            self.next_refresh = observation.expires_at
            return previous
        # Every fresh worker revalidates before reporting current readiness. A prior
        # failed DB write cannot always leave a durable failure observation.
        if self.next_refresh is not None and now < self.next_refresh:
            # Stale metadata must be refreshed immediately; failures use bounded polling.
            if previous.reason != "UNIVERSE_STALE":
                return previous
        # Stored metadata may belong to the previous Binance environment. The
        # first fetch in each newly started worker must not send that environment's
        # ETag; a cross-environment HTTP 304 would otherwise admit its old universe.
        # Only this worker's own successfully refreshed metadata can be conditional.
        etag = (
            observation.etag
            if self.next_refresh is not None and observation and observation.reason is None
            else None
        )
        reason = None
        try:
            reply = self.client.fetch(etag)
            if reply.status == 304:
                if observation is None or observation.raw_json is None:
                    raise FetchError("UNIVERSE_NOT_MODIFIED_WITHOUT_CACHE")
                raw = json.loads(observation.raw_json)
            else:
                raw = decode_payload(reply.body)
            headers = {k.lower(): v for k, v in reply.headers.items()}
            validator = headers.get("etag", etag if reply.status == 304 else None)
            if validator is not None and (
                len(validator) > 256 or any(ord(c) < 32 or ord(c) > 126 for c in validator)
            ):
                validator = None
            saved = self.registry.record(raw, utc(self.clock()), self.ttl, validator)
            if saved.metadata:
                self.client.set_weight_limit(saved.metadata.weight_limit)
            self.next_refresh = min(saved.observed_at + self.interval, saved.expires_at)
        except FetchError as error:
            reason = str(error)
        except ValueError:
            reason = "UNIVERSE_INVALID_METADATA"
        except PersistenceError:
            logger.error("DB_UNAVAILABLE: universe refresh halted")
            return RegistryView(None, False, "DB_UNAVAILABLE")
        if reason:
            logger.error("%s: universe refresh failed; new entries blocked", reason)
            failed_at = utc(self.clock())
            retry_at = None
            if reason == "UNIVERSE_RATE_LIMITED":
                delay = self.client.cooldown()
                available = (datetime.max.replace(tzinfo=UTC) - failed_at).total_seconds()
                retry_at = (
                    datetime.max.replace(tzinfo=UTC)
                    if delay >= available
                    else failed_at + timedelta(seconds=delay)
                )
            self.next_refresh = max(failed_at + timedelta(seconds=30), retry_at or failed_at)
            try:
                self.registry.record_failure(failed_at, reason, retry_at)
            except PersistenceError:
                return RegistryView(None, False, "DB_UNAVAILABLE")
            except ValueError:
                # A concurrent newer observation won. Do not overwrite it with older data.
                return RegistryView(None, False, "UNIVERSE_INVALID_METADATA")
        return self.registry.view(utc(self.clock()))

    def run(self, stop: Event) -> None:
        """Run on the gateway metadata worker; blocking I/O stays off market ingestion."""
        while not stop.is_set():
            self.tick()
            stop.wait(1.0)
