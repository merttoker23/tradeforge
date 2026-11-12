"""Bounded PostgreSQL connections and fail-closed, credential-free diagnostics."""

import logging
from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Connection, make_url
from sqlalchemy.exc import DBAPIError, IntegrityError, SQLAlchemyError, TimeoutError

from tradeforge.domain.common import ReasonCode
from tradeforge.domain.risk import TradingReadiness

logger = logging.getLogger(__name__)
SCHEMA_VERSION = "0009"
DATABASE_FAILURES = {
    "57014": "DB_STATEMENT_TIMEOUT",
    "55P03": "DB_LOCK_TIMEOUT",
    "42501": "DB_PERMISSION_DENIED",
    "40P01": "DB_DEADLOCK",
}


class PersistenceError(RuntimeError):
    """Only safe reason codes cross the database boundary."""


class EntryHalt(PersistenceError):
    """A storage failure must halt all new entry writes; never blindly retry."""


class Database:
    def __init__(self, url: str | URL) -> None:
        try:
            parsed = make_url(url)
            if parsed.drivername != "postgresql+psycopg":
                raise ValueError
        except Exception:
            raise ValueError("POSTGRESQL_PSYCOPG_URL_REQUIRED") from None
        self.sql_engine = create_engine(
            parsed,
            hide_parameters=True,
            echo=False,
            pool_pre_ping=True,
            pool_size=4,
            max_overflow=0,
            pool_timeout=3,
            connect_args={
                "connect_timeout": 3,
                "options": "-c timezone=UTC -c statement_timeout=5000 "
                "-c lock_timeout=2000 -c idle_in_transaction_session_timeout=15000",
            },
        )
        self.readiness = TradingReadiness(reason_codes=(ReasonCode.DB_UNAVAILABLE,))
        self._pinned_environment_revision: int | None = None

    def pin_environment(self, revision: int) -> None:
        """Fence all data access to the selected exchange generation.

        FOR SHARE held for the whole transaction makes the dashboard's FOR UPDATE
        selector wait for in-flight writes before committing the new generation.
        """
        if (
            type(revision) is not int
            or revision < 1
            or self._pinned_environment_revision is not None
        ):
            raise ValueError("BINANCE_ENVIRONMENT_PIN_INVALID")
        self._pinned_environment_revision = revision

    @contextmanager
    def transaction(self) -> Iterator[Connection]:
        try:
            with self.sql_engine.begin() as connection:
                if self._pinned_environment_revision is not None:
                    revision = connection.scalar(
                        text("SELECT revision FROM binance_runtime WHERE id=1 FOR SHARE")
                    )
                    if revision != self._pinned_environment_revision:
                        raise EntryHalt("BINANCE_ENVIRONMENT_RESTART_REQUIRED")
                yield connection
        except IntegrityError:
            self._halt()
            raise PersistenceError("DB_CONSTRAINT_REJECTED") from None
        except DBAPIError as error:
            sqlstate = getattr(error.orig, "sqlstate", None)
            reason = DATABASE_FAILURES.get(
                sqlstate if isinstance(sqlstate, str) else "", "DB_UNAVAILABLE"
            )
            self._halt(reason)
            raise EntryHalt(reason) from None
        except TimeoutError:
            self._halt("DB_POOL_TIMEOUT")
            raise EntryHalt("DB_POOL_TIMEOUT") from None
        except SQLAlchemyError:
            self._halt()
            raise EntryHalt("DB_UNAVAILABLE") from None

    def _halt(self, reason: str = "DB_UNAVAILABLE") -> None:
        self.readiness = TradingReadiness(reason_codes=(ReasonCode.DB_UNAVAILABLE,))
        logger.error("%s: new entries halted", reason)

    def probe(self) -> TradingReadiness:
        """Verify version, critical read paths and an actual write, then roll it back."""
        try:
            with self.transaction() as connection:
                if (
                    connection.scalar(text("SELECT version_num FROM schema_migrations"))
                    != SCHEMA_VERSION
                ):
                    raise EntryHalt("DB_SCHEMA_MISMATCH")
                connection.execute(text("SELECT intent_id FROM execution_intents LIMIT 0"))
                connection.execute(text("SELECT message_id FROM outbox LIMIT 0"))
                connection.execute(text("SELECT position_id FROM positions LIMIT 0"))
                savepoint = connection.begin_nested()
                connection.execute(
                    text("INSERT INTO audit_log (event_type, payload) VALUES ('write_probe', '{}')")
                )
                savepoint.rollback()
        except PersistenceError:
            self._halt()
        else:
            self.readiness = TradingReadiness(db_writable=True, outbox_ready=True)
        return self.readiness

    def close(self) -> None:
        self.sql_engine.dispose()
