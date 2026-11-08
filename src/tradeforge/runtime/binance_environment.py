"""Persisted USD-M endpoint selection and encrypted operator credentials.

The runtime mode and TF_DEMO_ORDERS remain independent, fail-closed execution gates.
The shared gateway token is a Docker secret; its domain-separated digest is never stored.
"""

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text

from tradeforge.persistence.db import Database

ENDPOINTS = {
    "testnet": (
        "demo-fapi.binance.com",
        "https://demo-fapi.binance.com",
        "https://demo-fstream.binance.com",
    ),
    "live": ("fapi.binance.com", "https://fapi.binance.com", "https://fstream.binance.com"),
}


@dataclass(frozen=True)
class BinanceEnvironment:
    name: str
    revision: int
    changed_at: datetime

    @property
    def rest_host(self) -> str:
        return ENDPOINTS[self.name][0]

    @property
    def rest_base(self) -> str:
        return ENDPOINTS[self.name][1]

    @property
    def stream_base(self) -> str:
        return ENDPOINTS[self.name][2]


def select_environment(db: Database) -> BinanceEnvironment:
    with db.transaction() as c:
        row = c.execute(
            text("SELECT environment,revision,changed_at FROM binance_runtime WHERE id=1")
        ).one()
    if row.environment not in ENDPOINTS or row.revision < 1:
        raise ValueError("BINANCE_ENVIRONMENT_INVALID")
    return BinanceEnvironment(row.environment, row.revision, row.changed_at)


def _passphrase(master: str) -> str:
    if len(master) < 32:
        raise ValueError("BINANCE_ENCRYPTION_SECRET_INVALID")
    return hashlib.sha256(b"tradeforge/binance/credentials/v1:" + master.encode()).hexdigest()


def public_state(db: Database) -> dict[str, object]:
    current = select_environment(db)
    with db.transaction() as c:
        configured = {
            row.environment: {
                "configured": True,
                "last_test_at": row.last_test_at.isoformat() if row.last_test_at else None,
                "connected": row.last_test_ok,
            }
            for row in c.execute(
                text("SELECT environment,last_test_at,last_test_ok FROM binance_credentials")
            ).all()
        }
    return {
        "environment": current.name,
        "revision": current.revision,
        "changed_at": current.changed_at.isoformat(),
        "credentials": {
            environment: configured.get(
                environment, {"configured": False, "last_test_at": None, "connected": None}
            )
            for environment in ENDPOINTS
        },
        "live_orders_enabled": False,
    }


def load_credentials(db: Database, master: str, environment: str) -> tuple[str, str] | None:
    if not isinstance(environment, str) or environment not in ENDPOINTS:
        raise ValueError("BINANCE_ENVIRONMENT_INVALID")
    with db.transaction() as c:
        row = c.execute(
            text(
                "SELECT pgp_sym_decrypt(api_key,:passphrase) AS api_key, "
                "pgp_sym_decrypt(api_secret,:passphrase) AS api_secret "
                "FROM binance_credentials WHERE environment=:environment"
            ),
            {"environment": environment, "passphrase": _passphrase(master)},
        ).one_or_none()
    if row is None:
        return None
    if not row.api_key or not row.api_secret:
        raise ValueError("BINANCE_CREDENTIALS_INVALID")
    return (row.api_key, row.api_secret)


def update_credentials(db: Database, master: str, environment: str, key: str, secret: str) -> None:
    if (
        not isinstance(environment, str)
        or environment not in ENDPOINTS
        or not all(
            isinstance(v, str) and 8 <= len(v) <= 256 and re.fullmatch(r"[A-Za-z0-9_+./=-]+", v)
            for v in (key, secret)
        )
    ):
        raise ValueError("BINANCE_CREDENTIALS_INVALID")
    with db.transaction() as c:
        c.execute(
            text(
                "INSERT INTO binance_credentials(environment,api_key,api_secret) VALUES "
                "(:environment,pgp_sym_encrypt(:key,:passphrase,'cipher-algo=aes256'),"
                "pgp_sym_encrypt(:secret,:passphrase,'cipher-algo=aes256')) "
                "ON CONFLICT(environment) DO UPDATE SET api_key=EXCLUDED.api_key, "
                "api_secret=EXCLUDED.api_secret,last_test_at=NULL,last_test_ok=NULL"
            ),
            {
                "environment": environment,
                "key": key,
                "secret": secret,
                "passphrase": _passphrase(master),
            },
        )
        c.execute(
            text(
                "UPDATE binance_runtime SET revision=revision+1, "
                "changed_at=clock_timestamp() WHERE id=1"
            )
        )


def switch_environment(db: Database, environment: str) -> bool:
    if not isinstance(environment, str) or environment not in ENDPOINTS:
        raise ValueError("BINANCE_ENVIRONMENT_INVALID")
    with db.transaction() as c:
        c.execute(text("SELECT pg_advisory_xact_lock(84122026)"))
        old = c.execute(
            text("SELECT environment FROM binance_runtime WHERE id=1 FOR UPDATE")
        ).scalar_one()
        if old == environment:
            return False
        c.execute(
            text(
                "UPDATE binance_runtime SET environment=:env,revision=revision+1, "
                "changed_at=clock_timestamp() WHERE id=1"
            ),
            {"env": environment},
        )
    return True


def record_connection_test(db: Database, environment: str, connected: bool) -> None:
    if not isinstance(environment, str) or environment not in ENDPOINTS:
        raise ValueError("BINANCE_ENVIRONMENT_INVALID")
    with db.transaction() as c:
        updated = c.execute(
            text(
                "UPDATE binance_credentials SET last_test_at=clock_timestamp(), "
                "last_test_ok=:connected WHERE environment=:environment"
            ),
            {"environment": environment, "connected": connected},
        )
        # A recorded test is a configuration event: keep the revision display in
        # step with what the operator just observed on the settings screen.
        if updated.rowcount:
            c.execute(
                text(
                    "UPDATE binance_runtime SET revision=revision+1, "
                    "changed_at=clock_timestamp() WHERE id=1"
                )
            )
