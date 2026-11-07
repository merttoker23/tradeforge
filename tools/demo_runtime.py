"""Linux Futures Demo GET-only account monitor; no order-capable startup."""

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event
from typing import Any

from sqlalchemy import text

from tradeforge.config.settings import load_settings
from tradeforge.execution.binance_client import BinanceTestnetClient
from tradeforge.execution.preflight import preflight
from tradeforge.market.bars import canonical
from tradeforge.runtime.config import local_database
from tradeforge.runtime.demo import load_demo_credentials
from tradeforge.runtime.signals import shutdown_signals

ROOT = Path(__file__).resolve().parents[1]
DEMO_HOME = ROOT / "var/demo-runtime"


def check(home: Path = DEMO_HOME, *, observer_home: Path | None = None) -> dict[str, Any]:
    at = datetime.now(UTC)
    state: dict[str, Any] = {
        "environment": "BINANCE_FUTURES_DEMO",
        "as_of": at.isoformat(),
        "expires_at": (at + timedelta(seconds=30)).isoformat(),
        "connection": "UNKNOWN",
        "account": "UNKNOWN",
        "balance": "UNKNOWN",
        "positions": "UNKNOWN",
        "clock": "UNKNOWN",
        "key_permissions": "UNKNOWN",
        "account_can_trade": None,
        "equity": None,
        "available_balance": None,
        "position_count": None,
        "open_order_count": None,
        "ready": False,
        "live": False,
        "orders_enabled": False,
        "reason_codes": ["DEMO_CREDENTIALS_MISSING", "GUARDIAN_NOT_RUNNING"],
    }
    if not all((home / name).is_file() for name in ("binance-demo.key", "binance-demo.secret")):
        return state
    try:
        credentials = load_demo_credentials(home, observer_home=observer_home)
        client = BinanceTestnetClient(credentials, mode="testnet", opted_in=True, read_only=True)
        state = preflight(client, terminal=True)
    except Exception:
        state["reason_codes"] = ["DEMO_SECRET_FILE_UNSAFE"]
    state["guardian"] = "NOT_RUNNING"
    # A running process is separate evidence and never grants this checker authority.
    from urllib.request import urlopen

    try:
        with urlopen("http://127.0.0.1:8103/health", timeout=2) as response:
            code = response.status
            health = json.loads(response.read(4096))
        if (
            code == 200
            and health.get("service") == "execution_guardian"
            and health.get("environment") == "BINANCE_FUTURES_DEMO"
            and health.get("live") is False
        ):
            state["guardian"] = "RUNNING"
    except Exception:
        pass
    if state["guardian"] != "RUNNING":
        state["reason_codes"].append("GUARDIAN_NOT_RUNNING")
    return state


def publish(database_file: Path, state: dict[str, Any]) -> None:
    # Only this sanitized projection crosses from demo into the observer DB.
    # Prices, positions, risk proofs and fills remain in the dedicated demo DB.
    db = local_database(database_file)
    try:
        with db.transaction() as c:
            c.execute(
                text(
                    "INSERT INTO audit_log(event_type,payload) "
                    "VALUES ('demo.preflight.v1',CAST(:payload AS jsonb))"
                ),
                {"payload": canonical(state)},
            )
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Local Futures Demo: no mainnet order capability")
    parser.add_argument("command", choices=["check", "watch"])
    parser.add_argument("--home", type=Path, default=DEMO_HOME)
    parser.add_argument("--database-file", type=Path, required=True)
    args = parser.parse_args()
    try:
        settings = load_settings()
        if settings.mode.value != "paper" or settings.live:
            raise ValueError("OBSERVER_PAPER_REQUIRED")
        stop = Event()
        with shutdown_signals(stop):
            while not stop.is_set():
                state = check(args.home)
                publish(args.database_file, state)
                print(json.dumps(state, sort_keys=True), flush=True)
                if args.command == "check":
                    return 0 if state["ready"] else 1
                stop.wait(15)
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception:
        parser.error("DEMO_LOCAL_RUNTIME_UNAVAILABLE")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
