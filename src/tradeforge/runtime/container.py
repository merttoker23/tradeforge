"""Linux Compose entrypoints; no browser dependency or mainnet order authority."""

import argparse
import asyncio
import hashlib
import http.client
import json
import os
import time
from pathlib import Path

from aiohttp import web
from sqlalchemy import text
from sqlalchemy.engine import URL, Connection

from tradeforge.config.policy import require_demo_trial
from tradeforge.config.settings import load_settings
from tradeforge.dashboard.auth import Authenticator
from tradeforge.dashboard.control import CommandQueue
from tradeforge.dashboard.read_model import ReadModel
from tradeforge.dashboard.sessions import Sessions
from tradeforge.execution.binance_client import BinanceTestnetClient, Credentials
from tradeforge.persistence.db import Database
from tradeforge.persistence.transactions import migrate
from tradeforge.runtime.healthcheck import PORTS as PORTS
from tradeforge.runtime.healthcheck import health
from tradeforge.runtime.maintenance import observe_storage, retain
from tradeforge.runtime.secrets import read_secret

ROLES = ("market", "quant", "guardian", "dashboard")


def secret(name: str) -> str:
    return read_secret(Path("/run/secrets") / name, maximum=4096)


def database(role: str) -> Database:
    if role not in (*ROLES, "owner"):
        raise ValueError("DATABASE_ROLE_INVALID")
    username = "tradeforge_owner" if role == "owner" else "tf_" + role
    password = secret("postgres_password" if role == "owner" else role + "_password")
    return Database(
        URL.create(
            "postgresql+psycopg",
            username=username,
            password=password,
            host="postgres",
            port=5432,
            database="tradeforge",
        )
    )


def grant_runtime_access(c: Connection, database_name: str) -> None:
    import re

    if not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", database_name):
        raise ValueError("DATABASE_NAME_INVALID")
    names = ",".join("tf_" + role for role in ROLES)
    for statement in (
        f"REVOKE ALL ON DATABASE {database_name} FROM PUBLIC",
        "REVOKE CREATE ON SCHEMA public FROM PUBLIC",
        f"GRANT CONNECT ON DATABASE {database_name} TO {names}",
        f"GRANT USAGE ON SCHEMA public TO {names}",
        f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {names}",
        "GRANT INSERT ON instruments,universe_snapshots,closed_bars,closed_bar_observations,"
        "terminal_chart_cache,runtime_observations TO tf_market",
        "GRANT UPDATE ON runtime_observations,terminal_chart_cache TO tf_market",
        "GRANT INSERT ON regime_snapshots,opportunities,runtime_observations,"
        "decision_lifecycle,audit_log TO tf_quant",
        "GRANT UPDATE ON runtime_observations TO tf_quant",
        "GRANT INSERT ON instruments,decisions,decision_reasons,execution_intents,risk_snapshots,"
        "outbox,orders,fills,positions,protective_orders,account_snapshots,risk_state,commands,"
        "audit_log,decision_lifecycle,runtime_observations TO tf_guardian",
        "GRANT UPDATE ON orders,outbox,positions,protective_orders,risk_state,commands,"
        "runtime_observations "
        "TO tf_guardian",
        "GRANT INSERT ON commands,audit_log TO tf_dashboard",
        f"GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO {names}",
        "REVOKE ALL ON binance_runtime,binance_credentials FROM PUBLIC",
        "REVOKE ALL ON binance_credentials FROM tf_market,tf_quant,tf_guardian,tf_dashboard",
        "GRANT SELECT ON binance_runtime TO tf_market,tf_quant,tf_guardian,tf_dashboard",
        "GRANT UPDATE(environment,revision,changed_at) ON binance_runtime TO tf_dashboard",
        "GRANT SELECT ON binance_credentials TO tf_dashboard,tf_guardian",
        "GRANT INSERT,UPDATE ON binance_credentials TO tf_dashboard",
        "GRANT EXECUTE ON FUNCTION pg_ls_waldir() TO tf_guardian",
        "GRANT EXECUTE ON FUNCTION retain_runtime_history() TO tf_guardian",
    ):
        c.execute(text(statement))


def initialize() -> None:
    db = database("owner")
    try:
        migrate(db, "head")
        with db.transaction() as c:
            for role in ROLES:
                name = "tf_" + role
                password = secret(role + "_password")
                if len(password) != 64 or any(ch not in "0123456789abcdef" for ch in password):
                    raise ValueError("DATABASE_PASSWORD_FORMAT_INVALID")
                if not c.scalar(text("SELECT 1 FROM pg_roles WHERE rolname=:name"), {"name": name}):
                    c.execute(
                        text(
                            f"CREATE ROLE {name} LOGIN NOSUPERUSER NOCREATEDB "
                            f"NOCREATEROLE NOREPLICATION PASSWORD '{password}'"
                        )
                    )
            grant_runtime_access(c, "tradeforge")
    finally:
        db.close()


async def paper_guardian(db: Database, expected_revision: int | None = None) -> None:
    from tradeforge.market.socket_manager import pause
    from tradeforge.runtime.signals import shutdown_signals

    stop = asyncio.Event()

    async def health(request: web.Request) -> web.Response:
        return web.json_response(
            {
                "service": "execution_guardian",
                "alive": True,
                "mode": "paper",
                "live": False,
                "orders_enabled": False,
                "reason": "EXPLICIT_DEMO_ENABLE_REQUIRED",
            },
            status=200 if request.path == "/health" else 503,
        )

    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_get("/readiness", health)
    runner = web.AppRunner(app, access_log=None)
    with shutdown_signals(stop):
        await runner.setup()
        try:
            await web.TCPSite(runner, "0.0.0.0", 8103).start()
            last_retention = float("-inf")
            while not stop.is_set():
                if expected_revision is not None:
                    from tradeforge.runtime.binance_environment import select_environment

                    if (
                        await asyncio.to_thread(select_environment, db)
                    ).revision != expected_revision:
                        raise RuntimeError("BINANCE_ENVIRONMENT_RESTART_REQUIRED")
                try:
                    await asyncio.to_thread(observe_storage, db)
                except Exception:
                    pass
                if time.monotonic() - last_retention >= 900:
                    try:
                        await asyncio.to_thread(retain, db)
                    except Exception:
                        pass
                    last_retention = time.monotonic()
                await pause(stop, 10)
        finally:
            await runner.cleanup()


def main() -> int:
    parser = argparse.ArgumentParser(description="Tradeforge autonomous Linux Compose runtime")
    parser.add_argument("role", choices=(*ROLES, "migrate", "retain", "health", "arm"))
    parser.add_argument("--service", choices=ROLES)
    args = parser.parse_args()
    if os.name != "posix":
        parser.error("LINUX_REQUIRED")
    db = None
    try:
        settings = load_settings()
        enabled = os.environ.get("TF_DEMO_ORDERS", "false")
        trial_enabled = require_demo_trial(
            settings.mode, orders=enabled, trial=os.environ.get("TF_DEMO_TRIAL", "false")
        )
        if enabled not in {"true", "false"}:
            raise ValueError("EXPLICIT_DEMO_ENABLE_REQUIRED")
        if args.role == "health":
            if args.service is None:
                raise ValueError("SERVICE_REQUIRED")
            return health(args.service)
        if args.role == "migrate":
            initialize()
            return 0
        if args.role == "retain":
            db = database("owner")
            retain(db)
            return 0
        if args.role == "arm":
            from tradeforge.risk.storage import RiskService

            if settings.mode.value != "testnet" or enabled != "true":
                raise ValueError("EXPLICIT_DEMO_ENABLE_REQUIRED")
            connection = http.client.HTTPConnection("127.0.0.1", 8103, timeout=3)
            try:
                connection.request("GET", "/health")
                response = connection.getresponse()
                state = json.loads(response.read(8192))
            finally:
                connection.close()
            if (
                state.get("live") is not False
                or state.get("stream_ready") is not True
                or state.get("protection", {}).get("ready") is not True
            ):
                raise ValueError("ACCOUNT_RECONCILIATION_REQUIRED")
            db = database("guardian")
            RiskService(db).set_kill_switch(False, reason="EXPLICIT_DEMO_OPERATOR_ARM")
            print("DEMO_ARMED: LIVE=false; per-order risk and context checks required")
            return 0
        db = database(args.role)
        if args.role == "market":
            from apps.market_gateway.runtime import serve

            asyncio.run(serve(db, 8101, host="0.0.0.0", internal_token=secret("gateway_token")))
        elif args.role == "quant":
            from apps.quant_core.runtime import serve as quant_serve

            asyncio.run(
                quant_serve(db, 8102, host="0.0.0.0", internal_token=secret("gateway_token"))
            )
        elif args.role == "guardian":
            from tradeforge.runtime.binance_environment import select_environment

            active = select_environment(db)
            db.pin_environment(active.revision)
            if active.name == "live":
                asyncio.run(paper_guardian(db, active.revision))
                return 0
            from tradeforge.runtime.binance_environment import load_credentials

            pair = load_credentials(db, secret("gateway_token"), "testnet")
            if pair is None and settings.mode.value == "paper" and enabled == "false":
                asyncio.run(paper_guardian(db, active.revision))
            elif pair is not None and (
                (settings.mode.value == "testnet" and enabled == "true")
                or (settings.mode.value == "paper" and enabled == "false")
            ):
                from apps.execution_guardian.runtime import serve as guardian_serve

                credentials = Credentials.model_validate(
                    {"api_key": pair[0], "api_secret": pair[1]}
                )
                client = BinanceTestnetClient(
                    credentials, mode="testnet", opted_in=True, read_only=False
                )
                asyncio.run(
                    guardian_serve(
                        db,
                        client,
                        8103,
                        host="0.0.0.0",
                        internal_token=secret("gateway_token"),
                        enable_entries=enabled == "true",
                        trial_enabled=trial_enabled,
                        expected_revision=active.revision,
                    )
                )
            else:
                raise ValueError("EXPLICIT_DEMO_ENABLE_REQUIRED")
        elif args.role == "dashboard":
            token = secret("dashboard_login")
            auth = Authenticator.from_json(
                json.dumps(
                    {
                        "tokens": [
                            {
                                "subject": "operator",
                                "scopes": ["read", "control"],
                                "sha256": hashlib.sha256(token.encode()).hexdigest(),
                            }
                        ]
                    }
                )
            )
            from tradeforge.runtime.dashboard_memory import dashboard_app

            app = dashboard_app(
                ReadModel(db),
                CommandQueue(db),
                sessions=Sessions(auth, secret("session_key").encode()),
                external_origin="https://" + os.environ["TF_DOMAIN"],
                internal_token=secret("gateway_token"),
                binance_master=secret("gateway_token"),
            )
            web.run_app(app, host="0.0.0.0", port=8104, access_log=None, print=None)
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception:
        parser.error("RUNTIME_FAILED_CLOSED")
    finally:
        if db is not None:
            db.close()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
