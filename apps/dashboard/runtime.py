"""Linux VPS terminal: non-trading and loopback-only; credentials never served to JS."""

import argparse
import hashlib
import json
import os
import secrets
from pathlib import Path

from aiohttp import ClientSession, ClientTimeout, web

from tradeforge.bootstrap import snapshot
from tradeforge.config.settings import load_settings
from tradeforge.dashboard.api import create_app
from tradeforge.dashboard.auth import Authenticator
from tradeforge.dashboard.control import CommandQueue
from tradeforge.dashboard.read_model import ReadModel
from tradeforge.runtime.config import local_database


async def notify_chart_demand(symbol: str, timeframe: str, *, port: int = 8101) -> None:
    """Fixed loopback public-data service; no credentials or proxy inheritance."""
    async with ClientSession(timeout=ClientTimeout(total=1), trust_env=False) as client:
        async with client.get(
            f"http://127.0.0.1:{port}/chart-demand",
            params={"symbol": symbol, "timeframe": timeframe},
            allow_redirects=False,
        ) as response:
            if response.status != 202:
                raise ValueError("CHART_DEMAND_UNAVAILABLE")


def provision(auth_file: Path, token_file: Path) -> None:
    """Optional separate operator authentication, never enabled for anonymous reads."""
    if auth_file.resolve() == token_file.resolve() or auth_file.exists() or token_file.exists():
        raise ValueError("DASHBOARD_PROVISION_FAILED")
    token = secrets.token_urlsafe(48)
    body = {
        "tokens": [
            {
                "subject": "operator",
                "scopes": ["read", "control"],
                "sha256": hashlib.sha256(token.encode()).hexdigest(),
            }
        ]
    }
    for path, data in ((auth_file, json.dumps(body)), (token_file, token)):
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            output.write(data)


def build_app(
    *,
    database_file: Path | None = None,
    auth_file: Path | None = None,
    read_only_loopback: bool = False,
    binance_master: str | None = None,
) -> web.Application:
    db = None
    try:
        load_settings()
        if read_only_loopback and auth_file is not None:
            raise ValueError
        auth = Authenticator.from_json(auth_file.read_text(encoding="utf-8")) if auth_file else None
        if database_file is not None:
            db = local_database(database_file)
        read_model = ReadModel(db) if db else None
        app = create_app(
            read_model,
            CommandQueue(db) if db and not read_only_loopback else None,
            auth,
            read_only_loopback=read_only_loopback,
            chart_demand=notify_chart_demand if read_only_loopback else None,
            binance_master=binance_master,
        )
        if db is not None:

            async def close_db(application: web.Application) -> None:
                assert db is not None
                db.close()

            app.on_cleanup.append(close_db)
        return app
    except Exception:
        if db is not None:
            db.close()
        raise ValueError("DASHBOARD_CONFIG_INVALID") from None


def main() -> int:
    parser = argparse.ArgumentParser(description="Tradeforge Linux VPS loopback terminal")
    parser.add_argument("--port", type=int, default=8104)
    parser.add_argument("--database-file", type=Path)
    parser.add_argument("--auth-file", type=Path)
    parser.add_argument("--provision-auth", action="store_true")
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--read-only-loopback", action="store_true")
    parser.add_argument(
        "--binance-master",
        type=str,
        help="Binance API kimlik şifreleme anahtarı (en az 32 karakter)",
    )
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if not 1024 <= args.port <= 65535:
        parser.error("INVALID_PORT")
    try:
        state = snapshot("dashboard")
    except ValueError:
        parser.error("RUNTIME_CONFIG_INVALID")
    if args.check:
        print(json.dumps(state))
        return 0
    if args.provision_auth:
        if args.auth_file is None or args.token_file is None:
            parser.error("AUTH_AND_TOKEN_FILES_REQUIRED")
        provision(args.auth_file, args.token_file)
        print("DASHBOARD_AUTH_PROVISIONED")
        return 0
    # Anonymous loopback reads MUST NOT inherit command-scoped bearer authorization.
    if args.read_only_loopback and args.auth_file is not None:
        parser.error("READ_ONLY_MODE_HAS_SEPARATE_AUTHORITY")
    try:
        app = build_app(
            database_file=args.database_file,
            auth_file=args.auth_file,
            read_only_loopback=args.read_only_loopback,
            binance_master=args.binance_master,
        )
        web.run_app(app, host="127.0.0.1", port=args.port, access_log=None, print=None)
    except (ValueError, OSError):
        parser.error("DASHBOARD_UNAVAILABLE")
    return 0
