"""Read-only Testnet account projection for the dashboard. No browser order authority."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from aiohttp import web

from tradeforge.dashboard.auth import Authenticator
from tradeforge.dashboard.sessions import COOKIE, Sessions
from tradeforge.execution.binance_client import BinanceTestnetClient, Credentials, number
from tradeforge.execution.broker import AckUnknown, ExchangeRejected
from tradeforge.execution.storage import audit
from tradeforge.execution.terminal_account import terminal_account
from tradeforge.persistence.db import Database
from tradeforge.runtime.binance_environment import load_credentials, select_environment

_INTERVAL_SECONDS = 20


def _client(db: Database, master: str | None, *, read_only: bool) -> BinanceTestnetClient:
    if select_environment(db).name != "testnet":
        raise ValueError("TESTNET_ENVIRONMENT_REQUIRED")
    if master is None:
        raise ValueError("BINANCE_MASTER_REQUIRED")
    stored = load_credentials(db, master, "testnet")
    if stored is None:
        raise ValueError("TESTNET_CREDENTIALS_MISSING")
    credentials = Credentials.model_validate({"api_key": stored[0], "api_secret": stored[1]})
    return BinanceTestnetClient(credentials, mode="testnet", opted_in=True, read_only=read_only)


def _read_account(db: Database, master: str) -> dict[str, Any]:
    """Signed GET-only account projection. No position mutation or order authority."""
    client = _client(db, master, read_only=True)
    started = datetime.now(UTC)
    account = client.request("GET", "/fapi/v3/account")
    balances = client.request("GET", "/fapi/v3/balance")
    positions = client.request("GET", "/fapi/v3/positionRisk")
    if (
        not isinstance(account, dict)
        or not isinstance(balances, list)
        or not isinstance(positions, list)
    ):
        raise ValueError("DEMO_RESPONSE_INVALID")
    equity, available = number(account["totalMarginBalance"]), number(account["availableBalance"])
    if equity < 0 or available < 0:
        raise ValueError("DEMO_RESPONSE_INVALID")
    usdt = [b for b in balances if isinstance(b, dict) and b.get("asset") == "USDT"]
    if len(usdt) != 1 or number(usdt[0]["availableBalance"]) < 0:
        raise ValueError("DEMO_USDT_BALANCE_UNAVAILABLE")
    active = []
    for row in positions:
        if not isinstance(row, dict) or not isinstance(row.get("symbol"), str):
            raise ValueError("DEMO_RESPONSE_INVALID")
        if number(row["positionAmt"]) != 0:
            active.append(row)
    if len(active) > 100:
        raise ValueError("DEMO_POSITION_CAPACITY")
    # A separate failure in trade history cannot erase verified balances.
    orders: list[Any] = []
    algos: list[Any] = []
    reasons: list[str] = ["EXCHANGE_HISTORY_SAMPLED"]
    try:
        result = client.request("GET", "/fapi/v1/openOrders")
        if isinstance(result, list) and len(result) <= 10000:
            orders = result
        else:
            reasons.append("DEMO_ORDERS_UNAVAILABLE")
    except (AckUnknown, ExchangeRejected, ValueError, KeyError):
        reasons.append("DEMO_ORDERS_UNAVAILABLE")
    try:
        result = client.request("GET", "/fapi/v1/openAlgoOrders")
        if isinstance(result, list) and len(result) <= 10000:
            algos = result
        else:
            reasons.append("DEMO_ALGO_ORDERS_UNAVAILABLE")
    except (AckUnknown, ExchangeRejected, ValueError, KeyError):
        reasons.append("DEMO_ALGO_ORDERS_UNAVAILABLE")
    # The terminal projection validates/limits its own fields and treats
    # missing income history as unknown PnL (never fabricated zero).
    projection = terminal_account(client, account, active, orders, algos)
    end = datetime.now(UTC)
    return {
        "environment": "BINANCE_FUTURES_DEMO",
        "as_of": started.isoformat(),
        "expires_at": (started + timedelta(seconds=40)).isoformat(),
        "connection": "OK",
        "account": "OK",
        "balance": "OK",
        "positions": "OK",
        "clock": "OK",
        "key_permissions": "UNKNOWN",
        "account_can_trade": None,
        "equity": str(equity),
        "available_balance": str(available),
        "position_count": len(active),
        "open_order_count": len(orders) + len(algos) if not reasons[1:] else None,
        "ready": True,
        "live": False,
        "orders_enabled": False,
        "reason_codes": reasons,
        "terminal": projection,
        "sample_completed_at": end.isoformat(),
    }


def _audit(db: Database, event: str, payload: dict[str, Any]) -> None:
    """Persist the read-only Testnet account sample for incident review."""
    with db.transaction() as connection:
        audit(connection, event, payload)


def attach_demo_operator(
    app: web.Application,
    db: Database | None,
    master: str | None,
    *,
    read_only_loopback: bool = False,
    operator_auth: Authenticator | None = None,
    sessions: Sessions | None = None,
    external_origin: str | None = None,
) -> None:
    """Install authenticated account evidence only; never expose an order endpoint."""
    if db is None:
        return
    latest: dict[str, Any] = {"status": "AWAITING_FIRST_SAMPLE"}

    def reader(request: web.Request) -> None:
        if sessions is not None:
            sessions.authenticate(request.cookies.get(COOKIE), "read")
        elif operator_auth is not None:
            headers = request.headers.getall("Authorization", [])
            operator_auth.authenticate(headers[0] if len(headers) == 1 else None, "read")

    async def account(request: web.Request) -> web.Response:
        reader(request)
        active = await asyncio.to_thread(select_environment, db)
        return web.json_response(
            {
                "environment": active.name,
                **latest,
                "account_confirmed": latest.get("status") == "OK",
            },
            headers={"Cache-Control": "no-store"},
        )

    async def status_script(request: web.Request) -> web.Response:
        reader(request)
        path = Path(__file__).with_name("static") / "demo_status.js"
        return web.Response(body=path.read_bytes(), content_type="text/javascript", charset="utf-8")

    async def status_stylesheet(request: web.Request) -> web.Response:
        reader(request)
        path = Path(__file__).with_name("static") / "demo_status.css"
        return web.Response(body=path.read_bytes(), content_type="text/css", charset="utf-8")

    @web.middleware
    async def document_script(
        request: web.Request, handler: Callable[[web.Request], Awaitable[web.StreamResponse]]
    ) -> web.StreamResponse:
        response = await handler(request)
        if (
            request.path == "/"
            and request.method == "GET"
            and isinstance(response, web.Response)
            and response.content_type == "text/html"
        ):
            body = response.body
            if body is None:
                return response
            html = body.decode("utf-8")
            html = html.replace(
                "</head>",
                '<link rel="stylesheet" href="/assets/demo_status.css">'
                '<script src="/assets/demo_status.js" defer></script></head>',
                1,
            )
            response.body = html.encode("utf-8")
        return response

    # Keep the existing security middleware in place. All these routes pass
    # through its loopback-only and same-origin request boundary first.
    app.middlewares.append(document_script)
    app.router.add_get("/api/demo/account", account)
    app.router.add_get("/assets/demo_status.js", status_script)
    app.router.add_get("/assets/demo_status.css", status_stylesheet)

    async def sampling_context(application: web.Application) -> AsyncIterator[None]:
        pending: list[asyncio.Task[dict[str, Any]] | None] = [None]

        def poll_once() -> dict[str, Any]:
            try:
                state = select_environment(db)
                if state.name != "testnet":
                    return {"status": "NOT_TESTNET"}
                if master is None:
                    return {"status": "BINANCE_MASTER_REQUIRED"}
                payload = _read_account(db, master)
                _audit(db, "demo.preflight.v1", payload)
                return {
                    "status": "OK",
                    "at": payload["as_of"],
                    "equity": payload["equity"],
                    "available_balance": payload["available_balance"],
                    "positions": payload["position_count"],
                }
            except ValueError as error:
                code = str(error)
                return {
                    "status": code
                    if code
                    in {
                        "BINANCE_MASTER_REQUIRED",
                        "TESTNET_CREDENTIALS_MISSING",
                        "DEMO_RESPONSE_INVALID",
                        "DEMO_USDT_BALANCE_UNAVAILABLE",
                        "BINANCE_CREDENTIALS_INVALID",
                        "TESTNET_ENVIRONMENT_REQUIRED",
                    }
                    else "ACCOUNT_UNAVAILABLE"
                }
            except ExchangeRejected as error:
                reason = str(error)
                return {
                    "status": reason
                    if reason
                    in {
                        "API_KEY_INVALID",
                        "API_KEY_PERMISSION_OR_IP_DENIED",
                        "SIGNATURE_INVALID",
                        "CLOCK_UNSAFE",
                        "RATE_LIMITED",
                    }
                    else "ACCOUNT_UNAVAILABLE"
                }
            except Exception:
                return {"status": "ACCOUNT_UNAVAILABLE"}

        async def sample() -> None:
            nonlocal latest
            while True:
                # Keep an in-flight database/exchange sample alive through the
                # server's cleanup phase; never close the DB under its thread.
                pending[0] = asyncio.create_task(asyncio.to_thread(poll_once))
                sampled = pending[0]
                if sampled is None:
                    continue
                latest = await asyncio.shield(sampled)
                pending[0] = None
                await asyncio.sleep(_INTERVAL_SECONDS)

        task = asyncio.create_task(sample())
        try:
            yield
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            if pending[0] is not None:
                await asyncio.gather(pending[0], return_exceptions=True)

    app.cleanup_ctx.append(sampling_context)
