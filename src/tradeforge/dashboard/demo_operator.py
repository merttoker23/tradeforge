"""Standalone, Testnet-only account viewer and manual order terminal.

This service intentionally never imports or instantiates execution_guardian.
Only explicit operator requests send an order; no autonomous strategies are wired
into this endpoint. Existing production order locks remain unchanged.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from uuid import uuid4

from aiohttp import web

from tradeforge.dashboard.auth import Authenticator, AuthError
from tradeforge.dashboard.sessions import COOKIE, Sessions
from tradeforge.execution.binance_client import BinanceTestnetClient, Credentials, number
from tradeforge.execution.broker import AckUnknown, ExchangeRejected
from tradeforge.execution.storage import audit
from tradeforge.execution.terminal_account import terminal_account
from tradeforge.persistence.db import Database
from tradeforge.runtime.binance_environment import load_credentials, select_environment

_SYMBOL = re.compile(r"[A-Z0-9_]{2,40}\Z")
_CLIENT_ID = re.compile(r"[A-Za-z0-9_-]{1,36}\Z")
_MAX_ORDER_BYTES = 4096
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
    reasons: list[str] = ["GUARDIAN_NOT_REQUIRED"]
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
        "guardian": "NOT_REQUIRED",
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


def _positive_decimal(raw: object, name: str) -> str:
    if not isinstance(raw, str) or len(raw) > 64 or not re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", raw):
        raise ValueError(name)
    try:
        n = Decimal(raw)
    except InvalidOperation:
        raise ValueError(name) from None
    if not n.is_finite() or n <= 0:
        raise ValueError(name)
    return format(n, "f")


def _order_params(body: Any) -> dict[str, str]:
    if not isinstance(body, dict) or set(body) - {
        "symbol",
        "side",
        "type",
        "quantity",
        "price",
        "timeInForce",
        "positionSide",
        "reduceOnly",
    }:
        raise ValueError("ORDER_INVALID")
    symbol, side, kind = body.get("symbol"), body.get("side"), body.get("type")
    if not isinstance(symbol, str) or not _SYMBOL.fullmatch(symbol):
        raise ValueError("ORDER_SYMBOL_INVALID")
    if side not in {"BUY", "SELL"} or kind not in {"MARKET", "LIMIT"}:
        raise ValueError("ORDER_INVALID")
    position = body.get("positionSide", "BOTH")
    if position not in {"BOTH", "LONG", "SHORT"}:
        raise ValueError("ORDER_INVALID")
    reduce_only = body.get("reduceOnly", False)
    if type(reduce_only) is not bool:
        raise ValueError("ORDER_INVALID")
    params = {
        "symbol": symbol,
        "side": side,
        "type": kind,
        "quantity": _positive_decimal(body.get("quantity"), "ORDER_QUANTITY_INVALID"),
        "positionSide": position,
        "newClientOrderId": "tfmanual_" + uuid4().hex[:24],
        "newOrderRespType": "ACK",
    }
    if kind == "LIMIT":
        if body.get("timeInForce", "GTC") not in {"GTC", "IOC", "FOK"}:
            raise ValueError("ORDER_INVALID")
        params["price"] = _positive_decimal(body.get("price"), "ORDER_PRICE_INVALID")
        params["timeInForce"] = body.get("timeInForce", "GTC")
    elif "price" in body or "timeInForce" in body:
        raise ValueError("ORDER_INVALID")
    if reduce_only:
        if position != "BOTH":
            raise ValueError("REDUCE_ONLY_ONE_WAY_REQUIRED")
        params["reduceOnly"] = "true"
    return params


def _audit(db: Database, event_type: str, body: dict[str, Any]) -> None:
    with db.transaction() as connection:
        audit(connection, event_type, body)


def _place(db: Database, master: str | None, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    params = _order_params(body)
    client = _client(db, master, read_only=False)
    # Persist a non-secret intent *before* sending; if storage is unavailable,
    # do not emit an unaudited exchange mutation.
    event = {
        "at": datetime.now(UTC).isoformat(),
        "client_order_id": params["newClientOrderId"],
        "symbol": params["symbol"],
        "side": params["side"],
        "type": params["type"],
        "quantity": params["quantity"],
        "state": "SUBMITTING",
        "environment": "testnet",
    }
    _audit(db, "demo.manual_order.v1", event)
    try:
        result = client.request("POST", "/fapi/v1/order", params)
    except AckUnknown:
        # Binance may have accepted the order; never automatically retry it.
        return 202, {
            "status": "ACK_UNKNOWN",
            "client_order_id": params["newClientOrderId"],
            "symbol": params["symbol"],
            "retry_safe": False,
        }
    except ExchangeRejected as error:
        reason = str(error)
        _audit(db, "demo.manual_order.v1", {**event, "state": "REJECTED", "reason": reason})
        return 422, {
            "status": "REJECTED",
            "reason": reason,
            "client_order_id": params["newClientOrderId"],
        }
    if not isinstance(result, dict) or not isinstance(result.get("orderId"), int):
        return 202, {
            "status": "ACK_UNKNOWN",
            "client_order_id": params["newClientOrderId"],
            "symbol": params["symbol"],
            "retry_safe": False,
        }
    status = result.get("status", "ACCEPTED")
    receipt = {
        "status": status
        if status in {"NEW", "FILLED", "PARTIALLY_FILLED", "ACCEPTED", "CANCELED"}
        else "ACCEPTED",
        "client_order_id": params["newClientOrderId"],
        "order_id": str(result["orderId"]),
        "symbol": params["symbol"],
        "environment": "testnet",
    }
    try:
        _audit(
            db,
            "demo.manual_order.v1",
            {**event, "state": "ACKNOWLEDGED", "order_id": receipt["order_id"]},
        )
    except Exception:
        # The exchange ACK is authoritative even if a later audit write fails.
        receipt["audit_status"] = "UNAVAILABLE"
    return 200, receipt


def _cancel(db: Database, master: str | None, body: Any) -> tuple[int, dict[str, Any]]:
    if not isinstance(body, dict) or set(body) != {"symbol", "client_order_id"}:
        raise ValueError("ORDER_INPUT_INVALID")
    symbol, cid = body["symbol"], body["client_order_id"]
    if (
        not isinstance(symbol, str)
        or not _SYMBOL.fullmatch(symbol)
        or not isinstance(cid, str)
        or not _CLIENT_ID.fullmatch(cid)
    ):
        raise ValueError("ORDER_INPUT_INVALID")
    client = _client(db, master, read_only=False)
    event = {
        "at": datetime.now(UTC).isoformat(),
        "environment": "testnet",
        "client_order_id": cid,
        "symbol": symbol,
        "state": "CANCEL_SUBMITTING",
    }
    _audit(db, "demo.manual_order.v1", event)
    try:
        response = client.request(
            "DELETE", "/fapi/v1/order", {"symbol": symbol, "origClientOrderId": cid}
        )
    except AckUnknown:
        return 202, {
            "status": "CANCEL_ACK_UNKNOWN",
            "symbol": symbol,
            "client_order_id": cid,
            "retry_safe": False,
        }
    except ExchangeRejected as error:
        return 422, {
            "status": "REJECTED",
            "reason": str(error),
            "symbol": symbol,
            "client_order_id": cid,
        }
    if not isinstance(response, dict) or response.get("symbol") != symbol:
        return 202, {
            "status": "CANCEL_ACK_UNKNOWN",
            "symbol": symbol,
            "client_order_id": cid,
            "retry_safe": False,
        }
    status = response.get("status", "CANCEL_REQUESTED")
    result = {
        "status": status if isinstance(status, str) and len(status) <= 32 else "CANCEL_REQUESTED",
        "client_order_id": cid,
        "symbol": symbol,
        "environment": "testnet",
    }
    try:
        _audit(db, "demo.manual_order.v1", {**event, "state": "CANCEL_ACKNOWLEDGED"})
    except Exception:
        result["audit_status"] = "UNAVAILABLE"
    return 200, result


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
    """Install read-only account projection and manual Testnet order endpoints."""
    if db is None:
        return
    latest: dict[str, Any] = {"status": "AWAITING_FIRST_SAMPLE"}

    def reader(request: web.Request) -> None:
        if sessions is not None:
            sessions.authenticate(request.cookies.get(COOKIE), "read")
        elif operator_auth is not None:
            headers = request.headers.getall("Authorization", [])
            operator_auth.authenticate(headers[0] if len(headers) == 1 else None, "read")

    def operator(request: web.Request) -> None:
        if read_only_loopback:
            raise web.HTTPForbidden()
        if sessions is not None:
            # A cookie-only request may be cross-site. Require explicit Origin
            # on all remote order mutations, in addition to api.boundary CSRF.
            if not external_origin or request.headers.get("Origin") != external_origin:
                raise web.HTTPForbidden()
            sessions.authenticate(request.cookies.get(COOKIE), "control").require("control")
        elif operator_auth is not None:
            headers = request.headers.getall("Authorization", [])
            principal = operator_auth.authenticate(
                headers[0] if len(headers) == 1 else None, "control"
            )
            principal.require("control")
        # With no separate auth, api.boundary enforces loopback and same-origin.

    async def account(request: web.Request) -> web.Response:
        reader(request)
        active = await asyncio.to_thread(select_environment, db)
        # Manual Testnet access is separate from autonomous order authority.
        # Only the fixed Testnet client is used by the order endpoint. This
        # flag describes UI/API availability, not exchange acceptance.
        can_control = not read_only_loopback
        if sessions is not None:
            try:
                sessions.authenticate(request.cookies.get(COOKIE), "control")
            except AuthError:
                can_control = False
        elif operator_auth is not None:
            try:
                values = request.headers.getall("Authorization", [])
                operator_auth.authenticate(values[0] if len(values) == 1 else None, "control")
            except AuthError:
                can_control = False
        manual_available = bool(active.name == "testnet" and master is not None and can_control)
        return web.json_response(
            {
                "environment": active.name,
                **latest,
                "orders_enabled": False,  # Autonomous execution is not enabled.
                "manual_orders_available": manual_available,
                "manual_account_confirmed": latest.get("status") == "OK",
            },
            headers={"Cache-Control": "no-store"},
        )

    async def order(request: web.Request) -> web.Response:
        operator(request)
        if master is None:
            return web.json_response({"error": "BINANCE_MASTER_REQUIRED"}, status=503)
        if request.content_length is not None and request.content_length > _MAX_ORDER_BYTES:
            raise web.HTTPRequestEntityTooLarge(
                max_size=_MAX_ORDER_BYTES, actual_size=request.content_length
            )
        try:
            body = await request.json()
            _order_params(body)  # fail before any DB mutation
            status, response = await asyncio.to_thread(_place, db, master, body)
            return web.json_response(response, status=status, headers={"Cache-Control": "no-store"})
        except (ValueError, TypeError):
            return web.json_response({"error": "ORDER_INPUT_INVALID"}, status=400)
        except AuthError:
            raise
        except Exception:
            return web.json_response({"error": "DEMO_ORDER_UNAVAILABLE"}, status=503)

    async def cancel(request: web.Request) -> web.Response:
        operator(request)
        if master is None:
            return web.json_response({"error": "BINANCE_MASTER_REQUIRED"}, status=503)
        try:
            body = await request.json()
            status, result = await asyncio.to_thread(_cancel, db, master, body)
            return web.json_response(result, status=status, headers={"Cache-Control": "no-store"})
        except (ValueError, TypeError):
            return web.json_response({"error": "ORDER_INPUT_INVALID"}, status=400)
        except Exception:
            return web.json_response({"error": "DEMO_CANCEL_UNAVAILABLE"}, status=503)

    async def check(request: web.Request) -> web.Response:
        operator(request)
        if master is None:
            return web.json_response({"error": "BINANCE_MASTER_REQUIRED"}, status=503)
        symbol, cid = request.query.get("symbol"), request.query.get("client_order_id")
        if (
            not isinstance(symbol, str)
            or not _SYMBOL.fullmatch(symbol)
            or not isinstance(cid, str)
            or not _CLIENT_ID.fullmatch(cid)
        ):
            return web.json_response({"error": "ORDER_INPUT_INVALID"}, status=400)
        try:
            client = await asyncio.to_thread(_client, db, master, read_only=True)
            row = await asyncio.to_thread(
                client.request,
                "GET",
                "/fapi/v1/order",
                {"symbol": symbol, "origClientOrderId": cid},
            )
            return web.json_response(
                {
                    "status": row.get("status") if isinstance(row, dict) else "NOT_FOUND",
                    "order_id": str(row["orderId"])
                    if isinstance(row, dict) and "orderId" in row
                    else None,
                    "client_order_id": cid,
                    "symbol": symbol,
                },
                headers={"Cache-Control": "no-store"},
            )
        except Exception:
            return web.json_response({"error": "DEMO_ORDER_UNAVAILABLE"}, status=503)

    async def stylesheet(request: web.Request) -> web.Response:
        reader(request)
        path = Path(__file__).with_name("static") / "demo_operator.css"
        return web.Response(body=path.read_bytes(), content_type="text/css", charset="utf-8")

    async def script(request: web.Request) -> web.Response:
        reader(request)
        path = Path(__file__).with_name("static") / "demo_operator.js"
        return web.Response(body=path.read_bytes(), content_type="text/javascript", charset="utf-8")

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
                '<link rel="stylesheet" href="/assets/demo_operator.css">'
                '<link rel="stylesheet" href="/assets/demo_status.css">'
                '<script src="/assets/demo_operator.js" defer></script>'
                '<script src="/assets/demo_status.js" defer></script></head>',
                1,
            )
            response.body = html.encode("utf-8")
        return response

    # Keep the existing security middleware in place. All these routes pass
    # through its loopback-only and same-origin request boundary first.
    app.middlewares.append(document_script)
    app.router.add_get("/api/demo/account", account)
    app.router.add_post("/api/demo/order", order)
    app.router.add_get("/api/demo/order", check)
    app.router.add_post("/api/demo/cancel", cancel)
    app.router.add_get("/assets/demo_operator.js", script)
    app.router.add_get("/assets/demo_operator.css", stylesheet)
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
