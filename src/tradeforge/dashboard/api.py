"""Loopback HTTP boundary with scoped bearer auth and no ambient browser authority."""

import asyncio
import ipaddress
import logging
import re
import time
from collections.abc import Awaitable, Callable
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from aiohttp import web

from tradeforge.dashboard.auth import Authenticator, AuthError, Principal
from tradeforge.dashboard.control import CommandQueue
from tradeforge.dashboard.read_model import ReadModel, refresh_freshness
from tradeforge.dashboard.realtime import RealtimeHub
from tradeforge.dashboard.sessions import COOKIE, Sessions
from tradeforge.domain.market import MarketQuote, MarketSnapshot

STATIC = Path(__file__).with_name("static")
HEADERS = {
    "Cache-Control": "no-store",
    "Content-Security-Policy": "default-src 'none'; script-src 'self'; style-src 'self'; "
    "connect-src 'self'; img-src 'self'; base-uri 'none'; "
    "frame-ancestors 'none'; form-action 'self'",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
}
LOGGER = logging.getLogger(__name__)


async def run_thread[**P, T](function: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
    """Keep DB admission until bounded synchronous work really ends on disconnect."""
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        try:
            await task
        except Exception:
            pass
        raise


def unavailable() -> dict[str, Any]:
    return {
        "status": "STALE",
        "reason": "READ_MODEL_UNAVAILABLE",
        "live": False,
        "orders_enabled": False,
        "sections": {},
        "poll_seconds": 5,
        "row_limit": 100,
        "alarms": [{"code": "READ_MODEL_UNAVAILABLE", "severity": "CRITICAL"}],
        "service_health": {
            "dashboard": "OK",
            "postgresql": "NOT_READY",
            "market_gateway": "UNKNOWN",
            "quant_core": "UNKNOWN",
        },
        "data_readiness": {"status": "UNKNOWN"},
        "trading_authority": {"status": "NOT_READY", "live": False, "orders_enabled": False},
    }


def create_app(
    model: ReadModel | None = None,
    queue: CommandQueue | None = None,
    auth: Authenticator | None = None,
    *,
    read_only_loopback: bool = False,
    chart_demand: Callable[[str, str], Awaitable[None]] | None = None,
    sessions: Sessions | None = None,
    external_origin: str | None = None,
    market_feed: Callable[[], tuple[MarketQuote | MarketSnapshot, ...]] | None = None,
    market_state: Callable[[], dict[str, Any]] | None = None,
    binance_master: str | None = None,
) -> web.Application:
    external = urlsplit(external_origin) if external_origin else None
    if external is not None and (
        external.scheme != "https"
        or not external.hostname
        or external.path
        or external.query
        or external.fragment
        or external.username
        or external.password
        or external.port not in {None, 443}
        or sessions is None
        or read_only_loopback
    ):
        raise ValueError("HTTPS_SESSION_CONFIG_REQUIRED")
    # One DB operation at a time. Admission fails immediately while busy;
    # psycopg connect/statement/lock timeouts bound work without orphaned threads.
    busy = asyncio.Lock()
    cached: dict[str, Any] | None = None
    sampled = float("-inf")
    rejected = 0
    rejection_window = time.monotonic()

    def reject_transport() -> None:
        if auth is not None:
            try:
                auth.authenticate(None, "read")
            except AuthError as error:
                if error.status == 429:
                    raise
        raise AuthError(403)

    @web.middleware
    async def boundary(
        request: web.Request, handler: Callable[[web.Request], Awaitable[web.StreamResponse]]
    ) -> web.StreamResponse:
        nonlocal rejected, rejection_window
        try:
            transport = request.transport
            address = transport.get_extra_info("sockname") if transport else None
            peer = transport.get_extra_info("peername") if transport else None
            external_request = external is not None and request.host == external.netloc
            if not external_request and (not peer or not ipaddress.ip_address(peer[0]).is_loopback):
                reject_transport()
            port = address[1] if address else None
            if not external_request and request.host not in {
                f"127.0.0.1:{port}",
                f"localhost:{port}",
                f"[::1]:{port}",
            }:
                reject_transport()
            if external is not None:
                if not external_request and request.path != "/health":
                    reject_transport()
                if request.method not in {"GET", "HEAD"} and (
                    request.headers.get("Origin") != external_origin
                    or request.headers.get("Sec-Fetch-Site") not in {None, "same-origin"}
                ):
                    reject_transport()
            navigation = (
                request.path == "/"
                and request.method == "GET"
                and request.headers.get("Sec-Fetch-Mode") == "navigate"
                and request.headers.get("Sec-Fetch-Dest") == "document"
            )
            if (
                "Origin" in request.headers
                and request.headers["Origin"]
                != (external_origin if external_request else f"http://{request.host}")
                and not navigation
            ):
                reject_transport()
            if (
                request.headers.get("Sec-Fetch-Site") in {"cross-site", "same-site"}
                and not navigation
            ):
                reject_transport()
            response = await handler(request)
        except AuthError as error:
            if time.monotonic() - rejection_window >= 60:
                rejected, rejection_window = 0, time.monotonic()
            record = rejected < 8 and error.status != 429
            if record:
                rejected += 1
                LOGGER.warning("dashboard_auth_rejected status=%d", error.status)
            # Rate-limited attempts do not amplify traffic into unbounded DB writes.
            if queue is not None and record and not busy.locked():
                try:
                    async with busy:
                        await run_thread(queue.audit_auth, error.status)
                except Exception:
                    return web.json_response(
                        {"error": "AUDIT_UNAVAILABLE"}, status=503, headers=HEADERS
                    )
            response = web.json_response({"error": str(error)}, status=error.status)
        except web.HTTPException as error:
            if 300 <= error.status < 400:
                response = error
            else:
                response = web.json_response({"error": "REQUEST_REJECTED"}, status=error.status)
        except ValueError:
            response = web.json_response({"error": "COMMAND_INVALID"}, status=400)
        except Exception:
            response = web.json_response({"error": "DASHBOARD_UNAVAILABLE"}, status=503)
        response.headers.update(HEADERS)
        return response

    def principal(request: web.Request, scope: str) -> Principal:
        if external is not None:
            assert sessions is not None
            return sessions.authenticate(request.cookies.get(COOKIE), scope)
        # Read-only API access is safe only behind a bound 127.0.0.1 listener
        # and an SSH tunnel/VPN with independent client authentication.
        # No ambient permissions for writes, setup, orders or control.
        if (
            read_only_loopback
            and request.method in {"GET", "HEAD"}
            and scope == "read"
            and request.path
            in {
                "/api/session",
                "/api/snapshot",
                "/readiness",
                "/api/market/symbols",
                "/api/market/quotes",
                "/api/market/chart",
                "/api/realtime",
            }
        ):
            return Principal("vps_observer", frozenset({"read"}))
        if read_only_loopback:
            raise AuthError(403)
        if auth is None:
            # Bound loopback listener with no separate auth file: grant local
            # operator scope so dashboard settings (e.g. Binance API credentials)
            # can be written without SSH-tunnel token ceremony.
            return Principal("loopback", frozenset({"read", "control"}))
        # Cookies, query strings, forwarded identity and browser form data have no authority.
        header = request.headers.get("Authorization")
        if len(request.headers.getall("Authorization", [])) != 1:
            header = None
        return auth.authenticate(header, scope)

    async def snapshot(*, wait: bool = False) -> dict[str, Any]:
        nonlocal cached, sampled
        if cached is not None and time.monotonic() - sampled < 5:
            return refresh_freshness(cached, datetime.now(UTC))
        if busy.locked() and not wait:
            raise web.HTTPTooManyRequests()
        async with busy:
            body = await run_thread(model.snapshot) if model else unavailable()
            cached = body
            sampled = time.monotonic()
            return refresh_freshness(body, datetime.now(UTC))

    async def read(request: web.Request) -> web.Response:
        principal(request, "read")
        body = await snapshot()
        status = (
            503
            if body["status"] == "STALE"
            or (request.path == "/readiness" and body["status"] != "OK")
            else 200
        )
        return web.json_response(body, status=status)

    market_cache: dict[tuple[str, ...], tuple[float, dict[str, Any]]] = {}

    def quote_rows(members: list[str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        now = datetime.now(UTC)
        state = (
            market_state()
            if market_state is not None
            else {"transport": "OK" if market_feed is not None else "UNAVAILABLE"}
        )
        available = {m.symbol: m for m in market_feed()} if market_feed is not None else {}
        eligible = set(state.get("eligible", available))
        invalid = set(state.get("invalid_symbols", ()))
        missing = set(state.get("missing_symbols", ()))
        rows = []
        for member in members:
            if member not in eligible:
                continue
            quote = available.get(member)
            row: dict[str, Any] = {
                "symbol": member,
                "price": None,
                "volume": None,
                "source_at": None,
                "volume_source_at": None,
                "source": None,
                "freshness": "STALE",
                "reason": "QUOTE_UNAVAILABLE",
                "execution_evidence": False,
            }
            if quote is not None:
                age = (now - quote.source_at).total_seconds()
                fresh = (
                    state.get("transport") == "OK"
                    and member not in invalid
                    and member not in missing
                    and 0 <= age < 15
                )
                volume_at = getattr(quote, "volume_source_at", None)
                volume_fresh = volume_at is not None and 0 <= (now - volume_at).total_seconds() < 15
                row.update(
                    price=str(quote.mark_price),
                    volume=str(quote.volume) if quote.volume is not None and volume_fresh else None,
                    source_at=quote.source_at.isoformat(),
                    volume_source_at=volume_at.isoformat() if volume_at is not None else None,
                    source=quote.source,
                    environment=quote.environment,
                    freshness="OK" if fresh else "STALE",
                    reason=None
                    if fresh
                    else (
                        "RECORD_INVALID"
                        if member in invalid
                        else "QUOTE_UNAVAILABLE"
                        if member in missing
                        else state.get("transport")
                        if state.get("transport") != "OK"
                        else "SOURCE_EXPIRED"
                    ),
                )
            rows.append(row)
        return rows, state

    async def quotes(request: web.Request) -> web.Response:
        principal(request, "read")
        # No DB or shared busy lock: this route only projects bounded RAM records.
        if set(request.query) != {"symbols"} or len(request.query.getall("symbols")) != 1:
            raise ValueError
        raw = request.query["symbols"]
        members = raw.split(",")
        if (
            len(raw) > 4000
            or not 1 <= len(members) <= 128
            or any(re.fullmatch(r"[A-Z0-9]{1,30}", member) is None for member in members)
        ):
            raise ValueError
        rows, state = quote_rows(list(dict.fromkeys(members)))
        return web.json_response(
            {
                "items": rows,
                "transport": state.get("transport", "UNAVAILABLE"),
                "execution_evidence": False,
                "poll_seconds": 0.5,
                "row_limit": 128,
            }
        )

    def market_projection(path: str, metadata: dict[str, Any]) -> dict[str, Any]:
        if path != "/api/market/symbols":
            return metadata
        body = deepcopy(metadata)
        now = datetime.now(UTC)
        overlays = {r["symbol"]: r for r in quote_rows([r["symbol"] for r in body["items"]])[0]}
        for row in body["items"]:
            if market_feed is not None:
                row.update(price=None, volume=None, source_at=None, source=None, freshness="STALE")
                row.update(overlays.get(row["symbol"], {}))
            else:
                try:
                    source_at = datetime.fromisoformat(row["source_at"])
                    fresh = 0 <= (now - source_at).total_seconds() < 15
                except (KeyError, TypeError, ValueError):
                    fresh = False
                if not fresh:
                    row.update(price=None, volume=None, freshness="STALE")
        return body

    async def market(request: web.Request) -> web.Response:
        principal(request, "read")
        return web.json_response(
            market_projection(request.path, await market_data(request.path, dict(request.query)))
        )

    async def market_data(
        path: str, query: dict[str, Any], *, wait: bool = False
    ) -> dict[str, Any]:
        if model is None:
            raise web.HTTPServiceUnavailable()
        key = (path, str(sorted(query.items())))
        cached_market = market_cache.get(key)
        if cached_market is not None and time.monotonic() - cached_market[0] < 5:
            return cached_market[1]
        if busy.locked() and not wait:
            raise web.HTTPTooManyRequests()
        async with busy:
            if path == "/api/market/symbols":
                if set(query) - {"search", "offset"}:
                    raise ValueError
                body = await run_thread(
                    model.terminal_symbols,
                    search=query.get("search", ""),
                    offset=int(query.get("offset", "0")),
                )
            else:
                if set(query) != {"symbol", "timeframe"}:
                    raise ValueError
                body = await run_thread(model.terminal_chart, query["symbol"], query["timeframe"])
                if chart_demand is not None:
                    try:
                        await chart_demand(query["symbol"], query["timeframe"])
                    except Exception:
                        pass  # Stored bars retain their original timestamps on failure.
        if len(market_cache) >= 16:
            market_cache.pop(next(iter(market_cache)))
        market_cache[key] = time.monotonic(), body
        return body

    async def socket_read(operation: str, params: dict[str, Any]) -> dict[str, Any]:
        if operation == "snapshot":
            return await snapshot(wait=True)
        path = "/api/market/" + operation
        data = await market_data(path, params, wait=True)
        # Apply exactly the same source freshness / RAM overlay as HTTP readers.
        return market_projection(path, data)

    async def session(request: web.Request) -> web.Response:
        person = principal(request, "read")
        body: dict[str, Any] = {"subject": person.subject, "scopes": sorted(person.scopes)}
        return web.json_response(body)

    async def request_chart(request: web.Request) -> web.Response:
        person = principal(request, "read")
        if queue is None:
            raise web.HTTPServiceUnavailable()
        if busy.locked():
            raise web.HTTPTooManyRequests()
        body = await request.json()
        async with busy:
            await run_thread(queue.request_chart, person, body)
        return web.json_response({"status": "REQUESTED"}, status=202)

    async def health(request: web.Request) -> web.Response:
        return web.json_response(
            {
                "service": "dashboard",
                "phase": "PHASE_16",
                "mode": "paper",
                "alive": True,
                "live": False,
                "orders_enabled": False,
            }
        )

    async def asset(request: web.Request) -> web.Response:
        name = request.match_info.get("name", "index.html")
        # Only the login presentation is public; terminal assets retain session checks.
        if external is not None and name not in {"login.css", "login.js"}:
            try:
                principal(request, "read")
            except AuthError:
                raise web.HTTPSeeOther("/login") from None
        content_type = {
            "index.html": "text/html",
            "app.js": "text/javascript",
            "binance_settings.js": "text/javascript",
            "app.css": "text/css",
            "chart.js": "text/javascript",
            "realtime.js": "text/javascript",
            "login.css": "text/css",
            "login.js": "text/javascript",
        }
        if name not in content_type:
            raise web.HTTPNotFound()
        return web.Response(
            body=(STATIC / name).read_bytes(), content_type=content_type[name], charset="utf-8"
        )

    async def login(request: web.Request) -> web.Response:
        if sessions is None or external is None:
            raise web.HTTPNotFound()
        if request.method == "GET":
            return web.Response(
                body=(STATIC / "login.html").read_bytes(),
                content_type="text/html",
                charset="utf-8",
            )
        body = await request.post()
        token = body.get("token")
        if not isinstance(token, str):
            raise AuthError(401)
        cookie = sessions.login(token)
        response = web.Response(status=303, headers={"Location": "/"})
        response.set_cookie(
            COOKIE,
            cookie,
            max_age=sessions.lifetime,
            secure=True,
            httponly=True,
            samesite="Strict",
            path="/",
        )
        return response

    async def logout(request: web.Request) -> web.Response:
        principal(request, "read")
        response = web.Response(status=303, headers={"Location": "/login"})
        response.del_cookie(COOKIE, path="/", secure=True, httponly=True, samesite="Strict")
        return response

    _BINANCE_ERROR_CODES = frozenset(
        {
            "BINANCE_SETTINGS_INVALID",
            "BINANCE_TEST_INVALID",
            "BINANCE_CREDENTIALS_INVALID",
            "BINANCE_ENVIRONMENT_INVALID",
            "BINANCE_ENCRYPTION_SECRET_INVALID",
            "BINANCE_OPEN_EXPOSURE_BLOCKS_SWITCH",
            "BINANCE_UNPARTITIONED_HISTORY_BLOCKS_SWITCH",
            "BINANCE_RISK_KILL_SWITCH_REQUIRED",
        }
    )

    def binance_failure(error: ValueError) -> web.Response:
        # Only allowlisted codes reach the browser; unexpected ValueError
        # details stay server-side. Status stays 400 for a rejected command.
        code = str(error) if str(error) in _BINANCE_ERROR_CODES else "BINANCE_SETTINGS_INVALID"
        return web.json_response({"error": code}, status=400)

    def clean_text(value: Any) -> Any:
        # Pasted keys often carry a trailing newline: surrounding blanks are
        # never part of a Binance key/secret, while inner blanks are still
        # rejected by the domain validation.
        return value.strip() if isinstance(value, str) else value

    async def binance_settings(request: web.Request) -> web.Response:
        person = principal(request, "control")
        if model is None or binance_master is None:
            raise web.HTTPServiceUnavailable()
        from tradeforge.runtime.binance_environment import public_state

        person.require("control")
        try:
            state = await run_thread(public_state, model.db)
        except ValueError as error:
            return binance_failure(error)
        return web.json_response(state)

    async def binance_write(request: web.Request) -> web.Response:
        principal(request, "control")
        if model is None or binance_master is None:
            raise web.HTTPServiceUnavailable()
        if busy.locked():
            raise web.HTTPTooManyRequests()
        try:
            body = await request.json()
        except (ValueError, TypeError):
            return binance_failure(ValueError("BINANCE_SETTINGS_INVALID"))
        if not isinstance(body, dict):
            return binance_failure(ValueError("BINANCE_SETTINGS_INVALID"))
        from tradeforge.runtime.binance_environment import (
            public_state,
            switch_environment,
            update_credentials,
        )

        try:
            async with busy:
                if set(body) == {"environment", "api_key", "api_secret"}:
                    await run_thread(
                        update_credentials,
                        model.db,
                        binance_master,
                        clean_text(body["environment"]),
                        clean_text(body["api_key"]),
                        clean_text(body["api_secret"]),
                    )
                elif set(body) == {"environment"}:
                    await run_thread(switch_environment, model.db, clean_text(body["environment"]))
                else:
                    raise ValueError("BINANCE_SETTINGS_INVALID")
            state = await run_thread(public_state, model.db)
        except ValueError as error:
            return binance_failure(error)
        return web.json_response(state, headers={"Cache-Control": "no-store"})

    async def binance_test(request: web.Request) -> web.Response:
        principal(request, "control")
        if model is None or binance_master is None:
            raise web.HTTPServiceUnavailable()
        try:
            body = await request.json()
        except (ValueError, TypeError):
            return binance_failure(ValueError("BINANCE_TEST_INVALID"))
        if not isinstance(body, dict) or set(body) != {"environment"}:
            return binance_failure(ValueError("BINANCE_TEST_INVALID"))
        from tradeforge.runtime.binance_environment import (
            ENDPOINTS,
            load_credentials,
            record_connection_test,
        )

        environment = clean_text(body["environment"])
        if not isinstance(environment, str) or environment not in ENDPOINTS:
            return binance_failure(ValueError("BINANCE_TEST_INVALID"))

        # Only fixed, credential-free codes are reported so the operator learns
        # whether to fix the key, the permissions, the clock or the network.
        rejection_reasons = {
            "API_KEY_INVALID": "BINANCE_TEST_API_KEY_INVALID",
            "API_KEY_PERMISSION_OR_IP_DENIED": "BINANCE_TEST_PERMISSION_DENIED",
            "SIGNATURE_INVALID": "BINANCE_TEST_SIGNATURE_INVALID",
            "CLOCK_UNSAFE": "BINANCE_TEST_CLOCK_UNSAFE",
            "RATE_LIMITED": "BINANCE_TEST_RATE_LIMITED",
        }

        def verify() -> tuple[bool, str | None]:
            from tradeforge.execution.binance_client import (
                BinanceLiveReadOnlyClient,
                BinanceTestnetClient,
                Credentials,
            )
            from tradeforge.execution.broker import AckUnknown, ExchangeRejected

            credentials = load_credentials(model.db, binance_master, environment)
            if credentials is None:
                return False, "BINANCE_TEST_CREDENTIALS_MISSING"
            try:
                pair = Credentials.model_validate(
                    {"api_key": credentials[0], "api_secret": credentials[1]}
                )
                if environment == "testnet":
                    client = BinanceTestnetClient(
                        pair, mode="testnet", opted_in=True, read_only=True
                    )
                else:
                    client = BinanceLiveReadOnlyClient(pair)
                client.request("GET", "/fapi/v1/ping", signed=False)
                balance = client.request("GET", "/fapi/v3/balance")
                positions = client.request("GET", "/fapi/v3/positionRisk")
            except ExchangeRejected as error:
                return False, rejection_reasons.get(str(error), "BINANCE_TEST_REJECTED")
            except AckUnknown:
                return False, "BINANCE_TEST_NETWORK_UNAVAILABLE"
            except ValueError:
                return False, "BINANCE_TEST_CREDENTIALS_INVALID"
            except Exception:
                return False, "BINANCE_TEST_FAILED"  # Never surface wire data or secrets.
            if not isinstance(balance, list) or not isinstance(positions, list):
                return False, "BINANCE_TEST_RESPONSE_INVALID"
            return True, None

        try:
            connected, reason = await run_thread(verify)
            await run_thread(record_connection_test, model.db, environment, connected)
        except ValueError as error:
            return binance_failure(error)
        return web.json_response(
            {"environment": environment, "connected": connected, "reason": reason},
            headers={"Cache-Control": "no-store"},
        )

    app = web.Application(middlewares=[boundary], client_max_size=4096)
    app.router.add_get("/login", login)
    app.router.add_post("/login", login)
    app.router.add_post("/logout", logout)
    app.router.add_get("/", asset)
    app.router.add_get("/assets/{name}", asset)
    app.router.add_get("/health", health)
    app.router.add_get("/readiness", read)
    app.router.add_get("/api/snapshot", read)
    app.router.add_get("/api/market/quotes", quotes)
    app.router.add_get("/api/market/symbols", market)
    app.router.add_get("/api/market/chart", market)
    app.router.add_post("/api/market/chart", request_chart)
    app.router.add_get("/api/session", session)
    app.router.add_get("/api/binance/settings", binance_settings)
    app.router.add_post("/api/binance/settings", binance_write)
    app.router.add_post("/api/binance/test", binance_test)
    RealtimeHub(
        model,
        sessions=sessions,
        origin=external_origin,
        auth=auth,
        read_only_loopback=read_only_loopback,
        market_feed=market_feed,
        market_state=market_state,
        read=socket_read,
    ).install(app)
    return app
