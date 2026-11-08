"""Authenticated, bounded dashboard projection transport.

The hub shares one snapshot sampler across viewers and uses RAM market handoffs
for quote notifications. No stream authorizes orders or changes trading state.
"""

import asyncio
import json
import logging
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from aiohttp import WSMsgType, web

from tradeforge.dashboard.auth import Authenticator, AuthError, Principal
from tradeforge.dashboard.read_model import ReadModel, refresh_freshness
from tradeforge.dashboard.sessions import COOKIE, Sessions
from tradeforge.domain.market import MarketQuote, MarketSnapshot

LOGGER = logging.getLogger(__name__)
STATIC = Path(__file__).with_name("static")
MEMBER = re.compile(r"[A-Z0-9]{1,30}\Z")
MAX_CLIENTS = 32
MAX_SYMBOLS = 128
SNAPSHOT_INTERVAL = 5.0
QUOTE_INTERVAL = 0.5


@dataclass(eq=False)
class Viewer:
    socket: web.WebSocketResponse
    symbols: tuple[str, ...] = ()
    quote_fingerprint: str | None = None
    sequence: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    request: web.Request | None = None
    subscriptions: dict[str, tuple[int, dict[str, Any]]] = field(default_factory=dict)
    fingerprints: dict[str, str] = field(default_factory=dict)


class RealtimeHub:
    """One projection publisher sharing canonical admission."""

    def __init__(
        self,
        model: ReadModel | None,
        *,
        sessions: Sessions | None = None,
        origin: str | None = None,
        auth: Authenticator | None = None,
        read_only_loopback: bool = False,
        market_feed: Callable[[], tuple[MarketQuote | MarketSnapshot, ...]] | None = None,
        market_state: Callable[[], dict[str, Any]] | None = None,
        read: Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]] | None = None,
    ) -> None:
        self.model = model
        self.sessions = sessions
        self.origin = origin
        self.auth = auth
        self.read_only_loopback = read_only_loopback
        self.market_feed = market_feed
        self.market_state = market_state
        self.read = read
        self.viewers: set[Viewer] = set()
        self.snapshot_lock = asyncio.Lock()
        self.snapshot_body: dict[str, Any] | None = None
        self.snapshot_fingerprint: str | None = None
        self.quote_dirty = asyncio.Event()
        self.tasks: list[asyncio.Task[None]] = []

    def install(self, app: web.Application) -> None:
        app.router.add_get("/api/realtime", self.websocket)
        app.router.add_get("/realtime.js", self.asset)
        app.on_cleanup.append(self.close)
        app.on_response_prepare.append(self.allow_same_origin_socket)

    async def allow_same_origin_socket(
        self, request: web.Request, response: web.StreamResponse
    ) -> None:
        policy = response.headers.get("Content-Security-Policy", "")
        if (
            getattr(request, "path", "/") in {"/", "/assets/app.js", "/assets/realtime.js"}
            and "connect-src 'self';" in policy
        ):
            # CSP 'self' is not consistently interpreted as wss: by browsers.
            authority = urlsplit(self.origin).netloc if self.origin else request.host
            scheme = "wss" if self.origin else "ws"
            response.headers["Content-Security-Policy"] = policy.replace(
                "connect-src 'self';",
                f"connect-src 'self' {scheme}://{authority};",
            )

    def authorize(self, request: web.Request) -> Principal:
        # A WebSocket is a GET upgrade: cookie-bound sockets must reject absent
        # and cross-site Origins, not rely on the HTTP write CSRF rule.
        if self.sessions is not None:
            if request.headers.get("Origin") != self.origin:
                raise AuthError(403)
            return self.sessions.authenticate(request.cookies.get(COOKIE), "read")
        elif self.read_only_loopback:
            # The containing HTTP middleware enforces the loopback binding/Host.
            if request.headers.get("Origin") not in (None, f"http://{request.host}"):
                raise AuthError(403)
            return Principal("vps_observer", frozenset({"read"}))
        elif self.auth is not None:
            headers = request.headers.getall("Authorization", [])
            return self.auth.authenticate(headers[0] if len(headers) == 1 else None, "read")
        else:
            # Local operator listener (apps/dashboard/runtime.py without a
            # separate auth file): the containing middleware binds the peer
            # and Host to loopback, mirroring dashboard.api.principal(). The
            # same-origin check keeps cross-site upgrades rejected even when
            # this hub is installed standalone.
            if request.headers.get("Origin") not in (None, f"http://{request.host}"):
                raise AuthError(403)
            return Principal("loopback", frozenset({"read", "control"}))

    async def asset(self, request: web.Request) -> web.Response:
        # A regular script fetch does not carry an Origin header.
        if self.sessions is not None:
            self.sessions.authenticate(request.cookies.get(COOKIE), "read")
        elif not self.read_only_loopback and self.auth is not None:
            headers = request.headers.getall("Authorization", [])
            self.auth.authenticate(headers[0] if len(headers) == 1 else None, "read")
        # Loopback with no auth file and read-only mode serve the public static
        # asset; the containing middleware still constrains peer and Host.
        return web.Response(
            body=(STATIC / "realtime.js").read_bytes(),
            content_type="text/javascript",
            charset="utf-8",
        )

    async def emit(self, viewer: Viewer, event: dict[str, Any]) -> None:
        if self.sessions is not None and viewer.request is not None:
            try:
                self.authorize(viewer.request)
            except AuthError:
                await viewer.socket.close(code=1008, message=b"AUTH_REQUIRED")
                return
        async with viewer.lock:
            viewer.sequence += 1
            try:
                await asyncio.wait_for(
                    viewer.socket.send_json({"seq": viewer.sequence, **event}),
                    timeout=2.0,
                )
            except (ConnectionError, RuntimeError, TimeoutError):
                await viewer.socket.close()

    def ensure_tasks(self) -> None:
        if not self.tasks:
            self.tasks = [
                asyncio.create_task(self.sample_snapshots()),
                asyncio.create_task(self.publish_quotes()),
                asyncio.create_task(self.publish_views()),
            ]

    async def sample_once(self) -> None:
        # One snapshot read per 5 s for all viewers; source freshness is
        # revalidated before publishing, even when a DB record is unchanged.
        if self.snapshot_lock.locked():
            return
        async with self.snapshot_lock:
            try:
                if self.read is not None:
                    raw = await self.read("snapshot", {})
                elif self.model is not None:
                    raw = await asyncio.to_thread(self.model.snapshot)
                else:
                    raise RuntimeError("READ_MODEL_UNAVAILABLE")
                body = refresh_freshness(raw, datetime.now(UTC))
            except Exception:
                LOGGER.warning("realtime_snapshot_unavailable")
                body = {
                    "status": "STALE",
                    "reason": "READ_MODEL_UNAVAILABLE",
                    "sections": {},
                    "service_health": {"dashboard": "OK", "postgresql": "NOT_READY"},
                    "data_readiness": {"status": "UNKNOWN"},
                    "orders_enabled": False,
                }
            fingerprint = json.dumps(body, ensure_ascii=False, sort_keys=True, default=str)
            self.snapshot_body = body
            if fingerprint != self.snapshot_fingerprint:
                self.snapshot_fingerprint = fingerprint
                await asyncio.gather(
                    *(
                        self.emit(v, {"type": "snapshot", "data": body})
                        for v in tuple(self.viewers)
                    ),
                    return_exceptions=True,
                )
            else:
                await asyncio.gather(
                    *(self.emit(v, {"type": "heartbeat"}) for v in tuple(self.viewers)),
                    return_exceptions=True,
                )

    async def sample_snapshots(self) -> None:
        while True:
            if self.viewers:
                await self.sample_once()
                await asyncio.sleep(SNAPSHOT_INTERVAL)
            else:
                await asyncio.sleep(1)

    def quotes(self, symbols: tuple[str, ...]) -> dict[str, Any]:
        state = (
            self.market_state()
            if self.market_state is not None
            else {"transport": "OK" if self.market_feed is not None else "UNAVAILABLE"}
        )
        available = (
            {record.symbol: record for record in self.market_feed()}
            if self.market_feed is not None
            else {}
        )
        eligible = set(state.get("eligible", available))
        invalid = set(state.get("invalid_symbols", ()))
        missing = set(state.get("missing_symbols", ()))
        now = datetime.now(UTC)
        rows = []
        for name in symbols:
            if name not in eligible:
                continue
            row: dict[str, Any] = {
                "symbol": name,
                "price": None,
                "volume": None,
                "source_at": None,
                "volume_source_at": None,
                "source": None,
                "freshness": "STALE",
                "reason": "QUOTE_UNAVAILABLE",
                "execution_evidence": False,
            }
            quote = available.get(name)
            if quote is not None:
                age = (now - quote.source_at).total_seconds()
                fresh = (
                    state.get("transport") == "OK"
                    and name not in invalid
                    and name not in missing
                    and 0 <= age < 15
                )
                volume_at = getattr(quote, "volume_source_at", None)
                volume_fresh = volume_at is not None and 0 <= (now - volume_at).total_seconds() < 15
                row.update(
                    price=str(quote.mark_price),
                    volume=str(quote.volume) if quote.volume is not None and volume_fresh else None,
                    source_at=quote.source_at.isoformat(),
                    volume_source_at=volume_at.isoformat() if volume_at else None,
                    source=quote.source,
                    environment=quote.environment,
                    freshness="OK" if fresh else "STALE",
                    reason=(
                        None
                        if fresh
                        else "RECORD_INVALID"
                        if name in invalid
                        else "QUOTE_UNAVAILABLE"
                        if name in missing
                        else state.get("transport")
                        if state.get("transport") != "OK"
                        else "SOURCE_EXPIRED"
                    ),
                )
            rows.append(row)
        return {
            "type": "quotes",
            "items": rows,
            "transport": state.get("transport", "UNAVAILABLE"),
            "execution_evidence": False,
        }

    async def send_quotes(self, viewer: Viewer) -> None:
        if not viewer.symbols:
            return
        data = self.quotes(viewer.symbols)
        fingerprint = json.dumps(data, sort_keys=True, default=str)
        if fingerprint != viewer.quote_fingerprint:
            viewer.quote_fingerprint = fingerprint
            await self.emit(viewer, data)

    async def publish_quotes(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self.quote_dirty.wait(), timeout=1.0)
            except TimeoutError:
                pass  # Expire source timestamps even when the upstream is silent.
            self.quote_dirty.clear()
            await asyncio.sleep(QUOTE_INTERVAL)
            await asyncio.gather(
                *(self.send_quotes(v) for v in tuple(self.viewers)),
                return_exceptions=True,
            )

    def notify_quotes(self) -> None:
        self.quote_dirty.set()

    async def publish_views(self) -> None:
        while True:
            await asyncio.sleep(SNAPSHOT_INTERVAL)
            for viewer in tuple(self.viewers):
                for operation, (identifier, params) in tuple(viewer.subscriptions.items()):
                    await self.project(viewer, identifier, operation, params, push=True)

    async def project(
        self,
        viewer: Viewer,
        identifier: int,
        operation: str,
        params: dict[str, Any],
        *,
        push: bool = False,
    ) -> None:
        try:
            if self.read is None:
                raise web.HTTPServiceUnavailable()
            data = await self.read(operation, params)
            frame = {"id": identifier, "ok": True, "data": data}
        except (ValueError, web.HTTPException) as error:
            frame = {
                "id": identifier,
                "ok": False,
                "status": getattr(error, "status", 400),
                "error": "PROJECTION_UNAVAILABLE",
            }
        except Exception:
            frame = {
                "id": identifier,
                "ok": False,
                "status": 503,
                "error": "PROJECTION_UNAVAILABLE",
            }
        # A user action may supersede a slow projection while it holds DB admission.
        if viewer.subscriptions.get(operation) != (identifier, params):
            return
        fingerprint = json.dumps(frame, sort_keys=True, default=str)
        if not push or viewer.fingerprints.get(operation) != fingerprint:
            viewer.fingerprints[operation] = fingerprint
            await self.emit(viewer, {"type": operation if push else "response", **frame})

    @staticmethod
    def validate_request(data: dict[str, Any]) -> tuple[int, str, dict[str, Any]]:
        if set(data) != {"type", "id", "op", "params"} or data["type"] != "request":
            raise ValueError
        identifier, operation, params = data["id"], data["op"], data["params"]
        if (
            type(identifier) is not int
            or not 1 <= identifier <= 2**53 - 1
            or not isinstance(params, dict)
        ):
            raise ValueError
        if operation == "symbols":
            if (
                set(params) != {"search", "offset"}
                or not isinstance(params["search"], str)
                or re.fullmatch(r"[A-Za-z0-9_]{0,40}", params["search"]) is None
                or type(params["offset"]) is not int
                or not 0 <= params["offset"] <= 10000
            ):
                raise ValueError
        elif operation == "chart":
            if (
                set(params) != {"symbol", "timeframe"}
                or not isinstance(params["symbol"], str)
                or MEMBER.fullmatch(params["symbol"]) is None
                or params["timeframe"] not in ("1m", "5m", "15m", "1h", "4h")
            ):
                raise ValueError
        elif operation == "snapshot":
            if params:
                raise ValueError
        elif operation == "quotes":
            if (
                set(params) != {"symbols"}
                or not isinstance(params["symbols"], list)
                or len(params["symbols"]) > MAX_SYMBOLS
                or any(
                    not isinstance(s, str) or MEMBER.fullmatch(s) is None for s in params["symbols"]
                )
            ):
                raise ValueError
        else:
            raise ValueError
        return identifier, operation, params

    async def requests(self, viewer: Viewer, inbox: asyncio.Queue[dict[str, Any]]) -> None:
        assert viewer.request is not None
        while True:
            data = await inbox.get()
            identifier, operation, params = self.validate_request(data)
            try:
                self.authorize(viewer.request)
                if operation == "snapshot":
                    await self.sample_once()
                    await self.emit(
                        viewer,
                        {
                            "type": "response",
                            "id": identifier,
                            "ok": True,
                            "data": self.snapshot_body,
                        },
                    )
                else:
                    if viewer.subscriptions.get(operation) == (identifier, params):
                        await self.project(viewer, identifier, operation, params)
            except AuthError as error:
                if error.status == 401:
                    await viewer.socket.close(code=1008, message=b"AUTH_REQUIRED")
                    return
                await self.emit(
                    viewer,
                    {
                        "type": "response",
                        "id": identifier,
                        "ok": False,
                        "status": error.status,
                        "error": str(error),
                    },
                )
            except Exception as error:
                await self.emit(
                    viewer,
                    {
                        "type": "response",
                        "id": identifier,
                        "ok": False,
                        "status": getattr(
                            error, "status", 400 if isinstance(error, ValueError) else 503
                        ),
                        "error": "COMMAND_REJECTED",
                    },
                )

    async def websocket(self, request: web.Request) -> web.StreamResponse:
        person = self.authorize(request)
        if len(self.viewers) >= MAX_CLIENTS:
            raise web.HTTPServiceUnavailable()
        socket = web.WebSocketResponse(
            heartbeat=20,
            max_msg_size=4096,
            autoping=True,
            compress=False,
        )
        await socket.prepare(request)
        viewer = Viewer(socket, request=request)
        self.viewers.add(viewer)
        inbox: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=8)
        worker = asyncio.create_task(self.requests(viewer, inbox))
        try:
            await self.emit(
                viewer,
                {
                    "type": "hello",
                    "protocol": 2,
                    "session": {"subject": person.subject, "scopes": sorted(person.scopes)},
                },
            )
            if self.snapshot_body is not None:
                body = refresh_freshness(self.snapshot_body, datetime.now(UTC))
                await self.emit(viewer, {"type": "snapshot", "data": body})
            else:
                await self.sample_once()
            self.ensure_tasks()
            window, messages = time.monotonic(), 0
            async for msg in socket:
                if msg.type != WSMsgType.TEXT:
                    if msg.type == WSMsgType.ERROR:
                        break
                    continue
                try:
                    self.authorize(request)
                    data = json.loads(msg.data)
                    now = time.monotonic()
                    if now - window >= 1:
                        window, messages = now, 0
                    messages += 1
                    if messages > 40:
                        await socket.close(code=1008, message=b"RATE_LIMITED")
                        break
                    if isinstance(data, dict) and "type" in data and data["type"] == "request":
                        try:
                            identifier, operation, params = self.validate_request(data)
                        except ValueError:
                            invalid_id = data["id"] if "id" in data else None
                            if type(invalid_id) is not int or not 1 <= invalid_id <= 2**53 - 1:
                                raise
                            await self.emit(
                                viewer,
                                {
                                    "type": "response",
                                    "id": invalid_id,
                                    "ok": False,
                                    "status": 400,
                                    "error": "REQUEST_INVALID",
                                },
                            )
                            continue
                        if operation == "quotes":
                            viewer.symbols = tuple(dict.fromkeys(params["symbols"]))
                            viewer.quote_fingerprint = None
                            await self.emit(
                                viewer,
                                {
                                    "type": "response",
                                    "id": identifier,
                                    "ok": True,
                                    "data": self.quotes(viewer.symbols),
                                },
                            )
                            continue
                        if operation in {"symbols", "chart"}:
                            viewer.subscriptions[operation] = identifier, params
                        inbox.put_nowait(data)
                        continue
                    if (
                        not isinstance(data, dict)
                        or set(data) != {"type", "symbols"}
                        or data["type"] != "subscribe"
                        or not isinstance(data["symbols"], list)
                        or len(data["symbols"]) > MAX_SYMBOLS
                        or any(
                            not isinstance(s, str) or MEMBER.fullmatch(s) is None
                            for s in data["symbols"]
                        )
                    ):
                        raise ValueError
                except (ValueError, TypeError, asyncio.QueueFull):
                    await socket.close(code=1008, message=b"INVALID_SUBSCRIPTION")
                    break
                viewer.symbols = tuple(dict.fromkeys(data["symbols"]))
                viewer.quote_fingerprint = None
                await self.send_quotes(viewer)
        finally:
            self.viewers.discard(viewer)
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            await socket.close()
        return socket

    async def close(self, _: web.Application) -> None:
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()
        await asyncio.gather(
            *(viewer.socket.close(code=1001) for viewer in tuple(self.viewers)),
            return_exceptions=True,
        )
        self.viewers.clear()
