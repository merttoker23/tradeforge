"""Bounded credential-free WebSocket transport, generation fencing and reconnects."""

import asyncio
import math
import random
import time
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Literal, Protocol
from urllib.parse import urlsplit

import aiohttp

from tradeforge.market.exchange_info import FetchError, decode_payload, milliseconds
from tradeforge.market.streams import MarketEvent, Route, StreamSpec, decode_frame


class StreamFailure(RuntimeError):
    def __init__(self, reason: str, retry_after: float = 0) -> None:
        super().__init__(reason)
        self.retry_after = retry_after


def retry_delay(status: int, header: str) -> float:
    minimum = 300.0 if status == 418 else 60.0
    try:
        retry = float(header)
    except ValueError:
        try:
            retry = (parsedate_to_datetime(header) - datetime.now(UTC)).total_seconds()
        except (ValueError, TypeError, OverflowError):
            retry = minimum
    return max(minimum, retry) if math.isfinite(retry) else minimum


@dataclass(frozen=True)
class WireFrame:
    kind: Literal["TEXT", "PING", "PONG", "CLOSE"]
    data: str | bytes = b""
    close_code: int | None = None


class MarketSocket(Protocol):
    async def send_json(self, value: dict[str, object]) -> None: ...
    async def receive(self) -> WireFrame: ...
    async def pong(self, data: bytes) -> None: ...


class _Socket:
    def __init__(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        self.ws = ws

    async def send_json(self, value: dict[str, object]) -> None:
        await self.ws.send_json(value)

    async def receive(self) -> WireFrame:
        frame = await self.ws.receive()
        if frame.type == aiohttp.WSMsgType.TEXT and isinstance(frame.data, str):
            return WireFrame("TEXT", frame.data)
        if frame.type == aiohttp.WSMsgType.PING:
            return WireFrame("PING", bytes(frame.data))
        if frame.type == aiohttp.WSMsgType.PONG:
            return WireFrame("PONG", bytes(frame.data))
        return WireFrame("CLOSE", close_code=self.ws.close_code)

    async def pong(self, data: bytes) -> None:
        await self.ws.pong(data)


class AiohttpConnector:
    """Fixed public hosts; the only override is HTTP loopback for disposable tests.

    No auth/header/key argument. Environment proxies, netrc and cookies disabled.
    A redirect is rejected by the trace hook before a subsequent request is made.
    """

    def __init__(self, *, base_url: str = "https://fstream.binance.com") -> None:
        parsed = urlsplit(base_url)
        if base_url not in {
            "https://fstream.binance.com",
            "https://demo-fstream.binance.com",
        } and not (
            parsed.scheme == "http"
            and parsed.hostname == "127.0.0.1"
            and parsed.port
            and parsed.path == ""
            and not parsed.query
            and not parsed.fragment
            and not parsed.username
            and not parsed.password
        ):
            raise ValueError("UNSAFE_MARKET_ENDPOINT")
        self.base_url = base_url
        self.session: aiohttp.ClientSession | None = None
        self.clock_not_before = 0.0

    def stream_url(self, route: Route) -> str:
        """Keep public depth and market data on their respective exchange routes."""
        if route not in ("market", "public"):
            raise ValueError("PUBLIC_MARKET_ROUTE_REQUIRED")
        return f"{self.base_url}/{route}/stream"

    async def __aenter__(self) -> "AiohttpConnector":
        async def reject_redirect(*args: object) -> None:
            raise StreamFailure("MARKET_REDIRECT_REJECTED")

        trace = aiohttp.TraceConfig()
        trace.on_request_redirect.append(reject_redirect)
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15),
            trust_env=False,
            cookie_jar=aiohttp.DummyCookieJar(),
            trace_configs=[trace],
        )
        return self

    async def __aexit__(self, *args: object) -> None:
        if self.session is not None:
            await self.session.close()

    @asynccontextmanager
    async def open(self, route: Route) -> AsyncIterator[MarketSocket]:
        if route not in ("market", "public") or self.session is None:
            raise ValueError("PUBLIC_MARKET_ROUTE_REQUIRED")
        try:
            async with self.session.ws_connect(
                self.stream_url(route),
                autoping=False,
                heartbeat=None,
                compress=0,
                max_msg_size=4 * 1024 * 1024,
                timeout=aiohttp.ClientWSTimeout(ws_close=2),
            ) as ws:
                yield _Socket(ws)
        except aiohttp.WSServerHandshakeError as error:
            if error.status in (429, 418):
                header = error.headers.get("Retry-After", "") if error.headers else ""
                raise StreamFailure("WS_RATE_LIMITED", retry_delay(error.status, header)) from None
            raise StreamFailure("WS_HANDSHAKE_FAILED") from None
        except (aiohttp.ClientError, OSError, TimeoutError):
            raise StreamFailure("WS_NETWORK_FAILED") from None

    async def server_time(self) -> datetime:
        if self.session is None:
            raise ValueError("CONNECTOR_NOT_OPEN")
        cooldown = self.clock_not_before - time.monotonic()
        if cooldown > 0:
            raise StreamFailure("CLOCK_RATE_LIMITED", cooldown)
        base = (
            "https://demo-fapi.binance.com"
            if self.base_url == "https://demo-fstream.binance.com"
            else "https://fapi.binance.com"
            if self.base_url.startswith("https:")
            else self.base_url
        )
        async with self.session.get(f"{base}/fapi/v1/time", allow_redirects=False) as response:
            if response.status in (429, 418):
                delay = retry_delay(response.status, response.headers.get("Retry-After", ""))
                self.clock_not_before = time.monotonic() + delay
                raise StreamFailure("CLOCK_RATE_LIMITED", delay)
            if response.status != 200:
                raise StreamFailure("CLOCK_UNAVAILABLE")
            body = await response.content.read(1025)
            if len(body) > 1024:
                raise StreamFailure("CLOCK_INVALID")
            payload = decode_payload(body)
            if not isinstance(payload, dict):
                raise StreamFailure("CLOCK_INVALID")
            return milliseconds(payload.get("serverTime"))


async def pause(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), max(0.001, seconds))
    except TimeoutError:
        pass


def _discard(task: asyncio.Future[WireFrame]) -> None:
    """Retrieve a finished transport task's outcome so it is never reported."""
    if not task.cancelled():
        task.exception()


class SocketManager:
    def __init__(
        self,
        streams: tuple[StreamSpec, ...],
        connect: Callable[[Route], AbstractAsyncContextManager[MarketSocket]],
        *,
        eligible: Callable[[], frozenset[str]],
        on_event: Callable[[MarketEvent], None],
        on_state: Callable[[str, int], None],
        on_streams: Callable[[tuple[StreamSpec, ...], tuple[StreamSpec, ...], int], None]
        | None = None,
        on_invalid_stream: Callable[[StreamSpec, int], None] | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
        jitter: Callable[[], float] = random.random,
        retry_base: float = 1,
        rotate_after: float = 23 * 3600 + 50 * 60,
        ack_timeout: float = 5,
        data_timeout: float | None = None,
        max_future: float = 0,
        clock_ready: Callable[[], bool] | None = None,
    ) -> None:
        if not streams or len(streams) > 1024 or len({s.route for s in streams}) != 1:
            raise ValueError("INVALID_SOCKET_GROUP")
        if not 0 < rotate_after < 86400 or not 0 < ack_timeout <= 30 or not 0 < retry_base <= 30:
            raise ValueError("INVALID_SOCKET_POLICY")
        if data_timeout is not None and not 0 < data_timeout <= 60:
            raise ValueError("INVALID_SOCKET_POLICY")
        self.streams, self.connect, self.eligible = streams, connect, eligible
        self.on_event, self.on_state = on_event, on_state
        self.on_streams = on_streams or (lambda added, removed, generation: None)
        self.on_invalid_stream = on_invalid_stream
        self.clock, self.monotonic, self.jitter = clock, monotonic, jitter
        self.retry_base, self.rotate_after, self.ack_timeout = retry_base, rotate_after, ack_timeout
        self.data_timeout = data_timeout
        self.max_future = max_future
        self.clock_ready = clock_ready or (lambda: True)
        self.generation = self.failures = 0
        self.next_control = 0.0
        self.reconnects = 0
        self.reason = "NOT_CONNECTED"
        # The desired stream set may change while the connection stays open.
        # A discretionary rebalance must not restart the retained streams.
        self._desired: tuple[StreamSpec, ...] = tuple(streams)
        self._pending = False
        # Streams removed by a live delta may still deliver a final frame.
        # They are ignored, never treated as a protocol violation.
        self._draining: set[str] = set()

    def update(self, streams: tuple[StreamSpec, ...]) -> None:
        """Replace the desired stream set without dropping the connection.

        The retained streams keep their subscription, so their book, flow and
        candle evidence is never interrupted by a discretionary rebalance.
        """
        if tuple(streams) != self._desired:
            self._desired = tuple(streams)
            self._pending = True

    async def _control(self) -> None:
        # Includes explicit PONGs. Four/s leaves headroom under Binance's ten/s.
        delay = self.next_control - self.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)
        self.next_control = self.monotonic() + 0.25

    async def run(self, stop: asyncio.Event) -> None:
        failures = 0
        try:
            while not stop.is_set():
                self.generation += 1
                self.on_state("SYNCING", self.generation)
                retry_after = 0.0
                pending: asyncio.Future[WireFrame] | None = None
                try:
                    # The connection waits for a trusted clock. Market evidence
                    # of a skewed clock is rejected, and a rejected event would
                    # restart the feed in a loop.
                    while not self.clock_ready():
                        if stop.is_set():
                            return
                        await asyncio.sleep(0.05)
                    async with self.connect(self.streams[0].route) as ws:
                        await self._control()
                        began = self.monotonic()
                        request_id = self.generation
                        await asyncio.wait_for(
                            ws.send_json(
                                {
                                    "method": "SUBSCRIBE",
                                    "params": [s.name for s in self._desired],
                                    "id": request_id,
                                }
                            ),
                            5,
                        )
                        acknowledged, sequence = False, 0
                        subscribed = {s.name for s in self._desired}
                        invalidated: set[str] = set()
                        last_valid = self.monotonic()
                        last_by_stream: dict[str, float] = {}
                        acknowledged_at = began
                        pending = asyncio.ensure_future(ws.receive())
                        pending.add_done_callback(_discard)
                        while not stop.is_set():
                            # A buffered receive (and ACK/ignored frame) may not
                            # suspend on Python 3.12. Leave probes and consumers
                            # a scheduling point even during a continuous burst.
                            await asyncio.sleep(0)
                            if stop.is_set():
                                break
                            if self._pending:
                                # A live subscription delta. The retained
                                # streams keep their subscription, so their
                                # book, flow and candle evidence survive.
                                self._pending = False
                                previous = self.streams
                                desired = {s.name for s in self._desired}
                                added = {s.name for s in self._desired} - subscribed
                                removed = subscribed - desired
                                if added or removed:
                                    await self._control()
                                    if added:
                                        await asyncio.wait_for(
                                            ws.send_json(
                                                {
                                                    "method": "SUBSCRIBE",
                                                    "params": sorted(added),
                                                    "id": request_id,
                                                }
                                            ),
                                            5,
                                        )
                                    if removed:
                                        await asyncio.wait_for(
                                            ws.send_json(
                                                {
                                                    "method": "UNSUBSCRIBE",
                                                    "params": sorted(removed),
                                                    "id": request_id,
                                                }
                                            ),
                                            5,
                                        )
                                subscribed = set(desired)
                                invalidated.intersection_update(subscribed)
                                self.streams = self._desired
                                self._draining.update(removed)
                                self.on_streams(
                                    tuple(s for s in self._desired if s.name in added),
                                    tuple(s for s in previous if s.name in removed),
                                    self.generation,
                                )
                            now = self.monotonic()
                            elapsed = now - began
                            if elapsed >= self.rotate_after:
                                self.reason = "WS_ROTATION"
                                break
                            if not acknowledged and elapsed > self.ack_timeout:
                                raise StreamFailure("WS_ACK_TIMEOUT")
                            if (
                                acknowledged
                                and self.data_timeout is not None
                                and any(
                                    now - last_by_stream.get(name, acknowledged_at)
                                    >= self.data_timeout
                                    for name in subscribed
                                )
                            ):
                                raise StreamFailure("WS_DATA_TIMEOUT")
                            # A pending receive must never be cancelled: the
                            # transport marks the connection abnormally closed
                            # when its read is cancelled, which would drop a
                            # healthy group on every idle half second.
                            done, _ = await asyncio.wait({pending}, timeout=0.5)
                            if not done:
                                continue
                            try:
                                frame = pending.result()
                            except TimeoutError:
                                # A transport read timeout is not a protocol
                                # failure. The connection stays open and the
                                # read is retried.
                                pending = asyncio.ensure_future(ws.receive())
                                pending.add_done_callback(_discard)
                                continue
                            pending = asyncio.ensure_future(ws.receive())
                            pending.add_done_callback(_discard)
                            if frame.kind == "CLOSE":
                                code = frame.close_code
                                reason = f"WS_DISCONNECTED_{code}" if code else "WS_DISCONNECTED"
                                raise StreamFailure(reason)
                            if frame.kind == "PING":
                                if not isinstance(frame.data, bytes):
                                    raise StreamFailure("WS_INVALID_PING")
                                await self._control()
                                await asyncio.wait_for(ws.pong(frame.data), 2)
                                continue
                            if frame.kind != "TEXT" or not isinstance(frame.data, str):
                                continue
                            sequence += 1
                            payload = decode_payload(frame.data.encode())
                            if not isinstance(payload, dict):
                                raise StreamFailure("WS_INVALID_FRAME")
                            if "code" in payload or "error" in payload:
                                raise StreamFailure("WS_SUBSCRIPTION_REJECTED", 60)
                            if "result" in payload:
                                if type(payload.get("id")) is int and payload["id"] == request_id:
                                    if payload["result"] is not None:
                                        raise StreamFailure("WS_INVALID_ACK")
                                    if not acknowledged:
                                        acknowledged = True
                                        acknowledged_at = last_valid = self.monotonic()
                                continue
                            if not acknowledged:
                                continue
                            name = payload.get("stream")
                            if not isinstance(name, str) or name not in subscribed:
                                if isinstance(name, str) and name in self._draining:
                                    # A final frame of a stream that a live
                                    # delta unsubscribed. It is not evidence
                                    # anymore and must not fail the group.
                                    continue
                                raise StreamFailure("WS_UNSUBSCRIBED_STREAM")
                            try:
                                events = decode_frame(
                                    payload,
                                    route=self.streams[0].route,
                                    received_at=self.clock(),
                                    generation=self.generation,
                                    sequence=sequence,
                                    eligible=self.eligible(),
                                    max_future=self.max_future,
                                    closed_only=True,
                                )
                            except ValueError as error:
                                stream = StreamSpec(name)
                                if stream.symbol is None or self.on_invalid_stream is None:
                                    raise
                                # A malformed pair is fenced and resynchronized.
                                # Independent pairs on this socket retain continuity.
                                if name not in invalidated:
                                    invalidated.add(name)
                                    self.on_invalid_stream(stream, self.generation)
                                if (
                                    str(error) == "STALE_OR_FUTURE_MARKET_EVENT"
                                    and self.monotonic() - last_valid > 10
                                ):
                                    raise StreamFailure("WS_STALE_BACKLOG") from None
                                continue
                            if events:
                                last_valid = self.monotonic()
                                invalidated.discard(name)
                                if self.data_timeout is not None:
                                    last_by_stream[name] = last_valid
                                # Back off consecutive connections that fail to
                                # deliver data, including ACK-only connections.
                                if self.data_timeout is None or subscribed <= last_by_stream.keys():
                                    failures = 0
                            for event in events:
                                self.on_event(event)
                except StreamFailure as error:
                    self.reason, retry_after = str(error), error.retry_after
                    failures += 1
                    self.failures += 1
                except (ValueError, FetchError, OSError, TimeoutError):
                    self.reason = "WS_INVALID_OR_UNAVAILABLE"
                    failures += 1
                    self.failures += 1
                except Exception:
                    # Unexpected parser/socket defects reconnect a single group;
                    # they must not terminate the gateway and every other feed.
                    self.reason = "WS_INTERNAL_ERROR"
                    failures += 1
                    self.failures += 1
                finally:
                    if pending is not None:
                        pending.cancel()
                    self.on_state("STALE", self.generation)
                if not stop.is_set():
                    self.reconnects += 1
                    delay = min(60, self.retry_base * 2 ** min(failures, 6))
                    # After repeated failures, open the breaker for at least a minute.
                    if failures >= 5:
                        delay = 60
                    await pause(stop, max(retry_after, delay + self.jitter() * delay))
        finally:
            self.on_state("STALE", self.generation)
