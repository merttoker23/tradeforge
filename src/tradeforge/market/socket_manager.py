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
from tradeforge.market.streams import MarketEvent, Route, StreamSpec, decode_events


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
        return WireFrame("CLOSE")

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

    async def __aenter__(self) -> "AiohttpConnector":
        async def reject_redirect(*args: object) -> None:
            raise StreamFailure("MARKET_REDIRECT_REJECTED")

        trace = aiohttp.TraceConfig()
        trace.on_request_redirect.append(reject_redirect)
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5),
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
                f"{self.base_url}/{route}/stream",
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


class SocketManager:
    def __init__(
        self,
        streams: tuple[StreamSpec, ...],
        connect: Callable[[Route], AbstractAsyncContextManager[MarketSocket]],
        *,
        eligible: Callable[[], frozenset[str]],
        on_event: Callable[[MarketEvent], None],
        on_state: Callable[[str, int], None],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
        jitter: Callable[[], float] = random.random,
        retry_base: float = 1,
        rotate_after: float = 23 * 3600 + 50 * 60,
        ack_timeout: float = 5,
    ) -> None:
        if not streams or len(streams) > 1024 or len({s.route for s in streams}) != 1:
            raise ValueError("INVALID_SOCKET_GROUP")
        if not 0 < rotate_after < 86400 or not 0 < ack_timeout <= 30 or not 0 < retry_base <= 30:
            raise ValueError("INVALID_SOCKET_POLICY")
        self.streams, self.connect, self.eligible = streams, connect, eligible
        self.on_event, self.on_state = on_event, on_state
        self.clock, self.monotonic, self.jitter = clock, monotonic, jitter
        self.retry_base, self.rotate_after, self.ack_timeout = retry_base, rotate_after, ack_timeout
        self.generation = self.failures = 0
        self.next_control = 0.0
        self.reconnects = 0
        self.reason = "NOT_CONNECTED"

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
                began = self.monotonic()
                retry_after = 0.0
                try:
                    async with self.connect(self.streams[0].route) as ws:
                        await self._control()
                        request_id = self.generation
                        await asyncio.wait_for(
                            ws.send_json(
                                {
                                    "method": "SUBSCRIBE",
                                    "params": [s.name for s in self.streams],
                                    "id": request_id,
                                }
                            ),
                            5,
                        )
                        acknowledged, sequence = False, 0
                        while not stop.is_set():
                            # A buffered receive (and ACK/ignored frame) may not
                            # suspend on Python 3.12. Leave probes and consumers
                            # a scheduling point even during a continuous burst.
                            await asyncio.sleep(0)
                            if stop.is_set():
                                break
                            elapsed = self.monotonic() - began
                            if elapsed >= self.rotate_after:
                                self.reason = "WS_ROTATION"
                                break
                            if not acknowledged and elapsed > self.ack_timeout:
                                raise StreamFailure("WS_ACK_TIMEOUT")
                            try:
                                frame = await asyncio.wait_for(ws.receive(), 0.5)
                            except TimeoutError:
                                continue
                            if frame.kind == "CLOSE":
                                raise StreamFailure("WS_DISCONNECTED")
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
                                    acknowledged = True
                                continue
                            if not acknowledged:
                                continue
                            if payload.get("stream") not in {s.name for s in self.streams}:
                                raise StreamFailure("WS_UNSUBSCRIBED_STREAM")
                            events = decode_events(
                                frame.data,
                                route=self.streams[0].route,
                                received_at=self.clock(),
                                generation=self.generation,
                                sequence=sequence,
                                eligible=self.eligible(),
                            )
                            for event in events:
                                self.on_event(event)
                            if elapsed >= 60:
                                failures = 0
                except StreamFailure as error:
                    self.reason, retry_after = str(error), error.retry_after
                    failures += 1
                    self.failures += 1
                except (ValueError, FetchError, OSError, TimeoutError):
                    self.reason = "WS_INVALID_OR_UNAVAILABLE"
                    failures += 1
                    self.failures += 1
                finally:
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
