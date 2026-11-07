"""Bounded public REST recovery; history becomes known only at local receipt."""

import asyncio
import math
import random
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime

import aiohttp

from tradeforge.domain.common import DataQuality, utc
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.domain.market import ClosedBar, Timeframe
from tradeforge.market.bars import make_bar
from tradeforge.market.exchange_info import (
    MAX_BODY,
    FetchError,
    Reply,
    decode_payload,
    milliseconds,
)
from tradeforge.market.market_state import MarketState
from tradeforge.market.order_book import LocalOrderBook
from tradeforge.market.streams import decimal_field

Get = Callable[[str, dict[str, str]], Awaitable[Reply]]


async def public_get(path: str, params: dict[str, str], *, environment: str = "live") -> Reply:
    from tradeforge.runtime.binance_environment import ENDPOINTS

    if environment not in ENDPOINTS:
        raise ValueError("BINANCE_ENVIRONMENT_INVALID")
    base_url = ENDPOINTS[environment][1]
    timeout = aiohttp.ClientTimeout(total=5, connect=2, sock_read=3)
    async with aiohttp.ClientSession(
        timeout=timeout, trust_env=False, cookie_jar=aiohttp.DummyCookieJar()
    ) as session:
        async with session.get(base_url + path, params=params, allow_redirects=False) as response:
            body = bytearray()
            async for chunk in response.content.iter_chunked(65536):
                body.extend(chunk)
                if len(body) > MAX_BODY:
                    raise FetchError("MARKET_RESPONSE_TOO_LARGE")
            return Reply(response.status, dict(response.headers), bytes(body))


class PublicMarketClient:
    def __init__(
        self,
        *,
        get: Get = public_get,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        min_interval: float = 1.0,
        max_concurrency: int = 1,
        environment: str = "live",
    ) -> None:
        from tradeforge.runtime.binance_environment import ENDPOINTS

        if environment not in ENDPOINTS:
            raise ValueError("BINANCE_ENVIRONMENT_INVALID")
        self.environment = environment
        self.base_url = ENDPOINTS[environment][1]
        if not 0 <= min_interval <= 60:
            raise ValueError("INVALID_REST_INTERVAL")
        if not 1 <= max_concurrency <= 4:
            raise ValueError("INVALID_REST_CONCURRENCY")
        from functools import partial

        self._default_transport = get is public_get
        self.get: Get = (
            partial(public_get, environment=environment) if self._default_transport else get
        )
        self.clock, self.monotonic, self.sleep = clock, monotonic, sleep
        self.min_interval = min_interval
        self._next = self._cooldown = 0.0
        self._lock = asyncio.Lock()
        self._inflight = asyncio.Semaphore(max_concurrency)
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "PublicMarketClient":
        if self._default_transport:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=5, connect=2, sock_read=3),
                trust_env=False,
                cookie_jar=aiohttp.DummyCookieJar(),
            )
            self.get = self._session_get
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None
            from functools import partial

            self.get = partial(public_get, environment=self.environment)

    async def _session_get(self, path: str, params: dict[str, str]) -> Reply:
        if self._session is None:
            raise FetchError("MARKET_CLIENT_CLOSED")
        async with self._session.get(
            self.base_url + path, params=params, allow_redirects=False
        ) as response:
            body = bytearray()
            async for chunk in response.content.iter_chunked(65536):
                body.extend(chunk)
                if len(body) > MAX_BODY:
                    raise FetchError("MARKET_RESPONSE_TOO_LARGE")
            return Reply(response.status, dict(response.headers), bytes(body))

    async def fetch(self, path: str, params: dict[str, str]) -> tuple[object, datetime]:
        if path not in (
            "/fapi/v1/klines",
            "/fapi/v1/depth",
            "/fapi/v1/ticker/bookTicker",
            "/fapi/v1/premiumIndex",
            "/fapi/v1/ticker/24hr",
        ):
            raise ValueError("PUBLIC_RESOURCE_REQUIRED")
        async with self._inflight:
            for attempt in range(3):
                # Reserve each actual attempt (including retries) on one shared
                # schedule, while slow responses only occupy a bounded slot.
                async with self._lock:
                    if self.monotonic() < self._cooldown:
                        raise FetchError("MARKET_RATE_LIMITED")
                    # Timers may wake early (notably on Windows). The monotonic
                    # deadline, rather than one completed sleep, grants a start.
                    while (delay := self._next - self.monotonic()) > 0:
                        await self.sleep(delay)
                    if self.monotonic() < self._cooldown:
                        raise FetchError("MARKET_RATE_LIMITED")
                    self._next = self.monotonic() + self.min_interval
                try:
                    reply = await asyncio.wait_for(self.get(path, params), timeout=6)
                except (TimeoutError, OSError, aiohttp.ClientError):
                    if attempt == 2:
                        raise FetchError("MARKET_REST_UNAVAILABLE") from None
                    await self.sleep(0.25 * 2**attempt + random.uniform(0, 0.1))
                    continue
                if reply.status in (418, 429):
                    value = next(
                        (v for k, v in reply.headers.items() if k.lower() == "retry-after"), "60"
                    )
                    fallback = 180 if reply.status == 418 else 60
                    try:
                        retry = float(value)
                    except ValueError:
                        try:
                            retry = (
                                parsedate_to_datetime(value) - utc(self.clock())
                            ).total_seconds()
                        except (ValueError, TypeError, OverflowError):
                            retry = float(fallback)
                    if not math.isfinite(retry) or retry < 0:
                        retry = float(fallback)
                    self._cooldown = max(self._cooldown, self.monotonic() + max(fallback, retry))
                    raise FetchError("MARKET_RATE_LIMITED")
                if reply.status != 200:
                    if reply.status >= 500 and attempt < 2:
                        await self.sleep(0.25 * 2**attempt + random.uniform(0, 0.1))
                        continue
                    raise FetchError("MARKET_REST_UNAVAILABLE")
                return decode_payload(reply.body), utc(self.clock())
        raise FetchError("MARKET_REST_UNAVAILABLE")


class MarketRecovery:
    def __init__(self, state: MarketState, client: PublicMarketClient) -> None:
        self.state, self.client = state, client

    async def bars(
        self,
        key: InstrumentKey,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
        *,
        valid: Callable[[], bool] = lambda: True,
    ) -> bool:
        utc(start)
        utc(end)
        if (
            timeframe not in (Timeframe.M1, Timeframe.M5, Timeframe.M15)
            or not start < end
            or start.timestamp() % timeframe.seconds
            or end.timestamp() % timeframe.seconds
            or (end - start).total_seconds() > timeframe.seconds * min(8000, self.state.capacity)
        ):
            raise ValueError("INVALID_BACKFILL_RANGE")
        if not valid():
            return False
        self.state.mark(key, timeframe, DataQuality.STALE, self.client.clock())
        cursor = start
        try:
            for _ in range(8):
                raw, observed = await self.client.fetch(
                    "/fapi/v1/klines",
                    {
                        "symbol": key.symbol,
                        "interval": timeframe.value,
                        "startTime": str(int(cursor.timestamp() * 1000)),
                        "endTime": str(int(end.timestamp() * 1000) - 1),
                        "limit": str(
                            min(1000, int((end - cursor).total_seconds() / timeframe.seconds))
                        ),
                    },
                )
                if not isinstance(raw, list) or not raw or len(raw) > 1000:
                    return False
                accepted: list[ClosedBar] = []
                for row in raw:
                    if not isinstance(row, list) or len(row) != 12:
                        raise ValueError("INVALID_REST_KLINE")
                    opened, closed = milliseconds(row[0]), milliseconds(row[6])
                    if opened >= end or closed > observed:
                        continue  # A currently forming candle is never finalized.
                    expected = cursor + timedelta(seconds=len(accepted) * timeframe.seconds)
                    if opened != expected:
                        raise ValueError("BACKFILL_GAP_OR_REORDER")
                    prices = {
                        name: decimal_field({name: row[index]}, name)
                        for name, index in (
                            ("open", 1),
                            ("high", 2),
                            ("low", 3),
                            ("close", 4),
                            ("volume", 5),
                        )
                    }
                    accepted.append(
                        make_bar(
                            key,
                            timeframe,
                            opened,
                            closed,
                            prices,
                            observed,
                            observed,
                            "binance:rest",
                        )
                    )
                if not accepted:
                    return False
                if not valid():
                    return False
                for bar in accepted:
                    self.state.ingest_bar(bar)
                cursor = accepted[-1].open_at + timedelta(seconds=timeframe.seconds)
                if cursor == end:
                    self.state.mark(key, timeframe, DataQuality.OK, self.client.clock())
                    return True
            return False
        except (FetchError, ValueError):
            return False

    async def depth(self, book: LocalOrderBook) -> bool:
        generation = book.generation
        try:
            raw, observed = await self.client.fetch(
                "/fapi/v1/depth", {"symbol": book.key.symbol, "limit": "1000"}
            )
            if generation != book.generation:
                return False
            book.install(raw, observed, generation=generation)
            return True  # Snapshot loaded; READY still requires a WS bridge.
        except (FetchError, ValueError):
            if generation == book.generation:
                book.invalidate()
            return False
