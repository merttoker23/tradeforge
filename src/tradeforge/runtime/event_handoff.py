"""Read-only bounded market-event wakeups across Tradeforge service boundaries.

This is *not* execution authority, a market data store, or a durable event log.
All trade decisions must still re-fetch and validate canonical market evidence.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import re
import time
from collections import deque
from dataclasses import dataclass
from typing import TypedDict, cast
from uuid import uuid4

from aiohttp import ClientSession, ClientTimeout, web

_SYMBOL = re.compile(r"^[A-Z0-9_]{2,40}$")


class MarketEventPacket(TypedDict):
    epoch: str
    sequence: int
    overflow: bool
    events: list[dict[str, object]]


@dataclass(frozen=True)
class MarketWakeup:
    sequence: int
    symbol: str
    kind: str

    def as_dict(self) -> dict[str, int | str]:
        return {"sequence": self.sequence, "symbol": self.symbol, "kind": self.kind}


class MarketEventHub:
    """Bounded in-process wakeups, with overflow fencing and no execution rights."""

    def __init__(self, *, capacity: int = 8192, interval: float = 0.2) -> None:
        if capacity < 100 or not 0 < interval <= 1:
            raise ValueError("INVALID_EVENT_HUB_BUDGET")
        self.epoch = uuid4().hex
        self.sequence = 0
        self.entries: deque[MarketWakeup] = deque(maxlen=capacity)
        self.changed = asyncio.Event()
        self.interval = interval
        self.last_wakeup: dict[tuple[str, str], float] = {}
        self.ignored = 0
        self._pending: dict[tuple[str, str], MarketWakeup] = {}
        self._timer: asyncio.TimerHandle | None = None

    def publish(self, event: object) -> None:
        stream = getattr(getattr(event, "stream", None), "name", "")
        symbol = getattr(event, "symbol", "")
        if not isinstance(symbol, str) or not _SYMBOL.fullmatch(symbol):
            return
        if "@kline_" in stream:
            # Only *closed* exchange candles may update durable strategy evidence.
            try:
                payload = json.loads(getattr(event, "payload_json", "{}"))
                if payload.get("k", {}).get("x") is not True:
                    return
            except (TypeError, ValueError, AttributeError):
                return
            kind = "FRAME" if stream.endswith("@kline_15m") else "BAR"
        elif "@depth" in stream:
            kind = "DEPTH"
        elif "@aggTrade" in stream:
            kind = "FLOW"
        elif "@bookTicker" in stream or stream == "!ticker@arr":
            kind = "PRICE"
        elif stream == "!markPrice@arr@1s":
            kind = "MARK"
        else:
            return
        # Multiple updates for the same instrument need one wakeup, not one
        # expensive SQL-backed analysis per exchange message.
        now = time.monotonic()
        # A 15m regime update is global; a flood of per-symbol closes should
        # not trigger a separate full-market recalculation for every symbol.
        key = ("*" if kind == "FRAME" else symbol, kind)
        if now - self.last_wakeup.get(key, -1e12) < self.interval:
            # Leading-edge-only throttling can miss the last trigger-crossing
            # quote of a burst. Keep a trailing wakeup as well.
            self.ignored += 1
            self._pending[key] = MarketWakeup(0, symbol, kind)
            if self._timer is None:
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    # Synchronous tests/offline calls have no event loop; the
                    # live market gateway always calls publish on one.
                    pass
                else:
                    self._timer = loop.call_later(self.interval, self._flush_pending)
            return
        self._emit(symbol, kind, now)

    def _emit(self, symbol: str, kind: str, now: float) -> None:
        self.last_wakeup[("*" if kind == "FRAME" else symbol), kind] = now
        if len(self.last_wakeup) > 10000:
            self.last_wakeup = {
                k: v for k, v in self.last_wakeup.items() if now - v < 120
            }
        self.sequence += 1
        self.entries.append(MarketWakeup(self.sequence, symbol, kind))
        self.changed.set()

    def _flush_pending(self) -> None:
        self._timer = None
        pending = tuple(self._pending.values())
        self._pending.clear()
        now = time.monotonic()
        for event in pending:
            self._emit(event.symbol, event.kind, now)

    def snapshot(self, after: int, epoch: str, *, limit: int = 1024) -> dict[str, object]:
        if after < 0 or limit < 1 or limit > 4096:
            raise ValueError("INVALID_MARKET_CURSOR")
        reset = bool(epoch and epoch != self.epoch) or after > self.sequence
        after = 0 if reset else after
        oldest = self.entries[0].sequence if self.entries else self.sequence + 1
        overflow = reset or (after + 1 < oldest)
        entries = [item.as_dict() for item in self.entries if item.sequence > after]
        # Return the *last* items if the ring has outrun the consumer.
        if len(entries) > limit:
            overflow = True
            entries = entries[-limit:]
        return {
            "epoch": self.epoch,
            "sequence": self.sequence,
            "overflow": overflow,
            "events": entries,
        }

    async def wait(self, after: int, epoch: str, seconds: float) -> dict[str, object]:
        if not 0 <= seconds <= 2.5:
            raise ValueError("INVALID_MARKET_WAIT")
        if after != self.sequence or (epoch and epoch != self.epoch):
            return self.snapshot(after, epoch)
        self.changed.clear()
        # publish and wait both run on this service's asyncio loop. A wakeup
        # between the cursor comparison and clear cannot occur without an await.
        try:
            await asyncio.wait_for(self.changed.wait(), timeout=seconds)
        except TimeoutError:
            pass
        return self.snapshot(after, epoch)


def add_market_events_route(
    app: web.Application,
    hub: MarketEventHub,
    *,
    token: str | None,
) -> None:
    if token is not None and len(token) < 43:
        raise ValueError("INTERNAL_TOKEN_REQUIRED")

    async def events(request: web.Request) -> web.Response:
        if token is not None:
            valid = (
                len(request.headers.getall("Authorization", [])) == 1
                and hmac.compare_digest(
                    request.headers.get("Authorization", ""), "Bearer " + token
                )
                and "Origin" not in request.headers
                and "Sec-Fetch-Site" not in request.headers
            )
        else:
            transport = request.transport
            peer = transport.get_extra_info("peername") if transport else None
            bound = transport.get_extra_info("sockname") if transport else None
            valid = bool(
                peer and peer[0] == "127.0.0.1"
                and bound and bound[0] == "127.0.0.1"
                and request.host == f"127.0.0.1:{bound[1]}"
                and "Origin" not in request.headers
                and "Sec-Fetch-Site" not in request.headers
                and "Authorization" not in request.headers
            )
        if not valid:
            raise web.HTTPForbidden()
        if set(request.query) - {"after", "epoch", "wait"}:
            raise web.HTTPBadRequest()
        try:
            after = int(request.query.get("after", "0"))
            wait = float(request.query.get("wait", "2"))
            epoch = request.query.get("epoch", "")
            if len(epoch) > 80 or not re.fullmatch(r"[a-z0-9]*", epoch):
                raise ValueError
            payload = await hub.wait(after, epoch, wait)
        except (ValueError, OverflowError):
            raise web.HTTPBadRequest() from None
        return web.json_response(payload, headers={"Cache-Control": "no-store"})

    app.router.add_get("/market-events", events)


async def fetch_market_events(
    session: ClientSession,
    token: str | None,
    *,
    after: int,
    epoch: str = "",
    wait: float = 2.0,
    base: str | None = None,
) -> MarketEventPacket:
    """Long-poll notification hint; ALWAYS re-validate canonical inputs."""
    if base is None:
        base = "http://127.0.0.1:8101" if token is None else "http://market_gateway:8101"
    headers = {} if token is None else {"Authorization": "Bearer " + token}
    async with session.get(
        base + "/market-events",
        params={"after": str(after), "epoch": epoch, "wait": str(wait)},
        headers=headers,
        allow_redirects=False,
        timeout=ClientTimeout(total=max(3.0, wait + 1.0)),
    ) as response:
        if response.status != 200:
            raise ValueError("MARKET_EVENTS_UNAVAILABLE")
        body = await response.content.read(256 * 1024 + 1)
        if len(body) > 256 * 1024:
            raise ValueError("MARKET_EVENT_CAPACITY")
        payload = json.loads(body)
        if (
            not isinstance(payload, dict)
            or type(payload.get("sequence")) is not int
            or payload["sequence"] < 0
            or type(payload.get("overflow")) is not bool
            or not isinstance(payload.get("epoch"), str)
            or not isinstance(payload.get("events"), list)
            or len(payload["events"]) > 4096
            or any(
                not isinstance(event, dict)
                or not isinstance(event.get("symbol"), str)
                or _SYMBOL.fullmatch(event["symbol"]) is None
                or event.get("kind") not in {"FRAME", "BAR", "DEPTH", "FLOW", "PRICE", "MARK"}
                for event in payload["events"]
            )
        ):
            raise ValueError("MARKET_EVENT_CORRUPT")
        return cast(MarketEventPacket, payload)
