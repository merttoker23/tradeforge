"""Guardian-only private stream; raw events and listen keys never enter diagnostics."""

import asyncio
import json
import random
import re
from collections.abc import Callable
from datetime import UTC, datetime

import aiohttp

from tradeforge.execution.binance_client import PRIVATE_WS, BinanceTestnetClient
from tradeforge.execution.broker import AckUnknown, ExchangeRejected
from tradeforge.execution.order_manager import stream_gap
from tradeforge.market.exchange_info import milliseconds
from tradeforge.persistence.db import Database, EntryHalt


class PrivateStream:
    def __init__(
        self,
        db: Database,
        reconcile: Callable[[str], None],
        *,
        recover: Callable[[], bool] | None = None,
        clock: Callable[[], datetime] | None = None,
        latch_gap: bool = True,
    ) -> None:
        self.db, self.reconcile = db, reconcile
        self.recover = recover
        self.clock = clock or (lambda: datetime.now(UTC))
        self.latch_gap = latch_gap
        self.transport_at = self.clock()
        self.connected = False
        self.ready = False
        self.last_event_at: datetime | None = None
        self.account_event_at: datetime | None = None
        self.connection_attempts = 0
        self.reconnects = 0
        self.last_close_code: int | None = None
        self.last_failure: str | None = None
        self.last_ping_at: datetime | None = None
        self.last_pong_at: datetime | None = None

    @property
    def transport_ready(self) -> bool:
        return self.connected and 0 <= (self.clock() - self.transport_at).total_seconds() <= 60

    def health(self) -> dict[str, object]:
        return {
            "connected": self.connected,
            "transport_ready": self.transport_ready,
            "transport_at": self.transport_at.isoformat(),
            "transport_age_seconds": (self.clock() - self.transport_at).total_seconds(),
            "account_ready": self.ready,
            "connection_attempts": self.connection_attempts,
            "reconnects": self.reconnects,
            "last_close_code": self.last_close_code,
            "last_failure": self.last_failure,
            "last_ping_at": self.last_ping_at.isoformat() if self.last_ping_at else None,
            "last_pong_at": self.last_pong_at.isoformat() if self.last_pong_at else None,
        }

    @property
    def ready(self) -> bool:
        return self._ready and 0 <= (self.clock() - self.transport_at).total_seconds() <= 60

    @ready.setter
    def ready(self, value: bool) -> None:
        self._ready = value
        if value:
            self.transport_at = self.clock()

    def gap(self) -> None:
        self.ready = False
        self.connected = False
        if self.latch_gap:
            stream_gap(self.db)

    def mark_recovered(self, recovered: bool) -> None:
        if (
            self.connected
            and recovered
            and 0 <= (self.clock() - self.transport_at).total_seconds() <= 60
        ):
            self._ready = True

    def on_event(self, payload: object) -> None:
        try:
            if not isinstance(payload, dict):
                raise ValueError
            event = payload.get("e")
            if event in {"listenKeyExpired", "MARGIN_CALL", "ACCOUNT_CONFIG_UPDATE"}:
                raise ValueError
            at = milliseconds(payload["E"])
            if at > datetime.now(UTC) or (
                self.last_event_at is not None and at < self.last_event_at
            ):
                raise ValueError
            self.last_event_at = at
            if event == "ORDER_TRADE_UPDATE":
                order = payload["o"]
                if not isinstance(order, dict) or not isinstance(order.get("c"), str):
                    raise ValueError
                self.reconcile(order["c"])
            elif event == "ACCOUNT_UPDATE":
                self.account_event_at = at
            elif event == "ALGO_UPDATE":
                if self.recover is None:
                    raise ValueError
            elif event != "TRADE_LITE":
                raise ValueError
            if self.recover is not None:
                self.ready = self.recover()
        except (KeyError, TypeError, ValueError, EntryHalt):
            self.gap()
            raise EntryHalt("PRIVATE_STREAM_GAP") from None

    async def consume(self, socket: aiohttp.ClientWebSocketResponse) -> None:
        self.ready = False
        self.connected = True
        self.transport_at = self.clock()
        self.last_failure = None
        self.last_event_at = None
        try:
            if self.recover is not None:
                self.ready = await asyncio.to_thread(self.recover)
            else:
                self.ready = True
            async for message in socket:
                self.transport_at = self.clock()
                if message.type is aiohttp.WSMsgType.PING:
                    await socket.pong(message.data)
                    continue
                if message.type is aiohttp.WSMsgType.PONG:
                    self.last_pong_at = self.clock()
                    continue
                if message.type is not aiohttp.WSMsgType.TEXT:
                    raise EntryHalt("PRIVATE_STREAM_INTERRUPTED")
                try:
                    body = json.loads(message.data)
                except (ValueError, TypeError):
                    raise EntryHalt("PRIVATE_STREAM_INVALID") from None
                await asyncio.to_thread(self.on_event, body)
        except (EntryHalt, aiohttp.ClientError):
            self.last_failure = "PRIVATE_STREAM_INTERRUPTED"
        finally:
            self.last_close_code = getattr(socket, "close_code", None)
            await asyncio.to_thread(self.gap)

    async def renew(
        self,
        client: BinanceTestnetClient,
        socket: aiohttp.ClientWebSocketResponse,
        listen_key: str,
    ) -> bool:
        try:
            renewed = await asyncio.to_thread(
                client.request,
                "PUT",
                "/fapi/v1/listenKey",
                signed=False,
            )
            if isinstance(renewed, dict) and renewed.get("listenKey") == listen_key:
                return True
        except (AckUnknown, ExchangeRejected):
            pass
        # A failed keeper must not leave an apparently ready socket for the rest
        # of the key's lifetime. Latch first, then close; no raw error is logged.
        try:
            self.last_failure = "PRIVATE_STREAM_KEEPALIVE_FAILED"
            await asyncio.to_thread(self.gap)
        finally:
            await socket.close()
        return False

    async def run(self, client: BinanceTestnetClient) -> None:
        delay = 1.0
        while True:
            self.connection_attempts += 1
            try:
                await self._connect(client)
                delay = 1.0
            except (
                EntryHalt,
                AckUnknown,
                ExchangeRejected,
                aiohttp.ClientError,
                TimeoutError,
                OSError,
            ):
                self.last_failure = "PRIVATE_STREAM_CONNECT_UNAVAILABLE"
                await asyncio.to_thread(self.gap)
            self.reconnects += 1
            await asyncio.sleep(delay + random.uniform(0, delay / 4))
            delay = min(30, delay * 2)

    async def _connect(self, client: BinanceTestnetClient) -> None:
        if client.environment != "testnet":
            raise EntryHalt("LIVE_PRIVATE_STREAM_LOCKED")
        key = await asyncio.to_thread(client.request, "POST", "/fapi/v1/listenKey", signed=False)
        if (
            not isinstance(key, dict)
            or not isinstance(key.get("listenKey"), str)
            or re.fullmatch(r"[A-Za-z0-9_-]{1,256}", key["listenKey"]) is None
        ):
            await asyncio.to_thread(self.gap)
            raise EntryHalt("PRIVATE_STREAM_KEY_INVALID")
        listen_key = key["listenKey"]
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None, sock_connect=3), trust_env=False
        ) as session:
            try:
                async with session.ws_connect(
                    PRIVATE_WS + listen_key, autoping=False, max_msg_size=1_000_000
                ) as socket:

                    async def keepalive() -> None:
                        while True:
                            await asyncio.sleep(1800)
                            if not await self.renew(client, socket, listen_key):
                                return

                    async def heartbeat() -> None:
                        while True:
                            await asyncio.sleep(20)
                            if (self.clock() - self.transport_at).total_seconds() > 60:
                                self.last_failure = "PRIVATE_STREAM_HEARTBEAT_STALE"
                                await asyncio.to_thread(self.gap)
                                await socket.close()
                                return
                            await socket.ping()
                            self.last_ping_at = self.clock()

                    keeper = asyncio.create_task(keepalive())
                    pinger = asyncio.create_task(heartbeat())
                    try:
                        # Rotate before the exchange's 24h hard disconnect; latch stays set.
                        await asyncio.wait_for(self.consume(socket), timeout=23 * 3600)
                    finally:
                        keeper.cancel()
                        pinger.cancel()
                        await asyncio.gather(keeper, pinger, return_exceptions=True)
            finally:
                await asyncio.to_thread(self.gap)
                try:
                    await asyncio.to_thread(
                        client.request, "DELETE", "/fapi/v1/listenKey", signed=False
                    )
                except Exception:
                    pass  # No raw transport error or key in shutdown diagnostics.
