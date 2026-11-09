"""Display public quotes from RAM; closed chart demand remains bounded and internal.

Testnet-only demo account reads and manual orders are separate from Guardian.
"""

import asyncio
import json
import re
import time
from collections.abc import AsyncIterator
from typing import Any

from aiohttp import ClientSession, ClientTimeout, web

from tradeforge.dashboard.api import create_app
from tradeforge.dashboard.control import CommandQueue
from tradeforge.dashboard.demo_operator import attach_demo_operator
from tradeforge.dashboard.local_terminal import TerminalReadModel
from tradeforge.dashboard.read_model import ReadModel
from tradeforge.dashboard.sessions import Sessions
from tradeforge.domain.market import MarketQuote
from tradeforge.runtime.market_link import fetch_memory


class QuoteCache:
    """Display-only last confirmation; never an execution evidence provider."""

    def __init__(self) -> None:
        self.quotes: tuple[MarketQuote, ...] = ()
        self.state: dict[str, Any] = {"transport": "UNAVAILABLE", "eligible": []}

    def consume(self, feed: dict[str, Any]) -> None:
        records, eligible = feed.get("quotes"), feed.get("eligible")
        if (
            not isinstance(records, list)
            or len(records) > 2000
            or not isinstance(eligible, list)
            or len(eligible) > 2000
            or any(
                not isinstance(s, str) or re.fullmatch(r"[A-Z0-9]{1,30}", s) is None
                for s in eligible
            )
        ):
            self.state = {**self.state, "transport": "RECORD_INVALID"}
            return
        members = set(eligible)
        previous = {q.symbol: q for q in self.quotes if q.symbol in members}
        if feed.get("clock_safe") is not True:
            self.quotes = tuple(previous.values())
            self.state = {"transport": "CLOCK_UNSAFE", "eligible": eligible}
            return
        accepted: dict[str, MarketQuote] = {}
        invalid = set()
        invalid_records = 0
        for record in records:
            try:
                quote = MarketQuote.model_validate_json(json.dumps(record))
                if quote.symbol not in members:
                    continue
                old = accepted.get(quote.symbol, previous.get(quote.symbol))
                accepted[quote.symbol] = (
                    old if old is not None and old.source_at > quote.source_at else quote
                )
            except (ValueError, TypeError):
                invalid_records += 1
                if (
                    isinstance(record, dict)
                    and isinstance(record.get("symbol"), str)
                    and record["symbol"] in members
                ):
                    invalid.add(record["symbol"])
        invalid -= accepted.keys()
        missing = previous.keys() - accepted.keys() - invalid
        for symbol in invalid | missing:
            if symbol in previous:
                accepted[symbol] = previous[symbol]
        self.quotes = tuple(accepted.values())
        self.state = {
            "transport": "OK",
            "eligible": eligible,
            "invalid_symbols": sorted(invalid),
            "missing_symbols": sorted(missing),
            "invalid_records": invalid_records,
        }

    def unavailable(self) -> None:
        self.state = {**self.state, "transport": "UNAVAILABLE"}


def dashboard_app(
    model: ReadModel,
    queue: CommandQueue,
    *,
    sessions: Sessions,
    external_origin: str,
    internal_token: str,
    binance_master: str | None = None,
) -> web.Application:
    cache = QuoteCache()
    connection: ClientSession | None = None

    async def chart(symbol: str, timeframe: str) -> None:
        if connection is None:
            return
        async with connection.post(
            "http://market_gateway:8101/chart-request",
            params={"symbol": symbol, "timeframe": timeframe},
            headers={"Authorization": "Bearer " + internal_token},
            timeout=ClientTimeout(total=3),
            allow_redirects=False,
        ) as response:
            await response.read()

    async def context(app: web.Application) -> AsyncIterator[None]:
        nonlocal connection

        async def poll() -> None:
            assert connection is not None
            while True:
                started = time.monotonic()
                try:
                    data = await fetch_memory(connection, internal_token, quotes_only=True)
                    cache.consume(data)
                except Exception:
                    cache.unavailable()
                await asyncio.sleep(max(0.1, 0.25 - (time.monotonic() - started)))

        async with ClientSession(trust_env=False) as session:
            connection = session
            task = asyncio.create_task(poll())
            try:
                yield
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                connection = None

    # Retain the previous closed-candle cache freshness fix on VPS/Docker.
    if model is not None:
        model = TerminalReadModel(model.db, clock=model.clock)
    app = create_app(
        model,
        queue,
        sessions=sessions,
        external_origin=external_origin,
        market_feed=lambda: cache.quotes,
        market_state=lambda: cache.state,
        chart_demand=chart,
        binance_master=binance_master,
    )
    # This serves authenticated, operator-initiated Testnet orders. It never
    # issues autonomous orders and has no production mutation host.
    if model is not None:
        attach_demo_operator(
            app,
            model.db,
            binance_master,
            sessions=sessions,
            external_origin=external_origin,
        )
    app.cleanup_ctx.append(context)
    return app
