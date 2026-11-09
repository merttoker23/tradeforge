"""Standalone dashboard's bounded, read-only live market projection.

Works on Windows, Linux and a co-located VPS without new credentials or flags.
The Gateway listens on loopback and its public quote-state route independently
rejects remote, browser-origin and spoofed Host requests. This module never
obtains API secrets or enables order execution.
"""

import asyncio
import json
import re
import time
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any

from aiohttp import ClientSession, ClientTimeout, web
from sqlalchemy import text

from tradeforge.dashboard.read_model import ReadModel
from tradeforge.domain.common import utc
from tradeforge.domain.market import ClosedBar, MarketQuote

_GATEWAY_QUOTES = "http://127.0.0.1:8101/quote-state"
_MAX_RESPONSE = 4_000_000
_INTERVAL = 1.0
_MAX_FEED_AGE = 8.0


class LocalQuoteFeed:
    """Read Gateway RAM once per second, never per dashboard viewer."""

    def __init__(self) -> None:
        self._session: ClientSession | None = None
        self._task: asyncio.Task[None] | None = None
        self._quotes: tuple[MarketQuote, ...] = ()
        self._eligible: frozenset[str] = frozenset()
        self._last_received = float("-inf")
        self._clock_safe = False

    def _fresh(self) -> bool:
        return self._clock_safe and time.monotonic() - self._last_received <= _MAX_FEED_AGE

    def quotes(self) -> tuple[MarketQuote, ...]:
        return self._quotes if self._fresh() else ()

    def state(self) -> dict[str, Any]:
        return {
            "transport": "OK" if self._fresh() else "UNAVAILABLE",
            "eligible": self._eligible,
        }

    async def start(self, _app: web.Application) -> None:
        self._session = ClientSession(timeout=ClientTimeout(total=3), trust_env=False)
        self._task = asyncio.create_task(self._poll())

    async def close(self, _app: web.Application) -> None:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _sample(self) -> None:
        assert self._session is not None
        async with self._session.get(_GATEWAY_QUOTES, allow_redirects=False) as response:
            if response.status != 200:
                raise ValueError("GATEWAY_QUOTES_UNAVAILABLE")
            # StreamReader.read(n) can return a *partial* JSON document even
            # when n is much larger than Content-Length. Assemble bounded
            # chunks until EOF before parsing the ~270 KB universe quote feed.
            raw = bytearray()
            async for chunk in response.content.iter_chunked(65536):
                if len(raw) + len(chunk) > _MAX_RESPONSE:
                    raise ValueError("GATEWAY_QUOTES_OVERSIZED")
                raw.extend(chunk)
            payload = json.loads(raw)
        if not isinstance(payload, dict) or payload.get("clock_safe") is not True:
            raise ValueError("GATEWAY_CLOCK_UNSAFE")
        now = datetime.now(UTC)
        try:
            observed = datetime.fromisoformat(payload["as_of"])
            age = (now - utc(observed)).total_seconds()
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("GATEWAY_QUOTES_INVALID_TIME") from error
        if not 0 <= age <= _MAX_FEED_AGE:
            raise ValueError("GATEWAY_QUOTES_STALE")
        members, records = payload.get("eligible"), payload.get("quotes")
        if (
            not isinstance(members, list)
            or len(members) > 4096
            or any(
                not isinstance(s, str) or not re.fullmatch(r"[A-Z0-9]{2,30}", s) for s in members
            )
            or not isinstance(records, list)
            or len(records) > 4096
        ):
            raise ValueError("GATEWAY_QUOTES_INVALID")
        eligible = frozenset(members)
        quotes = []
        seen: set[str] = set()
        for record in records:
            try:
                # Wire prices are decimal strings. Strict money validators
                # accept those only in JSON mode; Python-mode validation drops
                # otherwise valid Binance quotes without reporting an error.
                quote = MarketQuote.model_validate_json(json.dumps(record))
                if (
                    quote.symbol not in eligible
                    or quote.symbol in seen
                    or not 0 <= (now - quote.source_at).total_seconds() < 15
                ):
                    continue
                quotes.append(quote)
                seen.add(quote.symbol)
            except ValueError:
                continue  # A malformed individual quote cannot poison the entire feed.
        self._quotes = tuple(quotes)
        self._eligible = eligible
        self._clock_safe = True
        self._last_received = time.monotonic()

    async def _poll(self) -> None:
        while True:
            try:
                await self._sample()
            except Exception:
                # Do not renew an old source timestamp if Gateway is unavailable.
                self._clock_safe = False
                self._quotes = ()
            await asyncio.sleep(_INTERVAL)


def _render_bars(
    raw_rows: list[Any], symbol: str, timeframe: str, now: datetime
) -> list[dict[str, Any]]:
    """Only proven, chronological closed candles; no fabricated intrabar prices."""
    if len(raw_rows) > 300:
        raise ValueError("CHART_HISTORY_OVERSIZED")
    output = []
    last = float("-inf")
    for raw in raw_rows:
        body = raw if isinstance(raw, (str, bytes, bytearray)) else json.dumps(raw)
        bar = ClosedBar.model_validate_json(body, context={"now": now})
        opened = int(bar.open_at.timestamp())
        if (
            bar.symbol != symbol
            or bar.timeframe.value != timeframe
            or bar.close_at > now
            or opened <= last
        ):
            raise ValueError("CHART_SOURCE_INVALID")
        last = opened
        output.append(
            {
                "time": opened,
                **{
                    name: str(getattr(bar, name))
                    for name in ("open", "high", "low", "close", "volume")
                },
            }
        )
    return output


class TerminalReadModel(ReadModel):
    """Prefer the newest genuine bar series over an older 300-bar cache.

    The historical cache remains useful for a full 300-bar display, but it
    must never hide more recent canonical 15m closed observations.
    """

    def terminal_chart(self, symbol: str, timeframe: str) -> dict[str, Any]:
        if re.fullmatch(r"[A-Z0-9_]{2,40}", symbol) is None or timeframe not in {
            "1m",
            "5m",
            "15m",
            "1h",
            "4h",
        }:
            raise ValueError("CHART_QUERY_INVALID")
        now = utc(self.clock())
        params = {"symbol": symbol, "tf": timeframe, "at": now}
        with self.transaction() as connection:
            cache = connection.scalar(
                text("""
                SELECT t.payload->'bars' FROM terminal_chart_cache t
                JOIN instruments i USING(instrument_id)
                WHERE i.symbol=:symbol AND t.timeframe=:tf AND t.as_of<=:at LIMIT 1
            """),
                params,
            )
            cached: list[dict[str, Any]] = []
            if isinstance(cache, list) and 0 < len(cache) <= 300:
                try:
                    cached = _render_bars(cache, symbol, timeframe, now)
                except ValueError:
                    cached = []
            # A single indexed latest-observation probe avoids reloading 300
            # canonical rows if the saved 300-candle chart is already current.
            latest = connection.scalar(
                text("""
                SELECT b.payload FROM closed_bar_observations b
                JOIN instruments i USING(instrument_id)
                WHERE i.symbol=:symbol AND b.timeframe=:tf
                  AND b.observed_at<=:at AND b.close_at<=:at
                ORDER BY b.open_at DESC,b.observed_at DESC LIMIT 1
            """),
                params,
            )
            bars = cached
            if latest is not None:
                recent = _render_bars([latest], symbol, timeframe, now)
                if recent and (not cached or recent[-1]["time"] > cached[-1]["time"]):
                    rows = (
                        connection.execute(
                            text("""
                        SELECT payload FROM (
                          SELECT DISTINCT ON (b.open_at) b.open_at,b.payload
                          FROM closed_bar_observations b JOIN instruments i USING(instrument_id)
                          WHERE i.symbol=:symbol AND b.timeframe=:tf
                            AND b.observed_at<=:at AND b.close_at<=:at
                          ORDER BY b.open_at DESC,b.observed_at DESC LIMIT 300
                        ) selected ORDER BY open_at
                    """),
                            params,
                        )
                        .scalars()
                        .all()
                    )
                    bars = _render_bars(list(rows), symbol, timeframe, now)
            features = (
                connection.execute(
                    text("""
                SELECT x->>'volatility' AS volatility,
                  x->>'relative_return' AS relative_strength,r.as_of AS as_of
                FROM (SELECT as_of,payload FROM regime_snapshots
                  WHERE as_of<=:at ORDER BY as_of DESC LIMIT 1) r,
                  LATERAL jsonb_array_elements(r.payload->'features') x
                WHERE x#>>'{instrument,symbol}'=:symbol LIMIT 1
            """),
                    {"at": now, "symbol": symbol},
                )
                .mappings()
                .one_or_none()
            )
        metrics = dict(features) if features is not None else None
        if metrics is not None and isinstance(metrics.get("as_of"), datetime):
            metrics["as_of"] = metrics["as_of"].isoformat()
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "bars": bars,
            "as_of": now.isoformat(),
            "environment": "UNKNOWN",
            "execution_evidence": False,
            "metrics": metrics,
            "reason": "STORED_CLOSED_BARS" if bars else "CHART_HISTORY_MISSING",
        }
