"""Bounded internal RAM handoff. No public listener, price SQL or credential forwarding."""

import hmac
import json
import re
from datetime import datetime, timedelta
from typing import Any

from aiohttp import ClientSession, ClientTimeout, web

from tradeforge.alpha.base import MomentumInput
from tradeforge.alpha.reversal import ReversalInput
from tradeforge.domain.common import DataQuality, utc
from tradeforge.domain.market import (
    MarketSnapshot,
    OrderBookSnapshot,
    StreamState,
    Timeframe,
    TradeFlowWindow,
)
from tradeforge.intelligence.models import FeatureSnapshot, RegimePolicy
from tradeforge.market.canonical_feed import CANONICAL_BAR_REASONS
from tradeforge.market.market_gateway import MarketGateway
from tradeforge.market.market_state import MarketState
from tradeforge.market.runtime import GatewayPersistence, _merge_fresh
from tradeforge.market.storage import load_bars
from tradeforge.persistence.db import Database

MAX_BODY = 4_000_000


def memory_payload(
    gateway: MarketGateway, persistence: GatewayPersistence, *, quotes_only: bool = False
) -> dict[str, Any]:
    at = gateway.clock()
    safe = gateway.clock_sync.ready(at, gateway.monotonic())
    if quotes_only:
        # The 4Hz display poll needs prices only. Building all analysis/depth
        # evidence here would compete with ingestion and execution handoffs.
        return {
            "as_of": at.isoformat(),
            "clock_safe": safe,
            "eligible": sorted(gateway.eligible),
            "quotes": [q.model_dump(mode="json") for q in persistence.display_quotes(at)],
        }
    markets = {
        m.symbol: m
        for m in persistence.rest_samples
        if safe and m.symbol in gateway.eligible and m.costs_fresh(at, 15)
    }
    book_rows: list[object] = []
    for (name, _), (event, _) in gateway._latest.items():
        if "@bookTicker" not in name or gateway._generations.get(name) != event.generation:
            continue
        if not 0 <= (at - event.event_time).total_seconds() <= 5:
            continue
        book_rows.append(
            {
                "symbol": event.symbol,
                "time": int(event.event_time.timestamp() * 1000),
                "bidPrice": str(event.values["b"]),
                "askPrice": str(event.values["a"]),
            }
        )
    markets = {
        m.symbol: m
        for m in _merge_fresh(
            tuple(markets.values()),
            persistence.markets(book_rows, at) if safe else (),
            gateway.eligible,
            at,
        )
    }
    deep = []
    deep_status = []
    ready_symbols = set()
    for symbol, book in gateway.canonical.books.items():
        ready = safe and gateway.canonical.symbol_ready(symbol, at)
        if ready:
            ready_symbols.add(symbol)
        reason = (
            "CLOCK_UNSAFE"
            if not safe
            else "STORAGE_ADMISSION_HALTED"
            if not getattr(gateway.canonical, "admission_ready", True)
            else "CANONICAL_STORAGE_UNAVAILABLE"
            if not gateway.canonical.storage_ready
            else "BOOK_UNAVAILABLE"
            if book.state_at(at) is not StreamState.READY
            else "FLOW_UNAVAILABLE"
            if gateway.canonical.flows[symbol].snapshot(at).validity is not DataQuality.OK
            else (gateway.canonical.bar_rejections(symbol, at) or ("CANONICAL_DATA_NOT_READY",))[0]
            if not ready
            else "READY"
        )
        deep_status.append({"symbol": symbol, "ready": ready, "reason": reason})
    if safe:
        for symbol, book in gateway.canonical.books.items():
            if symbol not in ready_symbols:
                continue
            flow = gateway.canonical.flows[symbol]
            # Only nearby liquidity used by the alpha engines crosses this boundary.
            limited = book.snapshot(at, limit=20)
            deep.append(
                {
                    "symbol": symbol,
                    "book": limited.model_dump(mode="json"),
                    "flow": flow.snapshot(at).model_dump(mode="json"),
                    "previous_flow": flow.snapshot(at - timedelta(seconds=60)).model_dump(
                        mode="json"
                    ),
                }
            )
    status = gateway.status()
    composite_covered = sum(
        m.symbol in gateway.eligible and m.costs_fresh(at, 15) for m in markets.values()
    )
    status["analysis_market_covered"] = composite_covered
    status["analysis_volume_covered"] = sum(m.volume_fresh(at, 15) for m in markets.values())
    status["analysis_coverage_ready"] = bool(
        safe
        and status.get("data_ready") is True
        and gateway.eligible
        and composite_covered / len(gateway.eligible) >= RegimePolicy().minimum_coverage
    )
    return {
        "as_of": at.isoformat(),
        "status": status,
        "clock_safe": safe,
        "eligible": sorted(gateway.eligible),
        "quotes": [q.model_dump(mode="json") for q in persistence.display_quotes(at)],
        "markets": [m.model_dump(mode="json") for m in markets.values()],
        "deep": deep,
        "deep_status": deep_status,
    }


def add_memory_route(
    app: web.Application, gateway: MarketGateway, persistence: GatewayPersistence, token: str
) -> None:
    if len(token) < 43:
        raise ValueError("INTERNAL_TOKEN_REQUIRED")

    async def memory(request: web.Request) -> web.Response:
        if (
            len(request.headers.getall("Authorization", [])) != 1
            or not hmac.compare_digest(request.headers.get("Authorization", ""), "Bearer " + token)
            or "Origin" in request.headers
            or "Sec-Fetch-Site" in request.headers
        ):
            raise web.HTTPForbidden()
        if request.path == "/chart-request":
            if set(request.query) != {"symbol", "timeframe"}:
                raise web.HTTPBadRequest()
            persistence.request_chart(request.query["symbol"], request.query["timeframe"])
            return web.json_response({"status": "REQUESTED"}, status=202)
        return web.json_response(
            memory_payload(gateway, persistence, quotes_only=request.path == "/quote-state"),
            headers={"Cache-Control": "no-store"},
        )

    app.router.add_get("/market-state", memory)
    app.router.add_get("/quote-state", memory)
    app.router.add_post("/chart-request", memory)


async def fetch_memory(
    session: ClientSession, token: str, *, quotes_only: bool = False
) -> dict[str, Any]:
    async with session.get(
        "http://market_gateway:8101/quote-state"
        if quotes_only
        else "http://market_gateway:8101/market-state",
        headers={"Authorization": "Bearer " + token},
        timeout=ClientTimeout(total=3),
        allow_redirects=False,
    ) as response:
        if response.status != 200:
            raise ValueError("MARKET_HANDOFF_UNAVAILABLE")
        body = bytearray()
        async for chunk in response.content.iter_chunked(65536):
            if len(body) + len(chunk) > MAX_BODY:
                raise ValueError("MARKET_HANDOFF_CAPACITY")
            body.extend(chunk)
        feed = json.loads(body)
        if not isinstance(feed, dict) or not isinstance(
            feed.get("quotes" if quotes_only else "markets"), list
        ):
            raise ValueError("MARKET_HANDOFF_INVALID")
        return feed


def alpha_inputs(
    db: Database,
    features: FeatureSnapshot,
    feed: dict[str, Any],
    *,
    evaluated_at: datetime | None = None,
    diagnostics: dict[str, Any] | None = None,
) -> tuple[MomentumInput, ...]:
    """Join evidence known at evaluation, retaining the feature candle revision."""
    feed_at = utc(datetime.fromisoformat(feed["as_of"]))
    at = utc(evaluated_at) if evaluated_at is not None else feed_at
    detail = diagnostics if diagnostics is not None else {}
    rejected: list[dict[str, str]] = []
    detail["input_rejections"] = rejected

    def reject(symbol: object, reason: str) -> None:
        if (
            len(rejected) < 64
            and isinstance(symbol, str)
            and re.fullmatch(r"[A-Z0-9_]{2,40}", symbol)
        ):
            rejected.append({"symbol": symbol, "reason": reason})

    for status in feed.get("deep_status", [])[:16]:
        if status.get("ready") is not True:
            reason = status.get("reason")
            reject(
                status.get("symbol"),
                reason
                if reason
                in CANONICAL_BAR_REASONS
                | {
                    "CLOCK_UNSAFE",
                    "BOOK_UNAVAILABLE",
                    "FLOW_UNAVAILABLE",
                    "BAR_RECOVERY_REQUIRED",
                    "CANONICAL_DATA_NOT_READY",
                    "STORAGE_ADMISSION_HALTED",
                    "CANONICAL_STORAGE_UNAVAILABLE",
                }
                else "CANONICAL_DATA_NOT_READY",
            )
    deep = feed["deep"]
    if len(deep) > 16:
        raise ValueError("MARKET_HANDOFF_CAPACITY")
    if feed["status"].get("data_ready") is not True:
        for row in deep:
            reject(row.get("symbol"), "DATA_UNAVAILABLE")
        return ()
    if at < feed_at or features.frame.as_of > at:
        for row in deep:
            reject(row.get("symbol"), "FUTURE_INPUT")
        return ()
    markets = {}
    for row in feed["markets"]:
        try:
            market = MarketSnapshot.model_validate_json(json.dumps(row))
            if market.as_of <= at:
                markets[market.symbol] = market
        except (KeyError, TypeError, ValueError):
            continue
    keys = {k.symbol: k for k in features.frame.universe.eligible}
    feature_rows = {row.instrument.symbol: row for row in features.frame.observations}
    result: list[MomentumInput] = []
    for row in deep:
        symbol = row.get("symbol")
        if symbol not in keys:
            reject(symbol, "UNIVERSE_UNAVAILABLE")
            continue
        if symbol not in markets:
            reject(symbol, "MARKET_UNAVAILABLE")
            continue
        market = markets[symbol]
        if not market.costs_fresh(at, 5):
            reject(symbol, "MARKET_UNAVAILABLE")
            continue
        if not market.volume_fresh(at, 5):
            reject(symbol, "VOLUME_UNAVAILABLE")
            continue
        key = keys[symbol]
        reason = "BOOK_UNAVAILABLE"
        try:
            book = OrderBookSnapshot.model_validate_json(json.dumps(row["book"]))
            reason = "FLOW_UNAVAILABLE"
            flow = TradeFlowWindow.model_validate_json(json.dumps(row["flow"]))
            reason = "FEATURE_BAR_UNAVAILABLE"
            medium = load_bars(db, key, Timeframe.M15, features.frame.as_of, limit=128)
            medium = tuple(bar for bar in medium if bar.close_at <= features.frame.source_at)
            feature_row = feature_rows.get(symbol)
            if not medium or feature_row is None or medium[-1] != feature_row.current:
                reject(symbol, reason)
                continue
            state = MarketState(capacity=256)
            # A bounded SQL window can start partway through an hour. Its
            # missing prefix is outside the retained evidence, not a market gap.
            # Resample from the first full hour; interior/trailing gaps still fail.
            first_hour = medium[0].open_at.replace(minute=0, second=0, microsecond=0)
            if first_hour < medium[0].open_at:
                first_hour += timedelta(hours=1)
            for bar in medium:
                if bar.open_at >= first_hour:
                    state.ingest_bar(bar)
            reason = "BAR_RECOVERY_REQUIRED"
            setup = load_bars(db, key, Timeframe.M5, at, limit=64)
            higher = state.view(key, Timeframe.H1, at)
            common = dict(
                instrument=key,
                as_of=at,
                features=features,
                setup_bars=setup,
                medium_bars=medium[-64:],
                higher_bars=higher.bars[-64:],
                market=markets[symbol],
                book=book,
                flow=flow,
                quality=higher.quality,
            )
            reason = "ALPHA_INPUT_INVALID"
            sample = MomentumInput.model_validate(common)
            try:
                sample = ReversalInput.model_validate(
                    {
                        **common,
                        "previous_flow": TradeFlowWindow.model_validate_json(
                            json.dumps(row["previous_flow"])
                        ),
                    }
                )
            except (KeyError, TypeError, ValueError):
                reject(symbol, "PREVIOUS_FLOW_UNAVAILABLE")
            result.append(sample)
        except (KeyError, TypeError, ValueError):
            reject(symbol, reason)
    return tuple(result)
