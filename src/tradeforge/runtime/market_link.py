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
from tradeforge.runtime.event_handoff import trusted_loopback

MAX_BODY = 32_000_000
MAX_REQUEST_SYMBOLS = 512


def memory_payload(
    gateway: MarketGateway,
    persistence: GatewayPersistence,
    *,
    quotes_only: bool = False,
    symbols: frozenset[str] | None = None,
    summary_only: bool = False,
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
            "quotes": [
                q.model_dump(mode="json")
                for q in persistence.display_quotes(at)
                if symbols is None or q.symbol in symbols
            ],
        }
    environment = getattr(gateway, "environment", None)
    rest_markets = {
        m.symbol: m
        for m in persistence.rest_samples
        if safe
        and m.symbol in gateway.eligible
        and m.costs_fresh(at, 15)
        and (environment != "testnet" or m.environment == "testnet")
    }
    markets = {
        symbol: market
        for symbol, market in rest_markets.items()
        if symbols is None or symbol in symbols
    }
    book_rows: list[object] = []
    quoted_symbols = set()
    for (name, _), (event, _) in gateway._latest.items():
        if "@bookTicker" not in name or gateway._generations.get(name) != event.generation:
            continue
        if symbols is not None and event.symbol not in symbols:
            continue
        if not 0 <= (at - event.event_time).total_seconds() <= 5:
            continue
        quoted_symbols.add(event.symbol)
        book_rows.append(
            {
                "symbol": event.symbol,
                "time": int(event.event_time.timestamp() * 1000),
                "bidPrice": str(event.values["b"]),
                "askPrice": str(event.values["a"]),
            }
        )
    if safe and getattr(gateway, "full_depth", False) and gateway.environment == "testnet":
        for symbol, book in gateway.canonical.books.items():
            if symbols is not None and symbol not in symbols:
                continue
            if symbol in quoted_symbols:
                continue
            top = book.top_of_book(at)
            if top is not None:
                stamp, bid, ask = top
                book_rows.append(
                    {
                        "symbol": symbol,
                        "time": int(stamp.timestamp() * 1000),
                        "bidPrice": str(bid),
                        "askPrice": str(ask),
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
        if symbols is not None and symbol not in symbols:
            continue
        ready = safe and gateway.canonical.symbol_ready(symbol, at)
        if ready:
            ready_symbols.add(symbol)
        flow = gateway.canonical.flows.get(symbol)
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
            if flow is None or flow.validity_at(at) is not DataQuality.OK
            else (gateway.canonical.bar_rejections(symbol, at) or ("CANONICAL_DATA_NOT_READY",))[0]
            if not ready
            else "READY"
        )
        deep_status.append(
            {
                "symbol": symbol,
                "ready": ready,
                "reason": reason,
                "book_state": book.state_at(at).value,
                "flow_quality": flow.validity_at(at).value if flow is not None else "MISSING",
                "bar_rejections": gateway.canonical.bar_rejections(symbol, at),
            }
        )
    if safe and not summary_only:
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
                    "previous_flow": flow.snapshot(
                        at - timedelta(seconds=flow.period_seconds), known_at=at
                    ).model_dump(mode="json"),
                }
            )
    status = gateway.status()
    composite_covered = sum(
        m.symbol in gateway.eligible and m.costs_fresh(at, 15) for m in markets.values()
    )
    status["analysis_market_covered"] = composite_covered
    status["analysis_volume_covered"] = sum(m.volume_fresh(at, 15) for m in markets.values())
    minimum_coverage = RegimePolicy().minimum_coverage
    # The Testnet all-market socket can reset independently of REST and the
    # per-symbol book/flow sockets. Measure broad REST coverage before applying
    # the symbol filter so a follow-up decision request sees the same gate.
    rest_fallback = bool(
        safe
        and environment == "testnet"
        and status.get("reason") == "STREAMS_STALE"
        and gateway.eligible
        and sum(m.source == "binance:rest" for m in rest_markets.values()) / len(gateway.eligible)
        >= minimum_coverage
    )
    if rest_fallback:
        status.update(data_ready=True, market_data_source="REST_FALLBACK")
    status["analysis_coverage_ready"] = bool(
        safe
        and status.get("data_ready") is True
        and gateway.eligible
        and (rest_fallback or composite_covered / len(gateway.eligible) >= minimum_coverage)
    )
    return {
        "as_of": at.isoformat(),
        "environment": environment,
        "status": status,
        "clock_safe": safe,
        "eligible": sorted(gateway.eligible),
        "quotes": []
        if summary_only
        else [
            q.model_dump(mode="json")
            for q in persistence.display_quotes(at)
            if symbols is None or q.symbol in symbols
        ],
        "markets": [
            m.model_dump(mode="json")
            for m in markets.values()
            if symbols is None or m.symbol in symbols
        ],
        "deep": [r for r in deep if symbols is None or r["symbol"] in symbols],
        "deep_status": [r for r in deep_status if symbols is None or r["symbol"] in symbols],
    }


def requested_symbols(request: web.Request) -> frozenset[str] | None:
    if set(request.query) - {"symbols", "detail"} or (
        "detail" in request.query
        and (request.path != "/market-state" or request.query.getall("detail") != ["summary"])
    ):
        raise web.HTTPBadRequest()
    if "symbols" not in request.query:
        return None
    parts = request.query["symbols"].split(",")
    if (
        not 1 <= len(parts) <= MAX_REQUEST_SYMBOLS
        or any(re.fullmatch(r"[A-Z0-9_]{2,40}", part) is None for part in parts)
        or len(parts) != len(set(parts))
    ):
        raise web.HTTPBadRequest()
    return frozenset(parts)


def add_memory_route(
    app: web.Application,
    gateway: MarketGateway,
    persistence: GatewayPersistence,
    token: str,
    *,
    loopback: bool = False,
) -> None:
    if len(token) < 43:
        raise ValueError("INTERNAL_TOKEN_REQUIRED")

    def verified(request: web.Request) -> bool:
        if loopback and not trusted_loopback(request):
            return False
        if (
            len(request.headers.getall("Authorization", [])) == 1
            and hmac.compare_digest(request.headers.get("Authorization", ""), "Bearer " + token)
            and "Origin" not in request.headers
            and "Sec-Fetch-Site" not in request.headers
        ):
            return True
        if not loopback or request.method != "GET":
            return False
        # A co-located dashboard has no token file. Its read-only feed is
        # accepted only from the loopback listener itself, exactly like the
        # tokenless route, so a remote or browser-originated request still
        # cannot read market memory without the bearer token.
        return "Authorization" not in request.headers

    async def memory(request: web.Request) -> web.Response:
        if not verified(request):
            raise web.HTTPForbidden()
        if request.path == "/chart-request":
            if set(request.query) != {"symbol", "timeframe"}:
                raise web.HTTPBadRequest()
            persistence.request_chart(request.query["symbol"], request.query["timeframe"])
            return web.json_response({"status": "REQUESTED"}, status=202)
        selected = requested_symbols(request)
        if request.query.get("detail") == "summary":
            payload = memory_payload(gateway, persistence, symbols=selected, summary_only=True)
        elif selected is None:
            payload = memory_payload(
                gateway, persistence, quotes_only=request.path == "/quote-state"
            )
        else:
            payload = memory_payload(
                gateway,
                persistence,
                quotes_only=request.path == "/quote-state",
                symbols=selected,
            )
        return web.json_response(payload, headers={"Cache-Control": "no-store"})

    app.router.add_get("/market-state", memory)
    app.router.add_get("/quote-state", memory)
    app.router.add_post("/chart-request", memory)


def add_loopback_memory_route(
    app: web.Application, gateway: MarketGateway, persistence: GatewayPersistence
) -> None:
    """Read-only public market handoff for co-located standalone endpoints.

    The HTTP listener must bind to 127.0.0.1, and this route independently
    verifies both ends of the connection and the exact Host header. This
    blocks remote access and browser-based DNS-rebinding/CORS requests.
    Compose/VPS containers continue to use add_memory_route with their
    existing internal bearer token instead.
    """

    async def memory(request: web.Request) -> web.Response:
        if not trusted_loopback(request) or "Authorization" in request.headers:
            raise web.HTTPForbidden()
        selected = requested_symbols(request)
        if request.query.get("detail") == "summary":
            payload = memory_payload(gateway, persistence, symbols=selected, summary_only=True)
        elif selected is None:
            payload = memory_payload(
                gateway, persistence, quotes_only=request.path == "/quote-state"
            )
        else:
            payload = memory_payload(
                gateway,
                persistence,
                quotes_only=request.path == "/quote-state",
                symbols=selected,
            )
        return web.json_response(payload, headers={"Cache-Control": "no-store"})

    app.router.add_get("/market-state", memory)
    app.router.add_get("/quote-state", memory)


async def fetch_memory(
    session: ClientSession,
    token: str | None,
    *,
    quotes_only: bool = False,
    symbols: frozenset[str] | None = None,
    summary_only: bool = False,
    base: str | None = None,
) -> dict[str, Any]:
    """Explicit base wins; otherwise loopback without a token, Compose DNS with one."""
    if base is None:
        base = "http://127.0.0.1:8101" if token is None else "http://market_gateway:8101"
    headers = {} if token is None else {"Authorization": "Bearer " + token}
    params = {"detail": "summary"} if summary_only else {}
    if symbols and len(symbols) <= MAX_REQUEST_SYMBOLS:
        params["symbols"] = ",".join(sorted(symbols))
    async with session.get(
        base + ("/quote-state" if quotes_only else "/market-state"),
        headers=headers,
        params=params or None,
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


def require_market_environment(feed: dict[str, Any], expected: str | None) -> None:
    if expected is not None and feed.get("environment") != expected:
        raise ValueError("TESTNET_ENVIRONMENT_MISMATCH")


def alpha_inputs(
    db: Database,
    features: FeatureSnapshot,
    feed: dict[str, Any],
    *,
    evaluated_at: datetime | None = None,
    diagnostics: dict[str, Any] | None = None,
    symbols: frozenset[str] | None = None,
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
    deep = (
        [row for row in feed["deep"] if row.get("symbol") in symbols]
        if symbols is not None
        else feed["deep"]
    )
    if len(deep) > 2048:
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
            if market.as_of <= at and (
                feed.get("environment") != "testnet" or market.environment == "testnet"
            ):
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
                previous_flow = TradeFlowWindow.model_validate_json(
                    json.dumps(row["previous_flow"])
                )
                if previous_flow.validity is not DataQuality.OK:
                    raise ValueError("PREVIOUS_FLOW_UNAVAILABLE")
                sample = ReversalInput.model_validate({**common, "previous_flow": previous_flow})
            except (KeyError, TypeError, ValueError):
                reject(symbol, "PREVIOUS_FLOW_UNAVAILABLE")
            result.append(sample)
        except (KeyError, TypeError, ValueError):
            reject(symbol, reason)
    return tuple(result)
