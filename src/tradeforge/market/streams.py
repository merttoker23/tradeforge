"""Credential-free routed market wire contracts; no book or bar reconstruction."""

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Literal

from tradeforge.domain.common import utc
from tradeforge.market.exchange_info import FetchError, decode_payload, milliseconds

Route = Literal["public", "market"]
GLOBAL_STREAMS = ("!ticker@arr", "!markPrice@arr@1s")
PARTIAL_DEPTH = "depth20@500ms"
_STREAM = re.compile(
    r"[a-z0-9]+usdt@(?:bookTicker|depth(?:@100ms|@500ms|20@500ms)?|aggTrade|kline_(?:1m|5m|15m))"
)


@dataclass(frozen=True)
class StreamSpec:
    name: str

    def __post_init__(self) -> None:
        if self.name not in GLOBAL_STREAMS and not _STREAM.fullmatch(self.name):
            raise ValueError("UNSUPPORTED_PUBLIC_STREAM")

    @property
    def route(self) -> Route:
        return "public" if "@bookTicker" in self.name or "@depth" in self.name else "market"

    @property
    def symbol(self) -> str | None:
        return None if self.name.startswith("!") else self.name.split("@")[0].upper()

    @property
    def partial_depth(self) -> bool:
        return self.name.endswith("@" + PARTIAL_DEPTH)


@dataclass(frozen=True)
class MarketEvent:
    stream: StreamSpec
    symbol: str
    event_time: datetime
    received_at: datetime
    generation: int
    sequence: int  # Local per-socket receipt ordinal, NOT an exchange sequence.
    exchange_sequence: int | None
    values: Mapping[str, Decimal]
    payload_json: str
    schema_version: int = 1


def decimal_field(row: dict[str, object], key: str, *, signed: bool = False) -> Decimal:
    value = row.get(key)
    if not isinstance(value, str) or len(value) > 80:
        raise ValueError("INVALID_MARKET_DECIMAL")
    try:
        result = Decimal(value)
    except InvalidOperation:
        raise ValueError("INVALID_MARKET_DECIMAL") from None
    if not result.is_finite() or (result.is_zero() and result.is_signed()):
        raise ValueError("INVALID_MARKET_DECIMAL")
    if not signed and result < 0:
        raise ValueError("NEGATIVE_MARKET_VALUE")
    return result


def _counter(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("INVALID_MARKET_SEQUENCE")
    return value


def decode_events(
    message: str,
    *,
    route: Route,
    received_at: datetime,
    generation: int,
    sequence: int,
    eligible: frozenset[str],
    max_age: float = 15,
    max_future: float = 0,
) -> tuple[MarketEvent, ...]:
    """Eligible rows validate before publication; documented CM rows are excluded.

    The fstream route establishes USD-M provenance. Explicit market discriminators
    must agree. `st=1` denotes USD-M in the shared market payload, `st=2` COIN-M.
    Unknown symbols are filtered against the current authoritative registry.

    `max_future` bounds how far an exchange timestamp may appear ahead of the
    local clock. It must match the clock policy the caller trusts: a clock that
    is allowed a skew may not reject every event of that stream.
    """
    try:
        payload = decode_payload(message.encode())
    except FetchError:
        raise ValueError("INVALID_MARKET_JSON") from None
    return decode_frame(
        payload,
        route=route,
        received_at=received_at,
        generation=generation,
        sequence=sequence,
        eligible=eligible,
        max_age=max_age,
        max_future=max_future,
    )


def decode_frame(
    payload: object,
    *,
    route: Route,
    received_at: datetime,
    generation: int,
    sequence: int,
    eligible: frozenset[str],
    max_age: float = 15,
    max_future: float = 0,
    closed_only: bool = False,
) -> tuple[MarketEvent, ...]:
    utc(received_at)
    _counter(generation)
    _counter(sequence)
    if not isinstance(payload, dict) or not isinstance(payload.get("stream"), str):
        raise ValueError("COMBINED_MARKET_FRAME_REQUIRED")
    spec = StreamSpec(payload["stream"])
    if spec.route != route:
        raise ValueError("WRONG_MARKET_ROUTE")
    data = payload.get("data")
    rows = data if spec.symbol is None else [data]
    if not isinstance(rows, list):
        raise ValueError("INVALID_MARKET_ARRAY")
    result = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("INVALID_MARKET_ROW")
        symbol = row.get("s")
        if not isinstance(symbol, str):
            raise ValueError("MISSING_MARKET_SYMBOL")
        if "st" in row:
            if type(row["st"]) is not int or row["st"] not in (1, 2):
                raise ValueError("WRONG_MARKET_TYPE")
            if row["st"] == 2:
                if symbol in eligible:
                    raise ValueError("CONFLICTING_MARKET_IDENTITY")
                continue
        if any(row.get(k, "USD_M") != "USD_M" for k in ("market_type", "marketType")):
            raise ValueError("WRONG_MARKET_TYPE")
        if symbol not in eligible:
            continue
        if spec.symbol is not None and spec.symbol != symbol:
            raise ValueError("STREAM_SYMBOL_MISMATCH")
        if closed_only and "@kline_" in spec.name:
            candle = row.get("k")
            if isinstance(candle, dict) and candle.get("x") is False:
                continue
        source = milliseconds(row.get("E"))
        age = (received_at - source).total_seconds()
        if not -max_future <= age <= max_age:
            if spec.symbol is None and age > max_age:
                # A market array carries one row per symbol. A symbol that
                # has not traded keeps its own old timestamp; that row is
                # excluded, but the valid peers of the same frame must
                # survive. A future timestamp is a broken clock and fails
                # the whole frame.
                continue
            raise ValueError("STALE_OR_FUTURE_MARKET_EVENT")
        values: dict[str, Decimal] = {}
        exchange_sequence = None
        positive: tuple[str, ...]
        unsigned: tuple[str, ...]
        signed: tuple[str, ...]
        if spec.name == "!ticker@arr":
            expected, positive, unsigned, signed = (
                "24hrTicker",
                ("c", "o", "h", "l"),
                ("v", "q"),
                ("P",),
            )
        elif spec.name == "!markPrice@arr@1s":
            expected, positive, unsigned, signed = "markPriceUpdate", ("p", "i"), (), ("r",)
            if milliseconds(row.get("T")) < source:
                # Testnet can publish a stale next-funding timestamp for one
                # symbol. Exclude that row without discarding valid peers or
                # accepting stale funding evidence into readiness.
                continue
        elif "@bookTicker" in spec.name:
            expected, positive, unsigned, signed = "bookTicker", ("b", "a"), ("B", "A"), ()
            exchange_sequence = _counter(row.get("u"))
        elif "@aggTrade" in spec.name:
            expected, positive, unsigned, signed = "aggTrade", ("p",), ("q",), ()
            exchange_sequence = _counter(row.get("a"))
        elif "@depth" in spec.name:
            expected, positive, unsigned, signed = "depthUpdate", (), (), ()
            exchange_sequence = _counter(row.get("u"))
            if _counter(row.get("U")) > exchange_sequence:
                raise ValueError("INVERTED_DEPTH_SEQUENCE")
            _counter(row.get("pu"))
            for side in ("b", "a"):
                levels = row.get(side)
                if not isinstance(levels, list) or len(levels) > 10000:
                    raise ValueError("INVALID_DEPTH_LEVELS")
                for level in levels:
                    if not isinstance(level, list) or len(level) != 2:
                        raise ValueError("INVALID_DEPTH_LEVEL")
                    if decimal_field({"p": level[0]}, "p") <= 0:
                        raise ValueError("INVALID_DEPTH_PRICE")
                    decimal_field({"q": level[1]}, "q")
        else:
            expected, positive, unsigned, signed = "kline", (), (), ()
            bar = row.get("k")
            if not isinstance(bar, dict) or bar.get("s") != symbol:
                raise ValueError("INVALID_KLINE")
            if bar.get("i") != spec.name.split("_", 1)[1] or type(bar.get("x")) is not bool:
                raise ValueError("INVALID_KLINE_INTERVAL")
            for key in ("o", "h", "l", "c", "v"):
                values[key] = decimal_field(bar, key)
        if row.get("e") != expected:
            raise ValueError("WRONG_MARKET_EVENT_TYPE")
        for key in positive + unsigned + signed:
            values[key] = decimal_field(row, key, signed=key in signed)
            if key in positive and values[key] <= 0:
                raise ValueError("NONPOSITIVE_PRICE")
        if "@bookTicker" in spec.name and values["b"] > values["a"]:
            raise ValueError("CROSSED_BOOK")
        result.append(
            MarketEvent(
                spec,
                symbol,
                source,
                received_at,
                generation,
                sequence,
                exchange_sequence,
                MappingProxyType(values),
                json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False),
            )
        )
    return tuple(result)
