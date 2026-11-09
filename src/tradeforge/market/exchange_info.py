"""Binance USD-M public exchangeInfo parsing and bounded read-only HTTPS access."""

import http.client
import json
import math
import random
import re
import socket
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from threading import Event, Timer
from uuid import NAMESPACE_URL, uuid5

from tradeforge.domain.common import StrictModel, utc
from tradeforge.domain.instrument import ContractStatus, Instrument
from tradeforge.market.filter_normalizer import QuantityLimits, normalize_filters

FILTERING_RULES = (
    "BINANCE_USD_M_USDT_PERPETUAL_V1",
    "TRADING",
    "ACTIVE_DATES",
    "POSITIVE_KNOWN_FILTERS",
    "LIMIT_AND_MARKET",
)


class ContractMetadata(Instrument):
    market_lot: QuantityLimits
    constraints_json: str


class Exclusion(StrictModel):
    symbol: str
    reason: str


class ParsedUniverse(StrictModel):
    instruments: tuple[ContractMetadata, ...]
    excluded: tuple[Exclusion, ...]
    total: int
    weight_limit: int


def milliseconds(value: object) -> datetime:
    if type(value) is not int or not 0 <= value <= 253402300799000:
        raise ValueError("INVALID_EXCHANGE_DATE")
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=value)


def parse_exchange_info(payload: object, observed_at: datetime) -> ParsedUniverse:
    """The fixed fapi endpoint establishes provenance; explicit contradictions reject.

    serverTime is explicitly documented as ignored by Binance. These metadata
    observations become known at local UTC receipt, never the supplied serverTime.
    """
    utc(observed_at)
    if not isinstance(payload, dict) or payload.get("timezone") != "UTC":
        raise ValueError("INVALID_EXCHANGE_INFO")
    if payload.get("exchangeFilters") != []:
        raise ValueError("UNSUPPORTED_EXCHANGE_FILTERS")
    rows, limits = payload.get("symbols"), payload.get("rateLimits")
    if not isinstance(rows, list) or not rows or not isinstance(limits, list):
        raise ValueError("EMPTY_OR_INVALID_EXCHANGE_INFO")
    weight_limits = [
        r
        for r in limits
        if isinstance(r, dict)
        and r.get("rateLimitType") == "REQUEST_WEIGHT"
        and r.get("interval") == "MINUTE"
        and r.get("intervalNum") == 1
    ]
    if len(weight_limits) != 1:
        raise ValueError("MISSING_RATE_LIMIT")
    budget = weight_limits[0].get("limit")
    if type(budget) is not int or budget <= 0:
        raise ValueError("INVALID_RATE_LIMIT")
    instruments, excluded, seen = [], [], set()
    unusable_filters = 0
    for row in rows:
        required = ("symbol", "status", "contractType", "quoteAsset", "marginAsset")
        if not isinstance(row, dict) or any(
            not isinstance(row.get(k), str) or not row[k] for k in required
        ):
            raise ValueError("MISSING_CONTRACT_IDENTITY_OR_STATUS")
        name = row["symbol"]
        if name in seen:
            raise ValueError("DUPLICATE_SYMBOL")
        seen.add(name)
        reason = None
        if (
            row.get("marketType", "USD_M") != "USD_M"
            or row.get("market_type", "USD_M") != "USD_M"
            or row["quoteAsset"] != "USDT"
            or row["marginAsset"] != "USDT"
            or row["contractType"] != "PERPETUAL"
            or not name.endswith("USDT")
        ):
            reason = "UNSUPPORTED_CONTRACT"
        elif row["status"] != "TRADING":
            reason = "STATUS_NOT_TRADING"
        elif not re.fullmatch(r"[A-Z0-9]+USDT", name):
            reason = "UNSUPPORTED_SYMBOL_FORMAT"
        else:
            onboard = milliseconds(row.get("onboardDate"))
            delivery = milliseconds(row.get("deliveryDate"))
            if delivery <= onboard:
                raise ValueError("INVALID_CONTRACT_DATES")
            if not onboard <= observed_at < delivery:
                reason = "OUTSIDE_ACTIVE_DATES"
        if reason:
            excluded.append(Exclusion(symbol=name, reason=reason))
            continue
        orders = row.get("orderTypes")
        if not isinstance(orders, list) or not all(isinstance(o, str) for o in orders):
            raise ValueError("MISSING_ORDER_TYPES")
        if not {"LIMIT", "MARKET"} <= set(orders):
            raise ValueError("UNSUPPORTED_ORDER_CAPABILITIES")
        try:
            filters, market_lot, constraints = normalize_filters(row.get("filters"))
        except ValueError as error:
            reason = str(error)
            # Testnet can publish individual TRADING instruments with zero
            # order capacity or unusable numeric constraints. Never admit these
            # instruments, but do not let them block valid independent symbols.
            # Unknown/structural constraints still halt the entire refresh.
            if reason not in {
                "UNSUPPORTED_POSITION_CONTROL",
                "INVALID_ORDER_LIMIT",
                "UNSUPPORTED_FILTER_VALUE",
            }:
                raise
            excluded.append(Exclusion(symbol=name, reason=reason))
            if reason != "UNSUPPORTED_POSITION_CONTROL":
                unusable_filters += 1
            continue
        identifier = uuid5(NAMESPACE_URL, f"{name}/{observed_at.isoformat()}/{constraints}")
        instruments.append(
            ContractMetadata.model_validate(
                {
                    "schema_version": 1,
                    "event_id": identifier,
                    "source_at": observed_at,
                    "observed_at": observed_at,
                    "as_of": observed_at,
                    "symbol": name,
                    "exchange": "BINANCE",
                    "market_type": "USD_M",
                    "quote": "USDT",
                    "margin_asset": "USDT",
                    "contract_type": "PERPETUAL",
                    "contract_status": ContractStatus.TRADING,
                    "filters": filters,
                    "onboard_at": onboard,
                    "delist_at": delivery,
                    "market_lot": market_lot,
                    "constraints_json": constraints,
                },
                context={"now": observed_at},
            )
        )
    # A response consisting only of broken constraints must not be advertised
    # as a healthy empty market. Preserve the original all-ineligible behavior.
    if unusable_filters and not instruments:
        raise ValueError("UNUSABLE_UNIVERSE_FILTERS")
    return ParsedUniverse(
        instruments=tuple(sorted(instruments, key=lambda i: i.symbol)),
        excluded=tuple(sorted(excluded, key=lambda i: i.symbol)),
        total=len(rows),
        weight_limit=budget,
    )


# Transport has no API-key surface and can only GET the public metadata resource.

MAX_BODY = 4 * 1024 * 1024


class FetchError(RuntimeError):
    """Redacted metadata failure; callers must close the universe entry gate."""


@dataclass(frozen=True)
class Reply:
    status: int
    headers: Mapping[str, str]
    body: bytes


def decode_payload(body: bytes) -> object:
    def invalid_constant(value: str) -> object:
        raise ValueError("NONFINITE_JSON")

    def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("DUPLICATE_JSON_KEY")
            result[key] = value
        return result

    if len(body) > MAX_BODY:
        raise FetchError("UNIVERSE_RESPONSE_TOO_LARGE")
    try:
        return json.loads(body, object_pairs_hook=unique, parse_constant=invalid_constant)
    except (ValueError, UnicodeError, RecursionError):
        raise FetchError("UNIVERSE_INVALID_JSON") from None


def https_get(
    headers: Mapping[str, str], timeout: float, *, host: str = "fapi.binance.com"
) -> Reply:
    if host not in {"fapi.binance.com", "demo-fapi.binance.com"}:
        raise ValueError("UNSAFE_MARKET_ENDPOINT")
    deadline = time.monotonic() + timeout
    connection = http.client.HTTPSConnection(host, timeout=timeout)
    expired = Event()
    active_socket: list[socket.socket] = []

    def abort() -> None:
        expired.set()
        sock = active_socket[0] if active_socket else connection.sock
        if sock is not None:
            try:
                # Shutdown interrupts makefile reads, including a dripped header line.
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    timer = Timer(timeout, abort)
    timer.daemon = True
    timer.start()
    try:
        connection.request("GET", "/fapi/v1/exchangeInfo", headers=dict(headers))
        if connection.sock is not None:
            active_socket.append(connection.sock)
        response = connection.getresponse()
        if expired.is_set():
            raise TimeoutError
        chunks: list[bytes] = []
        size = 0
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError
            # read1 performs at most one underlying buffered read per deadline check.
            if connection.sock is not None:
                connection.sock.settimeout(remaining)
            chunk = response.read1(min(65536, MAX_BODY + 1 - size))
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
            if size > MAX_BODY:
                raise FetchError("UNIVERSE_RESPONSE_TOO_LARGE")
        if expired.is_set():
            raise TimeoutError
        return Reply(response.status, dict(response.getheaders()), b"".join(chunks))
    finally:
        timer.cancel()
        connection.close()


class ExchangeInfoClient:
    def __init__(
        self,
        *,
        transport: Callable[[Mapping[str, str], float], Reply] = https_get,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        jitter: Callable[[], float] = random.random,
        timeout: float = 5.0,
        attempts: int = 3,
        environment: str = "live",
    ) -> None:
        from tradeforge.runtime.binance_environment import ENDPOINTS

        if environment not in ENDPOINTS:
            raise ValueError("BINANCE_ENVIRONMENT_INVALID")
        if transport is https_get and environment != "live":
            from functools import partial

            transport = partial(https_get, host=ENDPOINTS[environment][0])
        if not math.isfinite(timeout) or not 0 < timeout <= 30:
            raise ValueError("INVALID_HTTP_TIMEOUT")
        if type(attempts) is not int or not 1 <= attempts <= 5:
            raise ValueError("INVALID_HTTP_ATTEMPTS")
        self.transport, self.clock, self.sleep, self.jitter = transport, clock, sleep, jitter
        self.timeout, self.attempts = timeout, attempts
        self.not_before = 0.0
        self.weight_limit = 5  # Bounded bootstrap budget; replaced by validated exchangeInfo.
        self.used_weight = 0
        self.weight_until = 0.0

    def set_weight_limit(self, limit: int) -> None:
        if type(limit) is not int or limit <= 0:
            raise ValueError("INVALID_WEIGHT_LIMIT")
        self.weight_limit = limit

    def cooldown(self) -> float:
        limit_until = self.weight_until if self.used_weight >= self.weight_limit else 0.0
        return max(0.0, max(self.not_before, limit_until) - self.clock())

    def fetch(self, etag: str | None = None) -> Reply:
        if etag is not None and (len(etag) > 256 or any(ord(c) < 32 or ord(c) > 126 for c in etag)):
            raise ValueError("INVALID_ETAG")
        if self.clock() < self.not_before:
            raise FetchError("UNIVERSE_RATE_LIMITED")
        headers = {"If-None-Match": etag} if etag else {}
        for attempt in range(self.attempts):
            if self.clock() >= self.weight_until:
                self.used_weight = 0
                self.weight_until = self.clock() + 60
            if self.used_weight >= self.weight_limit:
                raise FetchError("UNIVERSE_RATE_LIMITED")
            self.used_weight += 1  # Every attempt consumes request weight, including errors.
            try:
                reply = self.transport(headers, self.timeout)
            except (OSError, http.client.HTTPException):
                reason = "UNIVERSE_NETWORK_ERROR"
            else:
                lower = {k.lower(): v for k, v in reply.headers.items()}
                used = lower.get("x-mbx-used-weight-1m", "")
                if used.isdigit():
                    self.used_weight = max(self.used_weight, int(used))
                if reply.status in {418, 429}:
                    delay = 180.0 if reply.status == 418 else 60.0
                    value = lower.get("retry-after")
                    if value:
                        try:
                            supplied = float(value)
                        except ValueError:
                            try:
                                supplied = (
                                    parsedate_to_datetime(value) - datetime.now(UTC)
                                ).total_seconds()
                            except (ValueError, TypeError, OverflowError):
                                supplied = delay
                        if math.isfinite(supplied) and supplied >= 0:
                            # Honor Retry-After; invalid values use safe defaults.
                            delay = max(1.0, supplied)
                    self.not_before = self.clock() + delay
                    raise FetchError("UNIVERSE_RATE_LIMITED")
                if reply.status == 200:
                    decode_payload(reply.body)
                    return reply
                if reply.status == 304 and etag:
                    return reply
                if reply.status != 408 and not 500 <= reply.status <= 599:
                    raise FetchError("UNIVERSE_HTTP_REJECTED")
                reason = "UNIVERSE_HTTP_UNAVAILABLE"
            if attempt + 1 < self.attempts:
                delay = min(8.0, 2**attempt) * (0.5 + 0.5 * self.jitter())
                self.sleep(delay)
        raise FetchError(reason)
