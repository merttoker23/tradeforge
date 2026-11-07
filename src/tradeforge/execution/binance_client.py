"""Fixed-host TESTNET-only REST. No endpoint override, withdrawal or mainnet path.

Wire contract checked against Binance USD-M REST trade/general-info on 2026-10-20:
https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade
https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info
Normal LIMIT/MARKET use /order; conditional protection uses /algoOrder.
"""

import hashlib
import hmac
import http.client
import json
import math
import socket
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from threading import Event, Timer
from typing import Protocol
from urllib.parse import urlencode

from pydantic import Field, SecretStr

from tradeforge.domain.common import StrictModel, utc
from tradeforge.domain.decision import Side
from tradeforge.domain.execution import ExecutionIntent, PositionMode
from tradeforge.execution.broker import AckUnknown, ExchangeFill, ExchangeRejected, ExchangeReport
from tradeforge.execution.transport_diagnostics import transport_failure
from tradeforge.market.exchange_info import milliseconds

HOST = "demo-fapi.binance.com"
PRIVATE_WS = "wss://demo-fstream.binance.com/private/ws/"
MAX_BODY = 2_000_000


class Credentials(StrictModel):
    api_key: SecretStr = Field(repr=False)
    api_secret: SecretStr = Field(repr=False)


@dataclass(frozen=True)
class Response:
    status: int
    headers: Mapping[str, str]
    body: bytes


class Transport(Protocol):
    def __call__(
        self, method: str, path: str, params: str, headers: Mapping[str, str]
    ) -> Response: ...


def send_https(
    method: str, path: str, params: str, headers: Mapping[str, str], *, host: str = HOST
) -> Response:
    """One bounded attempt. No redirects, proxies, retry, URL logging or raw errors."""
    if host not in {"fapi.binance.com", "demo-fapi.binance.com"}:
        raise ValueError("UNSAFE_EXCHANGE_HOST")
    connection = http.client.HTTPSConnection(host, timeout=3)
    expired = Event()
    active_socket: socket.socket | None = None
    response: http.client.HTTPResponse | None = None
    phase = "CONNECTION"

    def abort() -> None:
        expired.set()
        try:
            current = connection.sock or active_socket
            if current is not None:
                current.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        finally:
            connection.close()

    timer = Timer(5, abort)
    timer.daemon = True
    timer.start()
    try:
        # Complete setup explicitly so a later request error really is SEND.
        connection.connect()
        # A watchdog-closed socket must not cause HTTPConnection.send to connect
        # implicitly and send the mutation after its deadline.
        connection.auto_open = 0
        active_socket = connection.sock
        if expired.is_set():
            raise TimeoutError
        target = path + ("?" + params if method == "GET" and params else "")
        phase = "SEND"
        connection.request(
            method, target, body=None if method == "GET" else params, headers=headers
        )
        phase = "RESPONSE_HEADERS"
        response = connection.getresponse()
        phase = "RESPONSE_BODY"
        body = response.read(MAX_BODY + 1)
        if expired.is_set():
            raise TimeoutError
        if len(body) > MAX_BODY:
            raise AckUnknown("EXCHANGE_RESPONSE_TOO_LARGE")
        reply = Response(response.status, dict(response.getheaders()), body)
    except (OSError, http.client.HTTPException) as error:
        raise transport_failure(error, phase, expired=expired.is_set()) from None
    finally:
        timer.cancel()
        timer.join()
        if response is not None:
            response.close()
        connection.close()
    # Join the watchdog before accepting success; it may have started just after
    # the read completed. It can no longer race this check or the next request.
    if expired.is_set():
        raise transport_failure(TimeoutError(), phase, expired=True) from None
    return reply


def number(value: object) -> Decimal:
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError("INVALID_EXCHANGE_NUMBER")
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        raise ValueError("INVALID_EXCHANGE_NUMBER") from None
    if not parsed.is_finite():
        raise ValueError("INVALID_EXCHANGE_NUMBER")
    return parsed


class BinanceTestnetClient:
    environment = "testnet"

    def __init__(
        self,
        credentials: Credentials,
        *,
        mode: str,
        opted_in: bool = False,
        read_only: bool = True,
        transport: Transport = send_https,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if mode != "testnet" or opted_in is not True:
            raise ValueError("TESTNET_OPT_IN_REQUIRED")
        if (
            not credentials.api_key.get_secret_value()
            or not credentials.api_secret.get_secret_value()
        ):
            raise ValueError("TESTNET_CREDENTIALS_REQUIRED")
        self.credentials, self.transport = credentials, transport
        self.read_only = read_only
        self.clock = clock or (lambda: datetime.now(UTC))
        self.retry_at = 0.0

    def request(
        self,
        method: str,
        path: str,
        params: Mapping[str, str] | None = None,
        *,
        signed: bool = True,
    ) -> object:
        if self.read_only and method != "GET":
            raise ValueError("DEMO_READ_ONLY")
        allowed = {
            ("GET", "/fapi/v1/ping"),
            ("GET", "/fapi/v3/balance"),
            ("POST", "/fapi/v1/order"),
            ("GET", "/fapi/v1/order"),
            ("DELETE", "/fapi/v1/order"),
            ("GET", "/fapi/v1/userTrades"),
            ("POST", "/fapi/v1/algoOrder"),
            ("GET", "/fapi/v1/algoOrder"),
            ("DELETE", "/fapi/v1/algoOrder"),
            ("GET", "/fapi/v3/positionRisk"),
            ("GET", "/fapi/v3/account"),
            ("GET", "/fapi/v1/symbolConfig"),
            ("POST", "/fapi/v1/marginType"),
            ("POST", "/fapi/v1/leverage"),
            ("GET", "/fapi/v1/positionSide/dual"),
            ("GET", "/fapi/v1/time"),
            ("GET", "/fapi/v1/openOrders"),
            ("GET", "/fapi/v1/openAlgoOrders"),
            ("GET", "/fapi/v1/accountConfig"),
            ("GET", "/fapi/v1/income"),
            ("GET", "/fapi/v1/commissionRate"),
            ("GET", "/fapi/v1/exchangeInfo"),
            ("GET", "/fapi/v1/ticker/bookTicker"),
            ("GET", "/fapi/v1/premiumIndex"),
            ("POST", "/fapi/v1/listenKey"),
            ("PUT", "/fapi/v1/listenKey"),
            ("DELETE", "/fapi/v1/listenKey"),
        }
        if (method, path) not in allowed:
            raise ValueError("ENDPOINT_NOT_ALLOWED")
        if time.monotonic() < self.retry_at:
            raise ExchangeRejected("RATE_LIMITED")
        values = dict(params or {})
        if signed:
            values.update(
                timestamp=str(int(utc(self.clock()).timestamp() * 1000)), recvWindow="3000"
            )
        encoded = urlencode(sorted(values.items()))
        if signed:
            signature = hmac.new(
                self.credentials.api_secret.get_secret_value().encode(),
                encoded.encode(),
                hashlib.sha256,
            ).hexdigest()
            encoded += "&signature=" + signature
        try:
            response = self.transport(
                method,
                path,
                encoded,
                {
                    "X-MBX-APIKEY": self.credentials.api_key.get_secret_value(),
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
            if len(response.body) > MAX_BODY:
                raise AckUnknown("EXCHANGE_RESPONSE_TOO_LARGE")
            if response.status in {418, 429}:
                header = next(
                    (v for k, v in response.headers.items() if k.lower() == "retry-after"), "60"
                )
                try:
                    delay = float(header)
                    if not math.isfinite(delay) or delay < 0:
                        raise ValueError
                except ValueError:
                    delay = 60
                self.retry_at = time.monotonic() + max(1, min(delay, 86400))
                raise ExchangeRejected("RATE_LIMITED")
            if response.status >= 500 or response.status == 408:
                raise AckUnknown("ACK_UNKNOWN")
            body = json.loads(response.body)
            if response.status != 200:
                code = body.get("code") if isinstance(body, dict) else None
                if code in {-2013, -2011} and method == "GET":
                    return None
                if code in {-1000, -1001, -1006, -1007}:
                    raise AckUnknown("ACK_UNKNOWN")
                reasons = {
                    -2019: "MARGIN_INSUFFICIENT",
                    -2014: "API_KEY_INVALID",
                    -2015: "API_KEY_PERMISSION_OR_IP_DENIED",
                    -1021: "CLOCK_UNSAFE",
                    -1022: "SIGNATURE_INVALID",
                }
                raise ExchangeRejected(
                    reasons.get(code, "ORDER_REJECTED")
                    if isinstance(code, int)
                    else "ORDER_REJECTED"
                )
            return body
        except (OSError, TimeoutError, ValueError, TypeError):
            raise AckUnknown("EXCHANGE_RESPONSE_INVALID") from None

    def _report(self, body: object, intent: ExecutionIntent) -> ExchangeReport:
        try:
            if not isinstance(body, dict):
                raise ValueError
            filled = number(body["executedQty"])
            trades = (
                self.request(
                    "GET",
                    "/fapi/v1/userTrades",
                    {
                        "symbol": intent.symbol,
                        "orderId": str(body["orderId"]),
                        "limit": "1000",
                    },
                )
                if filled
                else []
            )
            if not isinstance(trades, list):
                raise ValueError
            fills = []
            for row in trades:
                if (
                    not isinstance(row, dict)
                    or row["symbol"] != intent.symbol
                    or str(row["orderId"]) != str(body["orderId"])
                    or row["commissionAsset"] != "USDT"
                    or type(row["id"]) is not int
                ):
                    raise ValueError
                fills.append(
                    ExchangeFill(
                        trade_id=f"{intent.symbol}:{row['id']}",
                        qty=number(row["qty"]),
                        price=number(row["price"]),
                        fee=number(row["commission"]),
                        exchange_at=milliseconds(row["time"]),
                    )
                )
            return ExchangeReport(
                client_order_id=body["clientOrderId"],
                symbol=body["symbol"],
                order_id=str(body["orderId"]),
                side=body["side"],
                qty=number(body["origQty"]),
                filled_qty=filled,
                status=body["status"],
                avg_price=number(body["avgPrice"]) if filled else None,
                exchange_at=milliseconds(body["updateTime"]),
                fills=tuple(fills),
            )
        except (KeyError, TypeError, ValueError):
            raise AckUnknown("ORDER_REPORT_INVALID") from None

    def place(self, intent: ExecutionIntent) -> ExchangeReport:
        if intent.position_mode is not PositionMode.ONE_WAY:
            raise ExchangeRejected("HEDGE_MODE_UNSUPPORTED")
        values = {
            "symbol": intent.symbol,
            "side": "BUY" if intent.side is Side.LONG else "SELL",
            "positionSide": "BOTH",
            "type": "LIMIT" if intent.price is not None else "MARKET",
            "quantity": format(intent.qty, "f"),
            "newClientOrderId": intent.client_order_id,
            "reduceOnly": "true" if intent.reduce_only else "false",
            "newOrderRespType": "RESULT",
        }
        if intent.price is not None:
            values.update(price=format(intent.price, "f"), timeInForce="IOC")
        body = self.request("POST", "/fapi/v1/order", values)
        try:
            return self._report(body, intent)
        except ExchangeRejected:
            # A successful POST cannot become a rejected order because a later
            # read was throttled. Keep its durable reservation for reconciliation.
            raise AckUnknown("ACCEPTED_ORDER_EVIDENCE_UNAVAILABLE") from None

    def query(self, intent: ExecutionIntent) -> ExchangeReport | None:
        body = self.request(
            "GET",
            "/fapi/v1/order",
            {
                "symbol": intent.symbol,
                "origClientOrderId": intent.client_order_id,
            },
        )
        return None if body is None else self._report(body, intent)

    def cancel(self, intent: ExecutionIntent) -> ExchangeReport:
        return self._report(
            self.request(
                "DELETE",
                "/fapi/v1/order",
                {
                    "symbol": intent.symbol,
                    "origClientOrderId": intent.client_order_id,
                },
            ),
            intent,
        )

    def protect(self, intent: ExecutionIntent, client_id: str, *, query_only: bool = False) -> bool:
        if intent.stop_plan is None or intent.position_mode is not PositionMode.ONE_WAY:
            raise ExchangeRejected("STOP_PLAN_REQUIRED")
        params = {"clientAlgoId": client_id}
        if not query_only:
            params.update(
                algoType="CONDITIONAL",
                symbol=intent.symbol,
                side="SELL" if intent.side is Side.LONG else "BUY",
                positionSide="BOTH",
                type="STOP_MARKET",
                triggerPrice=format(intent.stop_plan.stop_price, "f"),
                closePosition="true",
                workingType="MARK_PRICE",
            )
        body = self.request("GET" if query_only else "POST", "/fapi/v1/algoOrder", params)
        return bool(
            isinstance(body, dict)
            and body.get("clientAlgoId") == client_id
            and body.get("symbol") == intent.symbol
            and body.get("algoStatus") == "NEW"
            and body.get("closePosition") is True
            and body.get("side") == ("SELL" if intent.side is Side.LONG else "BUY")
            and body.get("orderType") == "STOP_MARKET"
            and number(body.get("triggerPrice")) == intent.stop_plan.stop_price
        )


class BinanceLiveReadOnlyClient(BinanceTestnetClient):
    """Production account inspection; impossible to use for order mutations."""

    environment = "live"

    def __init__(
        self, credentials: Credentials, *, clock: Callable[[], datetime] | None = None
    ) -> None:
        from functools import partial

        if (
            not credentials.api_key.get_secret_value()
            or not credentials.api_secret.get_secret_value()
        ):
            raise ValueError("LIVE_CREDENTIALS_REQUIRED")
        # The live endpoint is hardcoded and never inherits an executable client.
        self.credentials = credentials
        self.transport = partial(send_https, host="fapi.binance.com")
        self.read_only = True
        self.clock = clock or (lambda: datetime.now(UTC))
        self.retry_at = 0.0

    def place(self, intent: ExecutionIntent) -> ExchangeReport:
        raise ExchangeRejected("LIVE_ORDERS_LOCKED")

    def cancel(self, intent: ExecutionIntent) -> ExchangeReport:
        raise ExchangeRejected("LIVE_ORDERS_LOCKED")

    def protect(self, intent: ExecutionIntent, client_id: str, *, query_only: bool = False) -> bool:
        if not query_only:
            raise ExchangeRejected("LIVE_ORDERS_LOCKED")
        return super().protect(intent, client_id, query_only=True)
