"""One GET-only HTTPS connection per diagnostic sample; no order-path changes."""

import http.client
import socket
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from threading import Event, Timer

from tradeforge.execution.binance_client import (
    HOST,
    MAX_BODY,
    BinanceTestnetClient,
    Response,
    Transport,
    send_https,
)
from tradeforge.execution.broker import AckUnknown
from tradeforge.execution.transport_diagnostics import (
    FAILURES,
    PHASES,
    TransportFailure,
    safe_exchange_reason,
    transport_failure,
)

PATHS = frozenset(
    {
        "/fapi/v1/ping",
        "/fapi/v1/time",
        "/fapi/v3/account",
        "/fapi/v3/balance",
        "/fapi/v3/positionRisk",
        "/fapi/v1/accountConfig",
        "/fapi/v1/positionSide/dual",
        "/fapi/v1/openOrders",
        "/fapi/v1/openAlgoOrders",
        "/fapi/v1/income",
        "/fapi/v1/userTrades",
    }
)


class DiagnosticTransport:
    def __init__(self) -> None:
        self.connection = http.client.HTTPSConnection(HOST, timeout=3)
        self.closed = False

    def close(self) -> None:
        self.closed = True
        self.connection.close()

    def __call__(self, method: str, path: str, params: str, headers: Mapping[str, str]) -> Response:
        if method != "GET":
            raise ValueError("DEMO_READ_ONLY")
        if path not in PATHS:
            raise ValueError("DIAGNOSTIC_ENDPOINT_NOT_ALLOWED")
        if self.closed:
            raise AckUnknown("EXCHANGE_TRANSPORT_UNKNOWN")
        public = path in {"/fapi/v1/ping", "/fapi/v1/time"}
        if public and params:
            raise ValueError("PUBLIC_DIAGNOSTIC_PARAMS_INVALID")
        expired = Event()
        phase = "CONNECTION"
        active_socket: socket.socket | None = None
        response: http.client.HTTPResponse | None = None

        def abort() -> None:
            expired.set()
            try:
                current = self.connection.sock or active_socket
                if current is not None:
                    current.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            finally:
                self.connection.close()

        timer = Timer(5, abort)
        timer.daemon = True
        timer.start()
        try:
            if self.connection.sock is None:
                self.connection.connect()
            self.connection.auto_open = 0
            active_socket = self.connection.sock
            if expired.is_set():
                raise TimeoutError
            target = path + ("?" + params if params else "")
            phase = "SEND"
            # Public probes carry neither credentials nor signed params.
            self.connection.request(method, target, headers={} if public else dict(headers))
            phase = "RESPONSE_HEADERS"
            response = self.connection.getresponse()
            phase = "RESPONSE_BODY"
            body = response.read(MAX_BODY + 1)
            if expired.is_set():
                raise TimeoutError
            if len(body) > MAX_BODY:
                self.close()
                raise AckUnknown("EXCHANGE_RESPONSE_TOO_LARGE")
            reply = Response(response.status, dict(response.getheaders()), body)
        except (OSError, http.client.HTTPException) as error:
            self.close()
            raise transport_failure(error, phase, expired=expired.is_set()) from None
        finally:
            timer.cancel()
            # A watchdog for this request must never close the next request's socket.
            timer.join()
            if response is not None:
                response.close()
        if expired.is_set():
            self.close()
            raise transport_failure(TimeoutError(), phase, expired=True) from None
        return reply


class DiagnosticClient(BinanceTestnetClient):
    def __init__(self, owner: BinanceTestnetClient, transport: Transport) -> None:
        super().__init__(
            owner.credentials,
            mode="testnet",
            opted_in=True,
            read_only=True,
            transport=transport,
            clock=owner.clock,
        )
        self.owner = owner
        self.retry_at = owner.retry_at
        self.account_requests: list[dict[str, object]] = []

    def request(
        self,
        method: str,
        path: str,
        params: Mapping[str, str] | None = None,
        *,
        signed: bool = True,
    ) -> object:
        self.retry_at = max(self.retry_at, self.owner.retry_at)
        started = time.perf_counter()
        outcome = "OK"
        evidence: dict[str, object] = {}
        try:
            return super().request(method, path, params, signed=signed)
        except Exception as error:
            # Record safely, then re-raise: programmer failures stay visible.
            outcome = safe_exchange_reason(error)
            if isinstance(error, TransportFailure):
                evidence = {
                    "phase": error.phase if error.phase in PHASES else "CONNECTION",
                    "failure": error.failure if error.failure in FAILURES else "IO",
                }
            raise
        finally:
            self.owner.retry_at = max(self.owner.retry_at, self.retry_at)
            if method == "GET" and path in PATHS and len(self.account_requests) < 32:
                self.account_requests.append(
                    {
                        "method": "GET",
                        "path": path,
                        "reason": outcome,
                        "duration_ms": round((time.perf_counter() - started) * 1000, 3),
                        **evidence,
                    }
                )


@contextmanager
def diagnostic_client(client: BinanceTestnetClient) -> Iterator[DiagnosticClient]:
    transport = DiagnosticTransport() if client.transport is send_https else None
    probe = DiagnosticClient(client, transport if transport is not None else client.transport)
    try:
        yield probe
    finally:
        client.retry_at = max(client.retry_at, probe.retry_at)
        if transport is not None:
            transport.close()
