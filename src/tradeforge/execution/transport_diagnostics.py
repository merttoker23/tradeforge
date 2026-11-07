"""Allowlisted transport evidence; never retain wire data or exception messages."""

import http.client
import socket
import ssl

from tradeforge.execution.broker import AckUnknown, ExchangeRejected

PHASES = frozenset(
    {"CONNECTION", "DNS", "CONNECT", "TLS", "SEND", "RESPONSE_HEADERS", "RESPONSE_BODY"}
)
FAILURES = frozenset({"IO", "HTTP", "TIMEOUT", "WATCHDOG"})
EXCHANGE_REASONS = frozenset(
    {
        "EXCHANGE_TRANSPORT_UNKNOWN",
        "EXCHANGE_RESPONSE_INVALID",
        "EXCHANGE_RESPONSE_TOO_LARGE",
        "ACK_UNKNOWN",
        "RATE_LIMITED",
        "MARGIN_INSUFFICIENT",
        "API_KEY_INVALID",
        "API_KEY_PERMISSION_OR_IP_DENIED",
        "CLOCK_UNSAFE",
        "SIGNATURE_INVALID",
        "ORDER_REJECTED",
    }
)


class TransportFailure(AckUnknown):
    """Keep acknowledgement ambiguity while carrying only bounded safe metadata."""

    def __init__(self, phase: str, failure: str) -> None:
        super().__init__("EXCHANGE_TRANSPORT_UNKNOWN")
        self.phase = phase if phase in PHASES else "CONNECTION"
        self.failure = failure if failure in FAILURES else "IO"


def transport_failure(error: Exception, phase: str, *, expired: bool) -> TransportFailure:
    # HTTPSConnection.connect includes DNS, TCP and TLS. Only exception classes
    # prove a finer phase; a generic setup error/timeout remains CONNECTION.
    if phase == "CONNECTION":
        if isinstance(error, socket.gaierror):
            phase = "DNS"
        elif isinstance(error, ssl.SSLError):
            phase = "TLS"
        elif isinstance(error, ConnectionRefusedError):
            phase = "CONNECT"
    if expired:
        failure = "WATCHDOG"
    elif isinstance(error, TimeoutError):
        failure = "TIMEOUT"
    elif isinstance(error, http.client.HTTPException):
        failure = "HTTP"
    else:
        failure = "IO"
    return TransportFailure(phase, failure)


def safe_exchange_reason(error: Exception) -> str:
    message = str(error)
    if isinstance(error, (AckUnknown, ExchangeRejected)) and message in EXCHANGE_REASONS:
        return message
    if isinstance(error, AckUnknown):
        return "ACK_UNKNOWN_RECONCILIATION_REQUIRED"
    if isinstance(error, ExchangeRejected):
        return "EXCHANGE_REJECTED"
    return "INPUT_INVALID"
