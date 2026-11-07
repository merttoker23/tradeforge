"""Real transport code with a fake network; no Binance connection is opened."""

import http.client
import json
import socket
import ssl

import pytest

from tests.unit.test_demo_readiness import demo_client
from tradeforge.execution import binance_client, diagnostics_transport
from tradeforge.execution.broker import AckUnknown
from tradeforge.execution.preflight import preflight

SECRET = "raw-exception api-key api-secret listenKey=private ?signature=private body-private"


class Watchdog:
    timers = []

    def __init__(self, seconds, callback):
        assert seconds == 5
        self.callback = callback
        self.cancelled = self.joined = False
        self.timers.append(self)

    def start(self):
        pass

    def cancel(self):
        self.cancelled = True

    def join(self):
        self.joined = True


def fake_network(
    monkeypatch,
    *,
    fail_at=None,
    error=None,
    watchdog=False,
    watchdog_before_send=False,
    watchdog_at_headers=False,
):
    calls = []
    Watchdog.timers = []

    class Socket:
        stopped = False

        def sendall(self, data):
            calls.append("write")

        def shutdown(self, how):
            assert how == socket.SHUT_RDWR
            self.stopped = True

    active = Socket()

    def step(stage):
        calls.append(stage)
        if stage == fail_at:
            raise error

    class Reply:
        status = 200
        closed = False

        def getheaders(self):
            if watchdog_at_headers:
                Watchdog.timers[-1].callback()
            return []

        def read(self, maximum):
            step("body")
            if watchdog:
                Watchdog.timers[-1].callback()
                assert active.stopped
            return b"{}"

        def close(self):
            self.closed = True

    reply = Reply()

    class Connection:
        auto_open = 1
        debuglevel = 0
        sock = None
        closed = False

        def __init__(self, host, timeout):
            assert host == "demo-fapi.binance.com" and timeout == 3

        def connect(self):
            step("connect")
            self.sock = active

        def request(self, *args, **kwargs):
            step("send")
            if watchdog_before_send:
                Watchdog.timers[-1].callback()
                http.client.HTTPConnection.send(self, b"private-mutation")
            if self.sock is None:
                self.connect()

        def getresponse(self):
            step("headers")
            self.sock = None  # Connection: close detaches the response-owned socket.
            return reply

        def close(self):
            self.closed = True
            self.sock = None

    monkeypatch.setattr(http.client, "HTTPSConnection", Connection)
    monkeypatch.setattr(binance_client, "Timer", Watchdog)
    monkeypatch.setattr(diagnostics_transport, "Timer", Watchdog)
    return calls, reply


def invoke(kind):
    transport = (
        diagnostics_transport.DiagnosticTransport()
        if kind == "diagnostic"
        else binance_client.send_https
    )
    return transport("GET", "/fapi/v1/time", "", {"X-MBX-APIKEY": "api-key"})


@pytest.mark.parametrize("kind", ["diagnostic", "execution"])
@pytest.mark.parametrize(
    "fail_at,error,phase,failure",
    [
        ("connect", socket.gaierror(SECRET), "DNS", "IO"),
        ("connect", ssl.SSLError(SECRET), "TLS", "IO"),
        ("connect", ConnectionRefusedError(SECRET), "CONNECT", "IO"),
        ("connect", OSError(SECRET), "CONNECTION", "IO"),
        ("connect", TimeoutError(SECRET), "CONNECTION", "TIMEOUT"),
        ("send", OSError(SECRET), "SEND", "IO"),
        ("send", TimeoutError(SECRET), "SEND", "TIMEOUT"),
        ("headers", http.client.HTTPException(SECRET), "RESPONSE_HEADERS", "HTTP"),
        ("headers", TimeoutError(SECRET), "RESPONSE_HEADERS", "TIMEOUT"),
        ("body", TimeoutError(SECRET), "RESPONSE_BODY", "TIMEOUT"),
        ("body", OSError(SECRET), "RESPONSE_BODY", "IO"),
    ],
)
def test_error_phase_is_safe_and_never_guesses_unobserved_connection_stage(
    monkeypatch, kind, fail_at, error, phase, failure
):
    calls, _ = fake_network(monkeypatch, fail_at=fail_at, error=error)
    with pytest.raises(AckUnknown) as captured:
        invoke(kind)
    assert str(captured.value) == "EXCHANGE_TRANSPORT_UNKNOWN"
    assert getattr(captured.value, "phase", None) == phase
    assert getattr(captured.value, "failure", None) == failure
    assert SECRET not in str(captured.value)
    assert calls.count("send") <= 1 and calls.count("connect") <= 1
    assert Watchdog.timers[0].cancelled


@pytest.mark.parametrize("kind", ["diagnostic", "execution"])
def test_watchdog_expiry_during_detached_response_is_ambiguous(monkeypatch, kind):
    calls, reply = fake_network(monkeypatch, watchdog=True)
    with pytest.raises(AckUnknown) as captured:
        invoke(kind)
    assert str(captured.value) == "EXCHANGE_TRANSPORT_UNKNOWN"
    assert getattr(captured.value, "phase", None) == "RESPONSE_BODY"
    assert getattr(captured.value, "failure", None) == "WATCHDOG"
    assert calls == ["connect", "send", "headers", "body"]
    assert reply.closed and Watchdog.timers[0].joined


def test_diagnostic_trace_contains_safe_transport_phase(monkeypatch):
    fake_network(monkeypatch, fail_at="connect", error=ssl.SSLError(SECRET))
    client, _ = demo_client()
    client.transport = binance_client.send_https
    result = preflight(client)
    assert result["failed_stage"] == "/fapi/v1/ping"
    trace = result["account_requests"][0]
    assert trace["phase"] == "TLS" and trace["failure"] == "IO"
    assert SECRET not in json.dumps(result)


@pytest.mark.parametrize(
    "method,path",
    [
        ("POST", "/fapi/v1/order"),
        ("POST", "/fapi/v1/algoOrder"),
        ("PUT", "/fapi/v1/listenKey"),
        ("DELETE", "/fapi/v1/order"),
    ],
)
def test_ambiguous_mutation_never_resends(monkeypatch, method, path):
    calls, _ = fake_network(monkeypatch, fail_at="headers", error=TimeoutError(SECRET))
    client, _ = demo_client(read_only=False)
    client.transport = binance_client.send_https
    with pytest.raises(AckUnknown) as captured:
        client.request(method, path, {"symbol": "BTCUSDT", "listenKey": "private"})
    assert str(captured.value) == "EXCHANGE_TRANSPORT_UNKNOWN"
    assert getattr(captured.value, "phase", None) == "RESPONSE_HEADERS"
    assert calls == ["connect", "send", "headers"]


@pytest.mark.parametrize("kind", ["diagnostic", "execution"])
def test_programmer_error_is_not_hidden_as_transport_failure(monkeypatch, kind):
    fake_network(monkeypatch, fail_at="send", error=AssertionError("programmer defect"))
    with pytest.raises(AssertionError, match="programmer defect"):
        invoke(kind)


@pytest.mark.parametrize("kind", ["diagnostic", "execution"])
def test_watchdog_cannot_trigger_implicit_reconnect_or_late_send(monkeypatch, kind):
    calls, _ = fake_network(monkeypatch, watchdog_before_send=True)
    with pytest.raises(AckUnknown) as captured:
        invoke(kind)
    assert getattr(captured.value, "phase", None) == "SEND"
    assert getattr(captured.value, "failure", None) == "WATCHDOG"
    assert calls == ["connect", "send"]


@pytest.mark.parametrize("kind", ["diagnostic", "execution"])
def test_watchdog_before_response_return_cannot_be_reported_as_success(monkeypatch, kind):
    fake_network(monkeypatch, watchdog_at_headers=True)
    with pytest.raises(AckUnknown) as captured:
        invoke(kind)
    assert getattr(captured.value, "phase", None) == "RESPONSE_BODY"
    assert getattr(captured.value, "failure", None) == "WATCHDOG"
