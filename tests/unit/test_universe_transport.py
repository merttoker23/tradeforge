import json

import pytest

from tests.universe_fixtures import payload
from tradeforge.market.exchange_info import ExchangeInfoClient, FetchError, Reply


class Wire:
    def __init__(self, *responses):
        self.responses = iter(responses)
        self.calls = []
        self.delays = []
        self.now = 0.0

    def send(self, headers, timeout):
        self.calls.append((dict(headers), timeout))
        value = next(self.responses)
        if isinstance(value, Exception):
            raise value
        return value

    def sleep(self, delay):
        self.delays.append(delay)
        self.now += delay

    def client(self, **kwargs):
        return ExchangeInfoClient(
            transport=self.send,
            clock=lambda: self.now,
            sleep=self.sleep,
            jitter=lambda: 0.5,
            **kwargs,
        )


def ok(headers=None):
    return Reply(200, headers or {}, json.dumps(payload()).encode())


def test_timeout_and_server_failure_retry_is_bounded_with_jitter():
    wire = Wire(TimeoutError(), Reply(503, {}, b"unavailable"), ok())
    result = wire.client(timeout=2).fetch('"v1"')
    assert result.status == 200
    assert wire.delays == [0.75, 1.5]
    assert all(
        headers == {"If-None-Match": '"v1"'} and timeout == 2 for headers, timeout in wire.calls
    )


def test_exhausted_network_error_is_redacted_and_not_retried_forever():
    wire = Wire(*(OSError("secret-network-detail") for _ in range(3)))
    with pytest.raises(FetchError, match="UNIVERSE_NETWORK_ERROR") as caught:
        wire.client().fetch()
    assert len(wire.calls) == 3 and len(wire.delays) == 2
    assert "secret" not in str(caught.value)


@pytest.mark.parametrize("status", [429, 418])
def test_throttling_and_bans_stop_requests_and_respect_cooldown(status):
    wire = Wire(Reply(status, {"Retry-After": "120"}, b""), ok())
    client = wire.client()
    with pytest.raises(FetchError):
        client.fetch()
    wire.now = 119
    with pytest.raises(FetchError):
        client.fetch()
    assert len(wire.calls) == 1 and not wire.delays
    wire.now = 120
    assert client.fetch().status == 200


def test_weight_exhaustion_stops_even_successful_response_followups():
    wire = Wire(ok({"X-MBX-USED-WEIGHT-1M": "2"}), ok())
    client = wire.client()
    client.set_weight_limit(2)
    assert client.fetch().status == 200
    wire.now = 30
    with pytest.raises(FetchError):
        client.fetch()
    assert len(wire.calls) == 1
    wire.now = 60
    assert client.fetch().status == 200


@pytest.mark.parametrize(
    "response",
    [
        Reply(200, {}, b"{broken"),
        Reply(200, {}, b'{"symbols":[],"symbols":[1]}'),
        Reply(200, {}, b"x" * (4 * 1024 * 1024 + 1)),
        Reply(302, {"Location": "https://untrusted.example"}, b""),
        Reply(401, {}, b"private detail"),
    ],
)
def test_bad_payloads_and_redirects_fail_without_retry(response):
    wire = Wire(response)
    with pytest.raises(FetchError):
        wire.client().fetch()
    assert len(wire.calls) == 1


def test_not_modified_without_validator_and_header_injection_rejected():
    wire = Wire(Reply(304, {}, b""))
    with pytest.raises(FetchError):
        wire.client().fetch()
    wire = Wire(ok())
    with pytest.raises(ValueError):
        wire.client().fetch('"bad\r\nX: injected"')
    assert not wire.calls


@pytest.mark.parametrize(
    "kwargs", [{"timeout": 0}, {"timeout": float("nan")}, {"attempts": 100}, {"attempts": True}]
)
def test_invalid_transport_budgets_rejected(kwargs):
    with pytest.raises(ValueError):
        Wire().client(**kwargs)


def test_nonfinite_json_cannot_become_valid_metadata():
    wire = Wire(Reply(200, {}, b'{"unexpected":NaN}'))
    with pytest.raises(FetchError):
        wire.client().fetch()


def test_real_http_transport_uses_only_public_get_and_never_follows_redirect(monkeypatch):
    import http.client
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from threading import Thread

    from tradeforge.market.exchange_info import https_get

    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            received.append((self.path, dict(self.headers)))
            self.send_response(302)
            self.send_header("Location", "/private/order")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        original = http.client.HTTPConnection
        destinations = []

        def local_connection(host, timeout):
            destinations.append(host)
            return original("127.0.0.1", server.server_port, timeout=timeout)

        monkeypatch.setattr(http.client, "HTTPSConnection", local_connection)
        try:
            client = ExchangeInfoClient(transport=https_get)
            with pytest.raises(FetchError):
                client.fetch('"v1"')
        finally:
            server.shutdown()
            thread.join(timeout=2)
    assert destinations == ["fapi.binance.com"]
    assert len(received) == 1 and received[0][0] == "/fapi/v1/exchangeInfo"
    assert received[0][1]["If-None-Match"] == '"v1"'
    assert not any(k.lower() in {"authorization", "x-mbx-apikey"} for k in received[0][1])


def test_slow_dripped_headers_cannot_extend_total_request_deadline(monkeypatch):
    import http.client
    import time
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from threading import Thread

    from tradeforge.market.exchange_info import https_get

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            try:
                for byte in b"HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\n{}":
                    self.wfile.write(bytes([byte]))
                    self.wfile.flush()
                    time.sleep(0.025)
            except OSError:
                pass

        def log_message(self, *args):
            pass

    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        original = http.client.HTTPConnection
        monkeypatch.setattr(
            http.client,
            "HTTPSConnection",
            lambda host, timeout: original("127.0.0.1", server.server_port, timeout=timeout),
        )
        started = time.monotonic()
        try:
            with pytest.raises((OSError, http.client.HTTPException)):
                https_get({}, 0.15)
            elapsed = time.monotonic() - started
        finally:
            server.shutdown()
            thread.join(timeout=2)
    assert elapsed < 0.6
