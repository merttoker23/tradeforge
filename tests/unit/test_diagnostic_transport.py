"""Preflight shares one GET-only connection without changing order transport."""

import http.client
import time
from urllib.parse import urlsplit

import pytest

from tests.unit.test_demo_readiness import NOW, demo_client
from tradeforge.execution.binance_client import BinanceTestnetClient, Credentials, send_https
from tradeforge.execution.broker import AckUnknown
from tradeforge.execution.preflight import preflight


def test_default_preflight_reuses_one_connection_and_closes_it(monkeypatch):
    reference, calls = demo_client()
    connections = []

    class Connection:
        sock = None

        def __init__(self, host, timeout):
            assert host == "demo-fapi.binance.com" and timeout == 3
            self.closed = False
            self.target = ""
            self.headers = {}
            connections.append(self)

        def connect(self):
            pass

        def request(self, method, target, body=None, *, headers):
            assert method == "GET"
            self.target, self.headers = target, headers

        def getresponse(self):
            parsed = urlsplit(self.target)
            response = reference.transport("GET", parsed.path, parsed.query, self.headers)

            class Reply:
                status = response.status

                def getheaders(self):
                    return list(response.headers.items())

                def read(self, maximum):
                    return response.body

                def close(self):
                    pass

            return Reply()

        def close(self):
            self.closed = True

    monkeypatch.setattr(http.client, "HTTPSConnection", Connection)
    client = BinanceTestnetClient(
        Credentials(api_key="dummy-key", api_secret="dummy-secret"),
        mode="testnet",
        opted_in=True,
        read_only=False,
        clock=lambda: NOW,
    )
    result = preflight(client)
    assert result["account"] == result["clock"] == "OK"
    assert len(connections) == 1 and connections[0].closed
    assert client.transport is send_https and not client.read_only
    assert len(calls) == 9 and all(row[0] == "GET" for row in calls)
    assert all(
        "X-MBX-APIKEY" not in headers
        for _, path, _, headers in calls
        if path in {"/fapi/v1/ping", "/fapi/v1/time"}
    )


def test_diagnostic_client_blocks_mutations_and_propagates_rate_limit():
    from tradeforge.execution.diagnostics_transport import diagnostic_client

    client, calls = demo_client(read_only=False)
    with diagnostic_client(client) as probe:
        assert probe.read_only and probe is not client
        assert probe.transport is client.transport
        with pytest.raises(ValueError, match="DEMO_READ_ONLY"):
            probe.request("POST", "/fapi/v1/order")
        probe.retry_at = client.retry_at + 100
    assert calls == [] and client.retry_at >= 100


def test_transport_failure_is_safe_closed_and_never_retried(monkeypatch):
    from tradeforge.execution.diagnostics_transport import DiagnosticTransport

    instances = []

    class Broken:
        sock = None

        def __init__(self, *args, **kwargs):
            self.calls, self.closed = 0, False
            instances.append(self)

        def connect(self):
            pass

        def request(self, *args, **kwargs):
            self.calls += 1
            raise http.client.HTTPException("PRIVATE_RAW_ERROR")

        def close(self):
            self.closed = True

    monkeypatch.setattr(http.client, "HTTPSConnection", Broken)
    transport = DiagnosticTransport()
    with pytest.raises(AckUnknown) as error:
        transport("GET", "/fapi/v1/time", "", {})
    assert "PRIVATE_RAW_ERROR" not in str(error.value)
    assert instances[0].closed and instances[0].calls == 1
    for method in ("POST", "PUT", "DELETE"):
        with pytest.raises(ValueError, match="DEMO_READ_ONLY"):
            transport(method, "/fapi/v1/order", "", {})
    assert instances[0].calls == 1


def test_slow_clock_probe_does_not_claim_clock_skew_or_query_account():
    from tests.unit.test_demo_clock_continuity import measured_client

    client, calls = measured_client([1.2])
    result = preflight(client)
    assert result["reason_codes"] == ["CLOCK_PROBE_SLOW"]
    assert result["clock"] == "UNKNOWN" and result["orders_enabled"] is False
    assert len(calls) == 4


def test_parent_backoff_is_seen_inside_existing_diagnostic_context():
    from tradeforge.execution.broker import ExchangeRejected
    from tradeforge.execution.diagnostics_transport import diagnostic_client

    client, calls = demo_client()
    with diagnostic_client(client) as probe:
        client.retry_at = time.monotonic() + 100
        with pytest.raises(ExchangeRejected, match="RATE_LIMITED"):
            probe.request("GET", "/fapi/v1/ping", signed=False)
    assert calls == []


def test_probe_backoff_reaches_parent_before_context_exit():
    from tradeforge.execution.binance_client import Response
    from tradeforge.execution.broker import ExchangeRejected
    from tradeforge.execution.diagnostics_transport import diagnostic_client

    client, _ = demo_client()
    client.transport = lambda *args: Response(429, {"Retry-After": "100"}, b"{}")
    with diagnostic_client(client) as probe:
        with pytest.raises(ExchangeRejected, match="RATE_LIMITED"):
            probe.request("GET", "/fapi/v1/ping", signed=False)
        assert client.retry_at > time.monotonic()


def test_watchdog_stops_response_after_connection_detaches_socket(monkeypatch):
    from tradeforge.execution import diagnostics_transport as module

    timers = []

    class Watchdog:
        def __init__(self, seconds, callback):
            assert seconds == 5
            self.callback = callback
            timers.append(self)

        def start(self):
            pass

        def cancel(self):
            pass

        def join(self):
            pass

    class Socket:
        stopped = False

        def shutdown(self, how):
            self.stopped = True

    active = Socket()

    class Reply:
        closed = False
        status = 200

        def read(self, maximum):
            timers[0].callback()
            assert active.stopped, "watchdog lost the response-owned socket"
            return b"{}"

        def close(self):
            self.closed = True

    reply = Reply()

    class Connection:
        sock = active

        def __init__(self, *args, **kwargs):
            pass

        def request(self, *args, **kwargs):
            pass

        def getresponse(self):
            self.sock = None
            return reply

        def close(self):
            pass

    monkeypatch.setattr(module, "Timer", Watchdog)
    monkeypatch.setattr(module.http.client, "HTTPSConnection", Connection)
    with pytest.raises(AckUnknown, match="EXCHANGE_TRANSPORT_UNKNOWN"):
        module.DiagnosticTransport()("GET", "/fapi/v1/time", "", {})
    assert reply.closed
