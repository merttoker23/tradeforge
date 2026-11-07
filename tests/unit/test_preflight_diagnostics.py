"""Endpoint failures retain prior validated evidence without exposing wire data."""

import json

import pytest

from tests.unit.test_demo_readiness import demo_client
from tradeforge.execution.binance_client import Response
from tradeforge.execution.broker import AckUnknown, ExchangeRejected
from tradeforge.execution.preflight import preflight

ENDPOINTS = [
    "/fapi/v1/ping",
    "/fapi/v1/time",
    "/fapi/v3/account",
    "/fapi/v3/balance",
    "/fapi/v3/positionRisk",
    "/fapi/v1/accountConfig",
    "/fapi/v1/positionSide/dual",
    "/fapi/v1/openOrders",
    "/fapi/v1/openAlgoOrders",
]


@pytest.mark.parametrize("failed_index", range(9))
def test_failure_identifies_endpoint_and_preserves_validated_prior_stages(failed_index):
    client, calls = demo_client()
    original = client.transport

    def transport(method, path, params, headers):
        if path == ENDPOINTS[failed_index]:
            calls.append((method, path, params, headers))
            raise AckUnknown("EXCHANGE_TRANSPORT_UNKNOWN")
        return original(method, path, params, headers)

    client.transport = transport
    result = preflight(client)

    assert result["failed_stage"] == ENDPOINTS[failed_index]
    assert [row["path"] for row in result["account_requests"]] == ENDPOINTS[: failed_index + 1]
    assert result["account_requests"][-1]["reason"] == "EXCHANGE_TRANSPORT_UNKNOWN"
    assert all(row["reason"] == "OK" for row in result["account_requests"][:-1])
    assert result["account"] == ("OK" if failed_index > 2 else "UNKNOWN")
    assert result["balance"] == ("OK" if failed_index > 3 else "UNKNOWN")
    assert result["positions"] == ("OK" if failed_index > 4 else "UNKNOWN")
    assert result["clock"] == ("OK" if failed_index > 1 else "UNKNOWN")
    assert result["ready"] is False and result["orders_enabled"] is False
    assert len(calls) == failed_index + 1


@pytest.mark.parametrize(
    "path,payload",
    [
        ("/fapi/v1/ping", []),
        ("/fapi/v1/time", {}),
        ("/fapi/v3/account", {"totalMarginBalance": "NaN", "availableBalance": "0"}),
        ("/fapi/v3/balance", [{"asset": "USDT", "availableBalance": "private-body"}]),
        ("/fapi/v3/positionRisk", [{"symbol": "BTCUSDT", "positionAmt": "private-body"}]),
        ("/fapi/v1/accountConfig", []),
        ("/fapi/v1/positionSide/dual", []),
        ("/fapi/v1/openOrders", {}),
        ("/fapi/v1/openAlgoOrders", {}),
    ],
)
def test_malformed_payload_is_attributed_before_later_requests(path, payload):
    client, calls = demo_client(patch={path: payload})
    result = preflight(client)

    assert result["failed_stage"] == path
    assert result["reason_codes"] == ["DEMO_RESPONSE_INVALID"]
    assert result["account_requests"][-1]["path"] == path
    assert result["account_requests"][-1]["reason"] == "DEMO_RESPONSE_INVALID"
    assert len(calls) == ENDPOINTS.index(path) + 1
    assert "private-body" not in json.dumps(result)


@pytest.mark.parametrize("error_class", [AckUnknown, ExchangeRejected])
def test_untrusted_broker_errors_cannot_disclose_wire_secrets(error_class):
    client, _ = demo_client()
    secrets = "api-key api-secret listenKey signature=secret ?query=secret raw-body"

    def transport(*args):
        raise error_class(secrets)

    client.transport = transport
    result = preflight(client)
    encoded = json.dumps(result)
    assert all(secret not in encoded for secret in secrets.split())
    assert result["failed_stage"] == "/fapi/v1/ping"
    assert result["account_requests"][0]["method"] == "GET"


def test_success_returns_bounded_safe_trace_and_no_failed_stage():
    client, calls = demo_client()
    result = preflight(client)
    assert result["ready"] is True and result["failed_stage"] is None
    assert len(calls) == len(result["account_requests"]) == 9
    assert all(row["duration_ms"] >= 0 for row in result["account_requests"])
    assert all(row["reason"] == "OK" for row in result["account_requests"])
    assert "offline-demo" not in json.dumps(result)
    assert all(
        set(row) <= {"method", "path", "reason", "duration_ms", "phase", "failure"}
        for row in result["account_requests"]
    )


def test_invalid_json_is_attributed_and_body_is_not_returned():
    client, calls = demo_client()
    original = client.transport

    def transport(method, path, params, headers):
        if path == "/fapi/v3/balance":
            calls.append((method, path, params, headers))
            return Response(200, {}, b"private-body api-key listenKey=secret")
        return original(method, path, params, headers)

    client.transport = transport
    result = preflight(client)
    assert result["account"] == "OK"
    assert result["failed_stage"] == "/fapi/v3/balance"
    assert result["account_requests"][-1]["reason"] == "EXCHANGE_RESPONSE_INVALID"
    assert "private-body" not in json.dumps(result)
    assert len(calls) == 4


def test_diagnostic_trace_is_bounded_and_omits_request_params_and_headers():
    from tradeforge.execution.diagnostics_transport import diagnostic_client

    client, calls = demo_client()
    with diagnostic_client(client) as probe:
        for _ in range(40):
            probe.request("GET", "/fapi/v3/account", {"listenKey": "private-listen-key"})
        trace = probe.account_requests
    assert len(calls) == 40 and len(trace) == 32
    encoded = json.dumps(trace)
    assert all(
        secret not in encoded
        for secret in (
            "private-listen-key",
            "offline-demo-key",
            "offline-demo-secret",
            "signature",
            "timestamp",
        )
    )


def test_mutated_transport_metadata_cannot_escape_the_allowlist():
    from tradeforge.execution.transport_diagnostics import TransportFailure

    client, _ = demo_client()
    error = TransportFailure("TLS", "IO")
    error.phase = "private-listen-key"
    error.failure = "private-api-key"

    def transport(*args):
        raise error

    client.transport = transport
    result = preflight(client)
    trace = result["account_requests"][0]
    assert trace["phase"] == "CONNECTION" and trace["failure"] == "IO"
    assert "private-" not in json.dumps(result)
