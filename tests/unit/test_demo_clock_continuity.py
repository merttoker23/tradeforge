"""Only an inconclusive, slow public clock sample can be sampled again."""

import json
from datetime import timedelta

import pytest

from tests.unit.test_demo_readiness import NOW, demo_client
from tradeforge.execution.binance_client import Response
from tradeforge.execution.preflight import preflight


def measured_client(durations, *, skew=0, reject_retry=False):
    client, calls = demo_client()
    transport = client.transport
    elapsed = 0.0
    samples = 0

    def request(method, path, params, headers):
        nonlocal elapsed, samples
        response = transport(method, path, params, headers)
        if path == "/fapi/v1/time":
            duration = durations[min(samples, len(durations) - 1)]
            samples += 1
            if samples == 2 and reject_retry:
                return Response(429, {"Retry-After": "100"}, b"{}")
            midpoint = NOW + timedelta(seconds=elapsed + duration / 2 + skew)
            elapsed += duration
            return Response(
                200, {}, json.dumps({"serverTime": int(midpoint.timestamp() * 1000)}).encode()
            )
        return response

    client.transport = request
    client.clock = lambda: NOW + timedelta(seconds=elapsed)
    return client, calls


def test_single_slow_measurement_recovers_without_extending_evidence_or_private_retries():
    client, calls = measured_client([1.2, 0.3])
    result = preflight(client)
    assert result["clock"] == result["account"] == result["balance"] == "OK"
    assert result["clock_request_ms"] == 300
    assert result["clock_attempts"] == 2
    assert result["as_of"] == NOW.isoformat()
    assert result["expires_at"] == (NOW + timedelta(seconds=30)).isoformat()
    assert result["orders_enabled"] is False and result["live"] is False
    assert [row[1] for row in calls].count("/fapi/v1/time") == 2
    assert len(calls) == 10 and all(row[0] == "GET" for row in calls)


def test_all_slow_measurements_stop_at_three_without_signed_requests():
    client, calls = measured_client([1.2])
    result = preflight(client)
    assert result["reason_codes"] == ["CLOCK_PROBE_SLOW"]
    assert result["clock_attempts"] == 3 and result["account"] == "UNKNOWN"
    assert [row[1] for row in calls] == ["/fapi/v1/ping"] + ["/fapi/v1/time"] * 3


@pytest.mark.parametrize("duration,skew", [(0.3, 2), (-0.1, 0), (1.2, 5)])
def test_clear_clock_faults_are_not_retried(duration, skew):
    client, calls = measured_client([duration], skew=skew)
    result = preflight(client)
    assert result["reason_codes"] == ["CLOCK_UNSAFE"]
    assert len(calls) == 2 and result["account"] == "UNKNOWN"


def test_rate_limit_during_clock_resampling_is_not_retried_or_bypassed():
    client, calls = measured_client([1.2, 0.3], reject_retry=True)
    result = preflight(client)
    assert result["reason_codes"] == ["RATE_LIMITED"]
    assert len(calls) == 3 and client.retry_at > 0
    assert result["clock"] == result["account"] == "UNKNOWN"
