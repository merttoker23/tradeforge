"""Behavioral gates: no activation, truthful readiness, offline process entrypoints."""

import importlib
import json
import os
import subprocess
import sys
from http.client import HTTPConnection
from pathlib import Path
from threading import Thread

import pytest

ROOT = Path(__file__).resolve().parents[1]
SERVICES = ("market_gateway", "quant_core", "execution_guardian", "dashboard")


def invoke(
    service: str, *args: str, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    environment = {**os.environ, "TRADEFORGE_MODE": "paper", "TRADEFORGE_LIVE": "false"}
    environment["PYTHONPATH"] = str(ROOT / "src") + os.pathsep + str(ROOT)
    environment.update(env or {})
    return subprocess.run(
        [sys.executable, "-m", f"apps.{service}", *args],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=10,
    )


@pytest.mark.parametrize("service", SERVICES)
def test_entrypoint_help_and_offline_check(service: str) -> None:
    help_result = invoke(service, "--help")
    assert help_result.returncode == 0, help_result.stderr
    assert "--check" in help_result.stdout
    result = invoke(service, "--check")
    assert result.returncode == 0, result.stderr
    status = json.loads(result.stdout)
    assert status == {
        "service": service,
        "phase": "PHASE_01",
        "mode": "paper",
        "live": False,
        "orders_enabled": False,
        "ready": False,
        "reason": "FOUNDATION_ONLY",
    }


@pytest.mark.parametrize("service", SERVICES)
@pytest.mark.parametrize(
    "override",
    [
        {"TRADEFORGE_MODE": "live"},
        {"TRADEFORGE_MODE": "unknown"},
        {"TRADEFORGE_MODE": ""},
        {"TRADEFORGE_LIVE": "true"},
        {"TRADEFORGE_LIVE": "1"},
        {"TRADEFORGE_LIVE": ""},
    ],
)
def test_unsafe_environment_refuses_start_without_echoing_values(
    service: str, override: dict[str, str]
) -> None:
    result = invoke(service, "--check", env=override)
    assert result.returncode == 2
    assert "RUNTIME_CONFIG_INVALID" in result.stderr
    assert result.stdout == ""


@pytest.mark.parametrize("service", SERVICES)
def test_default_environment_remains_paper(service: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRADEFORGE_MODE", raising=False)
    monkeypatch.delenv("TRADEFORGE_LIVE", raising=False)
    bootstrap = importlib.import_module("tradeforge.bootstrap")
    assert bootstrap.snapshot(service)["mode"] == "paper"
    assert bootstrap.snapshot(service)["live"] is False


@pytest.mark.parametrize("service", SERVICES)
def test_real_http_liveness_and_fail_closed_readiness(service: str) -> None:
    bootstrap = importlib.import_module("tradeforge.bootstrap")
    with bootstrap.create_server(service, 0) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            connection = HTTPConnection("127.0.0.1", server.server_port, timeout=2)
            for path, expected_status in [("/health", 200), ("/readiness", 503), ("/", 404)]:
                connection.request("GET", path)
                response = connection.getresponse()
                assert response.status == expected_status
                payload = json.loads(response.read())
                assert payload["live"] is False
                assert payload["orders_enabled"] is False
                assert payload["ready"] is False
                if path == "/health":
                    assert payload["alive"] is True
            connection.close()
        finally:
            server.shutdown()
            thread.join(timeout=3)
            assert not thread.is_alive()


@pytest.mark.parametrize("arg", ["-1", "65536", "not-a-port"])
def test_invalid_listen_port_is_rejected(arg: str) -> None:
    result = invoke("dashboard", "--port", arg)
    assert result.returncode == 2


def test_credential_environment_is_never_projected() -> None:
    result = invoke(
        "execution_guardian", "--check", env={"BINANCE_API_KEY": "TEST_SECRET_SENTINEL"}
    )
    assert result.returncode == 0
    assert "TEST_SECRET_SENTINEL" not in result.stdout + result.stderr


@pytest.mark.parametrize("service", SERVICES)
@pytest.mark.parametrize("mode", ["disabled", "testnet"])
def test_explicit_non_live_mode_has_no_order_capability(service: str, mode: str) -> None:
    result = invoke(service, "--check", env={"TRADEFORGE_MODE": mode})
    assert result.returncode == 0, result.stderr
    status = json.loads(result.stdout)
    assert status["mode"] == mode
    assert not status["live"] and not status["orders_enabled"] and not status["ready"]


@pytest.mark.parametrize("service", SERVICES)
def test_unknown_prefixed_secret_is_not_echoed(service: str) -> None:
    result = invoke(service, "--check", env={"TRADEFORGE_API_KEY": "TEST_SECRET_SENTINEL"})
    assert result.returncode == 2
    assert "TEST_SECRET_SENTINEL" not in result.stdout + result.stderr
