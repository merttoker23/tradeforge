"""The actual Compose probe uses only stdlib and honest HTTP liveness."""

import http.client
import os
import subprocess
import sys

import pytest


def test_probe_help_does_not_import_application_or_third_party_dependencies():
    script = """
import importlib.abc, runpy, sys
class Guard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if (fullname.split('.')[0] in {'aiohttp','sqlalchemy','pydantic'}
            or fullname.startswith(('tradeforge.execution','tradeforge.dashboard',
                                    'tradeforge.config'))):
            raise AssertionError('health probe imported service dependencies')
sys.meta_path.insert(0, Guard())
sys.argv = ['healthcheck', '--help']
runpy.run_module('tradeforge.runtime.healthcheck', run_name='__main__')
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env={**os.environ, "TF_DEMO_ORDERS": "invalid", "TF_DEMO_TRIAL": "invalid"},
        timeout=5,
    )
    assert result.returncode == 0, result.stderr
    assert "--service" in result.stdout


@pytest.mark.parametrize("status", [200, 401, 503])
def test_probe_uses_fixed_loopback_endpoint_and_status(monkeypatch, status):
    from tradeforge.runtime.healthcheck import health

    calls = []

    class Connection:
        def __init__(self, host, port, timeout):
            calls.append((host, port, timeout))

        def request(self, method, path):
            calls.append((method, path))

        def getresponse(self):
            return type("Response", (), {"status": status})()

        def close(self):
            calls.append("closed")

    monkeypatch.setattr(http.client, "HTTPConnection", Connection)
    assert health("market") == (0 if status == 200 else 1)
    assert calls == [("127.0.0.1", 8101, 3), ("GET", "/health"), "closed"]


def test_probe_timeout_fails_closed_and_closes_connection(monkeypatch):
    from tradeforge.runtime.healthcheck import health

    closed = []

    class Connection:
        def __init__(self, *args, **kwargs):
            pass

        def request(self, *args):
            raise TimeoutError("OFFLINE_PROBE_TIMEOUT")

        def close(self):
            closed.append(True)

    monkeypatch.setattr(http.client, "HTTPConnection", Connection)
    assert health("market") == 1
    assert closed == [True]
