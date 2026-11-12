"""Fail-closed autonomous execution with no separate order service."""
import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from tradeforge.runtime.auto_execution import execution_cycle, serve


def test_execution_requires_explicit_testnet_and_trial():
    for environment, entries, trial in (
        ("live", True, True), ("paper", True, True),
        ("testnet", False, True), ("testnet", True, False),
    ):
        with pytest.raises(ValueError, match="EXPLICIT_TESTNET_AUTO_EXECUTION_REQUIRED"):
            asyncio.run(serve(None, SimpleNamespace(environment=environment),
                              internal_token="not-used", enable_entries=entries,
                              trial_enabled=trial))
    with pytest.raises(ValueError, match="INTERNAL_MARKET_TOKEN_REQUIRED"):
        asyncio.run(serve(None, SimpleNamespace(environment="testnet"),
                          internal_token="", enable_entries=True, trial_enabled=True))


def test_no_outbox_dispatch_before_account_reconciliation():
    calls = []

    class Recovery:
        ready = False
        def reconcile(self):
            calls.append("reconcile")

    class Stream:
        ready = True
        def mark_recovered(self, ready):
            calls.append(("mark", ready))

    class Commands:
        def run_once(self, *, ready):
            calls.append(("command", ready()))

    class Worker:
        def run_once(self):
            calls.append("order")

    execution_cycle(Recovery(), Stream(), Worker(), Commands())
    assert calls == ["reconcile", ("mark", False), ("command", False)]


def test_old_service_not_in_deployment_or_dashboard():
    root = Path(__file__).resolve().parents[2]
    compose = (root / "deploy/docker-compose.yml").read_text()
    assert "execution_guardian:" not in compose
    assert "TF_AUTO_ORDERS" in compose
    assert not (root / "apps/execution_guardian").exists()
    html = (root / "src/tradeforge/dashboard/static/index.html").read_text()
    assert "Manuel emir" not in html
    assert "execution-summary" in html
