"""Projection scheduling preserves the existing source TTL without drift."""

import asyncio
from contextlib import contextmanager

import pytest


def test_projection_uses_start_to_start_interval_without_extending_evidence(monkeypatch):
    from tradeforge.runtime import demo_projection as module

    clock, starts, delays, published, publication_times = [0.0], [], [], [], []

    class DB:
        @contextmanager
        def transaction(self):
            yield object()

    def fetch(client, *, terminal):
        assert terminal
        starts.append(clock[0])
        clock[0] += 6
        return {"as_of": "SOURCE_TIME", "expires_at": "SOURCE_EXPIRY", "orders_enabled": True}

    def audit(connection, event, state):
        assert event == "demo.preflight.v1"
        clock[0] += 1
        published.append(state.copy())
        publication_times.append(clock[0])

    async def sleep(delay):
        delays.append(delay)
        clock[0] += delay
        if len(delays) == 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(module, "preflight", fetch)
    monkeypatch.setattr(module, "audit", audit)
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(module.asyncio, "sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(module.project_demo(DB(), object()))
    assert starts == [0, 20] and delays == [13, 13]
    assert publication_times == [7, 27]
    assert publication_times[1] < starts[0] + 30
    assert all(state["orders_enabled"] is False for state in published)
    assert all(
        state["as_of"] == "SOURCE_TIME" and state["expires_at"] == "SOURCE_EXPIRY"
        for state in published
    )
