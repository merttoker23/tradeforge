"""Decision diagnostics distinguish no setup, waiting trigger, and risk refusal."""

from contextlib import contextmanager
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest

from tests.decision_fixtures import NOW, change, validating
from tests.market_fixtures import instrument
from tradeforge.decision.state_machine import Journal
from tradeforge.domain.common import DataQuality
from tradeforge.domain.market import MarketSnapshot, OrderBookSnapshot, StreamState
from tradeforge.runtime.demo_decisions import DemoDecisions


class Rows:
    def __init__(self, rows):
        self.rows = rows

    def scalars(self):
        return self

    def all(self):
        return self.rows


class Database:
    def __init__(self, rows):
        self.rows = rows

    @contextmanager
    def transaction(self):
        yield self

    def execute(self, statement):
        return Rows(self.rows)


def facts():
    common = dict(
        **instrument().model_dump(), schema_version=1, source_at=NOW, observed_at=NOW, as_of=NOW
    )
    market = MarketSnapshot(
        **common,
        event_id=uuid4(),
        best_bid=Decimal("98"),
        best_ask=Decimal("99"),
        mark_price=Decimal("98.5"),
        index_price=Decimal("98.5"),
        volume=Decimal("10"),
        funding=Decimal("0.0001"),
        quality=DataQuality.OK,
    )
    book = OrderBookSnapshot(
        **common,
        event_id=uuid4(),
        initial_update_id=1,
        last_update_id=2,
        captured_at=NOW,
        stream_state=StreamState.READY,
        bids=((Decimal("98"), Decimal("1000")),),
        asks=((Decimal("99"), Decimal("1000")),),
    )
    return market, book


def test_no_candidates_is_distinct_from_waiting_for_trigger_and_risk_refusal(monkeypatch):
    client = SimpleNamespace(clock=lambda: NOW, environment="testnet")
    decisions = DemoDecisions(Database([]), client)
    decisions.tick({})
    status = decisions.status()["autonomous_decisions"]
    assert status["last_reason"] == "NO_ARMED_CANDIDATES"
    assert status["candidate_count"] == 0 and status["approvals"] == 0
    record = validating(Journal())
    decisions.db.rows = [record.model_dump_json()]
    market, book = facts()
    feed = {
        "status": {"data_ready": True},
        "markets": [market.model_dump(mode="json")],
        "deep": [{"symbol": "BTCUSDT", "book": book.model_dump(mode="json")}],
    }
    decisions.tick(feed)
    status = decisions.status()["autonomous_decisions"]
    assert status["last_reason"] == "TRIGGER_NOT_CROSSED"
    assert status["candidate_count"] == 1 and status["approvals"] == 0
    feed["markets"] = [change(market, best_ask=Decimal("100")).model_dump(mode="json")]

    def rejected(*args):
        decisions._note("RISK_REJECTED")
        return None

    monkeypatch.setattr(decisions, "evidence", rejected)
    monkeypatch.setattr(decisions, "_advance", lambda record, target: record)
    decisions.tick(feed)
    status = decisions.status()["autonomous_decisions"]
    assert status["last_reason"] == "RISK_REJECTED" and status["approvals"] == 0
    assert status["reason_counts"] == {
        "NO_ARMED_CANDIDATES": 1,
        "TRIGGER_NOT_CROSSED": 1,
        "RISK_REJECTED": 1,
    }
    status["reason_counts"].clear()
    assert len(decisions.status()["autonomous_decisions"]["reason_counts"]) == 3


@pytest.mark.parametrize("volume_age", [None, 6])
def test_current_risk_handoff_rejects_missing_or_stale_volume_before_evidence(volume_age):
    market, book = facts()
    market = change(
        market,
        book_source_at=NOW,
        mark_source_at=NOW,
        volume=None if volume_age is None else market.volume,
        volume_source_at=None if volume_age is None else NOW - timedelta(seconds=volume_age),
    )
    record = validating(Journal())
    decisions = DemoDecisions(
        Database([record.model_dump_json()]), SimpleNamespace(clock=lambda: NOW)
    )
    decisions.tick(
        {
            "status": {"data_ready": True},
            "markets": [market.model_dump(mode="json")],
            "deep": [{"symbol": "BTCUSDT", "book": book.model_dump(mode="json")}],
        }
    )
    status = decisions.status()["autonomous_decisions"]
    assert status["last_reason"] == "MARKET_EVIDENCE_STALE"
    assert status["approvals"] == 0
