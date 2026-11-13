"""Restarting the worker must retain confirmations from distinct closed periods."""

from copy import deepcopy
from datetime import timedelta

from tests.intelligence_fixtures import engine, frame
from tradeforge.intelligence.inputs import MarketFrame
from tradeforge.intelligence.runtime import QuantWorker
from tradeforge.persistence.repositories import add_instrument


def test_restart_preserves_pending_regime_confirmation(migrated, monkeypatch):
    detector = engine(cooldown=0)
    writer = QuantWorker(migrated)
    with migrated.transaction() as connection:
        for key in frame(1).universe.eligible:
            add_instrument(connection, key)
    for index in range(1, 5):
        writer._persist(detector.update(frame(index)))
    assert detector.latest.state.label.value == "TRANSITION"

    following = frame(5)
    restarted = QuantWorker(migrated, clock=lambda: following.as_of)
    restarted.detector = engine(cooldown=0)
    restarted.history = [
        observation.current for index in range(0, 5) for observation in frame(index).observations
    ]
    monkeypatch.setattr(restarted, "_frame", lambda at: (following, "ANALYZED"))

    result = restarted.tick(persist=False)
    assert result["healthy"] is True
    assert result["regime_label"] == "TREND_EXPANSION"
    assert result["regime_label"] == detector.update(following).state.label.value


def test_restart_does_not_count_same_period_revisions_as_confirmations(migrated):
    detector = engine(cooldown=0)
    writer = QuantWorker(migrated)
    with migrated.transaction() as connection:
        for key in frame(1).universe.eligible:
            add_instrument(connection, key)
    for index in range(1, 4):
        writer._persist(detector.update(frame(index)))
    original = frame(4)
    for seconds in range(3):
        at = original.as_of + timedelta(seconds=seconds)
        revised = MarketFrame.model_validate(
            {
                **original.model_dump(),
                "as_of": at,
                "observed_at": at,
            }
        )
        writer._persist(deepcopy(detector).update(revised))
    restarted = QuantWorker(migrated, clock=lambda: at)
    restarted.detector = engine(cooldown=0)
    restarted._load(at)
    assert restarted.detector.latest.state.label.value == "TRANSITION"
    assert restarted.detector._streak == 1
