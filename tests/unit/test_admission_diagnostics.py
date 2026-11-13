"""Trial exhaustion must stay visible without exposing arbitrary exception text."""

from types import SimpleNamespace

import pytest

from tradeforge.persistence.db import EntryHalt
from tradeforge.runtime.demo_decisions import DemoDecisions


@pytest.mark.parametrize(
    "code",
    [
        "TESTNET_TRIAL_BUDGET_EXHAUSTED",
        "TESTNET_TRIAL_SETTLEMENT_REQUIRED",
        "TESTNET_TRIAL_LOSS_LIMIT",
        "OOS_EMBARGO_OR_UNSETTLED_TRAIN",
    ],
)
def test_known_trial_admission_reasons_are_preserved_without_changing_halt(monkeypatch, code):
    decisions = DemoDecisions(None, SimpleNamespace())

    def halted(feed, *, symbols=None, pending=None):
        raise EntryHalt(code)

    monkeypatch.setattr(decisions, "_tick", halted)
    with pytest.raises(EntryHalt, match=code):
        decisions.tick({})
    status = decisions.status()["autonomous_decisions"]
    assert status["last_reason"] == code
    assert status["reason_counts"] == {code: 1}


def test_unrecognized_admission_exception_is_redacted_and_still_halts(monkeypatch):
    decisions = DemoDecisions(None, SimpleNamespace())

    def halted(feed, *, symbols=None, pending=None):
        raise EntryHalt("operator-token=private-value")

    monkeypatch.setattr(decisions, "_tick", halted)
    with pytest.raises(EntryHalt):
        decisions.tick({})
    assert "private-value" not in str(decisions.status())
    assert decisions.status()["autonomous_decisions"]["last_reason"] == "DEMO_ADMISSION_HALTED"
