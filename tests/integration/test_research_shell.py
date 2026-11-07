"""Executable local research shell, recorded actions and safe paper reconciliation."""

import json
import os
import subprocess
import sys

from tests.replay.fixtures import order, quote, universe


def test_quant_core_replay_and_paper_shell_are_offline_and_separate(tmp_path):
    from tradeforge.research.replay import RecordedAction, ReplaySession, ReplayTape

    session = ReplaySession(
        tape=ReplayTape(
            dataset_version="fixture1",
            events=(universe(), quote(1), quote(2), quote(3, bid="94", ask="95", qty="10")),
        ),
        actions=(RecordedAction(sequence=1, orders=(order(),)),),
        policy_version="recorded-policy1",
    )
    source = tmp_path / "session.json"
    source.write_text(session.model_dump_json(), encoding="utf-8")
    env = {
        **os.environ,
        "PYTHONPATH": "src;." if os.name == "nt" else "src:.",
        "LIVE": "true",
        "HTTP_PROXY": "http://127.0.0.1:9",
        "HTTPS_PROXY": "http://127.0.0.1:9",
    }
    for module, args, mode in (
        ("apps.quant_core", ["replay", "--input", str(source)], "BACKTEST"),
        ("tradeforge.execution.paper_broker", ["--input", str(source)], "PAPER"),
    ):
        run = subprocess.run(
            [sys.executable, "-m", module, *args],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
        result = json.loads(run.stdout)
        assert result["scorecard"]["mode"] == mode
        assert result["scorecard"]["status"] == "NEGATIVE"
        assert result["scorecard"]["live_eligible"] is False
        assert run.stderr == ""
    saved = tmp_path / "state.json"
    saved.write_text(result["checkpoint"], encoding="utf-8")
    run = subprocess.run(
        [sys.executable, "-m", "tradeforge.execution.paper_broker", "--check-state", str(saved)],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    assert json.loads(run.stdout)["mode"] == "PAPER"


def test_recorded_actions_cannot_silently_disappear_or_duplicate():
    import pytest

    from tradeforge.research.replay import RecordedAction, ReplaySession, ReplayTape

    tape = ReplayTape(dataset_version="fixture1", events=(universe(), quote(1)))
    for actions in (
        (RecordedAction(sequence=2, orders=(order(),)),),
        (RecordedAction(sequence=1, orders=()), RecordedAction(sequence=1, orders=())),
    ):
        with pytest.raises(ValueError, match="ACTION_SEQUENCE"):
            ReplaySession(tape=tape, actions=actions, policy_version="policy1")


def test_market_replay_drives_existing_decision_state_machine():
    from tests.decision_fixtures import discovery, register
    from tradeforge.decision.state_machine import Journal
    from tradeforge.execution.paper_broker import revised
    from tradeforge.research.replay import ReplayTape, replay

    def run():
        journal = Journal()

        def policy(event, view):
            if event.sequence == 1:
                record = journal.apply(register(discovery(at=view.at), at=view.at), now=view.at)
                return (revised(order(), decision_hash=record.digest),)
            return ()

        result = replay(
            ReplayTape(
                dataset_version="fixture1",
                events=(universe(), quote(1), quote(2), quote(3, bid="94", ask="95", qty="10")),
            ),
            policy,
            policy_version="existing-journal1",
        )
        return result, journal.head_hash

    assert run() == run()
