"""Replay exercises real engines; synthetic fixtures never become exchange evidence."""

from datetime import timedelta

import pytest

from tests.unit.test_momentum_alpha import change, evidence
from tests.unit.test_reversal_alpha import sample as reversal_sample
from tradeforge.domain.decision import Side


def recording(*samples, offsets=None):
    from tradeforge.research.alpha_replay import AlphaRecording, AlphaReplayInput

    offsets = offsets or [0] * len(samples)
    return AlphaRecording(
        dataset_version="synthetic-regression-v1",
        provenance="synthetic",
        records=tuple(
            AlphaReplayInput(sequence=i, evaluated_at=d.as_of + timedelta(seconds=offset), sample=d)
            for i, (d, offset) in enumerate(zip(samples, offsets, strict=True))
        ),
    )


@pytest.mark.parametrize("kind", ["momentum", "reversal"])
def test_complete_closed_inputs_can_arm_with_no_order_authority(kind):
    from tradeforge.research.alpha_replay import replay_alpha

    tape = recording(evidence() if kind == "momentum" else reversal_sample())
    result = replay_alpha(tape)
    armed = [
        r for r in result.results if r.candidate is not None and r.candidate.state.value == "ARMED"
    ]
    assert result.evaluated_count == (2 if kind == "momentum" else 4)
    assert len(armed) == 1 and armed[0].side is Side.LONG
    assert result.unique_armed_setups == 1
    assert armed[0].candidate.confidence is None
    assert armed[0].candidate.proof_state.value == "UNPROVEN"
    assert result.provenance == "synthetic" and result.mode == "ALPHA_REPLAY"
    assert result.execution_authorized is False and result.live_eligible is False


def test_replay_keeps_expiration_reasons_and_deduplicates_setup_counts():
    from tradeforge.research.alpha_replay import replay_alpha

    d = evidence()
    result = replay_alpha(recording(d, d, d, offsets=[0, 1, 901]))
    assert result.armed_observations == 1
    assert result.unique_armed_setups == 1
    reasons = dict(result.reason_counts)
    assert reasons["DUPLICATE_SETUP"] == 1
    assert reasons["EXPIRED"] == 2
    assert result.evaluated_count == 6


def test_replay_hashes_no_trade_filters_and_is_deterministic():
    from tradeforge.research.alpha_replay import replay_alpha

    d = evidence()
    blocked = change(d, setup_bars=())
    tape = recording(blocked)
    a, b = replay_alpha(tape), replay_alpha(tape)
    assert a == b
    assert a.unique_armed_setups == 0
    assert dict(a.reason_counts)["INSUFFICIENT_HISTORY"] == 2
    assert len({r.receipt_hash for r in a.results}) == 2
    assert a.recording_hash != replay_alpha(recording(d)).recording_hash


def test_future_evaluation_order_and_duplicate_sequences_are_rejected():
    from tradeforge.research.alpha_replay import AlphaRecording, AlphaReplayInput

    d = evidence()
    with pytest.raises(ValueError, match="FUTURE"):
        AlphaReplayInput(sequence=0, evaluated_at=d.as_of - timedelta(seconds=1), sample=d)
    row = AlphaReplayInput(sequence=0, evaluated_at=d.as_of, sample=d)
    with pytest.raises(ValueError, match="ORDER"):
        AlphaRecording(dataset_version="test", provenance="recorded", records=(row, row))
    later = AlphaReplayInput(sequence=1, evaluated_at=d.as_of + timedelta(seconds=1), sample=d)
    with pytest.raises(ValueError, match="ORDER"):
        AlphaRecording(dataset_version="test", provenance="recorded", records=(later, row))


def test_cli_uses_offline_input_and_suppresses_invalid_payload(tmp_path, capsys):
    from tradeforge.research.alpha_replay import main

    path = tmp_path / "recording.json"
    path.write_text(recording(evidence()).model_dump_json())
    assert main(["--input", str(path)]) == 0
    assert '"mode":"ALPHA_REPLAY"' in capsys.readouterr().out
    path.write_text('{"secret":"sensitive-input-value"}')
    with pytest.raises(SystemExit):
        main(["--input", str(path)])
    error = capsys.readouterr().err
    assert "INVALID_ALPHA_RECORDING" in error and "sensitive-input-value" not in error
