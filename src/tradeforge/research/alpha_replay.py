"""Offline replay of complete recorded alpha inputs, without fills or order authority."""

from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from tradeforge.alpha.base import AlphaCandidate, MomentumInput, MomentumPolicy
from tradeforge.alpha.momentum import MomentumAlpha
from tradeforge.alpha.reversal import ReversalAlpha, ReversalInput, ReversalPolicy
from tradeforge.domain.common import Count, Digest, StrictModel, Symbol, Text, Timestamp
from tradeforge.domain.decision import AlphaFamily, DecisionState, Side
from tradeforge.intelligence.models import digest


class AlphaReplayInput(StrictModel):
    sequence: Count
    evaluated_at: Timestamp
    sample: ReversalInput | MomentumInput

    @model_validator(mode="after")
    def available(self) -> Self:
        if self.sample.as_of > self.evaluated_at:
            raise ValueError("FUTURE_ALPHA_INPUT")
        # Revalidate nested evidence at the replay clock, including model_construct inputs.
        type(self.sample).model_validate(
            self.sample.model_dump(), context={"now": self.evaluated_at}
        )
        return self


class AlphaRecording(StrictModel):
    dataset_version: Text
    provenance: Literal["recorded", "synthetic"]
    momentum_policy: MomentumPolicy = MomentumPolicy()
    reversal_policy: ReversalPolicy = ReversalPolicy()
    records: Annotated[tuple[AlphaReplayInput, ...], Field(min_length=1, max_length=10_000)]

    @model_validator(mode="after")
    def ordered(self) -> Self:
        previous: tuple[datetime, int] | None = None
        sequences: set[int] = set()
        for row in self.records:
            key = (row.evaluated_at, row.sequence)
            if row.sequence in sequences or (previous is not None and key <= previous):
                raise ValueError("ALPHA_RECORDING_ORDER_VIOLATION")
            sequences.add(row.sequence)
            previous = key
        return self


class AlphaReplayEvaluation(StrictModel):
    sequence: Count
    evaluated_at: Timestamp
    symbol: Symbol
    strategy: AlphaFamily
    side: Side
    candidate: AlphaCandidate | None
    reason_codes: tuple[Text, ...]
    receipt_hash: Digest


class AlphaReplayResult(StrictModel):
    mode: Literal["ALPHA_REPLAY"] = "ALPHA_REPLAY"
    provenance: Literal["recorded", "synthetic"]
    recording_hash: Digest
    evaluated_count: Count
    armed_observations: Count
    unique_armed_setups: Count
    reason_counts: tuple[tuple[Text, Count], ...]
    results: tuple[AlphaReplayEvaluation, ...]
    execution_authorized: Literal[False] = False
    live_eligible: Literal[False] = False


def replay_alpha(recording: AlphaRecording) -> AlphaReplayResult:
    recording = AlphaRecording.model_validate(recording.model_dump())
    momentum, reversal = (
        MomentumAlpha(recording.momentum_policy),
        ReversalAlpha(recording.reversal_policy),
    )
    recording_hash = digest(recording.model_dump(mode="json"))
    previous_hash = recording_hash
    results = []
    reasons: Counter[str] = Counter()
    armed = 0
    unique: set[tuple[str, str, str, str]] = set()
    for row in recording.records:
        for side in (Side.LONG, Side.SHORT):
            evaluations = [
                (
                    AlphaFamily.MOMENTUM,
                    momentum.evaluate(row.sample, side=side, now=row.evaluated_at),
                )
            ]
            if isinstance(row.sample, ReversalInput):
                evaluations.append(
                    (
                        AlphaFamily.REVERSAL,
                        reversal.evaluate(row.sample, side=side, now=row.evaluated_at),
                    )
                )
            for family, result in evaluations:
                candidate = result.candidate
                reasons.update(result.reason_codes)
                if candidate is not None and candidate.state is DecisionState.ARMED:
                    armed += 1
                    unique.add(
                        (row.sample.instrument.symbol, family.value, side.value, candidate.setup_id)
                    )
                previous_hash = digest(
                    {
                        "previous": previous_hash,
                        "sequence": row.sequence,
                        "evaluated_at": row.evaluated_at.isoformat(),
                        "input_hash": row.sample.digest,
                        "strategy": family.value,
                        "side": side.value,
                        "result": result.model_dump(mode="json"),
                    }
                )
                results.append(
                    AlphaReplayEvaluation(
                        sequence=row.sequence,
                        evaluated_at=row.evaluated_at,
                        symbol=row.sample.instrument.symbol,
                        strategy=family,
                        side=side,
                        candidate=candidate,
                        reason_codes=result.reason_codes,
                        receipt_hash=previous_hash,
                    )
                )
    return AlphaReplayResult(
        provenance=recording.provenance,
        recording_hash=recording_hash,
        evaluated_count=len(results),
        armed_observations=armed,
        unique_armed_setups=len(unique),
        reason_counts=tuple(sorted(reasons.items())),
        results=tuple(results),
    )


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Offline replay of recorded alpha inputs")
    parser.add_argument("--input", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        with args.input.open("rb") as stream:
            raw = stream.read(16_000_001)
        if len(raw) > 16_000_000:
            raise ValueError("ALPHA_RECORDING_CAPACITY")
        result = replay_alpha(AlphaRecording.model_validate_json(raw))
    except (ValueError, OSError):
        parser.error("INVALID_ALPHA_RECORDING")
    print(result.model_dump_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
