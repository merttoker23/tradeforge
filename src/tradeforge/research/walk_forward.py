"""Select predeclared policies on completed past outcomes; evaluate the next fold.

No fitting, ML or parameter search. Labels crossing a boundary and revisions
unavailable at its cutoff are purged. Embargo is an explicit time gap.
"""

from collections import defaultdict
from datetime import timedelta
from decimal import Decimal
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from tradeforge.domain.common import Count, Money, StrictModel, Text, Timestamp
from tradeforge.research.fill_simulator import ZERO
from tradeforge.research.metrics import Status


class PolicyOutcome(StrictModel):
    policy_version: Text
    started_at: Timestamp
    ended_at: Timestamp
    available_at: Timestamp
    net_pnl: Money

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if not self.started_at < self.ended_at <= self.available_at:
            raise ValueError("INVALID_LABEL_AVAILABILITY")
        return self


class Fold(StrictModel):
    train_start: Timestamp
    train_end: Timestamp
    test_start: Timestamp
    test_end: Timestamp
    embargo_seconds: Annotated[int, Field(ge=0, le=31_536_000)]

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if not self.train_start < self.train_end <= self.test_start < self.test_end:
            raise ValueError("INVALID_FOLD_ORDER")
        if self.train_end + timedelta(seconds=self.embargo_seconds) > self.test_start:
            raise ValueError("EMBARGO_REQUIRED")
        return self


class FoldResult(StrictModel):
    fold: Fold
    selected_policy: Text
    train_count: Count
    test_count: Count
    purged_count: Count
    test_net_pnl: Money
    status: Status
    mode: Literal["OOS"] = "OOS"
    live_eligible: Literal[False] = False


def evaluate_fold(outcomes: tuple[PolicyOutcome, ...], fold: Fold) -> FoldResult:
    train: dict[str, list[Decimal]] = defaultdict(list)
    purged = 0
    for row in outcomes:
        if fold.train_start <= row.started_at < fold.train_end:
            if row.ended_at > fold.train_end or row.available_at > fold.train_end:
                purged += 1
            else:
                train[row.policy_version].append(row.net_pnl)
    if not train:
        raise ValueError("INSUFFICIENT_TRAIN_EVIDENCE")
    selected = min(train, key=lambda p: (-sum(train[p], ZERO) / len(train[p]), p))
    test = tuple(
        row
        for row in outcomes
        if row.policy_version == selected
        and fold.test_start <= row.started_at < row.ended_at <= fold.test_end
        and row.available_at <= fold.test_end
    )
    net = sum((row.net_pnl for row in test), ZERO)
    return FoldResult(
        fold=fold,
        selected_policy=selected,
        train_count=len(train[selected]),
        test_count=len(test),
        purged_count=purged,
        test_net_pnl=net,
        status="NEGATIVE" if net < 0 else "UNPROVEN",
    )


def walk_forward(
    outcomes: tuple[PolicyOutcome, ...], folds: tuple[Fold, ...]
) -> tuple[FoldResult, ...]:
    if any(b.test_start < a.test_end for a, b in zip(folds, folds[1:], strict=False)):
        raise ValueError("OOS_OVERLAP_OR_ORDER_VIOLATION")
    return tuple(evaluate_fold(outcomes, fold) for fold in folds)
