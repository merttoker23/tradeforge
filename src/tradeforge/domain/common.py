"""Shared strict types; Python money is Decimal, wire money is a decimal string."""

import math
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Annotated, Any, Self

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    ValidationInfo,
)


class StrictModel(BaseModel):
    model_config = ConfigDict(
        strict=True,
        frozen=True,
        extra="forbid",
        validate_default=True,
        revalidate_instances="always",
        allow_inf_nan=False,
        hide_input_in_errors=True,
    )

    def model_copy(self, *, update: Mapping[str, Any] | None = None, deep: bool = False) -> Self:
        # Pydantic's normal copy(update=...) skips validation. Refuse that unsafe API.
        if update:
            raise ValueError("UNVALIDATED_COPY_FORBIDDEN: use model_validate with a clock")
        return super().model_copy(deep=deep)


def utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("UTC_REQUIRED")
    return value


def clock(info: ValidationInfo) -> datetime:
    value = info.context.get("now") if isinstance(info.context, dict) else None
    if isinstance(info.context, dict) and "now" in info.context:
        if not isinstance(value, datetime):
            raise ValueError("INVALID_CLOCK")
        return utc(value)
    return datetime.now(UTC)


def money_input(value: object, info: ValidationInfo) -> Decimal:
    if info.mode == "json" and isinstance(value, str):
        try:
            value = Decimal(value)
        except InvalidOperation:
            raise ValueError("INVALID_DECIMAL") from None
    if not isinstance(value, Decimal):
        raise ValueError("DECIMAL_REQUIRED")
    if not value.is_finite() or (value.is_zero() and value.is_signed()):
        raise ValueError("NONFINITE_OR_NEGATIVE_ZERO")
    return value


def exact_bool(value: object) -> object:
    if type(value) is not bool:
        raise ValueError("BOOL_REQUIRED")
    return value


def exact_int(value: object) -> object:
    if type(value) is not int:
        raise ValueError("INT_REQUIRED")
    return value


def finite_stat(value: float) -> float:
    if not math.isfinite(value) or (value == 0 and math.copysign(1, value) < 0):
        raise ValueError("NONFINITE_OR_NEGATIVE_ZERO")
    return value


Timestamp = Annotated[datetime, AfterValidator(utc)]
Money = Annotated[Decimal, BeforeValidator(money_input)]
Positive = Annotated[Money, Field(gt=0)]
NonNegative = Annotated[Money, Field(ge=0)]
Text = Annotated[str, Field(min_length=1, max_length=256, pattern=r"^\S(?:.*\S)?$")]
Symbol = Annotated[str, Field(min_length=5, max_length=40, pattern=r"^[A-Z0-9]+USDT$")]
Digest = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]
Count = Annotated[int, Field(ge=0)]
StatFloat = Annotated[float, AfterValidator(finite_stat)]
UnitFloat = Annotated[StatFloat, Field(ge=0, le=1)]


class DataQuality(StrEnum):
    OK = "OK"
    MISSING = "MISSING"
    STALE = "STALE"
    GAP = "GAP"
    UNKNOWN = "UNKNOWN"


class ReasonCode(StrEnum):
    FOUNDATION_ONLY = "FOUNDATION_ONLY"
    DISABLED = "DISABLED"
    LIVE_LOCKED = "LIVE_LOCKED"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
    NO_TRADE = "NO_TRADE"
    DATA_MISSING = "DATA_MISSING"
    DATA_STALE = "DATA_STALE"
    DATA_GAP = "DATA_GAP"
    CLOCK_SKEW = "CLOCK_SKEW"
    UNSUPPORTED_CONTRACT = "UNSUPPORTED_CONTRACT"
    OOS_UNPROVEN = "OOS_UNPROVEN"
    NONPOSITIVE_EDGE = "NONPOSITIVE_EDGE"
    DB_UNAVAILABLE = "DB_UNAVAILABLE"
    ACCOUNT_NOT_READY = "ACCOUNT_NOT_READY"
    RISK_REJECTED = "RISK_REJECTED"
    PROTECTION_NOT_READY = "PROTECTION_NOT_READY"
    ACK_UNKNOWN = "ACK_UNKNOWN"
    KILL_SWITCH = "KILL_SWITCH"
    EXPIRED = "EXPIRED"
    CANDIDATE_VALIDATED = "CANDIDATE_VALIDATED"
    TESTNET_TRIAL_AUTHORIZED = "TESTNET_TRIAL_AUTHORIZED"


def unique_reasons(value: tuple[ReasonCode, ...]) -> tuple[ReasonCode, ...]:
    if len(set(value)) != len(value):
        raise ValueError("DUPLICATE_REASONS")
    return value


Reasons = Annotated[tuple[ReasonCode, ...], Field(min_length=1), AfterValidator(unique_reasons)]
