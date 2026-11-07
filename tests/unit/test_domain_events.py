"""Boundary failures must never be silently normalized into usable evidence."""

import json
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from tradeforge.domain.events import Event
from tradeforge.domain.market import ClosedBar

from .conftest import NOW


def test_event_roundtrip_and_injected_clock(event: dict[str, object]) -> None:
    record = Event.model_validate(event, context={"now": NOW})
    assert Event.model_validate_json(record.model_dump_json(), context={"now": NOW}) == record
    with pytest.raises(ValidationError):
        Event.model_validate(event, context={"now": NOW - timedelta(seconds=1)})


@pytest.mark.parametrize("field", ["source_at", "observed_at", "as_of"])
@pytest.mark.parametrize(
    "value",
    [
        NOW.replace(tzinfo=None),
        NOW.astimezone(timezone(timedelta(hours=3))),
        NOW + timedelta(seconds=1),
        "2026-10-15T12:00:00Z",
        0,
    ],
)
def test_bad_clocks_rejected(event: dict[str, object], field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        Event.model_validate({**event, field: value}, context={"now": NOW})


@pytest.mark.parametrize(
    "patch",
    [
        {"source_at": NOW + timedelta(microseconds=1)},
        {"observed_at": NOW - timedelta(seconds=2)},
        {"as_of": NOW - timedelta(seconds=1)},
        {"schema_version": 2},
        {"schema_version": True},
        {"event_id": str(uuid4())},
        {"sequence": -1},
        {"sequence": True},
        {"unknown": "ignored?"},
    ],
)
def test_event_contract_is_strict(event: dict[str, object], patch: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        Event.model_validate({**event, **patch}, context={"now": NOW})


@pytest.mark.parametrize("field", ["open", "high", "low", "close", "volume"])
@pytest.mark.parametrize(
    "value",
    [
        Decimal("NaN"),
        Decimal("sNaN"),
        Decimal("Infinity"),
        Decimal("-0"),
        Decimal("-1"),
        1.0,
        1,
        True,
        "100",
    ],
)
def test_money_is_finite_strict_and_unsigned(
    bar: dict[str, object], field: str, value: object
) -> None:
    with pytest.raises(ValidationError):
        ClosedBar.model_validate({**bar, field: value}, context={"now": NOW})


def test_valid_bar_json_uses_decimal_strings_and_preserves_key(bar: dict[str, object]) -> None:
    record = ClosedBar.model_validate(bar, context={"now": NOW})
    wire = record.model_dump_json()
    assert json.loads(wire)["close"] == "105"
    assert ClosedBar.model_validate_json(wire, context={"now": NOW}) == record
    numeric_wire = json.loads(wire)
    numeric_wire["close"] = 105.0
    with pytest.raises(ValidationError):
        ClosedBar.model_validate_json(json.dumps(numeric_wire), context={"now": NOW})
    from tradeforge.domain.market import Timeframe

    longer = {**bar, "timeframe": Timeframe.M5, "open_at": NOW - timedelta(minutes=5)}
    assert record.key != ClosedBar.model_validate(longer, context={"now": NOW}).key


def test_immutable_records_and_validated_replacement(bar: dict[str, object]) -> None:
    record = ClosedBar.model_validate(bar, context={"now": NOW})
    with pytest.raises(ValidationError):
        record.close = Decimal("0")
    with pytest.raises(ValueError):
        record.model_copy(update={"close": Decimal("NaN")})


@pytest.mark.parametrize(
    "clock", [datetime(2026, 10, 15), "2026-10-15", datetime.now(UTC).isoformat()]
)
def test_invalid_replay_clock_fails(event: dict[str, object], clock: object) -> None:
    with pytest.raises(ValidationError):
        Event.model_validate(event, context={"now": clock})


def test_json_rejects_future_observation(event: dict[str, object]) -> None:
    wire = json.loads(Event.model_validate(event).model_dump_json())
    wire["as_of"] = (NOW + timedelta(microseconds=1)).isoformat()
    with pytest.raises(ValidationError):
        Event.model_validate_json(json.dumps(wire), context={"now": NOW})


def test_nested_instances_are_revalidated(bar: dict[str, object]) -> None:
    record = ClosedBar.model_validate(bar)
    # Explicit trusted-only construction may bypass Pydantic; the ingestion boundary revalidates.
    corrupt = ClosedBar.model_construct(**{**record.model_dump(), "close": Decimal("NaN")})
    with pytest.raises(ValidationError):
        ClosedBar.model_validate(corrupt)
