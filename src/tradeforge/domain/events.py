"""All observations obey source <= observed <= as_of <= validation clock."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import BeforeValidator, ValidationInfo, model_validator

from tradeforge.domain.common import Count, StrictModel, Timestamp, clock, exact_int


class Event(StrictModel):
    schema_version: Annotated[Literal[1], BeforeValidator(exact_int)]
    event_id: UUID
    source_at: Timestamp
    observed_at: Timestamp
    as_of: Timestamp
    sequence: Count | None = None

    @model_validator(mode="after")
    def temporal_order(self, info: ValidationInfo) -> Self:
        if not self.source_at <= self.observed_at <= self.as_of <= clock(info):
            raise ValueError("INVALID_EVENT_TIME_ORDER")
        return self
