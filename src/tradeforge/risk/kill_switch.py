"""Durable control state; restart and midnight never silently rearm the account."""

from datetime import date, datetime
from decimal import Decimal
from typing import Self

from pydantic import model_validator

from tradeforge.domain.common import Count, StrictModel, Timestamp, utc
from tradeforge.risk.limits import RiskLimits


class RiskState(StrictModel):
    as_of: Timestamp
    version: Count = 0
    kill_switch: bool = True
    consecutive_losses: Count = 0
    cooldown_until: Timestamp | None = None
    daily_loss_day: date | None = None

    @model_validator(mode="after")
    def no_future_day(self) -> Self:
        if self.daily_loss_day is not None and self.daily_loss_day > self.as_of.date():
            raise ValueError("FUTURE_LOSS_DAY")
        return self

    def evolve(self, *, at: datetime, **patch: object) -> Self:
        utc(at)
        if at < self.as_of:
            raise ValueError("OUT_OF_ORDER_RISK_STATE")
        return type(self).model_validate(
            {
                **self.model_dump(),
                **patch,
                "as_of": at,
                "version": self.version + 1,
            }
        )

    def after_result(self, pnl: Decimal, *, at: datetime, policy: RiskLimits) -> Self:
        from datetime import timedelta

        if not isinstance(pnl, Decimal) or not pnl.is_finite():
            raise ValueError("FINITE_DECIMAL_PNL_REQUIRED")
        streak = (
            self.consecutive_losses + 1 if pnl < 0 else 0 if pnl > 0 else self.consecutive_losses
        )
        until = self.cooldown_until
        if pnl < 0:
            until = max(until or at, at + timedelta(seconds=policy.cooldown_seconds))
        return self.evolve(at=at, consecutive_losses=streak, cooldown_until=until)
