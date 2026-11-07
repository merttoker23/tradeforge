"""Bounded version ring. Late observations never rewrite earlier knowledge."""

from datetime import datetime

from tradeforge.domain.common import utc
from tradeforge.domain.market import ClosedBar


class BarWindow:
    def __init__(self, capacity: int = 2048) -> None:
        if type(capacity) is not int or not 1 <= capacity <= 100_000:
            raise ValueError("INVALID_WINDOW_CAPACITY")
        self.capacity = capacity
        self._versions: list[ClosedBar] = []
        self._key: tuple[str, ...] | None = None
        self._floor: datetime | None = None

    def __len__(self) -> int:
        return len(self._versions)

    @property
    def truncated(self) -> bool:
        return self._floor is not None

    def evidence(self, as_of: datetime) -> tuple[ClosedBar, ...]:
        utc(as_of)
        return tuple(b for b in self._versions if b.observed_at <= as_of and b.as_of <= as_of)

    def add(self, bar: ClosedBar) -> bool:
        if self._key is not None and self._key != bar.key:
            raise ValueError("MIXED_BAR_SERIES")
        for old in self._versions:
            if old.open_at == bar.open_at and old.observed_at == bar.observed_at:
                if old == bar:
                    return False
                raise ValueError("CONFLICTING_BAR_REVISION")
        self._key = bar.key
        self._versions.append(bar)
        self._versions.sort(key=lambda b: (b.observed_at, b.open_at, b.event_id))
        if len(self._versions) > self.capacity:
            removed = self._versions.pop(0)
            self._floor = max(self._floor or removed.observed_at, removed.observed_at)
        return True

    def at(self, as_of: datetime) -> tuple[ClosedBar, ...]:
        utc(as_of)
        if self._floor is not None and as_of <= self._floor:
            raise ValueError("HISTORY_NOT_RETAINED")
        latest: dict[datetime, ClosedBar] = {}
        for bar in self._versions:
            if bar.observed_at <= as_of and bar.as_of <= as_of:
                latest[bar.open_at] = bar
        return tuple(latest[key] for key in sorted(latest))
