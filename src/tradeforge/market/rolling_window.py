"""Bounded latest-revision ring. Revisions never grow memory per open period."""

from datetime import datetime

from tradeforge.domain.common import utc
from tradeforge.domain.market import ClosedBar


class BarWindow:
    """One latest revision per ``open_at``; capacity bounds retained periods.

    A later observation replaces the earlier revision of the same period in
    place. An older late revision is ignored instead of rewriting newer
    knowledge. Only a conflicting revision at the exact same observation time
    fails closed.
    """

    def __init__(self, capacity: int = 2048) -> None:
        if type(capacity) is not int or not 1 <= capacity <= 100_000:
            raise ValueError("INVALID_WINDOW_CAPACITY")
        self.capacity = capacity
        self._bars: dict[datetime, ClosedBar] = {}
        self._key: tuple[str, ...] | None = None
        self._floor: datetime | None = None
        self._ordered: tuple[datetime, ...] | None = None

    def __len__(self) -> int:
        return len(self._bars)

    def _openings(self) -> tuple[datetime, ...]:
        if self._ordered is None:
            self._ordered = tuple(sorted(self._bars))
        return self._ordered

    @property
    def truncated(self) -> bool:
        return self._floor is not None

    def evidence(self, as_of: datetime) -> tuple[ClosedBar, ...]:
        utc(as_of)
        return tuple(
            self._bars[opened]
            for opened in self._openings()
            if self._bars[opened].observed_at <= as_of and self._bars[opened].as_of <= as_of
        )

    def add(self, bar: ClosedBar) -> bool:
        if self._key is not None and self._key != bar.key:
            raise ValueError("MIXED_BAR_SERIES")
        current = self._bars.get(bar.open_at)
        if current is not None:
            if current == bar:
                return False
            if bar.observed_at < current.observed_at:
                return False
            if bar.observed_at == current.observed_at:
                raise ValueError("CONFLICTING_BAR_REVISION")
            self._key = bar.key
            self._bars[bar.open_at] = bar
            return True
        if len(self._bars) >= self.capacity:
            oldest = min(self._bars)
            if bar.open_at <= oldest:
                raise ValueError("HISTORY_NOT_RETAINED")
            removed = self._bars.pop(oldest)
            self._ordered = None
            self._floor = max(self._floor or removed.observed_at, removed.observed_at)
        self._key = bar.key
        self._bars[bar.open_at] = bar
        self._ordered = None
        return True

    def at(self, as_of: datetime) -> tuple[ClosedBar, ...]:
        utc(as_of)
        if self._floor is not None and as_of <= self._floor:
            raise ValueError("HISTORY_NOT_RETAINED")
        return tuple(
            self._bars[opened]
            for opened in self._openings()
            if self._bars[opened].observed_at <= as_of and self._bars[opened].as_of <= as_of
        )
