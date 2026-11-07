"""Bounded robust EWMA and rolling empirical quantiles; no learned thresholds."""

import math
from collections import deque
from typing import Literal


def finite(value: float) -> float:
    if not math.isfinite(value) or abs(value) > 1e6:
        raise ValueError("NONFINITE_OR_UNBOUNDED_STATISTIC")
    return float(value)


def window_size(value: int) -> int:
    if type(value) is not int or not 2 <= value <= 4096:
        raise ValueError("INVALID_STATISTICS_WINDOW")
    return value


class RollingVolatility:
    def __init__(self, window: int = 32, alpha: float = 0.2, clip: float = 0.05) -> None:
        if not 0 < finite(alpha) <= 1 or not 0 < finite(clip) <= 1:
            raise ValueError("INVALID_EWMA_POLICY")
        self._raw: deque[float] = deque(maxlen=window_size(window))
        self._clipped: deque[float] = deque(maxlen=window)
        self.alpha, self.clip, self._energy = alpha, clip, 0.0

    def add(self, value: float) -> None:
        value = finite(value)
        clipped = min(self.clip, abs(value))
        self._energy = (
            self.alpha * clipped**2 + (1 - self.alpha) * self._energy if self._raw else clipped**2
        )
        self._raw.append(value)
        self._clipped.append(clipped)

    @property
    def count(self) -> int:
        return len(self._raw)

    @property
    def volatility(self) -> float:
        return math.sqrt(self._energy)

    @property
    def efficiency(self) -> float:
        total = math.fsum(abs(x) for x in self._raw)
        return min(1.0, abs(math.fsum(self._raw)) / total) if total else 0.0

    @property
    def direction(self) -> Literal[-1, 0, 1]:
        total = math.fsum(self._raw)
        return 1 if total > 0 else (-1 if total < 0 else 0)

    def quantile(self, probability: float) -> float | None:
        if not 0 <= finite(probability) <= 1:
            raise ValueError("INVALID_QUANTILE")
        if not self._clipped:
            return None
        values = sorted(self._clipped)
        position = probability * (len(values) - 1)
        left, right = math.floor(position), math.ceil(position)
        return values[left] + (values[right] - values[left]) * (position - left)
