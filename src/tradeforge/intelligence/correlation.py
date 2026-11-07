"""Only synchronized pairs belong here; callers fence period/identity first."""

import math
from collections import deque

from tradeforge.intelligence.volatility import finite, window_size


class RollingPair:
    def __init__(self, window: int = 32, minimum: int = 8) -> None:
        window_size(window)
        if type(minimum) is not int or not 2 <= minimum <= window:
            raise ValueError("INVALID_PAIR_WARMUP")
        self._pairs: deque[tuple[float, float]] = deque(maxlen=window)
        self.minimum = minimum

    def add(self, benchmark: float, asset: float) -> None:
        pair = finite(benchmark), finite(asset)
        self._pairs.append(pair)

    @property
    def count(self) -> int:
        return len(self._pairs)

    def _moments(self) -> tuple[float, float, float] | None:
        if self.count < self.minimum:
            return None
        mx = math.fsum(x for x, _ in self._pairs) / self.count
        my = math.fsum(y for _, y in self._pairs) / self.count
        xx = math.fsum((x - mx) ** 2 for x, _ in self._pairs)
        yy = math.fsum((y - my) ** 2 for _, y in self._pairs)
        xy = math.fsum((x - mx) * (y - my) for x, y in self._pairs)
        # Near-constant history is insufficient evidence, never a default beta.
        if xx / self.count <= 1e-16 or yy / self.count <= 1e-16:
            return None
        return xx, yy, xy

    @property
    def beta(self) -> float | None:
        moments = self._moments()
        return moments[2] / moments[0] if moments else None

    @property
    def correlation(self) -> float | None:
        moments = self._moments()
        if not moments:
            return None
        xx, yy, xy = moments
        return max(-1.0, min(1.0, xy / math.sqrt(xx * yy))) + 0.0
