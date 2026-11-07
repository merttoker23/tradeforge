"""Two-sided CUSUM on log returns with fixed drift and reset after detection."""

from tradeforge.intelligence.volatility import finite


class Cusum:
    def __init__(self, drift: float = 0.001, threshold: float = 0.03) -> None:
        if not 0 <= finite(drift) < finite(threshold):
            raise ValueError("INVALID_CUSUM_POLICY")
        self.drift, self.threshold = drift, threshold
        self._up = self._down = 0.0

    def add(self, value: float) -> bool:
        value = finite(value)
        self._up = max(0.0, self._up + value - self.drift)
        self._down = min(0.0, self._down + value + self.drift)
        changed = self._up >= self.threshold or self._down <= -self.threshold
        if changed:
            self._up = self._down = 0.0
        return changed
