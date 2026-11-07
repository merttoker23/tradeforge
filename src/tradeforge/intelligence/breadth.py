"""Equal-weight cross-sectional breadth, with the historical universe denominator."""

from dataclasses import dataclass
from statistics import median

from tradeforge.intelligence.volatility import finite


@dataclass(frozen=True)
class Breadth:
    coverage: float
    advancing: float
    declining: float
    dispersion: float


def summarize(returns: tuple[float, ...], *, eligible: int, neutral: float = 0.0001) -> Breadth:
    if type(eligible) is not int or eligible < len(returns) or finite(neutral) < 0:
        raise ValueError("INVALID_CROSS_SECTION")
    for value in returns:
        finite(value)
    count = len(returns)
    center = median(returns) if count else 0.0
    return Breadth(
        count / eligible if eligible else 0.0,
        sum(x > neutral for x in returns) / count if count else 0.0,
        sum(x < -neutral for x in returns) / count if count else 0.0,
        median(abs(x - center) for x in returns) if count else 0.0,
    )
