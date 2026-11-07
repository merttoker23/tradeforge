"""Conservative connected components of absolute realized correlations."""

from datetime import datetime
from itertools import combinations

from tradeforge.risk.limits import RiskLimits
from tradeforge.risk.models import Correlation


def clusters(
    symbols: set[str],
    correlations: tuple[Correlation, ...],
    *,
    at: datetime,
    policy: RiskLimits,
) -> tuple[tuple[str, ...], ...] | None:
    pairs = {tuple(sorted((x.left, x.right))): x for x in correlations}
    neighbors = {s: {s} for s in symbols}
    for a, b in combinations(sorted(symbols), 2):
        pair = pairs.get((a, b))
        if (
            pair is None
            or pair.rho is None
            or pair.samples < policy.min_correlation_samples
            or not 0 <= (at - pair.as_of).total_seconds() <= policy.factor_age_seconds
        ):
            return None
        if abs(pair.rho) >= policy.correlation_threshold:
            neighbors[a].add(b)
            neighbors[b].add(a)
    result: list[tuple[str, ...]] = []
    remaining = set(symbols)
    while remaining:
        component, todo = set(), {min(remaining)}
        while todo:
            node = todo.pop()
            if node not in component:
                component.add(node)
                todo.update(neighbors[node] - component)
        remaining -= component
        result.append(tuple(sorted(component)))
    return tuple(sorted(result))
