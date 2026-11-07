"""BTC-relative log returns require an actual estimable paired beta."""

from tradeforge.intelligence.volatility import finite


def relative_strength(
    asset_return: float, btc_return: float | None, beta: float | None
) -> float | None:
    finite(asset_return)
    if btc_return is None or beta is None:
        return None
    return finite(asset_return - finite(beta) * finite(btc_return)) + 0.0
