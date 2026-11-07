import math

import pytest


def test_bounded_robust_volatility_quantile_and_directional_efficiency():
    from tradeforge.intelligence.volatility import RollingVolatility

    stats = RollingVolatility(window=4, alpha=0.5, clip=0.02)
    for value in (0.01, -0.01, 0.01, -0.01):
        stats.add(value)
    assert stats.volatility == pytest.approx(0.01)
    assert stats.efficiency == 0
    assert stats.quantile(0.5) == pytest.approx(0.01)
    stats.add(2.0)
    assert stats.count == 4
    assert stats.volatility == pytest.approx(math.sqrt(0.00025))
    assert stats.quantile(1.0) == pytest.approx(0.02)
    # Efficiency uses raw moves: a real shock is not hidden by robust clipping.
    assert stats.efficiency == pytest.approx(1.99 / 2.03)
    falling = RollingVolatility(window=4)
    for _ in range(4):
        falling.add(-0.01)
    assert falling.efficiency == 1
    assert falling.direction == -1


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf")])
def test_statistics_reject_nonfinite_without_mutation(bad):
    from tradeforge.intelligence.change_points import Cusum
    from tradeforge.intelligence.correlation import RollingPair
    from tradeforge.intelligence.volatility import RollingVolatility

    vol, pair, cusum = RollingVolatility(), RollingPair(), Cusum()
    with pytest.raises(ValueError, match="NONFINITE"):
        vol.add(bad)
    with pytest.raises(ValueError, match="NONFINITE"):
        pair.add(0.1, bad)
    with pytest.raises(ValueError, match="NONFINITE"):
        cusum.add(bad)
    assert vol.count == pair.count == 0


def test_paired_beta_and_relative_strength_are_not_fabricated():
    from tradeforge.intelligence.correlation import RollingPair
    from tradeforge.intelligence.relative_strength import relative_strength

    pair = RollingPair(window=4, minimum=3)
    pair.add(0.01, 0.02)
    assert pair.beta is None
    for btc, asset in ((-0.01, -0.02), (0.02, 0.04)):
        pair.add(btc, asset)
    assert pair.beta == pytest.approx(2.0)
    assert pair.correlation == pytest.approx(1.0)
    assert relative_strength(0.05, 0.02, pair.beta) == pytest.approx(0.01)
    assert relative_strength(0.05, None, pair.beta) is None
    assert relative_strength(0.05, 0.02, None) is None
    flat = RollingPair(window=4, minimum=3)
    for x in (0.01, 0.02, 0.03):
        flat.add(0.0, x)
    assert flat.beta is flat.correlation is None
    for _ in range(8):
        pair.add(0.0, 0.0)
    assert pair.count == 4
    assert pair.beta is None


def test_cross_section_breadth_dispersion_and_coverage():
    from tradeforge.intelligence.breadth import summarize

    result = summarize((0.01, -0.01, 0.0), eligible=4, neutral=0.001)
    assert result.coverage == 0.75
    assert result.advancing == result.declining == pytest.approx(1 / 3)
    assert result.dispersion == pytest.approx(0.01)
    assert summarize((), eligible=0).coverage == 0
    with pytest.raises(ValueError):
        summarize((0.1, 0.2), eligible=1)


def test_cusum_persistent_change_and_reset():
    from tradeforge.intelligence.change_points import Cusum

    cusum = Cusum(drift=0.001, threshold=0.02)
    assert not cusum.add(0.01)
    assert not cusum.add(0.01)
    assert cusum.add(0.01)
    assert not cusum.add(0.0)
    assert not cusum.add(-0.01)
    assert not cusum.add(-0.01)
    assert cusum.add(-0.01)
