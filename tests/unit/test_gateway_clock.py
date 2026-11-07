from datetime import UTC, datetime, timedelta

import pytest

from tradeforge.market.clock_sync import ClockSync

NOW = datetime(2026, 10, 16, 12, tzinfo=UTC)


def test_clock_requires_bounded_rtt_skew_and_fresh_sample():
    clock = ClockSync()
    assert not clock.ready(NOW, 1)
    clock.observe(NOW, NOW + timedelta(milliseconds=100), NOW + timedelta(milliseconds=50), 1)
    assert clock.ready(NOW + timedelta(seconds=10), 11)
    assert not clock.ready(NOW + timedelta(seconds=61), 62)
    clock.observe(NOW, NOW + timedelta(seconds=2), NOW, 70)
    assert not clock.ready(NOW + timedelta(seconds=2), 70)
    clock.observe(NOW, NOW, NOW + timedelta(seconds=10), 80)
    assert not clock.ready(NOW, 80)


def test_wall_clock_jump_and_reversed_sample_fail_closed():
    clock = ClockSync()
    clock.observe(NOW, NOW, NOW, 10)
    assert not clock.ready(NOW - timedelta(seconds=3), 11)
    assert not clock.ready(NOW + timedelta(seconds=20), 11)
    with pytest.raises(ValueError):
        clock.observe(NOW, NOW - timedelta(seconds=1), NOW, 1)
