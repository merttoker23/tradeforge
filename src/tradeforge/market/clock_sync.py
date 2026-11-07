"""Server time is a safety check, never a license to rewrite event timestamps."""

import math
from datetime import datetime

from tradeforge.domain.common import utc


class ClockSync:
    def __init__(self, *, ttl: float = 60, max_skew: float = 1, max_rtt: float = 1) -> None:
        if any(not math.isfinite(v) or v <= 0 for v in (ttl, max_skew, max_rtt)):
            raise ValueError("INVALID_CLOCK_POLICY")
        self.ttl, self.max_skew, self.max_rtt = ttl, max_skew, max_rtt
        self.sample_at: datetime | None = None
        self.sample_mono = 0.0
        self.valid = False
        self.reason = "CLOCK_UNSAMPLED"

    def observe(self, sent: datetime, received: datetime, server: datetime, mono: float) -> bool:
        rtt = (utc(received) - utc(sent)).total_seconds()
        if rtt < 0 or not math.isfinite(mono):
            self.valid = False
            self.reason = "CLOCK_INVALID"
            raise ValueError("INVALID_CLOCK_SAMPLE")
        offset = (utc(server) - (sent + (received - sent) / 2)).total_seconds()
        # A slow response is inconclusive unless its entire uncertainty interval
        # demonstrates real skew. Never refresh the deadline of an older sample.
        if abs(offset) - rtt / 2 > self.max_skew:
            self.valid = False
            self.reason = "CLOCK_SKEW"
            return False
        if rtt > self.max_rtt:
            self.reason = "CLOCK_RTT"
            return False
        self.sample_at, self.sample_mono = received, mono
        self.valid = rtt <= self.max_rtt and abs(offset) + rtt / 2 <= self.max_skew
        self.reason = "OK" if self.valid else "CLOCK_SKEW"
        return self.valid

    def ready(self, now: datetime, mono: float) -> bool:
        if not self.valid or self.sample_at is None or not math.isfinite(mono):
            return False
        age = mono - self.sample_mono
        wall_age = (utc(now) - self.sample_at).total_seconds()
        return 0 <= age <= self.ttl and abs(wall_age - age) <= self.max_skew

    def invalidate(self) -> None:
        self.valid = False
