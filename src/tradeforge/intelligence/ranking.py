"""O(N log N) all-eligible ranking using only the supplied historical evidence."""

from bisect import bisect_left, bisect_right
from datetime import datetime
from decimal import Decimal

from tradeforge.domain.common import DataQuality, utc
from tradeforge.intelligence.anomaly import danger_reasons
from tradeforge.intelligence.models import FeatureSnapshot
from tradeforge.intelligence.opportunity import RankedOpportunity, SurveillancePolicy


def rank_market(
    snapshot: FeatureSnapshot, policy: SurveillancePolicy, *, now: datetime
) -> tuple[RankedOpportunity, ...]:
    now = utc(now)
    frame = snapshot.frame
    if now < frame.as_of:
        raise ValueError("FUTURE_INPUT")
    rows = {r.instrument: r for r in frame.observations}
    features = {f.instrument: f for f in snapshot.features}
    if len(features) != len(snapshot.features) or not set(features) <= set(frame.universe.eligible):
        raise ValueError("INVALID_FEATURE_IDENTITIES")
    volumes = sorted(
        r.current.volume * r.current.close
        for r in frame.observations
        if r.quality is DataQuality.OK and r.current.volume > 0
    )
    source_hash = snapshot.digest
    output = []
    for instrument in sorted(frame.universe.eligible, key=lambda k: k.key):
        reasons: list[str] = []
        row, feature = rows.get(instrument), features.get(instrument)
        if (now - frame.source_at).total_seconds() > policy.max_age_seconds:
            reasons.append("STALE_INPUT")
        if (
            now - frame.universe.observed_at
        ).total_seconds() > snapshot.policy.universe_ttl_seconds:
            reasons.append("STALE_UNIVERSE")
        if row is None or feature is None:
            reasons.append("FEATURE_MISSING")
        else:
            if row.quality is not DataQuality.OK:
                reasons.append("INVALID_DATA_QUALITY")
            if feature.sample_count < snapshot.policy.minimum_history:
                reasons.append("INSUFFICIENT_HISTORY")
            if (
                feature.relative_return is None
                or feature.beta is None
                or feature.correlation is None
            ):
                reasons.append("RELATIVE_STRENGTH_UNKNOWN")
            if row.market is None:
                reasons.append("MARKET_MISSING")
            elif (
                row.market.quality is not DataQuality.OK
                or (now - row.market.source_at).total_seconds() > policy.max_age_seconds
            ):
                reasons.append("MARKET_STALE")
            if feature.spread_bps is None or feature.funding is None:
                reasons.append("COST_DATA_MISSING")
            reasons.extend(danger_reasons(row, feature, policy, snapshot.policy))
        components: tuple[tuple[str, float], ...] = ()
        score = None
        if not reasons and row is not None and feature is not None:
            quote_volume = row.current.volume * row.current.close
            # Midrank ties give identical evidence identical percentile scores.
            volume_rank = bisect_left(volumes, quote_volume) + bisect_right(volumes, quote_volume)
            normalized_volume = volume_rank / (2 * len(volumes))
            ratio = min(Decimal(2), row.current.volume / row.previous.volume)
            vol = feature.volatility
            assert feature.relative_return is not None and feature.spread_bps is not None
            components = (
                ("normalized_volume", normalized_volume),
                ("tradability", 1 - feature.spread_bps / policy.max_spread_bps),
                ("relative_strength", min(1.0, abs(feature.relative_return) / vol / 3)),
                ("abnormal_price", min(1.0, abs(feature.log_return) / vol / 3)),
                ("abnormal_volume", float(ratio / 2)),
                (
                    "volatility_suitability",
                    min(vol / policy.target_volatility, policy.target_volatility / vol),
                ),
                (
                    "costs",
                    1
                    - min(
                        1.0,
                        (
                            feature.spread_bps / policy.max_spread_bps
                            + abs(feature.funding or 0) / policy.max_funding
                        )
                        / 2,
                    ),
                ),
            )
            # Equal fixed weights describe surveillance priority, not independent
            # confirmations of alpha, an empirical probability or net expectancy.
            score = sum(value for _, value in components) / len(components)
        output.append(
            RankedOpportunity(
                instrument=instrument,
                score=score,
                components=components,
                reason_codes=tuple(reasons) or ("RANKED",),
                source_feature_hash=source_hash,
            )
        )
    return tuple(
        sorted(output, key=lambda r: (-(r.score if r.score is not None else -1), r.instrument.key))
    )
