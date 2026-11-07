"""Real observed bars must rebuild statistics without fabricated historical market quotes."""

from tests.intelligence_fixtures import engine, frame


def test_preexisting_feature_snapshot_keeps_its_original_input_hash():
    import hashlib
    import json

    from tradeforge.intelligence.models import FeatureSnapshot
    from tradeforge.market.bars import canonical

    snapshot = engine().update(frame(1))
    body = snapshot.model_dump(mode="json")
    body["algorithm_version"] = 1
    for row in body["frame"]["observations"]:
        row["market"].pop("environment", None)
        row["market"].pop("source", None)
    historical_input = {
        "frame": body["frame"],
        "policy": body["policy_hash"],
        "previous": body["previous_hash"],
        "algorithm": 1,
    }
    original = hashlib.sha256(canonical(historical_input).encode()).hexdigest()
    body["state"]["input_version"] = original
    loaded = FeatureSnapshot.model_validate_json(json.dumps(body))
    assert loaded.input_version == original


def test_seeded_closed_history_matches_continuous_return_statistics():
    from tradeforge.intelligence.warmup import seed_history

    continuous = engine()
    frames = []
    for index in range(1, 10):
        move = 0.001 * (1 + index % 4)
        frames.append(
            frame(
                index,
                moves=(move, move * 1.5, -move * 0.5),
                previous=frames[-1] if frames else None,
            )
        )
    for item in frames:
        continuous.update(item)
    restarted = engine()
    # A persisted cursor can predate restart by many periods even when the
    # newly observed closed history is complete and contiguous.
    restarted.update(frames[0])
    history = {}
    for item in frames[:-1]:
        for row in item.observations:
            history.setdefault(row.current.key, {})[row.previous.open_at] = row.previous
            history[row.current.key][row.current.open_at] = row.current
    bars = [b for series in history.values() for b in series.values()]
    seed_history(restarted, bars, frames[-1])
    snapshot = restarted.update(frames[-1])
    assert all(f.relative_return is not None for f in snapshot.features)
    for actual, expected in zip(snapshot.features, continuous.latest.features, strict=True):
        assert actual.sample_count == expected.sample_count
        assert actual.volatility == expected.volatility
        assert actual.beta == expected.beta
        assert actual.relative_return == expected.relative_return
    assert "INSUFFICIENT_HISTORY" not in snapshot.reason_codes


def test_seed_does_not_bridge_missing_period_or_future_observation():
    from tradeforge.intelligence.warmup import seed_history

    frames = [frame(i) for i in range(1, 10)]
    bars = [row.current for item in frames[:-2] for row in item.observations]
    restarted = engine()
    seed_history(restarted, bars, frames[-1])
    snapshot = restarted.update(frames[-1])
    assert all(f.sample_count == 1 for f in snapshot.features)
    assert all(f.relative_return is None for f in snapshot.features)
    assert "INSUFFICIENT_HISTORY" in snapshot.reason_codes


def test_warmup_history_revision_changes_the_feature_input_version():
    from decimal import Decimal

    from tests.risk_fixtures import change
    from tradeforge.intelligence.warmup import seed_history

    frames = [frame(i) for i in range(1, 7)]
    history = [row.current for item in frames[:-1] for row in item.observations]
    first = engine()
    seed_history(first, history, frames[-1])
    original = first.update(frames[-1])
    revised = list(history)
    old = revised[6]
    price = old.close * Decimal("1.001")
    revised[6] = change(old, open=price, high=price, low=price, close=price)
    second = engine()
    seed_history(second, revised, frames[-1])
    updated = second.update(frames[-1])
    assert original.features != updated.features
    assert original.input_version != updated.input_version
