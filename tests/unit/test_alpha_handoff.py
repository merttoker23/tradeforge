"""Point-in-time handoff never mixes revised features or hides rejected inputs."""

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from tests.unit.test_momentum_alpha import change, evidence
from tradeforge.domain.market import Timeframe
from tradeforge.runtime.market_link import alpha_inputs


def feed_for(sample):
    return {
        "as_of": sample.as_of.isoformat(),
        "status": {"data_ready": True},
        "markets": [sample.market.model_dump(mode="json")],
        "deep": [
            {
                "symbol": sample.instrument.symbol,
                "book": sample.book.model_dump(mode="json"),
                "flow": sample.flow.model_dump(mode="json"),
                "previous_flow": change(
                    sample.flow,
                    **{
                        name: getattr(sample.flow, name) - timedelta(seconds=60)
                        for name in (
                            "source_at",
                            "observed_at",
                            "as_of",
                            "source_start",
                            "source_end",
                        )
                    },
                ).model_dump(mode="json"),
            }
        ],
    }


def test_new_feature_observation_after_handoff_is_available_at_evaluation_time(monkeypatch):
    sample = evidence()
    feed = feed_for(sample)
    feed["as_of"] = (sample.as_of - timedelta(milliseconds=1)).isoformat()
    # The actual feed records were observed before its snapshot; the regime
    # calculation finishes one millisecond later. Neither source is in the future.
    for row in feed["markets"]:
        row["as_of"] = feed["as_of"]
        row["observed_at"] = feed["as_of"]
        row["source_at"] = feed["as_of"]
    for row in feed["deep"]:
        for kind in ("book", "flow", "previous_flow"):
            micro = row[kind]
            for name in (
                "source_at",
                "observed_at",
                "as_of",
                "captured_at",
                "source_start",
                "source_end",
            ):
                if name in micro:
                    micro[name] = (
                        datetime.fromisoformat(micro[name]) - timedelta(milliseconds=1)
                    ).isoformat()
    monkeypatch.setattr(
        "tradeforge.runtime.market_link.load_bars",
        lambda db, key, tf, at, limit: (
            sample.medium_bars if tf is Timeframe.M15 else sample.setup_bars
        ),
    )
    rows = alpha_inputs(None, sample.features, feed, evaluated_at=sample.as_of)
    assert len(rows) == 1
    assert rows[0].as_of == sample.as_of
    assert rows[0].market.source_at == sample.as_of - timedelta(milliseconds=1)


def test_medium_bars_are_read_at_feature_observation_not_later_handoff(monkeypatch):
    sample = evidence()
    later = sample.as_of + timedelta(seconds=1)
    feed = feed_for(sample)
    feed["as_of"] = later.isoformat()
    revised = change(sample.medium_bars[-1], observed_at=later, as_of=later)

    def load(db, key, tf, at, limit):
        if tf is Timeframe.M15:
            return (
                sample.medium_bars
                if at <= sample.features.frame.as_of
                else (*sample.medium_bars[:-1], revised)
            )
        return sample.setup_bars

    monkeypatch.setattr("tradeforge.runtime.market_link.load_bars", load)
    rows = alpha_inputs(None, sample.features, feed)
    assert len(rows) == 1
    assert rows[0].medium_bars[-1] == sample.medium_bars[-1]


def test_one_bad_deep_symbol_does_not_discard_other_symbol_evidence(monkeypatch):
    sample = evidence()
    feed = feed_for(sample)
    feed["deep"].insert(0, {**feed["deep"][0], "book": {"password": "DO_NOT_EXPOSE"}})
    diagnostics = {}
    monkeypatch.setattr(
        "tradeforge.runtime.market_link.load_bars",
        lambda db, key, tf, at, limit: (
            sample.medium_bars if tf is Timeframe.M15 else sample.setup_bars
        ),
    )
    rows = alpha_inputs(None, sample.features, feed, diagnostics=diagnostics)
    assert len(rows) == 1
    assert diagnostics["input_rejections"] == [{"symbol": "ETHUSDT", "reason": "BOOK_UNAVAILABLE"}]
    assert "DO_NOT_EXPOSE" not in str(diagnostics)


def test_missing_market_is_explained_without_constructing_alpha_input():
    sample = evidence()
    feed = feed_for(sample)
    feed["markets"] = []
    diagnostics = {}
    assert alpha_inputs(None, sample.features, feed, diagnostics=diagnostics) == ()
    assert diagnostics["input_rejections"] == [
        {"symbol": "ETHUSDT", "reason": "MARKET_UNAVAILABLE"}
    ]


@pytest.mark.parametrize(
    "reason", ["CANONICAL_DATA_NOT_READY", "CANONICAL_5M_GAP", "CANONICAL_15M_STALE"]
)
def test_memory_reports_selected_deep_symbol_waiting_for_canonical_recovery(reason):
    from types import SimpleNamespace

    from tradeforge.runtime.market_link import memory_payload

    sample = evidence()
    gateway = SimpleNamespace(
        clock=lambda: sample.as_of,
        _latest={},
        _generations={},
        eligible={sample.instrument.symbol},
        monotonic=lambda: 0,
        clock_sync=SimpleNamespace(ready=lambda *args: True),
        status=lambda: {"data_ready": True},
        canonical=SimpleNamespace(
            books={"ETHUSDT": SimpleNamespace(state_at=lambda at: sample.book.stream_state)},
            flows={
                "ETHUSDT": SimpleNamespace(
                    snapshot=lambda at: sample.flow, validity_at=lambda at: sample.flow.validity
                )
            },
            symbol_ready=lambda symbol, at: False,
            bar_rejections=lambda symbol, at: (
                () if reason == "CANONICAL_DATA_NOT_READY" else (reason,)
            ),
            storage_ready=True,
        ),
    )
    persistence = SimpleNamespace(
        rest_samples=(sample.market,), markets=lambda *args: (), display_quotes=lambda at: ()
    )
    result = memory_payload(gateway, persistence)
    assert result["deep"] == []
    (row,) = result["deep_status"]
    assert (row["symbol"], row["ready"], row["reason"]) == ("ETHUSDT", False, reason)
    assert row["book_state"] == "READY"
    assert row["flow_quality"] == "OK"
    diagnostics = {}
    assert alpha_inputs(None, sample.features, result, diagnostics=diagnostics) == ()
    assert diagnostics["input_rejections"] == [{"symbol": "ETHUSDT", "reason": reason}]


def test_completed_waiting_cycle_is_not_a_new_analysis_or_evaluation():
    from tradeforge.runtime.progress import RuntimeProgress

    sample = evidence()
    progress = RuntimeProgress(clock=lambda: sample.as_of)
    progress.complete(
        data_ready=False,
        analysis_ready=False,
        execution_ready=False,
        reason="WAITING_HISTORY_SWEEP",
    )
    assert progress.state["cycle_count"] == 1
    assert progress.state["last_analysis_at"] is None
    assert progress.state["last_analysis_source_at"] is None
    assert progress.state["last_evaluation_at"] is None


def test_last_error_stage_and_time_survive_later_success_without_exception_text():
    from tradeforge.runtime.progress import RuntimeProgress

    sample = evidence()
    progress = RuntimeProgress(clock=lambda: sample.as_of)
    progress.advance("ALPHA_INPUTS")
    progress.fail(ValueError("DO_NOT_EXPOSE"))
    progress.complete(
        data_ready=True, analysis_ready=True, execution_ready=False, reason="WAITING_CLOSED_BAR"
    )
    assert progress.state["last_error_stage"] == "ALPHA_INPUTS"
    assert progress.state["last_error_at"] == sample.as_of.isoformat()
    assert progress.state["last_error"] == "INPUT_INVALID"
    assert "DO_NOT_EXPOSE" not in str(progress.state)


def test_handoff_counts_fresh_composite_evidence_without_changed_only_ticker_coverage():
    from types import SimpleNamespace

    from tradeforge.runtime.market_link import memory_payload

    sample = evidence()
    at = [sample.as_of]
    gateway = SimpleNamespace(
        clock=lambda: at[0],
        _latest={},
        _generations={},
        eligible={"ETHUSDT"},
        canonical=SimpleNamespace(books={}, flows={}),
        clock_sync=SimpleNamespace(ready=lambda *args: True),
        monotonic=lambda: 0,
        status=lambda: {"data_ready": True, "analysis_coverage_ready": False},
    )
    persistence = SimpleNamespace(
        rest_samples=(sample.market,), markets=lambda *args: (), display_quotes=lambda at: ()
    )
    assert memory_payload(gateway, persistence)["status"]["analysis_coverage_ready"] is True
    at[0] += timedelta(seconds=16)
    assert memory_payload(gateway, persistence)["status"]["analysis_coverage_ready"] is False


@pytest.mark.parametrize(
    ("environment", "reason", "source_env", "age", "ready"),
    [
        ("testnet", "STREAMS_STALE", "testnet", 2, True),
        ("testnet", "STREAMS_STALE", "testnet", 16, False),
        ("testnet", "STREAMS_STALE", "mainnet", 2, False),
        ("testnet", "CLOCK_UNSAFE", "testnet", 2, False),
        ("testnet", "UNIVERSE_STALE", "testnet", 2, False),
        ("live", "STREAMS_STALE", "mainnet", 2, False),
    ],
)
def test_testnet_handoff_uses_fresh_rest_coverage_during_global_websocket_gap(
    environment, reason, source_env, age, ready
):
    from tradeforge.runtime.market_link import memory_payload

    sample = evidence()
    at = sample.as_of + timedelta(seconds=age)
    markets = tuple(
        change(sample.market, symbol=symbol, environment=source_env, source="binance:rest")
        for symbol in ("ETHUSDT", "BTCUSDT", "SOLUSDT", "BNBUSDT")
    )
    gateway = SimpleNamespace(
        environment=environment,
        clock=lambda: at,
        _latest={},
        _generations={},
        eligible={m.symbol for m in markets} | {"XRPUSDT"},
        canonical=SimpleNamespace(books={}, flows={}),
        clock_sync=SimpleNamespace(ready=lambda *args: reason != "CLOCK_UNSAFE"),
        monotonic=lambda: 0,
        status=lambda: {
            "data_ready": False,
            "reason": reason,
            "websocket_state": "NO_DATA",
        },
    )
    persistence = SimpleNamespace(
        rest_samples=markets, markets=lambda *args: (), display_quotes=lambda at: ()
    )
    for selected in (None, frozenset({"ETHUSDT"})):
        payload = memory_payload(gateway, persistence, symbols=selected)
        assert payload["status"]["data_ready"] is ready
        assert payload["status"]["analysis_coverage_ready"] is ready
        assert payload["status"]["websocket_state"] == "NO_DATA"
        if ready:
            assert payload["status"]["market_data_source"] == "REST_FALLBACK"
        if selected is not None:
            assert all(row["symbol"] in selected for row in payload["markets"])
