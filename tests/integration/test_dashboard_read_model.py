"""Real read-only PostgreSQL projections with explicit missing and stale facts."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import text


def test_read_transaction_is_enforced_and_empty_data_unknown(migrated):
    from tradeforge.dashboard.read_model import ReadModel
    from tradeforge.persistence.db import PersistenceError

    model = ReadModel(migrated)
    with pytest.raises(PersistenceError):
        with model.transaction() as c:
            c.execute(text("INSERT INTO audit_log(event_type,payload) VALUES ('bad','{}')"))
    result = model.snapshot()
    assert result["status"] == "DEGRADED"
    assert result["sections"]["universe"]["status"] == "UNKNOWN"
    assert result["sections"]["account"]["status"] == "UNKNOWN"
    assert result["live"] is False and result["orders_enabled"] is False
    assert result["sections"]["universe"]["data"] is None


def test_actual_automation_is_separate_from_dashboard_authority_and_expires(migrated):
    from tradeforge.dashboard.read_model import ReadModel, refresh_freshness
    from tradeforge.runtime.storage import save_runtime

    at = datetime.now(UTC)
    save_runtime(
        migrated,
        "execution_core",
        at,
        at + timedelta(seconds=15),
        {
            "healthy": True,
            "execution_ready": True,
            "entries_enabled": True,
            "trial_enabled": True,
            "reason": "READY",
            "blockers": [],
            "stream_ready": True,
            "protection_ready": True,
        },
    )
    save_runtime(
        migrated,
        "quant_core",
        at,
        at + timedelta(seconds=15),
        {
            "analysis_ready": True,
            "evaluated_count": 8,
            "analysis_symbols": 2,
            "symbol_results": [],
            "regime_label": "RANGE",
            "reason": "WAITING_CLOSED_BAR",
        },
    )
    model = ReadModel(migrated, clock=lambda: at)
    result = model.snapshot()
    assert result["trading_authority"]["orders_enabled"] is False
    assert result["execution_readiness"]["ready"] is True
    assert result["execution_readiness"]["entries_enabled"] is True
    assert result["execution_readiness"]["trial_enabled"] is True
    quant = next(r for r in result["sections"]["workers"]["data"] if r["service"] == "quant_core")
    assert quant["evaluated_count"] == 8 and quant["analysis_symbols"] == 2
    assert quant["input_rejections"] is None
    expired = refresh_freshness(result, at + timedelta(seconds=16))
    assert expired["execution_readiness"]["ready"] is False
    assert expired["execution_readiness"]["reason"] == "DISPATCH_STALE"


def test_completed_evaluation_remains_readable_after_worker_expiry(migrated):
    from tradeforge.dashboard.read_model import ReadModel, refresh_freshness
    from tradeforge.runtime.storage import save_runtime

    at = datetime.now(UTC)
    completed = {
        "as_of": (at - timedelta(seconds=30)).isoformat(),
        "evaluated_count": 2,
        "symbol_results": [{"symbol": "BTCUSDT", "state": "ARMED"}],
        "reason_counts": {"SPREAD_GATE": 1},
        "input_rejections": [{"symbol": "ETHUSDT", "reason": "BOOK_UNAVAILABLE"}],
    }
    save_runtime(
        migrated,
        "quant_core",
        at,
        at + timedelta(seconds=15),
        {
            "analysis_ready": False,
            "execution_ready": False,
            "evaluated_count": 0,
            "symbol_results": [],
            "last_evaluation": completed,
            "last_input_check": {
                "as_of": completed["as_of"],
                "input_rejections": completed["input_rejections"],
            },
        },
    )
    result = ReadModel(migrated, clock=lambda: at).snapshot()
    worker = next(r for r in result["sections"]["workers"]["data"] if r["service"] == "quant_core")
    assert worker["last_evaluation"] == completed
    assert worker["last_input_check"]["input_rejections"] == completed["input_rejections"]
    assert worker["evaluated_count"] == 0
    assert result["analysis_readiness"]["ready"] is False
    expired = refresh_freshness(result, at + timedelta(seconds=16))
    worker = next(r for r in expired["sections"]["workers"]["data"] if r["service"] == "quant_core")
    assert worker["last_evaluation"] == completed
    assert worker["last_input_check"]["input_rejections"] == completed["input_rejections"]
    assert worker["freshness"] == "STALE"
    assert expired["analysis_readiness"]["ready"] is False
    assert expired["execution_readiness"]["ready"] is False


def test_allowlist_and_age_do_not_expose_raw_payload(seeded):
    from tradeforge.dashboard.read_model import ReadModel
    from tradeforge.market.bars import canonical

    db, decision, _ = seeded
    now = datetime.now(UTC)
    with db.transaction() as c:
        c.execute(
            text(
                "INSERT INTO account_snapshots(snapshot_id,as_of,payload) "
                "VALUES (:id,:at,CAST(:body AS jsonb))"
            ),
            {
                "id": uuid4(),
                "at": now,
                "body": canonical(
                    {
                        "equity": "100.000000000000000001",
                        "available_balance": "50",
                        "api_secret": "DO_NOT_PROJECT_SENTINEL",
                    }
                ),
            },
        )
    result = ReadModel(db, clock=lambda: now).snapshot()
    assert "DO_NOT_PROJECT_SENTINEL" not in canonical(result)
    account = result["sections"]["account"]
    assert account["status"] == "OK"
    assert account["data"][0]["equity"] == "100.000000000000000001"
    signal = result["sections"]["signals"]["data"][0]
    assert signal["symbol"] == decision.symbol
    assert signal["net_ev_low_R"] == "0.5"
    assert "CANDIDATE_VALIDATED" in signal["reason_codes"]
    stale = ReadModel(db, clock=lambda: now + timedelta(seconds=61)).snapshot()
    assert stale["sections"]["account"]["status"] == "STALE"
    future = ReadModel(db, clock=lambda: now - timedelta(days=1)).snapshot()
    assert future["sections"]["account"]["status"] == "UNKNOWN"


def test_lifecycle_watch_and_rejection_reasons(migrated):
    from tests.decision_fixtures import NOW, advance, register
    from tradeforge.dashboard.read_model import ReadModel
    from tradeforge.decision.storage import DecisionService
    from tradeforge.domain.decision import DecisionState

    service = DecisionService(migrated)
    first = service.submit(register(), now=NOW)
    record = service.submit(advance(first, DecisionState.WATCHING), now=NOW)
    result = ReadModel(migrated, clock=lambda: NOW).snapshot()
    candidate = result["sections"]["candidates"]["data"][0]
    assert candidate["symbol"] == "BTCUSDT" and candidate["state"] == "WATCHING"
    assert candidate["net_ev_low_R"] is None
    service.submit(advance(record, DecisionState.REJECTED), now=NOW)
    signals = ReadModel(migrated, clock=lambda: NOW).snapshot()["sections"]["signals"]["data"]
    assert signals[0]["state"] == "REJECTED"
    assert signals[0]["reason_codes"]


def test_unavailable_database_returns_stale_and_no_raw_error(migrated, monkeypatch):
    from tradeforge.dashboard.read_model import ReadModel

    def fail():
        raise RuntimeError("postgresql://user:secret@host")

    monkeypatch.setattr(migrated.sql_engine, "begin", fail)
    result = ReadModel(migrated).snapshot()
    assert result["status"] == "STALE"
    assert result["reason"] == "READ_MODEL_UNAVAILABLE"
    assert "secret" not in str(result)


def test_real_universe_regime_context_and_durable_risk_projection(migrated):
    from tests.intelligence_fixtures import engine, frame
    from tests.universe_fixtures import payload
    from tradeforge.dashboard.read_model import ReadModel
    from tradeforge.intelligence.storage import save_snapshot
    from tradeforge.market.bars import canonical
    from tradeforge.market.instrument_registry import InstrumentRegistry
    from tradeforge.risk.storage import RiskService

    regime = engine().update(frame(1))
    at = regime.state.as_of
    InstrumentRegistry(migrated).record(payload(), at, timedelta(hours=1))
    save_snapshot(migrated, regime)
    RiskService(migrated, clock=lambda: at).state()
    with migrated.transaction() as c:
        c.execute(
            text(
                "INSERT INTO account_snapshots(snapshot_id,as_of,payload) "
                "VALUES (:id,:at,CAST(:body AS jsonb))"
            ),
            {
                "id": uuid4(),
                "at": at,
                "body": canonical(
                    {
                        "kind": "execution.context.v1",
                        "body": {
                            "account": {
                                "as_of": at.isoformat(),
                                "equity": "123.45",
                                "available_balance": "100",
                                "unrealized_pnl": "2",
                                "realized_pnl_today": "1",
                            },
                            "api_secret": "SENTINEL",
                        },
                    }
                ),
            },
        )
    result = ReadModel(migrated, clock=lambda: at + timedelta(seconds=60)).snapshot()
    assert result["sections"]["universe"]["status"] == "OK"
    assert result["sections"]["universe"]["data"][0]["total"] == 1
    assert result["sections"]["universe"]["data"][0]["eligible"] == 1
    assert result["sections"]["regime"]["data"][0]["label"] == regime.state.label.value
    assert result["sections"]["risk"]["status"] == "OK"
    assert result["sections"]["account"]["data"][0]["equity"] == "123.45"
    assert "SENTINEL" not in canonical(result)
    assert result["sections"]["account"]["data"][0]["freshness"] == "STALE"


def test_missing_economics_degrades_health_with_fresh_unknown_regime(migrated):
    from tests.intelligence_fixtures import engine, frame
    from tradeforge.dashboard.read_model import ReadModel
    from tradeforge.intelligence.storage import save_snapshot

    regime = engine().update(frame(1))
    at = regime.state.as_of
    save_snapshot(migrated, regime)
    with migrated.transaction() as c:
        c.execute(
            text("INSERT INTO account_snapshots(snapshot_id,as_of,payload) VALUES (:id,:at,'{}')"),
            {"id": uuid4(), "at": at},
        )
    result = ReadModel(migrated, clock=lambda: at).snapshot()
    assert result["sections"]["account"]["status"] == "UNKNOWN"
    assert result["sections"]["regime"]["status"] == "OK"
    assert result["sections"]["regime"]["data"][0]["label"] == "UNKNOWN"
    assert result["status"] == "DEGRADED"


def test_account_freshness_uses_embedded_account_time(migrated):
    from tradeforge.dashboard.read_model import ReadModel
    from tradeforge.market.bars import canonical

    now = datetime.now(UTC)
    with migrated.transaction() as c:
        c.execute(
            text(
                "INSERT INTO account_snapshots(snapshot_id,as_of,payload) "
                "VALUES (:id,:at,CAST(:body AS jsonb))"
            ),
            {
                "id": uuid4(),
                "at": now,
                "body": canonical(
                    {
                        "kind": "execution.context.v1",
                        "body": {
                            "account": {
                                "as_of": (now - timedelta(minutes=5)).isoformat(),
                                "equity": "100",
                                "available_balance": "50",
                            }
                        },
                    }
                ),
            },
        )
    account = ReadModel(migrated, clock=lambda: now).snapshot()["sections"]["account"]
    assert account["status"] == "STALE"
    assert datetime.fromisoformat(account["data"][0]["as_of"]) == now - timedelta(minutes=5)


def test_stale_position_cannot_hide_under_fresh_rows(seeded):
    from tradeforge.dashboard.read_model import ReadModel
    from tradeforge.persistence.repositories import instrument_id

    db, decision, _ = seeded
    now = datetime.now(UTC)
    with db.transaction() as c:
        for side, at in (("LONG", now), ("SHORT", now - timedelta(minutes=5))):
            c.execute(
                text(
                    "INSERT INTO positions"
                    "(position_id,instrument_id,side,qty,entry_price,mark_price,as_of) "
                    "VALUES (:id,:instrument,:side,1,100,100,:at)"
                ),
                {"id": uuid4(), "instrument": instrument_id(decision), "side": side, "at": at},
            )
    section = ReadModel(db, clock=lambda: now).snapshot()["sections"]["positions"]
    assert section["status"] == "STALE"
    assert {x["freshness"] for x in section["data"]} == {"OK", "STALE"}


def test_slow_snapshot_and_cached_observation_do_not_renew_freshness(migrated):
    from tradeforge.dashboard.read_model import ReadModel
    from tradeforge.market.bars import canonical

    at = datetime.now(UTC)
    with migrated.transaction() as c:
        c.execute(
            text(
                "INSERT INTO audit_log(event_type,payload,observed_at) "
                "VALUES ('protection.health.v1',CAST(:body AS jsonb),:at)"
            ),
            {
                "at": at,
                "body": canonical(
                    {
                        "ready": True,
                        "reconciled_at": at.isoformat(),
                        "reasons": [],
                        "unprotected": [],
                    }
                ),
            },
        )
    times = iter((at, at + timedelta(seconds=16)))
    slow = ReadModel(migrated, clock=lambda: next(times)).snapshot()
    assert slow["sections"]["guardian"]["status"] == "STALE"
    from tradeforge.dashboard.read_model import refresh_freshness

    frame = ReadModel(migrated, clock=lambda: at).snapshot()
    assert frame["sections"]["guardian"]["status"] == "OK"
    aged = refresh_freshness(frame, at + timedelta(seconds=16))
    assert aged["sections"]["guardian"]["status"] == "STALE"
    assert frame["sections"]["guardian"]["status"] == "OK"  # immutable cached sample


def test_worker_projection_preserves_stage_evidence_without_raw_payload(migrated):
    from tradeforge.dashboard.read_model import ReadModel
    from tradeforge.market.bars import canonical
    from tradeforge.runtime.storage import save_runtime

    at = datetime.now(UTC)
    times = {
        "last_analysis_at": (at - timedelta(seconds=5)).isoformat(),
        "last_analysis_source_at": (at - timedelta(minutes=10)).isoformat(),
        "last_evaluation_at": (at - timedelta(seconds=4)).isoformat(),
        "last_error_at": (at - timedelta(seconds=10)).isoformat(),
    }
    save_runtime(
        migrated,
        "quant_core",
        at,
        at + timedelta(seconds=15),
        {
            **times,
            "last_error_stage": "ALPHA_INPUTS",
            "selected_deep_count": 8,
            "ready_deep_count": 3,
            "input_rejections": [
                {
                    "symbol": "ETHUSDT",
                    "reason": "BOOK_UNAVAILABLE",
                    "api_secret": "DO_NOT_PROJECT_NESTED_SENTINEL",
                },
                {"symbol": "BTCUSDT", "reason": "raw/unsafe/error"},
            ],
            "evaluated_count": 0,
            "api_secret": "DO_NOT_PROJECT_STAGE_SENTINEL",
        },
    )
    result = ReadModel(migrated, clock=lambda: at).snapshot()
    worker = next(r for r in result["sections"]["workers"]["data"] if r["service"] == "quant_core")
    for key, expected in times.items():
        assert worker[key] == expected
    assert worker["last_error_stage"] == "ALPHA_INPUTS"
    assert worker["selected_deep_count"] == 8
    assert worker["ready_deep_count"] == 3
    assert worker["input_rejections"] == [{"symbol": "ETHUSDT", "reason": "BOOK_UNAVAILABLE"}]
    assert "DO_NOT_PROJECT_NESTED_SENTINEL" not in canonical(result)
    assert worker["evaluated_count"] == 0
    assert "DO_NOT_PROJECT_STAGE_SENTINEL" not in canonical(result)
    assert result["live"] is False and result["orders_enabled"] is False


def test_history_progress_projection_keeps_bounded_recovery_evidence(migrated):
    from tradeforge.dashboard.read_model import ReadModel
    from tradeforge.market.bars import canonical
    from tradeforge.runtime.storage import save_runtime

    at = datetime.now(UTC)
    history = {
        "history_total": 517,
        "history_completed": 410,
        "history_attempted": 415,
        "history_failed": 5,
        "history_pending": 107,
        "history_inflight": 4,
        "history_last_completed_symbol": "ETHUSDT",
        "history_last_failure_symbol": "BTCUSDT",
        "history_last_member_ms": 500.5,
        "history_last_failure_at": at.isoformat(),
        "history_last_failure_reason": "TRANSPORT_UNAVAILABLE",
        "history_elapsed_ms": 12000.5,
        "history_complete": False,
    }
    save_runtime(
        migrated,
        "market_gateway",
        at,
        at + timedelta(seconds=15),
        {**history, "private_error": "DO_NOT_PROJECT_HISTORY_SENTINEL"},
    )
    save_runtime(
        migrated,
        "quant_core",
        at,
        at + timedelta(seconds=15),
        {"history_current_members": 410, "history_current_coverage": 0.793},
    )
    result = ReadModel(migrated, clock=lambda: at).snapshot()
    stream = result["sections"]["streams"]["data"][0]
    for key, expected in history.items():
        assert stream[key] == expected
    quant = next(r for r in result["sections"]["workers"]["data"] if r["service"] == "quant_core")
    assert quant["history_current_members"] == 410
    assert quant["history_current_coverage"] == 0.793
    assert "DO_NOT_PROJECT_HISTORY_SENTINEL" not in canonical(result)


def test_live_analysis_never_turns_feed_coverage_into_analyzed_pairs(migrated):
    from tradeforge.dashboard.read_model import ReadModel
    from tradeforge.runtime.storage import save_runtime

    at = datetime.now(UTC)
    save_runtime(
        migrated,
        "market_gateway",
        at,
        at + timedelta(seconds=15),
        {
            "eligible": 10,
            "covered": 7,
            "measured": True,
            "clock_safe": True,
            "coverage_deadlines": [
                (at + timedelta(seconds=10)).isoformat()
                for _ in range(7)
            ],
            "source_at": (at - timedelta(seconds=1)).isoformat(),
            "websocket_state": "CONNECTED",
            "received_events": 100,
            "accepted_events": 90,
            "socket_reconnects": 2,
            "socket_failures": 1,
            "quote_failures": 0,
            "quote_last_reason": "OK",
            "quote_last_success_at": (at - timedelta(seconds=1)).isoformat(),
            "rest_state": "CONNECTED",
            "cycle_failures": {"CANONICAL": 1},
            "last_cycle_error": "MARKET_REST_UNAVAILABLE",
            "last_cycle_error_at": at.isoformat(),
            "last_cycle_error_stage": "CANONICAL",
        },
    )
    live = ReadModel(migrated, clock=lambda: at).snapshot()["live_analysis"]
    assert live["pairs"]["feed"] == 7
    assert live["pairs"]["analyzed"] == 0
    assert live["pairs_detail"] == []
    assert live["engine"]["state"] == "DELAYED"
    assert live["connections"]["binance_websocket"] == "CONNECTED"
    assert live["connections"]["binance_rest"] == "CONNECTED"
    assert live["connections"]["ws_reconnects"] == 2
    assert live["connections"]["worker_errors"] == {"CANONICAL": 1}
    assert live["errors"][0]["source"] == "market_gateway"
    assert live["errors"][0]["code"] == "MARKET_REST_UNAVAILABLE"


@pytest.mark.parametrize(
    ("worker_reason", "ready", "source_age", "heartbeat_ttl", "expected"),
    [
        ("WAITING_CLOSED_BAR", True, 300, 15, ("ACTIVE", "WAITING_CLOSED_BAR")),
        ("ANALYZED", True, 300, 0, ("DELAYED", "ANALYSIS_WORKER_LAG")),
        ("ANALYZED", True, 1351, 15, ("DELAYED", "ANALYSIS_SOURCE_LAG")),
        ("WAITING_HISTORY_SWEEP", False, None, 15, ("WAITING", "WAITING_HISTORY_SWEEP")),
        ("WAITING_HISTORY_SWEEP", False, 1351, 15, ("DELAYED", "WAITING_HISTORY_SWEEP")),
    ],
)
def test_live_analysis_distinguishes_closed_bar_wait_from_stale_worker_or_source(
    worker_reason, ready, source_age, heartbeat_ttl, expected,
):
    from tradeforge.dashboard.read_model import refresh_freshness

    at = datetime(2026, 11, 13, tzinfo=UTC)
    worker = {
        "service": "quant_core",
        "healthy": True,
        "freshness": "OK",
        "as_of": at.isoformat(),
        "fresh_until": (at + timedelta(seconds=heartbeat_ttl)).isoformat(),
        "analysis_ready": ready,
        "analysis_timeframe": "15m",
        "reason": worker_reason,
        "last_analysis_at": (at - timedelta(seconds=90)).isoformat() if ready else None,
        "last_source_at": (at - timedelta(seconds=source_age)).isoformat()
        if source_age is not None else None,
    }

    snapshot = refresh_freshness({"sections": {"workers": {"data": [worker]}}}, at)
    engine = snapshot["live_analysis"]["engine"]
    assert (engine["state"], engine["reason"]) == expected


def test_live_analysis_projects_real_engine_totals_and_evaluated_pairs(migrated):
    from tradeforge.dashboard.read_model import ReadModel
    from tradeforge.runtime.storage import save_runtime

    at = datetime.now(UTC)
    save_runtime(
        migrated,
        "market_gateway",
        at,
        at + timedelta(seconds=15),
        {
            "eligible": 10,
            "covered": 10,
            "measured": True,
            "clock_safe": True,
            "coverage_deadlines": [
                (at + timedelta(seconds=10)).isoformat()
                for _ in range(10)
            ],
            "source_at": (at - timedelta(seconds=1)).isoformat(),
            "websocket_state": "CONNECTED",
            "received_events": 500,
            "accepted_events": 480,
            "quote_failures": 0,
            "quote_last_reason": "OK",
            "quote_last_success_at": (at - timedelta(seconds=1)).isoformat(),
            "rest_state": "CONNECTED",
        },
    )
    summaries = [
        {
            "symbol": "ETHUSDT",
            "analyzed_at": at.isoformat(),
            "as_of": (at - timedelta(seconds=1)).isoformat(),
            "decision": "LONG",
            "strategy": "MOMENTUM",
            "reasons": ["CANDIDATE_VALIDATED"],
        },
        {
            "symbol": "BTCUSDT",
            "analyzed_at": at.isoformat(),
            "as_of": (at - timedelta(seconds=1)).isoformat(),
            "decision": "NO_TRADE",
            "strategy": "REVERSAL",
            "reasons": ["SPREAD_GATE"],
        },
    ]
    save_runtime(
        migrated,
        "quant_core",
        at,
        at + timedelta(seconds=15),
        {
            "healthy": True,
            "analysis_ready": True,
            "evaluated_count": 4,
            "analysis_symbols": 2,
            "analyzed_symbols": 2,
            "evaluation_total": 40,
            "analysis_cycle_total": 10,
            "last_analysis_at": at.isoformat(),
            "last_analysis_source_at": (at - timedelta(seconds=1)).isoformat(),
            "last_success_at": at.isoformat(),
            "last_evaluation_at": at.isoformat(),
            "symbol_summaries": summaries,
        },
    )
    live = ReadModel(migrated, clock=lambda: at).snapshot()["live_analysis"]
    assert live["engine"]["state"] == "ACTIVE"
    assert live["pairs"]["analyzed"] == 2
    assert live["pairs"]["evaluated"] == 4
    assert {row["symbol"] for row in live["pairs_detail"]} == {
        "ETHUSDT",
        "BTCUSDT",
    }
    assert live["rates"]["market_events_total"] == 500
    assert live["rates"]["evaluations_total"] == 40
    assert live["latency"]["market_seconds"] == 1.0
    assert live["latency"]["analysis_seconds"] == 1.0
