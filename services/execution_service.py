"""Opt-in execution service shadow/testnet/production runner for execution migration.

Disabled by default. Production/live order placement remains safe/off unless every
explicit production guard, confirmation string, low-risk limit, idempotency lock,
symbol lock, kill-switch check, and monolith duplicate guard passes.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
import time
from typing import Any, Mapping, Optional
from urllib.parse import urlparse

from core.execution_events import (
    ExecutionDecision,
    ExecutionHealth,
    PRODUCTION_EXECUTION_EVENT_TYPES,
    ShadowOrderIntent,
    normalize_execution_command,
    normalize_shadow_order_intent,
)
from core.nats_bus import nats_event_publisher
from services.common.execution_safety import (
    ExecutionSafetyDecision,
    refuse_mainnet_ordering,
    validate_execution_runtime_mode,
    validate_production_execution_runtime_mode,
)
from services.common.idempotency import ExecutionIdempotency
from services.common.nats_consumer import NatsConsumer
from services.common.order_clients import (
    BinanceFuturesProductionOrderClient,
    BinanceFuturesTestnetOrderClient,
    OrderPlacementResult,
    build_order_client,
)
from services.common.runtime_control import runtime_paused
from services.runtime import async_main, bootstrap_logging

# Authority wiring (PHASE-04): single Binance submission authority is FuturesOrderManager
try:
    from engine.execution.futures_order_manager import FuturesOrderManager  # type: ignore
except Exception:  # pragma: no cover - import guard for minimal envs
    FuturesOrderManager = None  # type: ignore

try:
    from engine.futures_risk_manager import FuturesRiskManager  # type: ignore
except Exception:  # pragma: no cover
    FuturesRiskManager = None  # type: ignore

DEFAULT_SUBJECTS = "trading.testnet.execution.intent.>"
LEGACY_SIGNAL_SUBJECTS = ("trading.Signal.>",)
PRODUCTION_REDIS_KEY_PREFIXES = (
    "trading:latest:execution:",
    "trading:latest:position:",
    "trading:latest:trade_result:",
    "trading:latest:orders:",
)
PRODUCTION_EXECUTION_REDIS_PREFIX = "trading:production:execution:"
SHADOW_REDIS_KEY_PREFIX = "trading:shadow:execution:"
TESTNET_ORDER_REFUSAL = "testnet order placement refused: safety guard failed"
PRODUCTION_ORDER_REFUSAL = "production order placement refused: safety guard failed"


def execution_shadow_subject(kind: str, asset: Optional[str] = None) -> str:
    """Return the unprefixed logical shadow execution subject."""
    normalized_kind = ".".join(part.strip() for part in str(kind or "").strip(".").split(".") if part.strip())
    if not normalized_kind:
        raise ValueError("shadow execution subject kind is required")
    subject = f"shadow.execution.{normalized_kind}"
    normalized_asset = str(asset or "").upper().strip()
    if normalized_asset:
        subject = f"{subject}.{normalized_asset}"
    return subject


def production_execution_subject(kind: str, asset: Optional[str] = None) -> str:
    normalized_kind = ".".join(part.strip() for part in str(kind or "").strip(".").split(".") if part.strip())
    if not normalized_kind:
        raise ValueError("production execution subject kind is required")
    subject = f"production.execution.{normalized_kind}"
    normalized_asset = str(asset or "").upper().strip()
    if normalized_asset:
        subject = f"{subject}.{normalized_asset}"
    return subject


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on", "enabled"}


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text == "":
        return default
    return text in {"1", "true", "yes", "on", "enabled"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_list(name: str) -> list[str]:
    raw = os.getenv(name, "")
    return [part.upper().strip() for part in raw.replace(";", ",").split(",") if part.strip()]


def _payload_float(payload: Mapping[str, Any], names: tuple[str, ...]) -> Optional[float]:
    for name in names:
        value = payload.get(name)
        if value is None or str(value).strip() == "":
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    return None


def _subjects_from_env(*, allow_legacy_signal_subjects: bool = False) -> list[str]:
    raw = os.getenv("EXECUTION_SERVICE_SUBJECTS", DEFAULT_SUBJECTS)
    subjects = [part.strip() for part in raw.replace(";", ",").split(",") if part.strip()]
    if allow_legacy_signal_subjects:
        for subject in LEGACY_SIGNAL_SUBJECTS:
            if subject not in subjects:
                subjects.append(subject)
    else:
        subjects = [subject for subject in subjects if subject not in LEGACY_SIGNAL_SUBJECTS and not subject.startswith("trading.Signal")]
    return subjects


def _side_to_long_short(side: Any) -> str:
    raw = str(side or "").strip().upper()
    if raw in {"BUY", "LONG", "CALL"}:
        return "LONG"
    if raw in {"SELL", "SHORT", "PUT"}:
        return "SHORT"
    return ""


def _normalize_execution_side(side: Any) -> str:
    raw = str(side or "").strip().upper()
    if raw in {"BUY", "SELL"}:
        return raw
    long_short = _side_to_long_short(raw)
    if long_short == "LONG":
        return "BUY"
    if long_short == "SHORT":
        return "SELL"
    return raw


def _build_snapshot_for_risk(payload: Mapping[str, Any], intent: Any, price: Optional[float]) -> dict[str, Any]:
    snap: dict[str, Any] = {}
    raw_snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), Mapping) else None
    if isinstance(raw_snapshot, Mapping):
        snap = dict(raw_snapshot)
    else:
        for key in (
            "last_price",
            "mark_price",
            "bid_price",
            "ask_price",
            "spread_bps",
            "last_update_ts",
            "updated_at",
            "ts",
            "order_book_synced",
            "order_book_status",
            "order_book_stale",
            "depth_bid_notional_5",
            "depth_ask_notional_5",
            "funding_rate",
            "market_data_status",
        ):
            if key in payload:
                snap[key] = payload[key]
        if price is not None:
            snap.setdefault("last_price", price)
    # Ensure freshness timestamp
    if "last_update_ts" not in snap and "updated_at" not in snap and "ts" not in snap:
        snap["last_update_ts"] = time.time()
    snap.setdefault("order_book_synced", True)
    snap.setdefault("order_book_status", "OK")
    snap.setdefault("market_data_status", "ONLINE")
    if "last_price" not in snap and price is not None:
        snap["last_price"] = price
    if "spread_bps" not in snap:
        try:
            bid = float(snap.get("bid_price") or payload.get("bid_price") or 0)
            ask = float(snap.get("ask_price") or payload.get("ask_price") or 0)
            mid = (bid + ask) / 2 if bid and ask else (float(price or 0) or 0)
            if bid and ask and mid:
                snap["spread_bps"] = abs(ask - bid) / mid * 10000
            else:
                snap["spread_bps"] = 2.0
        except Exception:
            snap["spread_bps"] = 2.0
    if "bid_price" not in snap and price:
        try:
            snap["bid_price"] = float(price) * 0.9999
        except Exception:
            pass
    if "ask_price" not in snap and price:
        try:
            snap["ask_price"] = float(price) * 1.0001
        except Exception:
            pass
    return snap


def _manager_result_to_testnet_payload(result: Any, intent: Any) -> dict[str, Any]:
    try:
        base = result.to_dict() if hasattr(result, "to_dict") else dict(result or {})
    except Exception:
        base = {}
    intent_obj = getattr(result, "intent", None) or intent
    client_order_id = getattr(intent_obj, "client_order_id", None) if hasattr(intent_obj, "client_order_id") else intent_obj.get("client_order_id") if isinstance(intent_obj, Mapping) else None
    if not client_order_id:
        client_order_id = getattr(intent, "client_order_id", None)
    order_id = getattr(result, "order_id", None) or base.get("order_id")
    return {
        "accepted": bool(getattr(result, "accepted", base.get("accepted", False))),
        "status": str(getattr(result, "status", base.get("status", "")) or ""),
        "reason": str(getattr(result, "reason", base.get("reason", "")) or ""),
        "asset": getattr(intent, "asset", "") or base.get("asset") or "",
        "side": getattr(intent, "side", "") or "",
        "client_order_id": client_order_id,
        "order_id": order_id,
        "testnet": True,
        "mock": False,
        "production": False,
        "dry_run": bool(getattr(result, "dry_run", base.get("dry_run", False))),
        "protective_intents": list(getattr(result, "protective_intents", []) or base.get("protective_intents") or []),
        "exchange_order": dict(getattr(result, "exchange_order", {}) or base.get("exchange_order") or {}),
        "exchange_protective_orders": list(getattr(result, "exchange_protective_orders", []) or base.get("exchange_protective_orders") or []),
        "security_contract": dict(getattr(result, "security_contract", {}) or base.get("security_contract") or {}),
        "protective_validation": dict(getattr(result, "protective_validation", {}) or base.get("protective_validation") or {}),
        "lifecycle": list(getattr(result, "lifecycle", []) or base.get("lifecycle") or []),
        "raw": {"manager": True, "base": base},
    }


def _manager_result_to_production_payload(result: Any, intent: Any) -> dict[str, Any]:
    payload = _manager_result_to_testnet_payload(result, intent)
    payload["testnet"] = False
    payload["production"] = True
    payload["mock"] = False
    return payload


class ExecutionService:
    def __init__(
        self,
        *,
        publisher: Optional[Any] = None,
        redis_client: Optional[Any] = None,
        order_client: Optional[Any] = None,
        idempotency: Optional[ExecutionIdempotency] = None,
        risk_manager: Optional[Any] = None,
        order_manager: Optional[Any] = None,
        connector: Optional[Any] = None,
    ) -> None:
        self.enabled = _env_bool("EXECUTION_SERVICE_ENABLED", False)
        self.shadow_only = _env_bool("EXECUTION_SERVICE_SHADOW_ONLY", True)
        self.accept_signals = _env_bool("EXECUTION_SERVICE_ACCEPT_SIGNALS", False)
        self.place_orders = _env_bool("EXECUTION_SERVICE_PLACE_ORDERS", False)
        self.write_redis = _env_bool("EXECUTION_SERVICE_WRITE_REDIS", False)
        self.testnet_only = _env_bool("EXECUTION_SERVICE_TESTNET_ONLY", True)
        self.arm_testnet_orders = _env_bool("EXECUTION_SERVICE_ARM_TESTNET_ORDERS", False)
        self.arm_production_orders = _env_bool("EXECUTION_SERVICE_ARM_PRODUCTION_ORDERS", False)
        self.production_cutover_enabled = _env_bool("EXECUTION_SERVICE_PRODUCTION_CUTOVER_ENABLED", False)
        self.confirm_real_money_orders = os.getenv("EXECUTION_SERVICE_CONFIRM_REAL_MONEY_ORDERS", "").strip()
        self.order_client_type = os.getenv("EXECUTION_SERVICE_ORDER_CLIENT", "mock").strip().lower() or "mock"
        self.order_mode = os.getenv("EXECUTION_SERVICE_ORDER_MODE", "testnet").strip().lower() or "testnet"
        self.require_testnet_url = _env_bool("EXECUTION_SERVICE_REQUIRE_TESTNET_URL", True)
        self.require_production_url = _env_bool("EXECUTION_SERVICE_REQUIRE_PRODUCTION_URL", True)
        self.allow_mainnet = _env_bool("EXECUTION_SERVICE_ALLOW_MAINNET", False)
        self.binance_futures_testnet = _env_bool("BINANCE_FUTURES_TESTNET", False)
        self.testnet_base_url = os.getenv("BINANCE_FUTURES_REST_URL", os.getenv("EXECUTION_SERVICE_TESTNET_BASE_URL", "https://demo-fapi.binance.com")).strip()
        self.rest_url = self.testnet_base_url
        self.max_notional_usdt = _env_float("EXECUTION_SERVICE_MAX_NOTIONAL_USDT", 25.0)
        self.max_production_notional_usdt = _env_float("EXECUTION_SERVICE_MAX_PRODUCTION_NOTIONAL_USDT", self.max_notional_usdt)
        self.max_order_qty = _env_float("EXECUTION_SERVICE_MAX_ORDER_QTY", 0.0)
        self.allowed_symbols = _env_list("EXECUTION_SERVICE_ALLOWED_SYMBOLS")
        self.require_monolith_execution_disabled = _env_bool("EXECUTION_SERVICE_REQUIRE_MONOLITH_EXECUTION_DISABLED", True)
        self.monolith_execution_enabled = _env_bool("MONOLITH_EXECUTION_ENABLED", False)
        self.analysis_publish_production_signals = _env_bool("ANALYSIS_SERVICE_PUBLISH_PRODUCTION_SIGNALS", False)
        self.allow_legacy_signal_subjects = _env_bool("EXECUTION_SERVICE_ALLOW_LEGACY_SIGNAL_SUBJECTS", False)
        self.publish_legacy_execution_events = _env_bool("EXECUTION_SERVICE_PUBLISH_LEGACY_EXECUTION_EVENTS", False)
        self.kill_switch_enabled = _env_bool("EXECUTION_SERVICE_KILL_SWITCH_ENABLED", True)
        self.kill_switch_file = os.getenv("EXECUTION_SERVICE_KILL_SWITCH_FILE", "/tmp/trading_execution_kill_switch").strip() or "/tmp/trading_execution_kill_switch"
        self.kill_switch_redis_key = os.getenv("EXECUTION_SERVICE_KILL_SWITCH_REDIS_KEY", "trading:kill_switch:execution").strip() or "trading:kill_switch:execution"
        self.symbol_lock_ttl_sec = _env_int("EXECUTION_SYMBOL_LOCK_TTL_SEC", 60)
        self.publisher = publisher or nats_event_publisher
        self.redis_client = redis_client
        self.idempotency = idempotency or ExecutionIdempotency(redis_client, ttl_sec=_env_int("EXECUTION_IDEMPOTENCY_TTL_SEC", 300))
        self.consumer = NatsConsumer(
            service_name=os.getenv("SERVICE_NAME", "execution_service"),
            subjects=_subjects_from_env(allow_legacy_signal_subjects=self.allow_legacy_signal_subjects),
            enabled_env="EXECUTION_SERVICE_ENABLED",
        )
        self.received_command_count = 0
        self.parsed_command_count = 0
        self.received_signal_count = 0
        self.parsed_signal_count = 0
        self.duplicate_signal_count = 0
        self.shadow_intent_count = 0
        self.placed_order_count = 0
        self.rejected_order_count = 0
        self.testnet_order_attempt_count = 0
        self.testnet_order_success_count = 0
        self.testnet_order_failed_count = 0
        self.received_intent_count = 0
        self.parsed_intent_count = 0
        self.order_place_attempt_count = 0
        self.real_order_submit_count = 0
        self.mock_order_submit_count = 0
        self.refused_order_count = 0
        self.duplicate_order_count = 0
        self.idempotency_hit_count = 0
        self.publish_testnet_result_count = 0
        self.received_production_intent_count = 0
        self.parsed_production_intent_count = 0
        self.production_order_attempt_count = 0
        self.real_production_order_submit_count = 0
        self.refused_production_order_count = 0
        self.duplicate_production_order_count = 0
        self.production_idempotency_hit_count = 0
        self.publish_production_result_count = 0
        self.last_client_order_id: Optional[str] = None
        self.last_asset: Optional[str] = None
        self.last_production_client_order_id: Optional[str] = None
        self.last_production_asset: Optional[str] = None
        self.safety_guard_reject_count = 0
        self.published_event_count = 0
        self.redis_write_count = 0
        self.skipped_event_count = 0
        self.last_command_at: Optional[float] = None
        self.last_signal_at: Optional[float] = None
        self.last_error: Optional[str] = None
        self.last_reject_reason: Optional[str] = None
        self.last_order_result: Optional[dict[str, Any]] = None
        self.last_signal_id: Optional[str] = None
        self.last_duplicate_signal_id: Optional[str] = None
        self.last_mock_order_id: Optional[str] = None
        self.last_published_subject: Optional[str] = None
        self.order_client = order_client
        if self.order_client is None:
            try:
                self.order_client = build_order_client(
                    self.order_client_type,
                    base_url=self.rest_url,
                    api_key=os.getenv("BINANCE_API_KEY", ""),
                    api_secret=os.getenv("BINANCE_API_SECRET", ""),
                )
            except Exception as exc:
                self.order_client = None
                self.last_error = str(exc)
                logging.debug("Execution order client disabled: %s", exc, exc_info=False)
        # Authority wiring: risk manager + order manager (single Binance authority)
        self._risk_manager = risk_manager
        self._order_manager = order_manager
        self._connector = connector
        self._authoritative_testnet_calls = 0
        self._authoritative_production_calls = 0
        self.last_risk_plan: Optional[dict[str, Any]] = None
        self.last_authority_result: Optional[dict[str, Any]] = None
        self._commands: list[dict[str, Any]] = []
        self._intents: list[ShadowOrderIntent] = []

    def _get_risk_manager(self) -> Optional[Any]:
        if getattr(self, "_risk_manager", None) is not None:
            return self._risk_manager
        if FuturesRiskManager is None:
            return None
        try:
            mgr = FuturesRiskManager()
            self._risk_manager = mgr
            return mgr
        except Exception:
            return None

    def _get_order_manager(self) -> Optional[Any]:
        if getattr(self, "_order_manager", None) is not None:
            return self._order_manager
        if FuturesOrderManager is None:
            return None
        try:
            connector = getattr(self, "_connector", None)
            if connector is None:
                # Attempt to create a lightweight connector for dry-run authority tests.
                # Real signed REST will still require credentials; without them manager
                # will return TESTNET_LIVE_NOT_ARMED as expected.
                try:
                    from exchange.binance_futures.connector import BinanceFuturesConnector  # type: ignore

                    connector = BinanceFuturesConnector()
                except Exception:
                    connector = None
            mgr = FuturesOrderManager(connector=connector)
            # Disable journal for service tests to avoid file I/O
            try:
                mgr.trade_journal = None
            except Exception:
                pass
            self._order_manager = mgr
            self._connector = connector
            return mgr
        except Exception as exc:
            logging.debug("Authority order manager unavailable: %s", exc, exc_info=False)
            return None

    def _should_use_authority(self) -> bool:
        # Mock mode stays on MockOrderClient for deterministic unit tests.
        # Any non-mock client type routes through the single authority.
        if self.order_client_type == "mock":
            return False
        if FuturesOrderManager is None or FuturesRiskManager is None:
            return False
        client = getattr(self, "order_client", None)
        if client is not None:
            # Refusing adapters must route to authority; custom injected fakes (test doubles)
            # keep legacy path for backward compat unless manager explicitly injected.
            if isinstance(client, (BinanceFuturesTestnetOrderClient, BinanceFuturesProductionOrderClient)):
                return True
            if getattr(self, "_order_manager", None) is not None or getattr(self, "_connector", None) is not None:
                return True
            # Detect injected test fakes by class name/module not being the real adapters
            if hasattr(client, "place_order") and client.__class__.__name__ not in {"MockOrderClient", "BinanceFuturesTestnetOrderClient", "BinanceFuturesProductionOrderClient"}:
                # Check module: real adapters are from services.common.order_clients
                if getattr(client.__class__, "__module__", "") != "services.common.order_clients":
                    return False
        return True

    def _resolve_entry_price(self, payload: Mapping[str, Any], intent: Any) -> Optional[float]:
        for key in ("price", "entry_price", "mark_price", "last_price", "close"):
            val = payload.get(key)
            if val is not None and str(val).strip() != "":
                try:
                    price = float(val)
                    if price > 0:
                        return price
                except (TypeError, ValueError):
                    continue
        try:
            price = float(getattr(intent, "price", None) or 0)
            if price > 0:
                return price
        except Exception:
            pass
        # Fallback to quantity+notional derived price
        qty = _payload_float(payload, ("quantity", "qty", "order_qty", "amount"))
        notional = _payload_float(payload, ("notional_usdt", "notional", "order_notional_usdt"))
        if qty and notional and qty != 0:
            try:
                return abs(notional / qty)
            except Exception:
                pass
        return None

    def _build_risk_decision(self, intent: Any, payload: Mapping[str, Any], price: float) -> Optional[Any]:
        risk_mgr = self._get_risk_manager()
        if risk_mgr is None or price is None or price <= 0:
            return None
        direction = _side_to_long_short(getattr(intent, "side", "") or payload.get("side") or payload.get("signal") or payload.get("direction") or "")
        if not direction:
            direction = _side_to_long_short(payload.get("signal") or payload.get("direction") or "")
        if direction not in {"LONG", "SHORT"}:
            return None
        snapshot = _build_snapshot_for_risk(payload, intent, price)
        indicators: dict[str, Any] = {}
        if isinstance(payload.get("indicators"), Mapping):
            indicators = dict(payload["indicators"])
        elif isinstance(payload.get("indicator_snapshot"), Mapping):
            indicators = dict(payload["indicator_snapshot"])
        elif isinstance(payload.get("snapshot"), Mapping) and isinstance(payload["snapshot"].get("indicators"), Mapping):
            indicators = dict(payload["snapshot"]["indicators"])
        # Preserve trace ids for deterministic clientOrderId
        trace_id = getattr(intent, "signal_id", None) or payload.get("signal_id") or payload.get("trace_id") or ""
        if trace_id:
            indicators.setdefault("trace_id", trace_id)
            indicators.setdefault("playbook", payload.get("playbook") or payload.get("strategy") or "orderflow_momentum_scalp")
        try:
            plan = risk_mgr.build_trade_plan(
                symbol=getattr(intent, "asset", "") or payload.get("asset") or payload.get("symbol") or "",
                direction=direction,
                entry_price=price,
                snapshot=snapshot,
                indicators=indicators,
            )
            return plan
        except Exception as exc:
            logging.debug("Risk plan build failed: %s", exc, exc_info=False)
            return None

    async def _execute_authoritative_testnet(self, intent: ShadowOrderIntent, payload: Mapping[str, Any]) -> Any:
        price = self._resolve_entry_price(payload, intent)
        if price is None:
            price = 100.0
        plan = self._build_risk_decision(intent, payload, price)
        if plan is None:
            # Fail closed: risk could not be built
            self.last_risk_plan = None
            return None
        self.last_risk_plan = plan.to_dict() if hasattr(plan, "to_dict") else dict(plan)
        if not getattr(plan, "allowed", False):
            # Risk rejected => return a synthetic FuturesOrderResult-like object with REJECTED_BY_RISK
            class _Rejected:
                accepted = False
                status = "REJECTED_BY_RISK"
                reason = getattr(plan, "reason", "risk_rejected")
                dry_run = True
                protective_intents: list = []
                exchange_order: dict = {}
                exchange_protective_orders: list = []
                security_contract: dict = {}
                protective_validation: dict = {}
                lifecycle: list = [{"stage": "RISK_REJECTED", "reason": reason}]
                order_id = None
                intent = None
                def to_dict(self_inner):
                    return {"accepted": False, "status": self_inner.status, "reason": self_inner.reason, "risk_plan": self.last_risk_plan}
            return _Rejected()
        mgr = self._get_order_manager()
        if mgr is None:
            return None
        try:
            self._authoritative_testnet_calls += 1
            result = await mgr.submit_entry(plan)
            return result
        except Exception as exc:
            logging.debug("Authoritative testnet execution failed: %s", exc, exc_info=False)
            class _Err:
                accepted = False
                status = "AUTHORITY_ERROR"
                reason = str(exc)
                dry_run = True
                protective_intents: list = []
                exchange_order: dict = {}
                exchange_protective_orders: list = []
                security_contract: dict = {}
                protective_validation: dict = {}
                lifecycle: list = [{"stage": "AUTHORITY_ERROR", "error": str(exc)}]
                order_id = None
                intent = None
                def to_dict(self_inner):
                    return {"accepted": False, "status": self_inner.status, "reason": self_inner.reason}
            return _Err()

    async def _execute_authoritative_production(self, intent: ShadowOrderIntent, payload: Mapping[str, Any]) -> Any:
        price = self._resolve_entry_price(payload, intent)
        if price is None:
            try:
                price = float(intent.quantity or 0) and float(payload.get("price") or 0) or 100.0
            except Exception:
                price = 100.0
        plan = self._build_risk_decision(intent, payload, price)
        if plan is None:
            self.last_risk_plan = None
            return None
        self.last_risk_plan = plan.to_dict() if hasattr(plan, "to_dict") else dict(plan)
        if not getattr(plan, "allowed", False):
            class _Rejected:
                accepted = False
                status = "REJECTED_BY_RISK"
                reason = getattr(plan, "reason", "risk_rejected")
                dry_run = True
                protective_intents: list = []
                exchange_order: dict = {}
                exchange_protective_orders: list = []
                security_contract: dict = {}
                protective_validation: dict = {}
                lifecycle: list = [{"stage": "RISK_REJECTED", "reason": reason}]
                order_id = None
                intent = None
                def to_dict(self_inner):
                    return {"accepted": False, "status": self_inner.status, "reason": self_inner.reason, "risk_plan": self.last_risk_plan}
            return _Rejected()
        mgr = self._get_order_manager()
        if mgr is None:
            return None
        try:
            self._authoritative_production_calls += 1
            result = await mgr.submit_entry(plan)
            return result
        except Exception as exc:
            class _Err:
                accepted = False
                status = "AUTHORITY_ERROR"
                reason = str(exc)
                dry_run = True
                protective_intents: list = []
                exchange_order: dict = {}
                exchange_protective_orders: list = []
                security_contract: dict = {}
                protective_validation: dict = {}
                lifecycle: list = [{"stage": "AUTHORITY_ERROR", "error": str(exc)}]
                order_id = None
                intent = None
                def to_dict(self_inner):
                    return {"accepted": False, "status": self_inner.status, "reason": self_inner.reason}
            return _Err()

    def _shadow_publish_allowed(self) -> bool:
        return bool(self.enabled and self.accept_signals)

    def _redis_write_allowed(self) -> bool:
        return bool(self.enabled and self.shadow_only and self.write_redis)

    def _production_redis_write_allowed(self) -> bool:
        return bool(self.enabled and self.write_redis and self.production_cutover_enabled)

    def _logical_subject(self, kind: str, asset: Optional[str] = None) -> str:
        if str(kind).startswith("order_") or kind == "health":
            normalized_asset = str(asset or "").upper().strip()
            subject = f"testnet.execution.{kind}"
            return f"{subject}.{normalized_asset}" if normalized_asset else subject
        return execution_shadow_subject(kind, asset)

    def _final_subject(self, logical_subject: str, asset: Optional[str] = None) -> str:
        prefix = getattr(self.publisher, "subject_prefix", None)
        if not isinstance(prefix, str) or not prefix.strip():
            prefix = os.getenv("NATS_SUBJECT_PREFIX", "trading")
        prefix = str(prefix or "").strip().strip(".")
        subject = logical_subject.strip(".")
        normalized_asset = str(asset or "").upper().strip()
        if normalized_asset and not subject.endswith(f".{normalized_asset}"):
            subject = f"{subject}.{normalized_asset}"
        return f"{prefix}.{subject}" if prefix else subject

    def _final_shadow_subject(self, kind: str, asset: Optional[str] = None) -> str:
        return self._final_subject(self._logical_subject(kind, asset), asset=None)

    async def _publish_shadow_event(self, kind: str, payload: Mapping[str, Any], *, asset: Optional[str] = None) -> None:
        if not self._shadow_publish_allowed():
            return
        event_payload = dict(payload)
        normalized_asset = str(asset or "").upper().strip()
        if normalized_asset:
            event_payload.setdefault("asset", normalized_asset)
            event_payload.setdefault("symbol", normalized_asset)
        await self.publisher.publish(self._logical_subject(kind), event_payload)
        self.last_published_subject = self._final_shadow_subject(kind, normalized_asset or None)
        self.published_event_count += 1

    async def _publish_production_event(self, kind: str, payload: Mapping[str, Any], *, asset: Optional[str] = None) -> None:
        if not (
            self.enabled
            and self.accept_signals
            and self.place_orders
            and self.arm_production_orders
            and self.production_cutover_enabled
            and not self.publish_legacy_execution_events
        ):
            return
        event_payload = dict(payload)
        normalized_asset = str(asset or payload.get("asset") or payload.get("symbol") or "").upper().strip()
        if normalized_asset:
            event_payload.setdefault("asset", normalized_asset)
            event_payload.setdefault("symbol", normalized_asset)
        event_payload["event_type"] = "ProductionExecutionOrderResult" if kind != "health" else "ProductionExecutionHealth"
        event_payload["production"] = True
        event_payload["testnet"] = False
        logical = production_execution_subject(kind)
        await self.publisher.publish(logical, event_payload)
        self.last_published_subject = self._final_subject(logical, normalized_asset or None)
        self.published_event_count += 1

    def _is_legacy_signal_subject(self, subject: str) -> bool:
        return str(subject or "").startswith("trading.Signal.")

    def _is_production_intent(self, event_type: str, payload: Mapping[str, Any], subject: str) -> bool:
        if _as_bool(payload.get("production"), False):
            return True
        if str(event_type or "").strip() in {"ProductionOrderIntent", "ProductionExecutionIntent"}:
            return True
        return ".production.execution.intent." in str(subject or "") or str(subject or "").endswith(".production.execution.intent")

    async def handle_event(self, event_type: str, payload: Mapping[str, Any], subject: str) -> None:
        if await runtime_paused(self.redis_client):
            self.skipped_event_count += 1
            self.last_error = "runtime paused"
            return
        if "cmd.execution" in subject or str(event_type).lower() in {"executioncommand", "command", "cmd"}:
            await self.handle_command(payload, subject)
            return
        await self.handle_signal(event_type, payload, subject)

    async def handle_command(self, payload: Mapping[str, Any], subject: str = "") -> None:
        self.received_command_count += 1
        self.last_command_at = time.time()
        try:
            state = normalize_execution_command(payload)
            if state.command_id:
                result = await self.idempotency.acquire_command(state.command_id)
                if result.duplicate:
                    self.idempotency_hit_count += 1
                    self.duplicate_order_count += 1
                    self.last_reject_reason = f"duplicate execution command: {state.command_id}"
                    return
        except Exception as exc:
            self.skipped_event_count += 1
            self.last_error = str(exc)
            return
        self.parsed_command_count += 1
        self._commands.append(state.to_dict())
        await self._publish_shadow_event("command", state.to_dict())

    async def handle_signal(self, event_type: str, payload: Mapping[str, Any], subject: str = "") -> None:
        self.received_signal_count += 1
        self.received_intent_count += 1
        production_intent = self._is_production_intent(event_type, payload, subject)
        if production_intent:
            self.received_production_intent_count += 1
        self.last_signal_at = time.time()
        if not self.accept_signals:
            self.skipped_event_count += 1
            return
        try:
            intent = normalize_shadow_order_intent(event_type, payload, subject=subject)
            self.last_signal_id = intent.signal_id
            if production_intent:
                self.parsed_signal_count += 1
                self.parsed_intent_count += 1
                self.parsed_production_intent_count += 1
                self.last_client_order_id = intent.client_order_id
                self.last_asset = intent.asset
                self.last_production_client_order_id = intent.client_order_id
                self.last_production_asset = intent.asset
                self.shadow_intent_count += 1
                self._intents.append(intent)
                await self._handle_production_intent(intent, payload)
                return

            duplicate = False
            if intent.signal_id:
                result = await self.idempotency.acquire_signal(intent.signal_id)
                duplicate = result.duplicate
            if duplicate:
                self.duplicate_signal_count += 1
                self.duplicate_order_count += 1
                self.idempotency_hit_count += 1
                self.last_duplicate_signal_id = intent.signal_id
                return
        except Exception as exc:
            self.skipped_event_count += 1
            self.last_error = str(exc)
            logging.debug("Execution signal skipped: %s", exc, exc_info=False)
            return
        self.parsed_signal_count += 1
        self.parsed_intent_count += 1
        self.last_client_order_id = intent.client_order_id
        self.last_asset = intent.asset
        self.shadow_intent_count += 1
        self._intents.append(intent)
        decision = ExecutionDecision(asset=intent.asset, shadow_only=self.shadow_only, place_orders=self.place_orders, intent=intent)
        if self.place_orders:
            decision = await self._attempt_testnet_order(intent, payload)
        await self._publish_shadow_event("intent", decision.to_dict(), asset=intent.asset)
        if self._redis_write_allowed():
            await self._write_shadow_redis(intent, decision)

    async def _handle_production_intent(self, intent: ShadowOrderIntent, payload: Mapping[str, Any]) -> None:
        decision = ExecutionDecision(
            asset=intent.asset,
            action="SKIP_PRODUCTION_ORDER",
            shadow_only=self.shadow_only,
            place_orders=self.place_orders,
            reason="production_order_placement_disabled",
            intent=intent,
        )
        if self.place_orders:
            decision = await self._attempt_production_order(intent, payload)
        await self._publish_shadow_event("intent", decision.to_dict(), asset=intent.asset)

    async def _attempt_testnet_order(self, intent: ShadowOrderIntent, payload: Mapping[str, Any]) -> ExecutionDecision:
        safety = validate_execution_runtime_mode(self._safety_config(intent=intent, payload=payload))
        if not safety.allowed:
            return self._record_safety_reject(intent, safety)
        self.testnet_order_attempt_count += 1
        self.order_place_attempt_count += 1
        # PHASE-04 authority path: non-mock order types route through FuturesOrderManager
        if self._should_use_authority():
            auth_result = await self._execute_authoritative_testnet(intent, payload)
            if auth_result is not None:
                result_payload = _manager_result_to_testnet_payload(auth_result, intent)
                self.last_order_result = result_payload
                self.last_authority_result = result_payload
                if result_payload.get("mock") and result_payload.get("order_id"):
                    self.last_mock_order_id = str(result_payload.get("order_id"))
                # Count success vs failure based on authority result
                status = str(result_payload.get("status") or "")
                accepted = bool(result_payload.get("accepted"))
                is_risk_reject = status == "REJECTED_BY_RISK"
                if accepted and not is_risk_reject:
                    self.testnet_order_success_count += 1
                    self.placed_order_count += 1
                    if result_payload.get("mock"):
                        self.mock_order_submit_count += 1
                    else:
                        self.real_order_submit_count += 1
                    action = "TESTNET_ORDER_PLACED"
                else:
                    self.testnet_order_failed_count += 1
                    if is_risk_reject:
                        action = "TESTNET_ORDER_REJECTED_BY_RISK"
                    else:
                        action = "TESTNET_ORDER_FAILED"
                    # Also count as refused/rejected for observability
                    if not accepted:
                        self.rejected_order_count += 1
                await self._publish_testnet_order_result(intent, result_payload)
                return ExecutionDecision(
                    asset=intent.asset,
                    action=action,
                    shadow_only=self.shadow_only,
                    place_orders=self.place_orders,
                    reason=str(result_payload.get("reason") or result_payload.get("status") or action),
                    intent=intent,
                )
            # Authority unavailable (risk build failed) -> fail closed
            self.testnet_order_failed_count += 1
            self.last_error = "authority execution unavailable: risk plan could not be built"
            result_payload = OrderPlacementResult(
                accepted=False,
                status="AUTHORITY_UNAVAILABLE",
                reason=self.last_error,
                asset=intent.asset,
                side=intent.side,
                testnet=True,
            ).to_dict()
            self.last_order_result = result_payload
            await self._publish_testnet_order_result(intent, result_payload)
            return ExecutionDecision(
                asset=intent.asset,
                action="TESTNET_ORDER_FAILED",
                shadow_only=self.shadow_only,
                place_orders=self.place_orders,
                reason=self.last_error,
                intent=intent,
            )
        if self.order_client is None:
            self.testnet_order_failed_count += 1
            self.last_error = "execution order client is unavailable"
            result_payload = OrderPlacementResult(
                accepted=False,
                status="FAILED",
                reason=self.last_error,
                asset=intent.asset,
                side=intent.side,
                testnet=True,
            ).to_dict()
            self.last_order_result = result_payload
            await self._publish_testnet_order_result(intent, result_payload)
            return ExecutionDecision(
                asset=intent.asset,
                action="TESTNET_ORDER_FAILED",
                shadow_only=self.shadow_only,
                place_orders=self.place_orders,
                reason=self.last_error,
                intent=intent,
            )
        try:
            result = self.order_client.place_order(intent)
            if asyncio.iscoroutine(result):
                result = await result
        except Exception as exc:
            self.testnet_order_failed_count += 1
            self.last_error = str(exc)
            result_payload = OrderPlacementResult(
                accepted=False,
                status="FAILED",
                reason=str(exc),
                asset=intent.asset,
                side=intent.side,
                testnet=True,
            ).to_dict()
            self.last_order_result = result_payload
            await self._publish_testnet_order_result(intent, result_payload)
            return ExecutionDecision(
                asset=intent.asset,
                action="TESTNET_ORDER_FAILED",
                shadow_only=self.shadow_only,
                place_orders=self.place_orders,
                reason=str(exc),
                intent=intent,
            )

        result_payload = self._order_result_to_dict(result, intent)
        self.last_order_result = result_payload
        if result_payload.get("mock") and result_payload.get("order_id"):
            self.last_mock_order_id = str(result_payload.get("order_id"))
        if self._order_result_succeeded(result_payload):
            self.testnet_order_success_count += 1
            self.placed_order_count += 1
            if result_payload.get("mock"):
                self.mock_order_submit_count += 1
            else:
                self.real_order_submit_count += 1
            action = "TESTNET_ORDER_PLACED"
        else:
            self.testnet_order_failed_count += 1
            action = "TESTNET_ORDER_FAILED"
        await self._publish_testnet_order_result(intent, result_payload)
        return ExecutionDecision(
            asset=intent.asset,
            action=action,
            shadow_only=self.shadow_only,
            place_orders=self.place_orders,
            reason=str(result_payload.get("reason") or result_payload.get("status") or action),
            intent=intent,
        )

    async def _attempt_production_order(self, intent: ShadowOrderIntent, payload: Mapping[str, Any]) -> ExecutionDecision:
        kill_switch_active = await self._kill_switch_active()
        safety = validate_production_execution_runtime_mode(self._production_safety_config(intent=intent, payload=payload, kill_switch_active=kill_switch_active))
        if not safety.allowed:
            return self._record_production_safety_reject(intent, safety)

        duplicate_reason = await self._acquire_production_locks(intent)
        if duplicate_reason:
            self.duplicate_order_count += 1
            self.idempotency_hit_count += 1
            self.duplicate_production_order_count += 1
            self.production_idempotency_hit_count += 1
            self.last_duplicate_signal_id = intent.signal_id
            self.last_reject_reason = duplicate_reason
            self.last_error = duplicate_reason
            return ExecutionDecision(
                asset=intent.asset,
                action="DUPLICATE_PRODUCTION_ORDER",
                shadow_only=self.shadow_only,
                place_orders=self.place_orders,
                duplicate=True,
                reason=duplicate_reason,
                intent=intent,
            )

        self.production_order_attempt_count += 1
        self.order_place_attempt_count += 1
        # PHASE-04 authority path for production: risk + FuturesOrderManager single authority
        if self._should_use_authority():
            auth_result = await self._execute_authoritative_production(intent, payload)
            if auth_result is not None:
                result_payload = _manager_result_to_production_payload(auth_result, intent)
                self.last_order_result = result_payload
                self.last_authority_result = result_payload
                status = str(result_payload.get("status") or "")
                accepted = bool(result_payload.get("accepted"))
                is_risk_reject = status == "REJECTED_BY_RISK"
                if accepted and not is_risk_reject and "REJECTED" not in status:
                    self.placed_order_count += 1
                    self.real_order_submit_count += 1
                    self.real_production_order_submit_count += 1
                    action = "PRODUCTION_ORDER_SUBMITTED"
                else:
                    self.refused_order_count += 1
                    self.refused_production_order_count += 1
                    if is_risk_reject:
                        action = "PRODUCTION_ORDER_REJECTED_BY_RISK"
                    else:
                        action = "PRODUCTION_ORDER_REJECTED"
                await self._publish_production_order_result(intent, result_payload)
                if self._production_redis_write_allowed():
                    try:
                        await self._write_production_redis(intent, result_payload)
                    except Exception:
                        pass
                return ExecutionDecision(
                    asset=intent.asset,
                    action=action,
                    shadow_only=self.shadow_only,
                    place_orders=self.place_orders,
                    reason=str(result_payload.get("reason") or result_payload.get("status") or action),
                    intent=intent,
                )
            # Authority unavailable -> fail closed
            self.refused_order_count += 1
            self.refused_production_order_count += 1
            self.last_error = "production authority unavailable: risk plan could not be built"
            result_payload = OrderPlacementResult(
                accepted=False,
                status="AUTHORITY_UNAVAILABLE",
                reason=self.last_error,
                asset=intent.asset,
                side=intent.side,
                testnet=False,
                production=True,
            ).to_dict()
            self.last_order_result = result_payload
            await self._publish_production_order_result(intent, result_payload)
            return ExecutionDecision(
                asset=intent.asset,
                action="PRODUCTION_ORDER_FAILED",
                shadow_only=self.shadow_only,
                place_orders=self.place_orders,
                reason=self.last_error,
                intent=intent,
            )
        if self.order_client is None:
            self.refused_order_count += 1
            self.refused_production_order_count += 1
            self.last_error = "execution production order client is unavailable"
            return ExecutionDecision(
                asset=intent.asset,
                action="PRODUCTION_ORDER_FAILED",
                shadow_only=self.shadow_only,
                place_orders=self.place_orders,
                reason=self.last_error,
                intent=intent,
            )

        try:
            result = self.order_client.place_order(intent)
            if asyncio.iscoroutine(result):
                result = await result
        except Exception as exc:
            self.refused_order_count += 1
            self.refused_production_order_count += 1
            self.last_error = str(exc)
            result_payload = OrderPlacementResult(
                accepted=False,
                status="FAILED",
                reason=str(exc),
                asset=intent.asset,
                side=intent.side,
                testnet=False,
                production=True,
            ).to_dict()
            self.last_order_result = result_payload
            await self._publish_production_order_result(intent, result_payload)
            return ExecutionDecision(
                asset=intent.asset,
                action="PRODUCTION_ORDER_FAILED",
                shadow_only=self.shadow_only,
                place_orders=self.place_orders,
                reason=str(exc),
                intent=intent,
            )

        result_payload = self._order_result_to_dict(result, intent)
        result_payload["testnet"] = False
        result_payload["production"] = True
        self.last_order_result = result_payload
        if self._order_result_succeeded(result_payload):
            self.placed_order_count += 1
            self.real_order_submit_count += 1
            self.real_production_order_submit_count += 1
            action = "PRODUCTION_ORDER_SUBMITTED"
        else:
            self.refused_order_count += 1
            self.refused_production_order_count += 1
            action = "PRODUCTION_ORDER_REJECTED"
        await self._publish_production_order_result(intent, result_payload)
        if self._production_redis_write_allowed():
            await self._write_production_redis(intent, result_payload)
        return ExecutionDecision(
            asset=intent.asset,
            action=action,
            shadow_only=self.shadow_only,
            place_orders=self.place_orders,
            reason=str(result_payload.get("reason") or result_payload.get("status") or action),
            intent=intent,
        )

    async def _acquire_production_locks(self, intent: ShadowOrderIntent) -> Optional[str]:
        checks: list[tuple[str, str, int]] = []
        if intent.command_id:
            checks.append(("command", self.idempotency.command_key(intent.command_id), self.idempotency.ttl_sec))
        if intent.signal_id:
            checks.append(("signal", self.idempotency.signal_key(intent.signal_id), self.idempotency.ttl_sec))
        if intent.client_order_id:
            checks.append(("order", self.idempotency.order_key(intent.client_order_id), self.idempotency.ttl_sec))
        checks.append(("symbol", self.idempotency.symbol_key(intent.asset), self.symbol_lock_ttl_sec))
        for name, key, ttl in checks:
            result = await self.idempotency.acquire_key(key, ttl_sec=ttl)
            if result.duplicate:
                return f"duplicate production {name} lock/idempotency hit: {key}"
        return None

    def _record_safety_reject(self, intent: ShadowOrderIntent, safety: ExecutionSafetyDecision) -> ExecutionDecision:
        self.rejected_order_count += 1
        self.refused_order_count += 1
        self.safety_guard_reject_count += 1
        self.skipped_event_count += 1
        reason = f"{TESTNET_ORDER_REFUSAL}: {safety.reason}"
        self.last_reject_reason = reason
        self.last_error = reason
        return ExecutionDecision(
            asset=intent.asset,
            action="REJECT_ORDER",
            shadow_only=self.shadow_only,
            place_orders=self.place_orders,
            reason=reason,
            intent=intent,
        )

    def _record_production_safety_reject(self, intent: ShadowOrderIntent, safety: ExecutionSafetyDecision) -> ExecutionDecision:
        self.rejected_order_count += 1
        self.refused_order_count += 1
        self.refused_production_order_count += 1
        self.safety_guard_reject_count += 1
        self.skipped_event_count += 1
        reason = f"{PRODUCTION_ORDER_REFUSAL}: {safety.reason}"
        self.last_reject_reason = reason
        self.last_error = reason
        self.last_production_client_order_id = intent.client_order_id
        self.last_production_asset = intent.asset
        return ExecutionDecision(
            asset=intent.asset,
            action="REJECT_PRODUCTION_ORDER",
            shadow_only=self.shadow_only,
            place_orders=self.place_orders,
            reason=reason,
            intent=intent,
        )

    async def _publish_testnet_order_result(self, intent: ShadowOrderIntent, result_payload: Mapping[str, Any]) -> None:
        payload = dict(result_payload)
        payload.setdefault("asset", intent.asset)
        payload.setdefault("symbol", intent.asset)
        payload["event_type"] = "ShadowTestnetOrderResult"
        payload["shadow"] = True
        payload["testnet"] = True
        kind = "order_submitted" if self._order_result_succeeded(payload) else "order_rejected"
        await self._publish_shadow_event("testnet_order", payload, asset=intent.asset)
        await self._publish_shadow_event(kind, payload, asset=intent.asset)
        self.publish_testnet_result_count += 1

    async def _publish_production_order_result(self, intent: ShadowOrderIntent, result_payload: Mapping[str, Any]) -> None:
        payload = dict(result_payload)
        payload.setdefault("asset", intent.asset)
        payload.setdefault("symbol", intent.asset)
        payload["shadow"] = False
        payload["production"] = True
        payload["testnet"] = False
        kind = "order_submitted" if self._order_result_succeeded(payload) else "order_rejected"
        await self._publish_production_event(kind, payload, asset=intent.asset)
        self.publish_production_result_count += 1

    def _safety_config(self, *, intent: Optional[ShadowOrderIntent] = None, payload: Optional[Mapping[str, Any]] = None) -> dict[str, Any]:
        payload = payload or {}
        quantity = _payload_float(payload, ("quantity", "qty", "order_qty", "amount"))
        price = _payload_float(payload, ("price", "mark_price", "entry_price"))
        notional = _payload_float(payload, ("notional_usdt", "notional", "order_notional_usdt", "order_notional"))
        if notional is None and quantity is not None and price is not None:
            notional = abs(quantity * price)
        return {
            "enabled": self.enabled,
            "shadow_only": self.shadow_only,
            "accept_signals": self.accept_signals,
            "place_orders": self.place_orders,
            "testnet_only": self.testnet_only,
            "arm_testnet_orders": self.arm_testnet_orders,
            "order_client_type": self.order_client_type,
            "testnet_base_url": self.testnet_base_url,
            "BINANCE_FUTURES_REST_URL": self.testnet_base_url,
            "order_mode": self.order_mode,
            "require_testnet_url": self.require_testnet_url,
            "allow_mainnet": self.allow_mainnet,
            "binance_futures_testnet": self.binance_futures_testnet,
            "api_key": os.getenv("BINANCE_API_KEY", ""),
            "api_secret": os.getenv("BINANCE_API_SECRET", ""),
            "order_type": getattr(intent, "order_type", payload.get("order_type", "MARKET")) if intent else payload.get("order_type", "MARKET"),
            "max_notional_usdt": self.max_notional_usdt,
            "max_order_qty": self.max_order_qty,
            "allowed_symbols": self.allowed_symbols,
            "require_monolith_execution_disabled": self.require_monolith_execution_disabled,
            "monolith_execution_enabled": self.monolith_execution_enabled,
            "asset": intent.asset if intent else payload.get("asset") or payload.get("symbol"),
            "side": _normalize_execution_side(intent.side if intent else payload.get("side") or payload.get("signal") or payload.get("direction") or ""),
            "quantity": quantity if quantity is not None else (getattr(intent, "quantity", None) if intent else None),
            "notional_usdt": notional,
        }

    def _production_safety_config(self, *, intent: Optional[ShadowOrderIntent] = None, payload: Optional[Mapping[str, Any]] = None, kill_switch_active: bool = False) -> dict[str, Any]:
        cfg = self._safety_config(intent=intent, payload=payload)
        payload = payload or {}
        quantity = _payload_float(payload, ("quantity", "qty", "order_qty", "amount"))
        price = _payload_float(payload, ("price", "mark_price", "entry_price"))
        notional = _payload_float(payload, ("notional_usdt", "notional", "order_notional_usdt", "order_notional"))
        if notional is None and quantity is not None and price is not None:
            notional = abs(quantity * price)
        cfg.update(
            {
                "arm_production_orders": self.arm_production_orders,
                "production_cutover_enabled": self.production_cutover_enabled,
                "confirm_real_money_orders": self.confirm_real_money_orders,
                "order_mode": self.order_mode,
                "order_client_type": self.order_client_type,
                "require_production_url": self.require_production_url,
                "require_monolith_execution_disabled": self.require_monolith_execution_disabled,
                "analysis_publish_production_signals": self.analysis_publish_production_signals,
                "kill_switch_active": kill_switch_active,
                "production": payload.get("production"),
                "risk_ack": payload.get("risk_ack"),
                "client_order_id": getattr(intent, "client_order_id", None) if intent else payload.get("client_order_id"),
                "signal_id": getattr(intent, "signal_id", None) if intent else payload.get("signal_id"),
                "reduce_only": getattr(intent, "reduce_only", False) if intent else payload.get("reduce_only"),
                "max_production_notional_usdt": self.max_production_notional_usdt,
                "notional_usdt": notional,
                "BINANCE_FUTURES_REST_URL": self.rest_url,
                "binance_futures_testnet": self.binance_futures_testnet,
                "BINANCE_API_KEY": os.getenv("BINANCE_API_KEY", ""),
                "BINANCE_API_SECRET": os.getenv("BINANCE_API_SECRET", ""),
            }
        )
        return cfg

    @staticmethod
    def _order_result_to_dict(result: Any, intent: ShadowOrderIntent) -> dict[str, Any]:
        if hasattr(result, "to_dict") and callable(result.to_dict):
            payload = result.to_dict()
        elif isinstance(result, Mapping):
            payload = dict(result)
        else:
            payload = {
                "accepted": False,
                "status": "FAILED",
                "reason": f"unsupported order client result type: {type(result).__name__}",
            }
        payload.setdefault("asset", intent.asset)
        payload.setdefault("side", intent.side)
        payload.setdefault("testnet", True)
        return payload

    @staticmethod
    def _order_result_succeeded(result_payload: Mapping[str, Any]) -> bool:
        status = str(result_payload.get("status") or "").upper()
        if status in {"FAILED", "REJECTED", "ERROR", "NOT_IMPLEMENTED"}:
            return False
        return bool(result_payload.get("accepted"))

    async def _write_shadow_redis(self, intent: ShadowOrderIntent, decision: ExecutionDecision) -> None:
        if self.redis_client is None:
            return
        key = f"{SHADOW_REDIS_KEY_PREFIX}intent:{intent.asset}"
        if not key.startswith(SHADOW_REDIS_KEY_PREFIX) or key.startswith(PRODUCTION_REDIS_KEY_PREFIXES):
            raise RuntimeError("refusing to write execution data to a production Redis key")
        writer = getattr(self.redis_client, "set", None)
        if not callable(writer):
            raise RuntimeError("redis client does not provide set()")
        result = writer(key, str(decision.to_dict()))
        if asyncio.iscoroutine(result):
            await result
        self.redis_write_count += 1

    async def _write_production_redis(self, intent: ShadowOrderIntent, result_payload: Mapping[str, Any]) -> None:
        if self.redis_client is None:
            return
        writer = getattr(self.redis_client, "set", None)
        if not callable(writer):
            raise RuntimeError("redis client does not provide set()")
        client_order_id = str(intent.client_order_id or "").strip()
        if not client_order_id:
            raise RuntimeError("refusing production Redis write without client_order_id")
        writes = {
            f"{PRODUCTION_EXECUTION_REDIS_PREFIX}order:{client_order_id}": dict(result_payload),
            f"{PRODUCTION_EXECUTION_REDIS_PREFIX}last:{intent.asset}": dict(result_payload),
            f"{PRODUCTION_EXECUTION_REDIS_PREFIX}health": self.health_snapshot(),
        }
        for key, payload in writes.items():
            if not key.startswith(PRODUCTION_EXECUTION_REDIS_PREFIX):
                raise RuntimeError("refusing to write production execution data outside controlled namespace")
            result = writer(key, str(payload))
            if asyncio.iscoroutine(result):
                await result
            self.redis_write_count += 1

    async def _kill_switch_active(self) -> bool:
        if not self.kill_switch_enabled:
            return False
        if self.kill_switch_file and Path(self.kill_switch_file).exists():
            return True
        if self.redis_client is None:
            return False
        getter = getattr(self.redis_client, "get", None)
        if callable(getter):
            value = getter(self.kill_switch_redis_key)
            if asyncio.iscoroutine(value):
                value = await value
            return _as_bool(value, False)
        store = getattr(self.redis_client, "store", None)
        if isinstance(store, Mapping):
            return _as_bool(store.get(self.kill_switch_redis_key), False)
        return False

    def _kill_switch_active_sync(self) -> bool:
        if not self.kill_switch_enabled:
            return False
        if self.kill_switch_file and Path(self.kill_switch_file).exists():
            return True
        store = getattr(self.redis_client, "store", None)
        if isinstance(store, Mapping):
            return _as_bool(store.get(self.kill_switch_redis_key), False)
        return False


    def _production_health_guard_config(self) -> dict[str, Any]:
        asset = self.allowed_symbols[0] if self.allowed_symbols else "BTCUSDT"
        max_qty = self.max_order_qty if self.max_order_qty > 0 else 0.001
        max_notional = self.max_production_notional_usdt if self.max_production_notional_usdt > 0 else 10.0
        qty = min(max_qty, 0.001)
        notional = min(max_notional, 10.0)
        return self._production_safety_config(
            intent=ShadowOrderIntent(
                asset=asset,
                side="BUY",
                signal_id="health-check-signal",
                client_order_id="health-check-client-order",
                order_type="MARKET",
                quantity=qty,
            ),
            payload={
                "production": True,
                "risk_ack": True,
                "asset": asset,
                "side": "BUY",
                "signal_id": "health-check-signal",
                "client_order_id": "health-check-client-order",
                "order_type": "MARKET",
                "quantity": qty,
                "notional_usdt": notional,
            },
            kill_switch_active=self._kill_switch_active_sync(),
        )

    def health_snapshot(self) -> dict[str, Any]:
        health = ExecutionHealth(
            enabled=self.enabled,
            shadow_only=self.shadow_only,
            accept_signals=self.accept_signals,
            place_orders=self.place_orders,
            write_redis=self.write_redis,
            received_command_count=self.received_command_count,
            parsed_command_count=self.parsed_command_count,
            received_signal_count=self.received_signal_count,
            parsed_signal_count=self.parsed_signal_count,
            duplicate_signal_count=self.duplicate_signal_count,
            shadow_intent_count=self.shadow_intent_count,
            placed_order_count=self.placed_order_count,
            published_event_count=self.published_event_count,
            redis_write_count=self.redis_write_count,
            skipped_event_count=self.skipped_event_count,
            last_command_at=self.last_command_at,
            last_signal_at=self.last_signal_at,
            last_error=self.last_error,
        ).to_dict()
        health["monolith_execution_enabled"] = self.monolith_execution_enabled
        health["rejected_order_count"] = self.rejected_order_count
        health["testnet_order_attempt_count"] = self.testnet_order_attempt_count
        health["testnet_order_success_count"] = self.testnet_order_success_count
        health["testnet_order_failed_count"] = self.testnet_order_failed_count
        health["safety_guard_reject_count"] = self.safety_guard_reject_count
        health["order_client_type"] = self.order_client_type
        health["order_mode"] = self.order_mode
        health["require_testnet_url"] = self.require_testnet_url
        health["allow_mainnet"] = self.allow_mainnet
        health["binance_futures_testnet"] = self.binance_futures_testnet
        health["rest_url_host"] = urlparse(self.rest_url).hostname
        health["real_testnet_order_enabled"] = validate_execution_runtime_mode(self._safety_config()).allowed and self.order_client_type != "mock"
        health["mock_order_mode"] = self.order_client_type == "mock"
        health["testnet_order_mode"] = self.order_mode == "testnet"
        health["testnet_only"] = self.testnet_only
        health["arm_testnet_orders"] = self.arm_testnet_orders
        health["max_notional_usdt"] = self.max_notional_usdt
        health["max_order_qty"] = self.max_order_qty
        health["allowed_symbols"] = list(self.allowed_symbols)
        health["last_reject_reason"] = self.last_reject_reason
        health["last_order_result"] = self.last_order_result
        health["last_signal_id"] = self.last_signal_id
        health["last_duplicate_signal_id"] = self.last_duplicate_signal_id
        health["last_mock_order_id"] = self.last_mock_order_id
        health["received_intent_count"] = self.received_intent_count
        health["parsed_intent_count"] = self.parsed_intent_count
        health["order_place_attempt_count"] = self.order_place_attempt_count
        health["real_order_submit_count"] = self.real_order_submit_count
        health["mock_order_submit_count"] = self.mock_order_submit_count
        health["refused_order_count"] = self.refused_order_count
        health["duplicate_order_count"] = self.duplicate_order_count
        health["idempotency_hit_count"] = self.idempotency_hit_count
        health["publish_testnet_result_count"] = self.publish_testnet_result_count
        health["last_client_order_id"] = self.last_client_order_id
        health["last_asset"] = self.last_asset
        health["last_published_subject"] = self.last_published_subject
        health["production_cutover_enabled"] = self.production_cutover_enabled
        health["arm_production_orders"] = self.arm_production_orders
        health["require_production_url"] = self.require_production_url
        health["real_production_order_enabled"] = validate_production_execution_runtime_mode(self._production_health_guard_config()).allowed
        health["kill_switch_active"] = self._kill_switch_active_sync()
        health["kill_switch_file"] = self.kill_switch_file
        health["kill_switch_redis_key"] = self.kill_switch_redis_key
        health["received_production_intent_count"] = self.received_production_intent_count
        health["parsed_production_intent_count"] = self.parsed_production_intent_count
        health["production_order_attempt_count"] = self.production_order_attempt_count
        health["real_production_order_submit_count"] = self.real_production_order_submit_count
        health["refused_production_order_count"] = self.refused_production_order_count
        health["duplicate_production_order_count"] = self.duplicate_production_order_count
        health["production_idempotency_hit_count"] = self.production_idempotency_hit_count
        health["publish_production_result_count"] = self.publish_production_result_count
        health["last_production_client_order_id"] = self.last_production_client_order_id
        health["last_production_asset"] = self.last_production_asset
        health["allow_legacy_signal_subjects"] = self.allow_legacy_signal_subjects
        health["publish_legacy_execution_events"] = self.publish_legacy_execution_events
        health["analysis_publish_production_signals"] = self.analysis_publish_production_signals
        health["authoritative_testnet_calls"] = getattr(self, "_authoritative_testnet_calls", 0)
        health["authoritative_production_calls"] = getattr(self, "_authoritative_production_calls", 0)
        health["last_risk_plan"] = getattr(self, "last_risk_plan", None)
        health["last_authority_result"] = getattr(self, "last_authority_result", None)
        health["authority_mode"] = self._should_use_authority()
        # Expose readiness without forcing creation side-effects in health check
        health["risk_manager_ready"] = getattr(self, "_risk_manager", None) is not None
        health["order_manager_ready"] = getattr(self, "_order_manager", None) is not None
        health["consumer"] = self.consumer.health_snapshot()
        return health

    async def write_health_snapshot(self) -> None:
        if not self.write_redis:
            return
        if self.redis_client is None:
            import redis
            self.redis_client = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)
        key = "trading:latest:service_health:execution_service"
        payload = dict(self.health_snapshot())
        payload.update({"event_type": "ExecutionHealth", "source": "execution_service", "service": "execution_service", "timestamp": time.time()})
        writer = getattr(self.redis_client, "set", None)
        if not callable(writer):
            raise RuntimeError("redis client does not provide set()")
        result = writer(key, json.dumps(payload, sort_keys=True, default=str))
        if asyncio.iscoroutine(result):
            await result
        self.redis_write_count += 1

    async def publish_health(self) -> None:
        await self._publish_shadow_event("health", self.health_snapshot())
        await self.write_health_snapshot()
        if self.production_cutover_enabled:
            await self._publish_production_event("health", self.health_snapshot())

    async def run_forever(self) -> None:
        if not self.enabled:
            logging.info("Execution service disabled by EXECUTION_SERVICE_ENABLED=false.")
            return
        if self.order_mode != "production" and refuse_mainnet_ordering(self._safety_config()):
            self.last_error = TESTNET_ORDER_REFUSAL
            logging.error("Execution service startup refused: %s", self.last_error)
            return
        await self.consumer.start(self.handle_event)
        while True:
            await asyncio.sleep(30.0)
            logging.info("Execution service health: %s", self.health_snapshot())
            await self.publish_health()


async def _run(_shutdown: Any) -> None:
    service = ExecutionService()
    await service.run_forever()


def main() -> None:
    bootstrap_logging("execution_service")
    async_main(_run, service_name="execution_service")


if __name__ == "__main__":
    main()
