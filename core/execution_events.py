"""Side-effect-free shadow execution models for the future execution service.

These models intentionally do not reuse production Trader/FuturesExecution or
TradeResult contracts. They are safe DTOs for parsing commands/signals and for
shadow-only health/intent snapshots.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import time
from typing import Any, Mapping, Optional

COMMAND_TYPES = {"start", "stop", "reconnect", "restart"}
PRODUCTION_EXECUTION_EVENT_TYPES = {"FuturesExecution", "TradeResult", "Balance", "TraderStatus"}


@dataclass(frozen=True)
class ShadowOrderIntent:
    asset: str
    side: str = "WAIT"
    signal_id: Optional[str] = None
    command_id: Optional[str] = None
    client_order_id: Optional[str] = None
    order_type: str = "MARKET"
    quantity: Optional[float] = None
    price: Optional[float] = None
    reduce_only: bool = False
    source_event_type: str = "Signal"
    reason: str = "shadow_execution_intent_only"
    observed_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["shadow"] = True
        payload["event_type"] = "ShadowOrderIntent"
        return payload


@dataclass(frozen=True)
class ExecutionCommandState:
    command: str
    command_id: Optional[str] = None
    accepted: bool = True
    reason: str = "shadow_command_state_only"
    observed_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["shadow"] = True
        payload["event_type"] = "ExecutionCommandState"
        return payload


@dataclass(frozen=True)
class ExecutionDecision:
    asset: str
    action: str = "SKIP_ORDER"
    shadow_only: bool = True
    place_orders: bool = False
    duplicate: bool = False
    reason: str = "shadow_only_no_order_placement"
    intent: ShadowOrderIntent | None = None
    observed_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["shadow"] = True
        payload["event_type"] = "ExecutionDecision"
        if self.intent is not None:
            payload["intent"] = self.intent.to_dict()
        return payload


@dataclass(frozen=True)
class ExecutionHealth:
    enabled: bool
    shadow_only: bool
    accept_signals: bool
    place_orders: bool
    write_redis: bool
    received_command_count: int = 0
    parsed_command_count: int = 0
    received_signal_count: int = 0
    parsed_signal_count: int = 0
    duplicate_signal_count: int = 0
    shadow_intent_count: int = 0
    placed_order_count: int = 0
    published_event_count: int = 0
    redis_write_count: int = 0
    skipped_event_count: int = 0
    last_command_at: Optional[float] = None
    last_signal_at: Optional[float] = None
    last_error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_execution_command(payload: Mapping[str, Any]) -> ExecutionCommandState:
    if not isinstance(payload, Mapping):
        raise ValueError("execution command payload is not a mapping")
    command = str(payload.get("command") or payload.get("action") or payload.get("type") or "").strip().lower()
    if command not in COMMAND_TYPES:
        raise ValueError(f"unsupported execution command: {command or '<empty>'}")
    command_id = payload.get("command_id") or payload.get("id")
    return ExecutionCommandState(command=command, command_id=str(command_id) if command_id else None)


def normalize_shadow_order_intent(event_type: str, payload: Mapping[str, Any], *, subject: str = "") -> ShadowOrderIntent:
    if not isinstance(payload, Mapping):
        raise ValueError("execution signal payload is not a mapping")
    asset = str(payload.get("asset") or payload.get("symbol") or payload.get("pair") or "").upper().strip()
    if not asset:
        parts = [part for part in str(subject or "").split(".") if part]
        asset = parts[-1].upper() if len(parts) >= 3 else ""
    if not asset:
        raise ValueError("execution signal payload missing asset/symbol")
    signal = str(payload.get("signal") or payload.get("side") or payload.get("action") or "WAIT").upper().strip()
    # PHASE-04: canonical strategy directions LONG/SHORT must be consumable as execution intents.
    # Map LONG->BUY, SHORT->SELL while preserving BUY/SELL/CALL/PUT compat.
    if signal in {"LONG"}:
        signal = "BUY"
    elif signal in {"SHORT"}:
        signal = "SELL"
    side = signal if signal in {"BUY", "SELL", "CALL", "PUT", "WAIT"} else "WAIT"
    signal_id = payload.get("signal_id") or payload.get("id") or payload.get("event_id")
    command_id = payload.get("command_id")
    client_order_id = payload.get("client_order_id")
    order_type = str(payload.get("order_type") or payload.get("type") or "MARKET").upper().strip()
    def _float(name):
        v = payload.get(name)
        if v is None or str(v).strip() == "":
            return None
        return float(v)
    quantity = _float("quantity") or _float("qty") or _float("amount")
    price = _float("price")
    reduce_only = str(payload.get("reduce_only", "false")).lower() in {"1", "true", "yes", "on"}
    return ShadowOrderIntent(
        asset=asset,
        side=side,
        signal_id=str(signal_id) if signal_id else None,
        command_id=str(command_id) if command_id else None,
        client_order_id=str(client_order_id) if client_order_id else None,
        order_type=order_type,
        quantity=quantity,
        price=price,
        reduce_only=reduce_only,
        source_event_type=str(event_type or "Signal"),
    )
