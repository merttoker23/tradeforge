"""Bounded dispatch telemetry. Exception payloads never become public diagnostics."""

import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from aiohttp import ClientError

from tradeforge.execution.broker import AckUnknown, ExchangeRejected
from tradeforge.persistence.db import EntryHalt, PersistenceError

SAFE_REASONS = frozenset(
    {
        "ACCOUNT_RECONCILIATION_REQUIRED",
        "PUBLIC_MARKET_RECOVERY_REQUIRED",
        "ACCOUNT_EVIDENCE_STALE",
        "UNKNOWN_EXCHANGE_ORDER",
        "FLAT_ACCOUNT_REQUIRED",
        "INCOME_HISTORY_INCOMPLETE",
        "INCOME_HISTORY_INVALID",
        "RISK_FACTOR_UNAVAILABLE",
        "DB_UNAVAILABLE",
        "DB_STATEMENT_TIMEOUT",
        "DB_LOCK_TIMEOUT",
        "DB_PERMISSION_DENIED",
        "DB_DEADLOCK",
        "DB_POOL_TIMEOUT",
        "DB_CONSTRAINT_REJECTED",
        "MARKET_HANDOFF_UNAVAILABLE",
        "MARKET_HANDOFF_INVALID",
        "MARKET_HANDOFF_CAPACITY",
        "CLOCK_UNSAFE",
        "PRIVATE_STREAM_NOT_READY",
        "UNRECONCILED_EXCHANGE_ORDER",
        "TESTNET_TRIAL_LIMIT",
        "TESTNET_ENVIRONMENT_MISMATCH",
    }
)


def safe_failure(error: Exception) -> str:
    message = str(error)
    if message in SAFE_REASONS:
        return message
    if isinstance(error, AckUnknown):
        return "ACK_UNKNOWN_RECONCILIATION_REQUIRED"
    if isinstance(error, ExchangeRejected):
        return "EXCHANGE_REJECTED"
    if isinstance(error, EntryHalt):
        return "ENTRY_HALTED"
    if isinstance(error, PersistenceError):
        return "STORAGE_UNAVAILABLE"
    if isinstance(error, (ClientError, TimeoutError, OSError)):
        return "TRANSPORT_UNAVAILABLE"
    return "INPUT_INVALID"


class RuntimeProgress:
    def __init__(
        self,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.clock, self.monotonic = clock, monotonic
        self.started = monotonic()
        self.last_mono = float("-inf")
        self.state: dict[str, Any] = {
            "alive": True,
            "healthy": True,
            "live": False,
            "data_ready": False,
            "analysis_ready": False,
            "execution_ready": False,
            "orders_enabled": False,
            "stage": "STARTING",
            "reason": "STARTING",
            "last_progress_at": None,
            "last_success_at": None,
            "last_analysis_at": None,
            "last_analysis_source_at": None,
            "last_evaluation_at": None,
            "last_error": None,
            "last_error_at": None,
            "last_error_stage": None,
            "error_count": 0,
            "cycle_count": 0,
        }

    def advance(self, stage: str) -> None:
        self.state["stage"] = stage
        self.state["last_progress_at"] = self.clock().isoformat()
        self.last_mono = self.monotonic()

    def complete(
        self,
        *,
        data_ready: bool,
        analysis_ready: bool,
        execution_ready: bool,
        reason: str,
    ) -> None:
        self.advance("DISPATCH_COMPLETE")
        self.state.update(
            healthy=True,
            data_ready=data_ready,
            analysis_ready=analysis_ready,
            execution_ready=execution_ready,
            orders_enabled=execution_ready,
            reason=reason,
            last_success_at=self.clock().isoformat(),
            cycle_count=self.state["cycle_count"] + 1,
            cycle_ms=round((self.monotonic() - self.started) * 1000, 3),
        )
        self.started = self.monotonic()

    def fail(self, error: Exception) -> None:
        reason = safe_failure(error)
        self.state.update(
            data_ready=False,
            analysis_ready=False,
            execution_ready=False,
            orders_enabled=False,
            reason=reason,
            last_error=reason,
            last_error_at=self.clock().isoformat(),
            last_error_stage=self.state["stage"],
            error_count=self.state["error_count"] + 1,
        )

    def snapshot(self) -> dict[str, Any]:
        state = dict(self.state)
        if not 0 <= self.monotonic() - self.last_mono <= 15:
            state.update(
                data_ready=False,
                analysis_ready=False,
                execution_ready=False,
                orders_enabled=False,
                reason="DISPATCH_STALE",
            )
        return state
