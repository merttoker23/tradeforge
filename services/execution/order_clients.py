"""Compatibility exports for execution-service order clients (PHASE-04).

Single Binance execution authority is engine/execution/futures_order_manager.py.
This wrapper remains only for test/import compatibility; it re-exports the
non-authoritative adapters (which intentionally refuse direct network placement)
and MockOrderClient for deterministic unit tests. No new network order path
is introduced here.
"""
from services.common.order_clients import (
    BinanceFuturesProductionOrderClient,
    BinanceFuturesTestnetOrderClient,
    MockOrderClient,
    OrderPlacementResult,
    build_order_client,
)

__all__ = [
    "BinanceFuturesProductionOrderClient",
    "BinanceFuturesTestnetOrderClient",
    "MockOrderClient",
    "OrderPlacementResult",
    "build_order_client",
]
