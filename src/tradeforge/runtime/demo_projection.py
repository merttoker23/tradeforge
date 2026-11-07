"""GET-only Demo evidence publication on a bounded start-to-start cadence."""

import asyncio
import time
from typing import Any

from tradeforge.execution.binance_client import BinanceTestnetClient
from tradeforge.execution.preflight import preflight
from tradeforge.execution.storage import audit
from tradeforge.persistence.db import Database, PersistenceError


async def project_demo(db: Database, client: BinanceTestnetClient) -> None:
    def publish(state: dict[str, Any]) -> None:
        with db.transaction() as connection:
            audit(connection, "demo.preflight.v1", state)

    while True:
        started = time.monotonic()
        state = await asyncio.to_thread(preflight, client, terminal=True)
        state["guardian"] = "RUNNING"
        state["orders_enabled"] = False  # Diagnostics never authorize orders.
        try:
            await asyncio.to_thread(publish, state)
        except PersistenceError:
            pass
        # Keep the source TTL intact; fetching must not add drift to every period.
        await asyncio.sleep(max(1, 20 - (time.monotonic() - started)))
