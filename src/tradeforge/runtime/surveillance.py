"""Apply existing hysteresis/position-priority allocation to live subscriptions."""

import asyncio

from sqlalchemy import text

from tradeforge.domain.instrument import InstrumentKey
from tradeforge.intelligence.models import FeatureSnapshot
from tradeforge.intelligence.opportunity import SurveillancePolicy
from tradeforge.intelligence.subscription_allocator import SubscriptionAllocator
from tradeforge.market.bars import canonical
from tradeforge.market.market_gateway import MarketGateway
from tradeforge.market.socket_manager import pause
from tradeforge.persistence.db import Database


class RuntimeSurveillance:
    def __init__(self, gateway: MarketGateway, db: Database) -> None:
        self.gateway, self.db = gateway, db
        # Applying a plan restarts socket groups. Leave time for both 60s flow
        # windows to warm before the next discretionary rebalance.
        self.allocator = SubscriptionAllocator(
            SurveillancePolicy(rebalance_seconds=300, dwell_seconds=600)
        )
        self.watch: tuple[str, ...] = ()
        self.deep: tuple[str, ...] = ()
        self.protected: tuple[str, ...] = ()

    def selection(self) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
        return self.watch, self.deep, self.protected

    def tick(self) -> None:
        at = self.gateway.clock()
        with self.db.transaction() as c:
            payload = c.scalar(
                text(
                    "SELECT payload FROM regime_snapshots WHERE as_of<=:at "
                    "ORDER BY as_of DESC LIMIT 1"
                ),
                {"at": at},
            )
            rows = (
                c.execute(
                    text(
                        "SELECT i.symbol FROM positions p JOIN instruments i "
                        "USING(instrument_id) WHERE p.qty>0"
                    )
                )
                .scalars()
                .all()
            )
        positions = tuple(
            InstrumentKey(
                symbol=s,
                exchange="BINANCE",
                market_type="USD_M",
                quote="USDT",
                margin_asset="USDT",
                contract_type="PERPETUAL",
            )
            for s in rows
        )
        self.protected = tuple(k.symbol for k in positions)
        if payload is None:
            self.watch, self.deep = (), ()
            return
        features = FeatureSnapshot.model_validate_json(canonical(payload), context={"now": at})
        allocation = self.allocator.update(
            features,
            now=at,
            positions=positions,
            queue_pressure=float(len(self.gateway.queue) / self.gateway.queue.capacity),
        )
        self.watch = tuple(k.symbol for k in allocation.universe.watched)
        self.deep = tuple(k.symbol for k in allocation.universe.deep)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await asyncio.to_thread(self.tick)
            except Exception:
                self.watch, self.deep = (), ()
            await pause(stop, 30)
