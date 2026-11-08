"""The actual canonical worker must operate with the deployed market role."""

import asyncio
from types import SimpleNamespace

from sqlalchemy import event, text

from tests.market_fixtures import bar, instrument
from tradeforge.domain.market import Timeframe
from tradeforge.market.canonical_feed import CanonicalFeed
from tradeforge.persistence.db import Database
from tradeforge.runtime.container import grant_runtime_access


def test_canonical_cycle_persists_as_market_without_feature_write_authority(migrated):
    from apps.market_gateway import runtime

    with migrated.transaction() as c:
        for role in ("tf_market", "tf_quant", "tf_dashboard", "tf_guardian"):
            c.execute(text(f"CREATE ROLE {role} NOLOGIN"))
        grant_runtime_access(c, migrated.sql_engine.url.database)
    market = Database(migrated.sql_engine.url)

    @event.listens_for(market.sql_engine, "connect")
    def use_market(connection, record):
        with connection.cursor() as cursor:
            cursor.execute("SET ROLE tf_market")
        connection.commit()

    sample = bar()
    feed = CanonicalFeed(clock=lambda: sample.as_of)
    feed.configure((instrument(),), ())
    feed.state.ingest_bar(sample)
    feed._dirty.add(("BTCUSDT", Timeframe.M15))
    gateway = SimpleNamespace(canonical=feed, clock=lambda: sample.as_of)
    try:
        asyncio.run(runtime.canonical_cycle(gateway, market, None, require_admission=False))
        assert feed.storage_ready, feed.storage_error
        with migrated.transaction() as c:
            assert c.scalar(text("SELECT count(*) FROM closed_bar_observations")) == 1
            assert c.scalar(text("SELECT count(*) FROM feature_snapshots")) == 0
            assert not c.scalar(
                text("SELECT has_table_privilege('tf_market','feature_snapshots','INSERT')")
            )
    finally:
        market.close()
        with migrated.transaction() as c:
            for role in ("tf_market", "tf_quant", "tf_dashboard", "tf_guardian"):
                c.execute(text(f"DROP OWNED BY {role}"))
                c.execute(text(f"DROP ROLE {role}"))
