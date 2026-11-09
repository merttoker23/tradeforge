"""A worker must not reuse an ETag from the previously active Binance environment."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

from tradeforge.market.exchange_info import Reply
from tradeforge.market.instrument_registry import RegistryView
from tradeforge.market.universe_refresh import UniverseRefresher


def test_new_worker_ignores_persisted_etag_on_first_metadata_fetch():
    now = datetime(2026, 11, 8, 12, tzinfo=UTC)
    old = SimpleNamespace(reason=None, expires_at=now + timedelta(minutes=30), etag='"live"')
    registry = Mock()
    registry.view.return_value = RegistryView(old, True, "UNIVERSE_READY")
    registry.record.return_value = SimpleNamespace(
        metadata=SimpleNamespace(weight_limit=200),
        observed_at=now,
        expires_at=now + timedelta(hours=1),
    )
    client = Mock()
    client.fetch.return_value = Reply(status=200, headers={}, body=b"{}")
    refresher = UniverseRefresher(registry, client, clock=lambda: now)

    refresher.tick()

    client.fetch.assert_called_once_with(None)
    registry.record.assert_called_once()
    client.set_weight_limit.assert_called_once_with(200)
