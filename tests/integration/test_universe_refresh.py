import json
from datetime import timedelta

from tests.universe_fixtures import NOW, payload, symbol
from tradeforge.market.exchange_info import ExchangeInfoClient, Reply
from tradeforge.market.instrument_registry import InstrumentRegistry
from tradeforge.market.universe_refresh import UniverseRefresher


class Feed:
    def __init__(self, *replies):
        self.now = NOW
        self.replies = iter(replies)
        self.requests = []

    def send(self, headers, timeout):
        self.requests.append(dict(headers))
        result = next(self.replies)
        if isinstance(result, Exception):
            raise result
        return result

    def refresher(self, db, **kwargs):
        client = ExchangeInfoClient(
            transport=self.send, clock=lambda: self.now.timestamp(), attempts=1
        )
        return UniverseRefresher(InstrumentRegistry(db), client, clock=lambda: self.now, **kwargs)


def response(data=None, etag='"v1"'):
    return Reply(200, {"ETag": etag} if etag else {}, json.dumps(data or payload()).encode())


def test_periodic_refresh_uses_etag_and_304_renews_durable_ttl(migrated):
    feed = Feed(response(), Reply(304, {}, b""))
    worker = feed.refresher(migrated, interval=timedelta(minutes=15), ttl=timedelta(hours=1))
    first = worker.tick()
    assert first.ready
    feed.now += timedelta(minutes=14)
    assert worker.tick().ready and len(feed.requests) == 1
    feed.now += timedelta(minutes=1)
    renewed = worker.tick()
    assert renewed.ready and renewed.observation.observed_at == feed.now
    assert feed.requests == [{}, {"If-None-Match": '"v1"'}]
    assert first.observation.version_hash != renewed.observation.version_hash
    assert InstrumentRegistry(migrated).view(feed.now).ready


def test_wrong_response_alarms_and_blocks_immediately_and_after_restart(migrated, caplog):
    feed = Feed(response(), Reply(200, {}, b'{"symbols":[]}'), response(payload(symbol("NEWUSDT"))))
    worker = feed.refresher(migrated)
    assert worker.tick().ready
    feed.now += timedelta(minutes=15)
    failed = worker.tick()
    assert not failed.ready and failed.reason == "UNIVERSE_INVALID_METADATA"
    assert "UNIVERSE_INVALID_METADATA" in caplog.text
    assert not InstrumentRegistry(migrated).view(feed.now).ready
    # Restart doesn't restore the old ETag or skip the failure.
    feed.now += timedelta(seconds=30)
    recovered = feed.refresher(migrated).tick()
    assert recovered.ready and recovered.observation.universe.eligible[0].symbol == "NEWUSDT"
    assert feed.requests[-1] == {}


def test_304_rechecks_delivery_date_instead_of_keeping_delisted_symbol(migrated):
    delivery = NOW + timedelta(minutes=10)
    feed = Feed(
        response(payload(symbol(deliveryDate=int(delivery.timestamp() * 1000)))),
        Reply(304, {}, b""),
    )
    worker = feed.refresher(migrated)
    assert worker.tick().ready
    feed.now = delivery
    view = worker.tick()
    assert not view.ready and view.eligible == 0 and view.excluded == 1


def test_transport_failure_cannot_use_previous_universe_during_ttl(migrated):
    feed = Feed(response(), TimeoutError("sensitive network detail"))
    worker = feed.refresher(migrated)
    assert worker.tick().ready
    feed.now += timedelta(minutes=15)
    view = worker.tick()
    assert not view.ready and view.reason == "UNIVERSE_NETWORK_ERROR"


def test_expired_metadata_triggers_refresh_before_long_interval(migrated):
    feed = Feed(response(), response(payload(symbol("NEWUSDT"))))
    worker = feed.refresher(migrated, interval=timedelta(days=1), ttl=timedelta(minutes=2))
    worker.tick()
    feed.now += timedelta(minutes=2)
    assert worker.tick().observation.universe.eligible[0].symbol == "NEWUSDT"


def test_304_without_cached_observation_fails_closed(migrated):
    feed = Feed(Reply(304, {}, b""))
    assert not feed.refresher(migrated).tick().ready


def test_long_or_unsafe_response_etag_is_not_reused(migrated):
    feed = Feed(response(etag="x\r\nheader: injected"), response())
    worker = feed.refresher(migrated)
    assert worker.tick().ready
    feed.now += timedelta(minutes=15)
    assert worker.tick().ready and feed.requests[-1] == {}


def test_restart_revalidates_before_trusting_prior_cache(migrated):
    feed = Feed(response(), Reply(200, {}, b'{"symbols":[]}'))
    assert feed.refresher(migrated).tick().ready
    feed.now += timedelta(seconds=1)
    # A prior DB-write failure may be impossible to persist. Startup must revalidate.
    view = feed.refresher(migrated).tick()
    assert not view.ready and len(feed.requests) == 2


def test_rate_limit_cooldown_survives_refresher_restart(migrated):
    feed = Feed(Reply(429, {"Retry-After": "120"}, b""), response())
    assert not feed.refresher(migrated).tick().ready
    feed.now += timedelta(seconds=30)
    restarted = feed.refresher(migrated)
    assert not restarted.tick().ready and len(feed.requests) == 1
    feed.now += timedelta(seconds=90)
    assert restarted.tick().ready and len(feed.requests) == 2
