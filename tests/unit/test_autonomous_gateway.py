"""The gateway hands off bounded RAM data; never tick/book database history."""

from datetime import timedelta
from types import SimpleNamespace

from tests.unit.test_momentum_alpha import evidence


def test_memory_payload_ages_without_renewing_price_evidence():
    from tradeforge.runtime.market_link import memory_payload

    sample = evidence()
    at = [sample.as_of]
    gateway = SimpleNamespace(
        clock=lambda: at[0],
        _latest={},
        _generations={},
        stale_after=15,
        eligible={sample.instrument.symbol},
        canonical=SimpleNamespace(books={}, flows={}),
        status=lambda: {"data_ready": True},
        clock_sync=SimpleNamespace(ready=lambda *a: True),
        monotonic=lambda: 0,
    )
    persistence = SimpleNamespace(
        rest_samples=(sample.market,), markets=lambda *a: (), display_quotes=lambda *a: ()
    )
    assert len(memory_payload(gateway, persistence)["markets"]) == 1
    at[0] += timedelta(seconds=16)
    assert memory_payload(gateway, persistence)["markets"] == []


def test_handoff_builds_only_one_bounded_book_per_ready_symbol():
    from tradeforge.domain.market import StreamState
    from tradeforge.runtime.market_link import memory_payload

    sample = evidence()
    calls = []
    readiness = []

    def snapshot(at, *, limit=None):
        assert limit == 20, "Full-depth model validation must not run on the handoff path"
        calls.append(at)
        return sample.book

    def ready(symbol, at):
        readiness.append(symbol)
        return True

    canonical = SimpleNamespace(
        books={
            sample.instrument.symbol: SimpleNamespace(
                state_at=lambda at: StreamState.READY, snapshot=snapshot
            )
        },
        flows={sample.instrument.symbol: SimpleNamespace(snapshot=lambda at: sample.flow)},
        symbol_ready=ready,
        storage_ready=True,
        admission_ready=True,
    )
    gateway = SimpleNamespace(
        clock=lambda: sample.as_of,
        _latest={},
        _generations={},
        stale_after=15,
        eligible={sample.instrument.symbol},
        canonical=canonical,
        status=lambda: {"data_ready": True},
        clock_sync=SimpleNamespace(ready=lambda *a: True),
        monotonic=lambda: 0,
    )
    persistence = SimpleNamespace(
        rest_samples=(sample.market,), markets=lambda *a: (), display_quotes=lambda *a: ()
    )
    payload = memory_payload(gateway, persistence)
    assert len(payload["deep"]) == 1
    assert len(calls) == 1 and readiness == [sample.instrument.symbol]
