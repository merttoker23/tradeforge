"""Sampled public I/O workers. Market ingestion never blocks on PostgreSQL."""

import asyncio
import time
from collections.abc import Callable, Collection, Iterator
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import text

from tradeforge.domain.common import DataQuality
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.domain.market import MarketQuote, MarketSnapshot, Timeframe
from tradeforge.intelligence.models import RegimePolicy
from tradeforge.market.chart_history import fetch_chart, save_chart
from tradeforge.market.exchange_info import FetchError, milliseconds
from tradeforge.market.market_gateway import MarketGateway
from tradeforge.market.market_state import MarketState
from tradeforge.market.recovery import MarketRecovery, PublicMarketClient
from tradeforge.market.socket_manager import pause
from tradeforge.market.storage import save_view
from tradeforge.market.streams import GLOBAL_STREAMS, decimal_field
from tradeforge.persistence.db import Database
from tradeforge.runtime.storage import save_runtime


def _merge_fresh[Sample: MarketQuote | MarketSnapshot](
    old: tuple[Sample, ...], new: tuple[Sample, ...], eligible: Collection[str], at: datetime
) -> tuple[Sample, ...]:
    members: dict[str, Sample] = {}
    for sample in (*old, *new):
        if sample.symbol not in eligible or not 0 <= (at - sample.source_at).total_seconds() <= 15:
            continue
        previous = members.get(sample.symbol)
        if isinstance(previous, MarketSnapshot) and isinstance(sample, MarketSnapshot):
            members[sample.symbol] = cast(Sample, _merge_market(previous, sample))
        elif previous is None or sample.source_at >= previous.source_at:
            members[sample.symbol] = sample
    return tuple(members[s] for s in sorted(members))


def _merge_market(old: MarketSnapshot, new: MarketSnapshot) -> MarketSnapshot:
    """Join independently newer evidence without renewing any source timestamp."""
    book = max((old, new), key=lambda m: m.book_source_at or m.source_at)
    mark = max((old, new), key=lambda m: m.mark_source_at or m.source_at)
    volumes = [m for m in (old, new) if m.volume is not None]
    volume = max(volumes, key=lambda m: m.volume_source_at or m.source_at) if volumes else None
    book_at, mark_at = book.book_source_at or book.source_at, mark.mark_source_at or mark.source_at
    components = (book, mark, *((volume,) if volume else ()))
    return MarketSnapshot.model_validate(
        {
            **new.model_dump(),
            "event_id": uuid5(
                NAMESPACE_URL,
                f"market-merge/{book.event_id}/{mark.event_id}/{volume.event_id if volume else ''}",
            ),
            "source_at": min(book_at, mark_at),
            "observed_at": max(m.observed_at for m in components),
            "as_of": max(m.as_of for m in components),
            "best_bid": book.best_bid,
            "best_ask": book.best_ask,
            "mark_price": mark.mark_price,
            "index_price": mark.index_price,
            "funding": mark.funding,
            "volume": volume.volume if volume else None,
            "book_source_at": book_at,
            "mark_source_at": mark_at,
            "volume_source_at": (volume.volume_source_at or volume.source_at) if volume else None,
        }
    )


class GatewayPersistence:
    def __init__(
        self, gateway: MarketGateway, db: Database, *, require_admission: bool = False
    ) -> None:
        self.gateway, self.db = gateway, db
        self.require_admission = require_admission
        self.history_complete = False
        self.history_reason = "HISTORY_STARTING"
        self.history_period: datetime | None = None
        self.history_progress: dict[str, int | float | str | None] = {}
        self._history_started = 0.0
        self.storage_ready = False
        self.rest_samples: tuple[MarketSnapshot, ...] = ()
        self.rest_quote_samples: tuple[MarketQuote, ...] = ()
        self.last_chart: tuple[str, str, datetime] | None = None
        self.chart_demand: dict[tuple[str, str], datetime] = {}
        self.completed_charts: dict[tuple[str, str], datetime] = {}

    def request_chart(self, symbol: str, timeframe: str) -> None:
        """Bounded public data demand, no commands, credentials or DB writes."""
        if symbol not in self.gateway.eligible or timeframe not in {"1m", "5m", "15m", "1h", "4h"}:
            raise ValueError("CHART_QUERY_INVALID")
        now = self.gateway.clock()
        self.chart_demand = {
            key: at for key, at in self.chart_demand.items() if 0 <= (now - at).total_seconds() < 30
        }
        self.completed_charts = {
            key: end for key, end in self.completed_charts.items() if key in self.chart_demand
        }
        key = (symbol, timeframe)
        if key not in self.chart_demand and len(self.chart_demand) >= 4:
            raise ValueError("CHART_RATE_LIMITED")
        self.chart_demand.setdefault(key, now)

    async def chart_once(self, client: PublicMarketClient) -> bool:
        if self.require_admission:
            from tradeforge.runtime.maintenance import observation_admission

            if not await asyncio.to_thread(observation_admission, self.db):
                return False
        now = self.gateway.clock()
        if not self.gateway.view.ready or not self.gateway.clock_sync.ready(
            now, self.gateway.monotonic()
        ):
            return False

        def load() -> object:
            with self.db.transaction() as c:
                return c.scalar(
                    text(
                        "SELECT payload FROM audit_log WHERE event_type='dashboard.chart.v1' "
                        "AND observed_at BETWEEN :since AND :at "
                        "ORDER BY observed_at DESC,audit_id DESC LIMIT 1"
                    ),
                    {"since": now - timedelta(seconds=30), "at": now},
                )

        demand = next(
            (
                key
                for key, at in reversed(self.chart_demand.items())
                if 0 <= (now - at).total_seconds() < 30
                and self.completed_charts.get(key)
                != datetime.fromtimestamp(
                    int(now.timestamp() - 2)
                    // Timeframe(key[1]).seconds
                    * Timeframe(key[1]).seconds,
                    UTC,
                )
            ),
            None,
        )
        body = (
            {"symbol": demand[0], "timeframe": demand[1]}
            if demand
            else await asyncio.to_thread(load)
        )
        if not isinstance(body, dict) or set(body) != {"symbol", "timeframe"}:
            return False
        timeframe = Timeframe(body["timeframe"])
        if timeframe not in {Timeframe.M1, Timeframe.M5, Timeframe.M15, Timeframe.H1, Timeframe.H4}:
            return False
        observation = self.gateway.view.observation
        if observation is None or observation.universe is None:
            return False
        key = next(
            (member for member in observation.universe.eligible if member.symbol == body["symbol"]),
            None,
        )
        if key is None:
            return False
        end = datetime.fromtimestamp(
            int(now.timestamp() - 2) // timeframe.seconds * timeframe.seconds, UTC
        )
        identity = (key.symbol, timeframe.value, end)
        if self.last_chart == identity:
            if demand is not None:
                self.completed_charts[demand] = end
            return True

        def valid() -> bool:
            return key.symbol in self.gateway.eligible and self.gateway.clock_sync.ready(
                self.gateway.clock(), self.gateway.monotonic()
            )

        view = await fetch_chart(client, key, timeframe, end)
        if not valid():
            return False
        await asyncio.to_thread(save_chart, self.db, view)
        self.last_chart = identity
        if demand is not None:
            self.completed_charts[demand] = end
        return True

    async def charts(self, stop: asyncio.Event) -> None:
        async with PublicMarketClient(
            min_interval=1, environment=getattr(self.gateway, "environment", "live")
        ) as client:
            while not stop.is_set():
                try:
                    await self.chart_once(client)
                except Exception:
                    pass  # Missing chart history never becomes decision or execution evidence.
                await pause(stop, 5)

    async def telemetry(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            at = self.gateway.clock()
            payload, deadline = self.gateway.telemetry()
            payload.update(
                **self.history_progress,
                history_elapsed_ms=(
                    round((time.perf_counter() - self._history_started) * 1000, 3)
                    if self._history_started
                    else 0.0
                ),
                history_complete=self.history_complete,
                history_reason=self.history_reason,
                history_period=self.history_period.isoformat() if self.history_period else None,
                market_environment=(
                    "BINANCE_FUTURES_TESTNET"
                    if getattr(self.gateway, "environment", "live") == "testnet"
                    else "BINANCE_MAINNET_PUBLIC"
                ),
                rest_market_covered=sum(
                    0 <= (at - s.source_at).total_seconds() <= 15 for s in self.rest_samples
                ),
                market_source="WS_AND_REST_SEPARATE",
                canonical_storage_ready=self.gateway.canonical.storage_ready,
                canonical_storage_error=self.gateway.canonical.storage_error,
                storage_admission_ready=self.gateway.canonical.admission_ready,
            )
            try:
                await asyncio.to_thread(
                    save_runtime, self.db, "market_gateway", at, deadline, payload
                )
                self.storage_ready = True
            except Exception:
                self.storage_ready = False
            await pause(stop, 2)

    async def history(self, stop: asyncio.Event) -> None:
        # Network latency overlaps, but every attempt and retry shares the same
        # four-starts/second schedule. Only four symbols are held in flight.
        async with PublicMarketClient(
            min_interval=0.25,
            max_concurrency=4,
            environment=getattr(self.gateway, "environment", "live"),
        ) as client:
            completed: set[str] = set()
            attempted: set[str] = set()
            failed: set[str] = set()
            last_complete: dict[str, datetime] = {}
            cycle: tuple[datetime, str] | None = None
            while not stop.is_set():
                if self.require_admission:
                    from tradeforge.runtime.maintenance import observation_admission

                    if not await asyncio.to_thread(observation_admission, self.db):
                        self.history_complete = False
                        self.history_reason = "STORAGE_UNAVAILABLE"
                        await pause(stop, 10)
                        continue
                if not self.gateway.clock_sync.ready(
                    self.gateway.clock(), self.gateway.monotonic()
                ):
                    self.history_complete = False
                    self.history_reason = "CLOCK_UNSAFE"
                    await pause(stop, 2)
                    continue
                observation = self.gateway.view.observation
                if (
                    not self.gateway.view.ready
                    or observation is None
                    or observation.universe is None
                ):
                    self.history_complete = False
                    self.history_reason = "UNIVERSE_UNAVAILABLE"
                    await pause(stop, 2)
                    continue
                universe = observation.universe
                now = self.gateway.clock()
                # Clock error is <=1s; wait another second beyond that bound.
                end = datetime.fromtimestamp(int(now.timestamp() - 2) // 900 * 900, UTC)
                if cycle != (end, observation.version_hash):
                    cycle = end, observation.version_hash
                    completed.clear()
                    attempted.clear()
                    failed.clear()
                    self.history_complete = False
                    self.history_period = end
                    self._history_started = time.perf_counter()
                    self.history_progress = {
                        "history_total": len(universe.eligible),
                        "history_completed": 0,
                        "history_attempted": 0,
                        "history_failed": 0,
                        "history_pending": len(universe.eligible),
                        "history_inflight": 0,
                        "history_last_completed_symbol": None,
                        "history_last_failure_symbol": None,
                        "history_last_member_ms": 0.0,
                        "history_last_failure_at": None,
                        "history_last_failure_reason": None,
                    }
                if len(completed) == len(universe.eligible):
                    self.history_complete = True
                    self.history_reason = "HISTORY_SYNCED"
                    await pause(stop, 2)
                    continue
                self.history_reason = (
                    "HISTORY_PARTIAL" if self.history_complete else "HISTORY_SYNCING"
                )
                expected_version = observation.version_hash

                def valid(expected_version: str = expected_version, end: datetime = end) -> bool:
                    current = self.gateway.view.observation
                    current_end = datetime.fromtimestamp(
                        int(self.gateway.clock().timestamp() - 2) // 900 * 900, UTC
                    )
                    return bool(
                        self.gateway.view.ready
                        and current is not None
                        and current.version_hash == expected_version
                        and current_end == end
                        and self.gateway.clock_sync.ready(
                            self.gateway.clock(), self.gateway.monotonic()
                        )
                    )

                async def recover(
                    key: InstrumentKey,
                    end: datetime = end,
                    total: int = len(universe.eligible),
                    valid: Callable[[], bool] = valid,
                ) -> None:
                    started = time.perf_counter()
                    state = MarketState(capacity=33)
                    recovery = MarketRecovery(state, client)
                    self.history_progress["history_inflight"] = (
                        int(self.history_progress["history_inflight"] or 0) + 1
                    )
                    reason = "HISTORY_RECOVERY_INCOMPLETE"
                    try:
                        start = max(
                            last_complete.get(key.symbol, end - timedelta(minutes=15 * 33)),
                            end - timedelta(minutes=15 * 33),
                        )
                        async with asyncio.timeout(12):
                            success = (start == end and valid()) or await recovery.bars(
                                key, Timeframe.M15, start, end, valid=valid
                            )
                        if success and valid():
                            if start != end:
                                await asyncio.to_thread(
                                    save_view,
                                    self.db,
                                    state.view(key, Timeframe.M15, self.gateway.clock()),
                                    persist_features=False,
                                )
                            if valid():
                                completed.add(key.symbol)
                                last_complete[key.symbol] = end
                                failed.discard(key.symbol)
                                self.history_progress["history_last_completed_symbol"] = key.symbol
                                return
                        if not valid():
                            reason = "HISTORY_CYCLE_CHANGED"
                    except TimeoutError:
                        reason = "HISTORY_RECOVERY_TIMEOUT"
                    except Exception:
                        reason = "HISTORY_STORAGE_OR_INPUT_UNAVAILABLE"
                    finally:
                        attempted.add(key.symbol)
                        self.history_progress.update(
                            history_inflight=int(self.history_progress["history_inflight"] or 0)
                            - 1,
                            history_completed=len(completed),
                            history_attempted=len(attempted),
                            history_pending=total - len(completed),
                            history_last_member_ms=round((time.perf_counter() - started) * 1000, 3),
                        )
                    failed.add(key.symbol)
                    self.history_progress.update(
                        history_failed=len(failed),
                        history_last_failure_at=self.gateway.clock().isoformat(),
                        history_last_failure_reason=reason,
                        history_last_failure_symbol=key.symbol,
                    )

                # Give every member a first attempt before retrying failed ones.
                pending = [k for k in universe.eligible if k.symbol not in completed]
                unattempted = [k for k in pending if k.symbol not in attempted]
                members = iter(unattempted or pending)

                async def recover_pending(members: Iterator[InstrumentKey]) -> None:
                    # Reuse a free slot immediately; one slow symbol must not
                    # hold an entire batch. The shared REST client still paces
                    # every request/retry and caps network concurrency at four.
                    for key in members:
                        if stop.is_set() or not valid():
                            return
                        await recover(key)

                async with asyncio.TaskGroup() as group:
                    for _ in range(4):
                        group.create_task(recover_pending(members))
                self.history_progress["history_failed"] = len(failed)
                if not valid():
                    self.history_complete = False
                    self.history_reason = "HISTORY_CYCLE_CHANGED"
                    continue
                self.history_complete = len(attempted) == len(universe.eligible)
                if self.history_complete:
                    self.history_reason = "HISTORY_PARTIAL" if failed else "HISTORY_SYNCED"
                await pause(stop, 30 if self.history_complete and failed else 0.25)

    def display_quotes(self, at: datetime) -> tuple[MarketQuote, ...]:
        gateway = self.gateway
        observation = gateway.view.observation
        if (
            not gateway.clock_sync.ready(at, gateway.monotonic())
            or not gateway.view.ready
            or observation is None
            or not observation.observed_at <= at < observation.expires_at
        ):
            return ()
        result = {
            q.symbol: q
            for q in self.rest_quote_samples
            if q.symbol in gateway.eligible
            and 0 <= (at - q.source_at).total_seconds() <= gateway.stale_after
        }
        for (name, symbol), (event, _) in gateway._latest.items():
            if name != GLOBAL_STREAMS[1] or not gateway.fresh(name, symbol):
                continue
            ticker = gateway._latest.get((GLOBAL_STREAMS[0], symbol))
            if ticker and (
                gateway._generations.get(GLOBAL_STREAMS[0]) != ticker[0].generation
                or ticker[0].event_time > event.received_at
            ):
                ticker = None
            quote = MarketQuote(
                symbol=symbol,
                exchange="BINANCE",
                market_type="USD_M",
                quote="USDT",
                margin_asset="USDT",
                contract_type="PERPETUAL",
                schema_version=1,
                event_id=uuid5(
                    NAMESPACE_URL, f"mark/{symbol}/{event.generation}/{event.event_time}"
                ),
                source_at=event.event_time,
                observed_at=event.received_at,
                as_of=at,
                mark_price=event.values["p"],
                volume=ticker[0].values["v"] if ticker else None,
                volume_source_at=ticker[0].event_time if ticker else None,
                source="binance:ws-mark",
                environment=(
                    "testnet"
                    if getattr(self.gateway, "environment", "live") == "testnet"
                    else "mainnet"
                ),
            )
            previous = result.get(symbol)
            if previous is None or quote.source_at >= previous.source_at:
                result[symbol] = quote
        return tuple(result[s] for s in sorted(result))

    def rest_quotes(self, marks: object, tickers: object, at: datetime) -> tuple[MarketQuote, ...]:
        if not isinstance(marks, list) or len(marks) > 100_000:
            return ()
        volumes = (
            {row.get("symbol"): row for row in tickers if isinstance(row, dict)}
            if isinstance(tickers, list) and len(tickers) <= 100_000
            else {}
        )
        result = []
        for row in marks:
            try:
                if not isinstance(row, dict) or row.get("symbol") not in self.gateway.eligible:
                    continue
                source = milliseconds(row["time"])
                if not 0 <= (at - source).total_seconds() <= self.gateway.stale_after:
                    continue
                volume, volume_at = None, None
                ticker = volumes.get(row["symbol"])
                if ticker is not None:
                    try:
                        stamp = milliseconds(ticker["closeTime"])
                        if stamp <= at:
                            volume, volume_at = decimal_field(ticker, "volume"), stamp
                    except (KeyError, TypeError, ValueError):
                        pass
                result.append(
                    MarketQuote(
                        symbol=row["symbol"],
                        exchange="BINANCE",
                        market_type="USD_M",
                        quote="USDT",
                        margin_asset="USDT",
                        contract_type="PERPETUAL",
                        schema_version=1,
                        event_id=uuid5(
                            NAMESPACE_URL, f"rest-mark/{row['symbol']}/{source.isoformat()}"
                        ),
                        source_at=source,
                        observed_at=at,
                        as_of=at,
                        mark_price=decimal_field(row, "markPrice"),
                        volume=volume,
                        volume_source_at=volume_at,
                        source="binance:rest-mark",
                        environment=(
                            "testnet"
                            if getattr(self.gateway, "environment", "live") == "testnet"
                            else "mainnet"
                        ),
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
        return tuple(result)

    async def quote_once(self, client: PublicMarketClient) -> bool:
        gateway = self.gateway
        at = gateway.clock()
        observation = gateway.view.observation
        if (
            not gateway.clock_sync.ready(at, gateway.monotonic())
            or not gateway.view.ready
            or observation is None
            or not observation.observed_at <= at < observation.expires_at
            or gateway.plan is None
        ):
            return False
        selected = {s.symbol for group in gateway.plan.groups for s in group if s.symbol}
        rest_fresh = {
            s.symbol
            for s in self.rest_samples
            if s.symbol in gateway.eligible
            and 0 <= (at - s.source_at).total_seconds() <= max(0, gateway.stale_after - 5)
        }
        # Regime classification requires broad spread/funding evidence before
        # ranking can select a watchlist. Waiting for selected symbols here
        # deadlocks cold startup at STRESS_DATA_MISSING.
        strict_missing = len(rest_fresh) < len(
            gateway.eligible
        ) * RegimePolicy().minimum_coverage or any(
            s not in rest_fresh
            and not all(gateway.fresh(n, s) for n in (*GLOBAL_STREAMS, f"{s.lower()}@bookTicker"))
            for s in selected
        )
        if not strict_missing and all(
            gateway.fresh(GLOBAL_STREAMS[1], s) for s in gateway.eligible
        ):
            return False
        epoch, eligible, version = gateway._epoch, gateway.eligible, observation.version_hash

        def valid() -> bool:
            now = gateway.clock()
            current = gateway.view.observation
            return (
                gateway._epoch == epoch
                and gateway.eligible == eligible
                and gateway.view.ready
                and current is not None
                and current.version_hash == version
                and current.observed_at <= now < current.expires_at
                and gateway.clock_sync.ready(now, gateway.monotonic())
            )

        async def fetch_marks() -> tuple[object, datetime]:
            marks, received = await asyncio.wait_for(client.fetch("/fapi/v1/premiumIndex", {}), 8)
            if valid():
                # Publish usable marks even if an independent cost endpoint fails.
                self.rest_quote_samples = _merge_fresh(
                    self.rest_quote_samples,
                    self.rest_quotes(marks, [], received),
                    eligible,
                    gateway.clock(),
                )
            return marks, received

        self.rest_samples = _merge_fresh(self.rest_samples, (), eligible, at)
        self.rest_quote_samples = _merge_fresh(self.rest_quote_samples, (), eligible, at)
        if not strict_missing:
            await fetch_marks()
            return valid()
        results = await asyncio.gather(
            fetch_marks(),
            asyncio.wait_for(client.fetch("/fapi/v1/ticker/24hr", {}), 8),
            asyncio.wait_for(client.fetch("/fapi/v1/ticker/bookTicker", {}), 8),
            return_exceptions=True,
        )
        mark_result, ticker_result, book_result = results
        for result in (mark_result, book_result):
            if isinstance(result, BaseException):
                raise result
        # Narrow each endpoint independently: no failed or malformed batch can
        # erase another member's unexpired proof or renew its original timestamp.
        assert not isinstance(mark_result, BaseException)
        assert not isinstance(book_result, BaseException)
        marks, mark_at = mark_result
        rows, book_at = book_result
        # Volume transport failure must not discard independent cost evidence.
        tickers, ticker_at = (
            ([], max(mark_at, book_at))
            if isinstance(ticker_result, BaseException)
            else ticker_result
        )
        if not valid() or not isinstance(rows, list) or len(rows) > 100_000:
            return False
        received = max(mark_at, ticker_at, book_at)
        self.rest_quote_samples = _merge_fresh(
            self.rest_quote_samples,
            self.rest_quotes(marks, tickers, received),
            eligible,
            gateway.clock(),
        )
        self.rest_samples = _merge_fresh(
            self.rest_samples,
            self.rest_markets(rows, marks, tickers, received),
            eligible,
            gateway.clock(),
        )
        return True

    async def quotes(self, stop: asyncio.Event) -> None:
        async with PublicMarketClient(
            min_interval=1,
            max_concurrency=3,
            environment=getattr(self.gateway, "environment", "live"),
        ) as client:
            while not stop.is_set():
                try:
                    self.gateway.quote_attempts += 1
                    if await self.quote_once(client):
                        self.gateway.quote_last_reason = "OK"
                except Exception as error:
                    # Never publish transport bodies/exception text or renew old evidence.
                    self.gateway.quote_failures += 1
                    self.gateway.quote_last_failure_at = self.gateway.clock().isoformat()
                    self.gateway.quote_last_reason = (
                        "QUOTE_RATE_LIMITED"
                        if isinstance(error, FetchError) and str(error) == "MARKET_RATE_LIMITED"
                        else "QUOTE_TIMEOUT"
                        if isinstance(error, TimeoutError)
                        else "QUOTE_UNAVAILABLE"
                    )
                await pause(stop, 2)

    def rest_markets(
        self, quotes: list[object], marks: object, tickers: object, at: datetime
    ) -> tuple[MarketSnapshot, ...]:
        if (
            not isinstance(marks, list)
            or len(quotes) > 100_000
            or len(marks) > 100_000
            or (isinstance(tickers, list) and len(tickers) > 100_000)
            or not self.gateway.clock_sync.ready(self.gateway.clock(), self.gateway.monotonic())
        ):
            return ()
        mark_map = {x.get("symbol"): x for x in marks if isinstance(x, dict)}
        ticker_map = (
            {x.get("symbol"): x for x in tickers if isinstance(x, dict)}
            if isinstance(tickers, list)
            else {}
        )
        result = []
        for quote in quotes:
            try:
                if not isinstance(quote, dict) or quote.get("symbol") not in self.gateway.eligible:
                    continue
                symbol = str(quote["symbol"])
                mark = mark_map[symbol]
                timestamps = (milliseconds(quote["time"]), milliseconds(mark["time"]))
                if any(not 0 <= (at - stamp).total_seconds() <= 15 for stamp in timestamps):
                    continue
                source = min(timestamps)
                volume, volume_at = None, None
                try:
                    ticker = ticker_map[symbol]
                    ticker_at = milliseconds(ticker["closeTime"])
                    ticker_volume = decimal_field(ticker, "volume")
                    if ticker_at <= at:
                        volume, volume_at = ticker_volume, ticker_at
                except (KeyError, TypeError, ValueError):
                    pass  # A missing ticker cannot erase valid spread/funding.
                result.append(
                    MarketSnapshot(
                        symbol=symbol,
                        exchange="BINANCE",
                        market_type="USD_M",
                        quote="USDT",
                        margin_asset="USDT",
                        contract_type="PERPETUAL",
                        schema_version=1,
                        event_id=uuid5(NAMESPACE_URL, f"public-rest/{symbol}/{at.isoformat()}"),
                        source_at=source,
                        observed_at=at,
                        as_of=at,
                        environment=(
                            "testnet"
                            if getattr(self.gateway, "environment", "live") == "testnet"
                            else "mainnet"
                        ),
                        source="binance:rest",
                        best_bid=decimal_field(quote, "bidPrice"),
                        best_ask=decimal_field(quote, "askPrice"),
                        mark_price=decimal_field(mark, "markPrice"),
                        index_price=decimal_field(mark, "indexPrice"),
                        funding=decimal_field(mark, "lastFundingRate", signed=True),
                        volume=volume,
                        book_source_at=timestamps[0],
                        mark_source_at=timestamps[1],
                        volume_source_at=volume_at,
                        quality=DataQuality.OK,
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
        return tuple(result)

    def markets(self, rows: list[object], at: datetime) -> tuple[MarketSnapshot, ...]:
        if not self.gateway.clock_sync.ready(self.gateway.clock(), self.gateway.monotonic()):
            return ()
        result = []
        for row in rows:
            if not isinstance(row, dict) or row.get("symbol") not in self.gateway.eligible:
                continue
            symbol = str(row["symbol"])
            ticker = self.gateway._latest.get((GLOBAL_STREAMS[0], symbol))
            mark = self.gateway._latest.get((GLOBAL_STREAMS[1], symbol))
            if mark is None:
                continue
            try:
                event = mark[0]
                book_at, mark_at = milliseconds(row.get("time")), event.event_time
                source = min(book_at, mark_at)
                if (
                    any(
                        not 0 <= (at - stamp).total_seconds() <= self.gateway.stale_after
                        for stamp in (book_at, mark_at)
                    )
                    or self.gateway._generations.get(event.stream.name) != event.generation
                ):
                    continue
                volume, volume_at = None, None
                if ticker is not None:
                    ticker_event = ticker[0]
                    if (
                        self.gateway._generations.get(ticker_event.stream.name)
                        == ticker_event.generation
                        and ticker_event.event_time <= at
                    ):
                        try:
                            ticker_volume = decimal_field({"v": str(ticker_event.values["v"])}, "v")
                            volume, volume_at = ticker_volume, ticker_event.event_time
                        except (ValueError, KeyError, TypeError):
                            pass  # Invalid volume cannot erase independent costs.
                key = InstrumentKey(
                    symbol=symbol,
                    exchange="BINANCE",
                    market_type="USD_M",
                    quote="USDT",
                    margin_asset="USDT",
                    contract_type="PERPETUAL",
                )
                result.append(
                    MarketSnapshot(
                        **key.model_dump(),
                        schema_version=1,
                        event_id=uuid5(NAMESPACE_URL, f"public/{symbol}/{at.isoformat()}"),
                        source_at=source,
                        observed_at=at,
                        as_of=at,
                        best_bid=decimal_field(row, "bidPrice"),
                        best_ask=decimal_field(row, "askPrice"),
                        mark_price=event.values["p"],
                        index_price=event.values["i"],
                        funding=event.values["r"],
                        volume=volume,
                        book_source_at=book_at,
                        mark_source_at=mark_at,
                        volume_source_at=volume_at,
                        quality=DataQuality.OK,
                        environment=(
                            "testnet"
                            if getattr(self.gateway, "environment", "live") == "testnet"
                            else "mainnet"
                        ),
                        source="binance:ws-and-rest",
                    )
                )
            except (ValueError, KeyError, TypeError):
                continue
        return tuple(result)
