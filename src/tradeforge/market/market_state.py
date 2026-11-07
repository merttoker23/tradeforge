"""Point-in-time canonical bar memory. No DB or network I/O on ingestion."""

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime

from tradeforge.domain.common import DataQuality, utc
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.domain.market import ClosedBar, Timeframe
from tradeforge.market.bars import bar_hash, canonical, closed_kline, resample
from tradeforge.market.quality import bar_quality
from tradeforge.market.rolling_window import BarWindow
from tradeforge.market.streams import MarketEvent


@dataclass(frozen=True)
class BarView:
    instrument: InstrumentKey
    timeframe: Timeframe
    as_of: datetime
    bars: tuple[ClosedBar, ...]
    quality: DataQuality
    evidence: tuple[ClosedBar, ...] = ()

    @property
    def payload(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "instrument": self.instrument.model_dump(mode="json"),
            "timeframe": self.timeframe.value,
            "as_of": self.as_of.isoformat(),
            "quality": self.quality.value,
            "inputs": [bar_hash(b) for b in self.bars],
            "bars": [b.model_dump(mode="json") for b in self.bars],
            "provenance": [bar_hash(b) for b in self.evidence],
        }

    @property
    def digest(self) -> str:
        return hashlib.sha256(canonical(self.payload).encode()).hexdigest()


class MarketState:
    def __init__(self, capacity: int = 2048, *, max_series: int = 4096) -> None:
        BarWindow(capacity)
        if type(max_series) is not int or not 1 <= max_series <= 100_000:
            raise ValueError("INVALID_SERIES_BUDGET")
        self.capacity, self.max_series = capacity, max_series
        self._series: dict[tuple[str, ...], BarWindow] = {}
        self._failures: dict[tuple[str, ...], list[tuple[datetime, DataQuality]]] = {}

    def retain(self, keys: tuple[InstrumentKey, ...]) -> None:
        allowed = {key.key for key in keys}
        self._series = {k: v for k, v in self._series.items() if k[:-1] in allowed}
        self._failures = {k: v for k, v in self._failures.items() if k[:-1] in allowed}

    def mark(
        self,
        key: InstrumentKey,
        timeframe: Timeframe,
        quality: DataQuality,
        at: datetime | None = None,
    ) -> None:
        identity = (*key.key, timeframe.value)
        if identity not in self._series:
            if len(self._series) >= self.max_series:
                raise ValueError("SERIES_BUDGET_EXHAUSTED")
            self._series[identity] = BarWindow(self.capacity)
        history = self._failures.setdefault(identity, [])
        history.append((utc(at or datetime.now(UTC)), quality))
        history.sort(key=lambda pair: pair[0])
        if len(history) > self.capacity:
            del history[: -self.capacity]

    def ingest_bar(self, bar: ClosedBar) -> bool:
        if bar.key not in self._series:
            if len(self._series) >= self.max_series:
                raise ValueError("SERIES_BUDGET_EXHAUSTED")
            self._series[bar.key] = BarWindow(self.capacity)
        return self._series[bar.key].add(bar)

    def ingest(self, event: MarketEvent, key: InstrumentKey) -> bool:
        bar = closed_kline(event, key)
        return False if bar is None else self.ingest_bar(bar)

    def view(self, key: InstrumentKey, timeframe: Timeframe, as_of: datetime) -> BarView:
        utc(as_of)
        derived = timeframe in (Timeframe.H1, Timeframe.H4, Timeframe.H12)
        native = Timeframe.M15 if derived else timeframe
        series = self._series.get((*key.key, native.value))
        try:
            bars = series.at(as_of) if series else ()
        except ValueError:
            return BarView(key, timeframe, as_of, (), DataQuality.UNKNOWN)
        incomplete = False
        if derived:
            starts = sorted(
                {
                    datetime.fromtimestamp(
                        int(b.open_at.timestamp()) // timeframe.seconds * timeframe.seconds,
                        tz=b.open_at.tzinfo,
                    )
                    for b in bars
                }
            )
            output = []
            for opened in starts:
                result = resample(bars, timeframe, opened, as_of)
                if result is not None:
                    output.append(result)
                elif opened.timestamp() + timeframe.seconds <= as_of.timestamp() and not (
                    series is not None
                    and series.truncated
                    and bars
                    and opened < bars[0].open_at
                    and opened == starts[0]
                ):
                    incomplete = True
            bars = tuple(output)
        quality = DataQuality.GAP if incomplete else bar_quality(bars, timeframe, as_of)
        flags = [q for t, q in self._failures.get((*key.key, native.value), ()) if t <= as_of]
        if flags and flags[-1] is not DataQuality.OK:
            quality = flags[-1]
        return BarView(
            key, timeframe, as_of, bars, quality, series.evidence(as_of) if series else ()
        )
