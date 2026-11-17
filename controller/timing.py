"""Simple timer helpers using time.perf_counter()."""

import time
from dataclasses import dataclass, field


class Timer:
    """A simple perf_counter-based timer that can be started and stopped."""

    def __init__(self) -> None:
        self._start: float | None = None
        self._stop: float | None = None

    def start(self) -> "Timer":
        self._start = time.perf_counter()
        self._stop = None
        return self

    def stop(self) -> float:
        if self._start is None:
            raise RuntimeError("Timer was never started")
        self._stop = time.perf_counter()
        return self.elapsed

    @property
    def elapsed(self) -> float:
        if self._start is None:
            return 0.0
        end = self._stop if self._stop is not None else time.perf_counter()
        return end - self._start


@dataclass
class TimingBreakdown:
    """Tracks the separate timing components of an ETL run."""

    total: float = 0.0
    ingest: float = 0.0
    aggregate: float = 0.0
    load: float = 0.0
    _ingest_parts: list[float] = field(default_factory=list)

    def add_ingest(self, seconds: float) -> None:
        """Accumulate a single batch's ingestion time."""
        self._ingest_parts.append(seconds)
        self.ingest = sum(self._ingest_parts)
