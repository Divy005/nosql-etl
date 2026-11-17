"""Base engine interface and shared data structures for all ETL execution backends."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class BatchStats:
    """Ingestion statistics for a single batch of log records."""

    record_count: int  # Total lines received in this batch
    parsed_count: int  # Successfully parsed records
    malformed_count: int  # Records that failed the parsing regex
    # Sample malformed records for inspection: list of (line_no, raw_line), capped at 5
    malformed_samples: list[tuple[int, str]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.parsed_count + self.malformed_count != self.record_count:
            raise ValueError(
                f"BatchStats invariant violated: parsed ({self.parsed_count}) + "
                f"malformed ({self.malformed_count}) != record_count ({self.record_count})"
            )


@dataclass
class QueryResult:
    """Results and execution timing for an analytical query (q1, q2, or q3)."""

    query_id: str  # 'q1' | 'q2' | 'q3'
    rows: list[dict]  # Output rows matching Contract 3 dictionary keys
    runtime_sec: float  # In-engine query execution time in seconds


@dataclass
class FinalResult:
    """Aggregated analytical results produced across all batches of a run."""

    q1: QueryResult
    q2: QueryResult
    q3: QueryResult


class Engine(ABC):
    """Abstract base class that all four execution backends must implement."""

    name: str  # 'mongodb' | 'hive' | 'pig' | 'mapreduce' | 'dummy'

    @abstractmethod
    def setup(self, run_id: int) -> None:
        """Initialize engine-side state/collections/tables for a new execution run.

        Called before the timer starts.
        """
        ...

    @abstractmethod
    def process_batch(
        self,
        run_id: int,
        batch_id: int,
        lines: list[str],
        first_line_no: int,
    ) -> BatchStats:
        """Ingest, parse, and store a batch of raw log lines inside the engine.

        The engine performs all regex parsing and derivation internally.
        Called within the ingestion timer.
        """
        ...

    @abstractmethod
    def finalize(self, run_id: int) -> FinalResult:
        """Execute Q1, Q2, and Q3 queries inside the engine over all accumulated batches.

        Called within the aggregation timer.
        """
        ...

    def cleanup(self, run_id: int) -> None:
        """Drop temporary tables/collections or intermediate files.

        Optional; called after the timer stops.
        """
        pass
