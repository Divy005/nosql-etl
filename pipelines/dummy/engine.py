"""Mock execution engine for testing the controller, batcher, and database loader."""

import time

from pipelines.base import BatchStats, Engine, FinalResult, QueryResult


class DummyEngine(Engine):
    """A dummy engine that simulates batch ingestion and returns valid mock results."""

    name = "dummy"

    def __init__(self) -> None:
        self.total_lines: int = 0
        self.batches_received: int = 0

    def setup(self, run_id: int) -> None:
        self.total_lines = 0
        self.batches_received = 0

    def process_batch(
        self,
        run_id: int,
        batch_id: int,
        lines: list[str],
        first_line_no: int,
    ) -> BatchStats:
        time.sleep(0.001)  # Simulate minimal processing
        count = len(lines)
        self.total_lines += count
        self.batches_received += 1
        return BatchStats(
            record_count=count,
            parsed_count=count,
            malformed_count=0,
            malformed_samples=[],
        )

    def finalize(self, run_id: int) -> FinalResult:
        # Generate mock records conforming to Contract 3 schemas
        q1_rows = [
            {
                "log_date": "1995-07-01",
                "status_code": 200,
                "request_count": max(1, self.total_lines),
                "total_bytes": 102400,
            },
            {
                "log_date": "1995-07-01",
                "status_code": 404,
                "request_count": 5,
                "total_bytes": 1250,
            },
        ]
        q2_rows = [
            {
                "rank": i + 1,
                "resource_path": f"/images/logo_{i+1}.gif",
                "request_count": 100 - i * 3,
                "total_bytes": (100 - i * 3) * 512,
                "distinct_host_count": 50 - i,
            }
            for i in range(20)
        ]
        q3_rows = [
            {
                "log_date": "1995-07-01",
                "log_hour": 0,
                "error_request_count": 5,
                "total_request_count": max(10, self.total_lines),
                "error_rate": round(5 / max(10, self.total_lines), 4),
                "distinct_error_hosts": 3,
            }
        ]

        return FinalResult(
            q1=QueryResult(query_id="q1", rows=q1_rows, runtime_sec=0.01),
            q2=QueryResult(query_id="q2", rows=q2_rows, runtime_sec=0.02),
            q3=QueryResult(query_id="q3", rows=q3_rows, runtime_sec=0.01),
        )

    def cleanup(self, run_id: int) -> None:
        self.total_lines = 0
        self.batches_received = 0
