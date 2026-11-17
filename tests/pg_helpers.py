"""Helpers that put a small, complete run into PostgreSQL through the real loader."""

from __future__ import annotations

import copy
from datetime import UTC, datetime
from typing import Any

import psycopg

from db.loader import create_run, finish_run, load_results, record_batch
from pipelines.base import BatchStats, FinalResult, QueryResult

FULL_INPUT = ["data/NASA_access_log_Jul95", "data/NASA_access_log_Aug95"]
NOW = datetime(1995, 7, 1, 12, 0, tzinfo=UTC)


def default_result() -> FinalResult:
    """A tiny result set shaped like the real ones (Q1 3 rows, Q2 2 rows, Q3 2 rows)."""
    q1 = [
        {"log_date": "1995-07-01", "status_code": 200, "request_count": 5, "total_bytes": 900},
        {"log_date": "1995-07-01", "status_code": 404, "request_count": 2, "total_bytes": 0},
        {"log_date": "1995-07-02", "status_code": 200, "request_count": 7, "total_bytes": 1234},
    ]
    q2 = [
        {
            "rank": 1,
            "resource_path": "/a.gif",
            "request_count": 9,
            "total_bytes": 90,
            "distinct_host_count": 3,
        },
        {
            "rank": 2,
            "resource_path": "/b.html",
            "request_count": 4,
            "total_bytes": 40,
            "distinct_host_count": 2,
        },
    ]
    q3 = [
        {
            "log_date": "1995-07-01",
            "log_hour": 0,
            "error_request_count": 1,
            "total_request_count": 32,
            "error_rate": 0.0313,
            "distinct_error_hosts": 1,
        },
        {
            "log_date": "1995-07-01",
            "log_hour": 1,
            "error_request_count": 0,
            "total_request_count": 4,
            "error_rate": 0.0,
            "distinct_error_hosts": 0,
        },
    ]
    return FinalResult(
        QueryResult("q1", q1, 1.5), QueryResult("q2", q2, 2.5), QueryResult("q3", q3, 3.5)
    )


def seed_run(
    conn: psycopg.Connection,
    pipeline: str,
    *,
    batch_size: int = 7,
    input_files: list[str] | None = None,
    result: FinalResult | None = None,
    runtime_sec: float = 10.0,
    malformed: int = 1,
) -> int:
    """Create a succeeded run with 2 batches (14 records) and the given results; return run_id."""
    run_id = create_run(conn, pipeline, batch_size, input_files or FULL_INPUT)
    samples = [(8, "alyssa.p")] if malformed else []
    record_batch(conn, run_id, 1, BatchStats(7, 7, 0), 1, NOW, NOW, 0.5)
    stats = BatchStats(7, 7 - malformed, malformed, samples)
    record_batch(conn, run_id, 2, stats, 8, NOW, NOW, 0.4)
    load_results(conn, run_id, pipeline, 2, result or default_result(), NOW)
    finish_run(
        conn,
        run_id,
        total_records=14,
        parsed_records=14 - malformed,
        malformed_records=malformed,
        num_batches=2,
        runtime_sec=runtime_sec,
        ingest_runtime_sec=runtime_sec * 0.8,
        aggregate_runtime_sec=runtime_sec * 0.19,
        load_runtime_sec=runtime_sec * 0.01,
    )
    return run_id


def changed(result: FinalResult, query_id: str, row: int, column: str, value: Any) -> FinalResult:
    """A deep copy of ``result`` with one cell changed."""
    out = copy.deepcopy(result)
    getattr(out, query_id).rows[row][column] = value
    return out
