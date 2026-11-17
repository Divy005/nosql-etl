"""Draft PostgreSQL loader — implements Contract 4 function signatures.

This is a draft implementation by Divy. A teammate owns the final loader;
the function signatures must remain exactly as specified so their version
can replace this file.

Uses psycopg (v3) with executemany for bulk inserts.
"""

from datetime import UTC, datetime

import psycopg
from psycopg.rows import dict_row

from pipelines.base import BatchStats, FinalResult


def _now() -> datetime:
    return datetime.now(UTC)


def create_run(
    conn: psycopg.Connection,
    pipeline: str,
    batch_size: int,
    input_files: list[str],
) -> int:
    """Insert a new ETL run row with status='running'. Returns the run_id."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            INSERT INTO etl_runs (pipeline, batch_size, input_files, status, started_at)
            VALUES (%s, %s, %s, 'running', %s)
            RETURNING run_id
            """,
            (pipeline, batch_size, input_files, _now()),
        )
        row = cur.fetchone()
        assert row is not None
        run_id: int = row["run_id"]
    conn.commit()
    return run_id


def record_batch(
    conn: psycopg.Connection,
    run_id: int,
    batch_id: int,
    stats: BatchStats,
    first_line_no: int,
    started_at: datetime,
    finished_at: datetime,
    runtime_sec: float,
) -> None:
    """Insert a row into etl_batches and any malformed samples for this batch."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_batches
                (run_id, batch_id, record_count, parsed_count, malformed_count,
                 first_line_no, started_at, finished_at, runtime_sec)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                batch_id,
                stats.record_count,
                stats.parsed_count,
                stats.malformed_count,
                first_line_no,
                started_at,
                finished_at,
                runtime_sec,
            ),
        )
        # Insert malformed samples (up to 5 per batch)
        if stats.malformed_samples:
            cur.executemany(
                """
                INSERT INTO etl_malformed_samples (run_id, batch_id, line_no, raw_line)
                VALUES (%s, %s, %s, %s)
                """,
                [
                    (run_id, batch_id, line_no, raw_line)
                    for line_no, raw_line in stats.malformed_samples[:5]
                ],
            )
    conn.commit()


def load_results(
    conn: psycopg.Connection,
    run_id: int,
    pipeline: str,
    last_batch_id: int,
    result: FinalResult,
    executed_at: datetime,
) -> None:
    """Load Q1/Q2/Q3 result rows and etl_query_runs in one transaction."""
    with conn.transaction():
        with conn.cursor() as cur:
            # --- etl_query_runs ---
            for qr in (result.q1, result.q2, result.q3):
                cur.execute(
                    """
                    INSERT INTO etl_query_runs
                        (run_id, pipeline, query_id, runtime_sec, row_count, executed_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        run_id,
                        pipeline,
                        qr.query_id,
                        qr.runtime_sec,
                        len(qr.rows),
                        executed_at,
                    ),
                )

            # --- Q1: q1_daily_traffic ---
            if result.q1.rows:
                cur.executemany(
                    """
                    INSERT INTO q1_daily_traffic
                        (run_id, batch_id, pipeline, log_date, status_code,
                         request_count, total_bytes, executed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            run_id,
                            last_batch_id,
                            pipeline,
                            row["log_date"],
                            row["status_code"],
                            row["request_count"],
                            row["total_bytes"],
                            executed_at,
                        )
                        for row in result.q1.rows
                    ],
                )

            # --- Q2: q2_top_resources ---
            if result.q2.rows:
                cur.executemany(
                    """
                    INSERT INTO q2_top_resources
                        (run_id, batch_id, pipeline, rank, resource_path,
                         request_count, total_bytes, distinct_host_count, executed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            run_id,
                            last_batch_id,
                            pipeline,
                            row["rank"],
                            row["resource_path"],
                            row["request_count"],
                            row["total_bytes"],
                            row["distinct_host_count"],
                            executed_at,
                        )
                        for row in result.q2.rows
                    ],
                )

            # --- Q3: q3_hourly_errors ---
            if result.q3.rows:
                cur.executemany(
                    """
                    INSERT INTO q3_hourly_errors
                        (run_id, batch_id, pipeline, log_date, log_hour,
                         error_request_count, total_request_count, error_rate,
                         distinct_error_hosts, executed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            run_id,
                            last_batch_id,
                            pipeline,
                            row["log_date"],
                            row["log_hour"],
                            row["error_request_count"],
                            row["total_request_count"],
                            row["error_rate"],
                            row["distinct_error_hosts"],
                            executed_at,
                        )
                        for row in result.q3.rows
                    ],
                )


def finish_run(
    conn: psycopg.Connection,
    run_id: int,
    *,
    total_records: int,
    parsed_records: int,
    malformed_records: int,
    num_batches: int,
    runtime_sec: float,
    ingest_runtime_sec: float,
    aggregate_runtime_sec: float,
    load_runtime_sec: float,
) -> None:
    """Update the run row to status='succeeded' with final metrics."""
    avg_batch_size = total_records / num_batches if num_batches > 0 else 0.0
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE etl_runs
            SET status = 'succeeded',
                total_records = %s,
                parsed_records = %s,
                malformed_records = %s,
                num_batches = %s,
                avg_batch_size = %s,
                finished_at = %s,
                runtime_sec = %s,
                ingest_runtime_sec = %s,
                aggregate_runtime_sec = %s,
                load_runtime_sec = %s
            WHERE run_id = %s
            """,
            (
                total_records,
                parsed_records,
                malformed_records,
                num_batches,
                round(avg_batch_size, 2),
                _now(),
                round(runtime_sec, 4),
                round(ingest_runtime_sec, 4),
                round(aggregate_runtime_sec, 4),
                round(load_runtime_sec, 4),
                run_id,
            ),
        )
    conn.commit()


def fail_run(
    conn: psycopg.Connection,
    run_id: int,
    error_message: str,
) -> None:
    """Mark the run as failed and store the error message."""
    # If the failure happened mid-transaction, the connection is in an aborted state
    # and would reject the UPDATE below; discard the partial work first.
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE etl_runs
            SET status = 'failed',
                finished_at = %s,
                error_message = %s
            WHERE run_id = %s
            """,
            (_now(), error_message, run_id),
        )
    conn.commit()
