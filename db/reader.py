"""Read side of the PostgreSQL schema, shared by report.py and compare.py.

Everything the report and the equivalence checker need is already in PostgreSQL after any
run: ``etl_runs`` (metadata), ``etl_batches``, ``etl_malformed_samples``, ``etl_query_runs``
(per-query runtime) and the three result tables, which are always filtered by ``run_id``.
"""

from __future__ import annotations

from pathlib import PurePath
from typing import Any

import psycopg
from psycopg.rows import dict_row

from controller.config import get_pg_dsn

# The official input pair: runs on anything else (the 63-line fixture, a sample file, ...)
# are not comparable with a full-data run.
FULL_DATA_INPUT = ("NASA_access_log_Jul95", "NASA_access_log_Aug95")

QUERY_TITLES = {
    "q1": "Q1  Daily Traffic Summary",
    "q2": "Q2  Top Requested Resources",
    "q3": "Q3  Hourly Error Analysis",
}

# Result columns in the order of the project statement; sort order matches spec/parsing_spec.md.
RESULT_TABLES: dict[str, tuple[str, tuple[str, ...], str]] = {
    "q1": (
        "q1_daily_traffic",
        ("log_date", "status_code", "request_count", "total_bytes"),
        "log_date, status_code",
    ),
    "q2": (
        "q2_top_resources",
        ("rank", "resource_path", "request_count", "total_bytes", "distinct_host_count"),
        "rank",
    ),
    "q3": (
        "q3_hourly_errors",
        (
            "log_date",
            "log_hour",
            "error_request_count",
            "total_request_count",
            "error_rate",
            "distinct_error_hosts",
        ),
        "log_date, log_hour",
    ),
}

Row = dict[str, Any]


def connect() -> psycopg.Connection[Row]:
    """Open a read connection to the reporting database (settings from ``.env``)."""
    return psycopg.connect(get_pg_dsn(), row_factory=dict_row, connect_timeout=5)


def input_names(run: Row) -> tuple[str, ...]:
    """File names (no folders) of a run's inputs, so runs from different machines compare."""
    return tuple(PurePath(str(p).replace("\\", "/")).name for p in run["input_files"])


def _fetch(conn: psycopg.Connection[Row], sql: str, params: tuple = ()) -> list[Row]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def get_run(conn: psycopg.Connection[Row], run_id: int) -> Row | None:
    rows = _fetch(conn, "SELECT * FROM etl_runs WHERE run_id = %s", (run_id,))
    return rows[0] if rows else None


def list_runs(
    conn: psycopg.Connection[Row],
    *,
    pipeline: str | None = None,
    batch_size: int | None = None,
    status: str | None = "succeeded",
    inputs: tuple[str, ...] | None = None,
) -> list[Row]:
    """Runs matching the filters, newest first. ``inputs`` compares input file names."""
    where, params = ["TRUE"], []
    if status is not None:
        where.append("status = %s")
        params.append(status)
    if pipeline is not None:
        where.append("pipeline = %s")
        params.append(pipeline)
    if batch_size is not None:
        where.append("batch_size = %s")
        params.append(batch_size)
    runs = _fetch(
        conn,
        f"SELECT * FROM etl_runs WHERE {' AND '.join(where)} ORDER BY run_id DESC",
        tuple(params),
    )
    if inputs is not None:
        runs = [r for r in runs if input_names(r) == tuple(inputs)]
    return runs


def latest_run(conn: psycopg.Connection[Row], **filters: Any) -> Row | None:
    runs = list_runs(conn, **filters)
    return runs[0] if runs else None


def get_query_runs(conn: psycopg.Connection[Row], run_id: int) -> list[Row]:
    return _fetch(
        conn, "SELECT * FROM etl_query_runs WHERE run_id = %s ORDER BY query_id", (run_id,)
    )


def get_batches(conn: psycopg.Connection[Row], run_id: int) -> list[Row]:
    return _fetch(conn, "SELECT * FROM etl_batches WHERE run_id = %s ORDER BY batch_id", (run_id,))


def get_malformed_samples(conn: psycopg.Connection[Row], run_id: int) -> list[Row]:
    return _fetch(
        conn,
        "SELECT batch_id, line_no, raw_line FROM etl_malformed_samples "
        "WHERE run_id = %s ORDER BY line_no",
        (run_id,),
    )


def get_results(conn: psycopg.Connection[Row], run_id: int, query_id: str) -> list[Row]:
    """The result rows of one query of one run, columns and order as in the spec."""
    table, columns, order_by = RESULT_TABLES[query_id]
    return _fetch(
        conn,
        f"SELECT {', '.join(columns)} FROM {table} WHERE run_id = %s ORDER BY {order_by}",
        (run_id,),
    )


def result_batch_id(conn: psycopg.Connection[Row], run_id: int) -> int | None:
    """The ``batch_id`` stored on the result rows (= the run's last batch; they cover 1..N)."""
    rows = _fetch(
        conn,
        """
        SELECT MAX(batch_id) AS b FROM (
            SELECT batch_id FROM q1_daily_traffic WHERE run_id = %(r)s
            UNION ALL SELECT batch_id FROM q2_top_resources WHERE run_id = %(r)s
            UNION ALL SELECT batch_id FROM q3_hourly_errors WHERE run_id = %(r)s
        ) t
        """,
        {"r": run_id},  # type: ignore[arg-type]
    )
    return rows[0]["b"] if rows else None
