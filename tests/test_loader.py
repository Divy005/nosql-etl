"""Tests for db/loader.py against a real PostgreSQL (skipped when none is reachable).

The tables from db/schema.sql are created inside a throw-away PostgreSQL *schema*
(search_path = that schema only), so the tests can never touch or wipe the ``public``
tables that hold real runs, even though schema.sql starts with ``DROP TABLE IF EXISTS``.
"""

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest
from psycopg.rows import dict_row

from controller.config import get_pg_dsn
from db.loader import create_run, fail_run, finish_run, load_results, record_batch
from pipelines.base import BatchStats, FinalResult, QueryResult

SCHEMA_SQL = Path(__file__).resolve().parent.parent / "db" / "schema.sql"
NOW = datetime(1995, 7, 1, 12, 0, tzinfo=UTC)


@pytest.fixture
def conn() -> Iterator[psycopg.Connection]:
    try:
        connection = psycopg.connect(get_pg_dsn(), autocommit=False, connect_timeout=3)
    except psycopg.OperationalError:
        pytest.skip("PostgreSQL service unavailable")

    schema = f"loader_test_{uuid.uuid4().hex[:8]}"
    connection.execute(f'CREATE SCHEMA "{schema}"')
    connection.execute(f'SET search_path TO "{schema}"')
    connection.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
    connection.commit()  # the SET must be committed or a later rollback would undo it
    try:
        yield connection
    finally:
        connection.rollback()
        connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
        connection.commit()
        connection.close()


def _stats(n: int, malformed: int = 0, samples: int = 0, first: int = 1) -> BatchStats:
    return BatchStats(
        record_count=n,
        parsed_count=n - malformed,
        malformed_count=malformed,
        malformed_samples=[(first + i, f"bad line {i}") for i in range(samples)],
    )


def _result() -> FinalResult:
    return FinalResult(
        q1=QueryResult(
            "q1",
            [
                {
                    "log_date": "1995-07-01",
                    "status_code": 200,
                    "request_count": 5,
                    "total_bytes": 90,
                },
                {
                    "log_date": "1995-07-01",
                    "status_code": 404,
                    "request_count": 2,
                    "total_bytes": 0,
                },
            ],
            1.5,
        ),
        q2=QueryResult(
            "q2",
            [
                {
                    "rank": 1,
                    "resource_path": "/a.gif",
                    "request_count": 5,
                    "total_bytes": 90,
                    "distinct_host_count": 3,
                }
            ],
            2.5,
        ),
        q3=QueryResult(
            "q3",
            [
                {
                    "log_date": "1995-07-01",
                    "log_hour": 0,
                    "error_request_count": 1,
                    "total_request_count": 32,
                    "error_rate": 0.03125,  # exact tie: must be stored as 0.0313
                    "distinct_error_hosts": 1,
                },
                {
                    "log_date": "1995-07-01",
                    "log_hour": 1,
                    "error_request_count": 0,
                    "total_request_count": 4,
                    "error_rate": Decimal("0.0000"),
                    "distinct_error_hosts": 0,
                },
            ],
            3.5,
        ),
    )


def _scalar(conn: psycopg.Connection, sql: str, *params: object) -> object:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        assert row is not None
        return row[0]


def _new_run(conn: psycopg.Connection) -> int:
    return create_run(conn, "hive", 7, ["a.log", "b.log"])


def test_create_run_inserts_running_row(conn: psycopg.Connection) -> None:
    run_id = _new_run(conn)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM etl_runs WHERE run_id = %s", (run_id,))
        row = cur.fetchone()
    assert row is not None
    assert row["pipeline"] == "hive"
    assert row["batch_size"] == 7
    assert row["input_files"] == ["a.log", "b.log"]
    assert row["status"] == "running"
    # create_run commits: a rollback by the caller must not remove the run row
    conn.rollback()
    assert _scalar(conn, "SELECT count(*) FROM etl_runs") == 1


def test_record_batch_caps_malformed_samples_at_five(conn: psycopg.Connection) -> None:
    run_id = _new_run(conn)
    record_batch(conn, run_id, 1, _stats(10, malformed=7, samples=7), 1, NOW, NOW, 0.25)
    assert _scalar(conn, "SELECT count(*) FROM etl_malformed_samples") == 5
    assert _scalar(conn, "SELECT malformed_count FROM etl_batches WHERE batch_id = 1") == 7
    conn.rollback()  # record_batch committed, so everything survives a rollback
    assert _scalar(conn, "SELECT count(*) FROM etl_batches") == 1


def test_full_flow_matches_controller_contract(conn: psycopg.Connection) -> None:
    run_id = _new_run(conn)
    record_batch(conn, run_id, 1, _stats(7), 1, NOW, NOW, 0.5)
    record_batch(conn, run_id, 2, _stats(3, malformed=1, samples=1, first=8), 8, NOW, NOW, 0.4)

    executed_at = datetime(1995, 7, 2, tzinfo=UTC)
    load_results(conn, run_id, "hive", 2, _result(), executed_at)
    conn.rollback()  # load_results must already have committed

    finish_run(
        conn,
        run_id,
        total_records=10,
        parsed_records=9,
        malformed_records=1,
        num_batches=2,
        runtime_sec=12.34567,
        ingest_runtime_sec=8.0,
        aggregate_runtime_sec=4.0,
        load_runtime_sec=0.3,
    )

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM etl_runs WHERE run_id = %s", (run_id,))
        run = cur.fetchone()
        cur.execute("SELECT * FROM etl_query_runs ORDER BY query_id")
        qruns = cur.fetchall()
    assert run is not None
    assert run["status"] == "succeeded"
    assert run["avg_batch_size"] == Decimal("5.00")  # total / non-empty batches = 10 / 2
    assert run["runtime_sec"] == Decimal("12.3457")
    assert run["finished_at"] is not None
    assert [(q["query_id"], q["row_count"]) for q in qruns] == [("q1", 2), ("q2", 1), ("q3", 2)]
    assert [q["runtime_sec"] for q in qruns] == [Decimal("1.5"), Decimal("2.5"), Decimal("3.5")]

    # every result row: the run's pipeline/run_id, the LAST batch id and one executed_at
    for table in ("q1_daily_traffic", "q2_top_resources", "q3_hourly_errors"):
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f"SELECT DISTINCT run_id, batch_id, pipeline, executed_at FROM {table}")
            distinct = cur.fetchall()
        assert distinct == [
            {"run_id": run_id, "batch_id": 2, "pipeline": "hive", "executed_at": executed_at}
        ], table
    for q in qruns:
        assert q["executed_at"] == executed_at

    rates = _scalar(conn, "SELECT array_agg(error_rate ORDER BY log_hour) FROM q3_hourly_errors")
    assert rates == [Decimal("0.0313"), Decimal("0.0000")]


def test_load_results_is_all_or_nothing(conn: psycopg.Connection) -> None:
    run_id = _new_run(conn)
    record_batch(conn, run_id, 1, _stats(7), 1, NOW, NOW, 0.5)

    # batch 99 does not exist -> the foreign key on the result tables rejects the rows,
    # after etl_query_runs has already been written inside the same transaction
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        load_results(conn, run_id, "hive", 99, _result(), NOW)

    for table in ("etl_query_runs", "q1_daily_traffic", "q2_top_resources", "q3_hourly_errors"):
        assert _scalar(conn, f"SELECT count(*) FROM {table}") == 0, table
    # the connection is usable again and the earlier batch row is untouched
    assert _scalar(conn, "SELECT count(*) FROM etl_batches") == 1


def test_fail_run_after_aborted_transaction(conn: psycopg.Connection) -> None:
    run_id = _new_run(conn)
    with pytest.raises(psycopg.errors.UndefinedTable):
        conn.execute("SELECT * FROM table_that_does_not_exist")
    # the connection is now in an aborted transaction; fail_run must roll back first
    fail_run(conn, run_id, "boom: something broke")

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT status, error_message, finished_at FROM etl_runs")
        row = cur.fetchone()
    assert row is not None
    assert row["status"] == "failed"
    assert row["error_message"] == "boom: something broke"
    assert row["finished_at"] is not None
