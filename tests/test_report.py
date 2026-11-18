"""report.py: metadata + results are shown, CSV export is exact, --compare lists pipelines."""

import csv

import report
from db import reader
from tests.pg_helpers import seed_run


def test_run_report_shows_required_metadata_and_results(pg_conn, capsys) -> None:
    run_id = seed_run(pg_conn, "hive", batch_size=7, runtime_sec=12.5)
    report.print_run_report(pg_conn, reader.get_run(pg_conn, run_id), limit=None, all_batches=True)
    out = capsys.readouterr().out

    # statement: pipeline, query name, runtime, batch ID, batch size, average batch size
    assert "pipeline: HIVE" in out
    for needle in ("Q1  Daily Traffic Summary", "Q2  Top Requested Resources", "Hourly Error"):
        assert needle in out
    assert "Batch ID of the results" in out and "(the queries cover batches 1..2)" in out
    assert "Batch size (records per batch)  7" in out
    assert "Average batch size              7.00" in out  # 14 records / 2 batches
    assert "1.500" in out and "2.500" in out and "3.500" in out  # per-query runtimes
    assert "12.500 s" in out  # total runtime
    # the three result sets
    assert "/a.gif" in out and "1995-07-02" in out and "0.0313" in out
    # malformed records are reported, with their line
    assert "'alyssa.p'" in out and "never dropped" in out


def test_long_results_are_truncated_unless_full(pg_conn, capsys) -> None:
    run = reader.get_run(pg_conn, seed_run(pg_conn, "hive"))
    report.print_run_report(pg_conn, run, limit=1, all_batches=False)
    assert "more rows not shown" in capsys.readouterr().out  # Q1 has 3 rows, limit 1
    report.print_run_report(pg_conn, run, limit=None, all_batches=False)
    assert "more rows not shown" not in capsys.readouterr().out


def test_export_writes_exact_csv(pg_conn, tmp_path) -> None:
    run = reader.get_run(pg_conn, seed_run(pg_conn, "hive"))
    paths = report.export_run(pg_conn, run, tmp_path)
    assert {p.name for p in paths} == {
        "hive_q1_daily_traffic.csv",
        "hive_q2_top_resources.csv",
        "hive_q3_hourly_errors.csv",
        "hive_runs.csv",
    }
    with open(tmp_path / "hive_q3_hourly_errors.csv", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert rows[0] == {
        "log_date": "1995-07-01",
        "log_hour": "0",
        "error_request_count": "1",
        "total_request_count": "32",
        "error_rate": "0.0313",
        "distinct_error_hosts": "1",
    }


def test_runtime_comparison_lists_every_pipeline_and_marks_fastest(pg_conn, capsys) -> None:
    seed_run(pg_conn, "mongodb", runtime_sec=100.0)
    seed_run(pg_conn, "hive", runtime_sec=250.0)
    seed_run(pg_conn, "hive", runtime_sec=300.0)  # newer run replaces the older one
    seed_run(pg_conn, "mongodb", runtime_sec=1.0, input_files=["spec/test.log"])  # excluded
    assert report.print_runtime_comparison(pg_conn, None, any_input=False, all_runs=False)
    out = capsys.readouterr().out
    assert out.count("| hive") == 1 and out.count("| mongodb") == 1
    assert "1.00x" in out and "3.00x" in out  # 300 / 100
    assert "250.000" not in out  # the older hive run is hidden unless --all-runs


def test_runtime_comparison_without_runs(pg_conn, capsys) -> None:
    assert not report.print_runtime_comparison(pg_conn, None, any_input=False, all_runs=False)
    assert "No succeeded runs" in capsys.readouterr().out
