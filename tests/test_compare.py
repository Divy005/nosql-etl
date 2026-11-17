"""compare.py: identical results PASS, any difference FAILs, wrong inputs are refused."""

import pytest

import compare
from db import reader
from tests.pg_helpers import changed, default_result, seed_run


def _run(conn, run_id):
    run = reader.get_run(conn, run_id)
    assert run is not None
    return run


def test_identical_results_pass(pg_conn) -> None:
    a = seed_run(pg_conn, "hive", batch_size=7)
    b = seed_run(pg_conn, "mongodb", batch_size=5)  # batch size does not matter for equivalence
    checks = compare.compare_runs(pg_conn, _run(pg_conn, a), _run(pg_conn, b))
    assert [c.name for c in checks] == [
        "record counts",
        "Q1  Daily Traffic Summary",
        "Q2  Top Requested Resources",
        "Q3  Hourly Error Analysis",
    ]
    assert all(c.passed for c in checks), [c for c in checks if not c.passed]


@pytest.mark.parametrize(
    ("query_id", "row", "column", "value", "expected_check"),
    [
        ("q1", 1, "request_count", 3, "Q1"),
        ("q1", 0, "total_bytes", 901, "Q1"),
        ("q2", 0, "distinct_host_count", 4, "Q2"),
        ("q2", 1, "resource_path", "/c.html", "Q2"),
        ("q3", 0, "error_rate", 0.0312, "Q3"),  # one unit in the 4th decimal must be caught
        ("q3", 1, "distinct_error_hosts", 1, "Q3"),
    ],
)
def test_single_cell_difference_fails(
    pg_conn, query_id, row, column, value, expected_check
) -> None:
    a = seed_run(pg_conn, "hive")
    other = changed(default_result(), query_id, row, column, value)
    b = seed_run(pg_conn, "mongodb", result=other)
    checks = compare.compare_runs(pg_conn, _run(pg_conn, a), _run(pg_conn, b))
    failed = [c for c in checks if not c.passed]
    assert len(failed) == 1
    assert failed[0].name.startswith(expected_check)
    assert column in " ".join(failed[0].details)


def test_missing_and_extra_rows_fail(pg_conn) -> None:
    short = default_result()
    short.q1.rows.pop()  # one Q1 row fewer
    a = seed_run(pg_conn, "hive", result=short)
    b = seed_run(pg_conn, "mongodb")
    q1 = compare.compare_runs(pg_conn, _run(pg_conn, a), _run(pg_conn, b))[1]
    assert not q1.passed
    assert any("row count" in d for d in q1.details)
    assert any("only in B" in d for d in q1.details)


def test_record_count_difference_fails(pg_conn) -> None:
    a = seed_run(pg_conn, "hive", malformed=1)
    b = seed_run(pg_conn, "mongodb", malformed=0)
    counts = compare.compare_runs(pg_conn, _run(pg_conn, a), _run(pg_conn, b))[0]
    assert not counts.passed
    assert any("parsed_records" in d for d in counts.details)
    assert any("malformed_records" in d for d in counts.details)


def test_pick_pair_uses_latest_runs_on_the_same_input(pg_conn) -> None:
    ref_old = seed_run(pg_conn, "mongodb")
    ref_new = seed_run(pg_conn, "mongodb")
    seed_run(pg_conn, "mongodb", input_files=["spec/test.log"])  # newest, but other input
    hive = seed_run(pg_conn, "hive")
    run, ref = compare.pick_pair(pg_conn, "hive", "mongodb", batch_size=None, any_input=False)
    assert (run["run_id"], ref["run_id"]) == (hive, ref_new)
    assert ref["run_id"] != ref_old


def test_pick_pair_refuses_missing_runs(pg_conn) -> None:
    seed_run(pg_conn, "mongodb")
    with pytest.raises(compare.MissingRun, match="no succeeded 'hive' run"):
        compare.pick_pair(pg_conn, "hive", "mongodb", batch_size=None, any_input=False)
    seed_run(pg_conn, "hive", input_files=["spec/test.log"])  # hive ran on a different input
    with pytest.raises(compare.MissingRun, match="same input"):
        compare.pick_pair(pg_conn, "hive", "mongodb", batch_size=None, any_input=True)
