"""Equivalence checker: do two pipelines give the same answers on the same data?

The project statement demands the same dataset, the same logical ETL steps and the same query
definitions in every pipeline, so their stored results must be identical. This script reads
the stored results of two runs from PostgreSQL and prints PASS or FAIL per check.

    python compare.py                          # Hive vs MongoDB (latest succeeded full-data runs)
    python compare.py --engine hive            # the same, spelled out
    python compare.py --engine all             # every pipeline that has a run, against MongoDB
    python compare.py --engine pig --reference hive
    python compare.py --run-a 3 --run-b 2      # two explicit run ids

Compared per query, on the result columns only (``id``, ``run_id``, ``batch_id``,
``pipeline`` and ``executed_at`` differ by design and are ignored):

* record counts of the run: ``total_records``, ``parsed_records``, ``malformed_records``
* Q1 rows matched by ``(log_date, status_code)``; Q3 rows by ``(log_date, log_hour)``;
  Q2 rows by ``rank``; the row counts must also be equal
* ``error_rate`` is NUMERIC(7,4) and is compared exactly

Exit status: 0 all PASS, 1 at least one FAIL, 2 a needed run or the database is missing.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from typing import Any

import psycopg
from tabulate import tabulate

from db import reader
from db.reader import Row
from pipelines import canonical_engine_name

DEFAULT_REFERENCE = "mongodb"
ENGINES = ("mongodb", "hive", "pig", "mapreduce")
COUNT_FIELDS = ("total_records", "parsed_records", "malformed_records")

# query -> (columns that form the match key, columns that must be equal)
QUERY_KEYS: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "q1": (("log_date", "status_code"), ("request_count", "total_bytes")),
    "q2": (
        ("rank",),
        ("resource_path", "request_count", "total_bytes", "distinct_host_count"),
    ),
    "q3": (
        ("log_date", "log_hour"),
        ("error_request_count", "total_request_count", "error_rate", "distinct_error_hosts"),
    ),
}


@dataclass
class Check:
    """Outcome of one comparison (one line of the PASS/FAIL list)."""

    name: str
    passed: bool
    summary: str
    details: list[str] = field(default_factory=list)


def compare_counts(run_a: Row, run_b: Row) -> Check:
    diffs = [f"{f}: {run_a[f]:,} vs {run_b[f]:,}" for f in COUNT_FIELDS if run_a[f] != run_b[f]]
    summary = (
        f"{run_a['total_records']:,} records = {run_a['parsed_records']:,} parsed "
        f"+ {run_a['malformed_records']:,} malformed"
    )
    return Check("record counts", not diffs, summary if not diffs else "differ", diffs)


def compare_rows(query_id: str, rows_a: list[Row], rows_b: list[Row], max_diffs: int = 5) -> Check:
    """Match rows by key and compare the value columns; report row-count and cell differences."""
    key_cols, value_cols = QUERY_KEYS[query_id]

    def keyed(rows: list[Row]) -> dict[tuple, Row]:
        return {tuple(r[c] for c in key_cols): r for r in rows}

    a, b = keyed(rows_a), keyed(rows_b)
    problems: list[str] = []
    if len(rows_a) != len(rows_b):
        problems.append(f"row count: {len(rows_a):,} vs {len(rows_b):,}")
    if len(a) != len(rows_a) or len(b) != len(rows_b):
        problems.append("duplicate keys in a result set")
    for key in sorted(set(a) - set(b), key=str):
        problems.append(f"only in A: {dict(zip(key_cols, key, strict=True))}")
    for key in sorted(set(b) - set(a), key=str):
        problems.append(f"only in B: {dict(zip(key_cols, key, strict=True))}")
    for key in sorted(set(a) & set(b), key=str):
        for col in value_cols:
            if a[key][col] != b[key][col]:
                where = ", ".join(f"{c}={v}" for c, v in zip(key_cols, key, strict=True))
                problems.append(f"{where}: {col} = {a[key][col]!r} vs {b[key][col]!r}")

    name = reader.QUERY_TITLES[query_id]
    if not problems:
        return Check(name, True, f"{len(rows_a):,} rows identical")
    shown = problems[:max_diffs]
    if len(problems) > max_diffs:
        shown.append(f"... and {len(problems) - max_diffs:,} more differences")
    return Check(name, False, f"{len(problems):,} difference(s)", shown)


def compare_runs(
    conn: psycopg.Connection[Row], run_a: Row, run_b: Row, max_diffs: int = 5
) -> list[Check]:
    checks = [compare_counts(run_a, run_b)]
    for query_id in QUERY_KEYS:
        rows_a = reader.get_results(conn, run_a["run_id"], query_id)
        rows_b = reader.get_results(conn, run_b["run_id"], query_id)
        checks.append(compare_rows(query_id, rows_a, rows_b, max_diffs))
    return checks


# ------------------------------------------------------------------------------------------
# choosing which runs to compare
# ------------------------------------------------------------------------------------------
class MissingRun(Exception):
    """A pipeline has no (suitable) succeeded run in the database."""


def pick_pair(
    conn: psycopg.Connection[Row],
    engine: str,
    reference: str,
    batch_size: int | None,
    any_input: bool,
) -> tuple[Row, Row]:
    """The latest succeeded run of ``engine`` and the latest reference run on the SAME input."""
    inputs = None if any_input else reader.FULL_DATA_INPUT
    run = reader.latest_run(conn, pipeline=engine, batch_size=batch_size, inputs=inputs)
    if run is None:
        where = "any input" if any_input else "the full NASA data"
        size = f" at batch size {batch_size:,}" if batch_size else ""
        raise MissingRun(f"no succeeded '{engine}' run on {where}{size} in PostgreSQL")
    ref = reader.latest_run(conn, pipeline=reference, inputs=reader.input_names(run))
    if ref is None:
        raise MissingRun(
            f"no succeeded '{reference}' run on the same input "
            f"({', '.join(reader.input_names(run))}) to compare '{engine}' with"
        )
    return run, ref


# ------------------------------------------------------------------------------------------
# output
# ------------------------------------------------------------------------------------------
def _describe(run: Row) -> str:
    return f"{run['pipeline']} (run {run['run_id']}, batch size {run['batch_size']:,})"


def print_comparison(run_a: Row, run_b: Row, checks: list[Check]) -> bool:
    print(f"\n  A: {_describe(run_a)}")
    print(f"  B: {_describe(run_b)}\n")
    table = [["PASS" if c.passed else "FAIL", c.name, c.summary] for c in checks]
    print(tabulate(table, headers=["", "Check", "Result"], tablefmt="simple"))
    for c in checks:
        for line in c.details:
            print(f"      [{c.name}] {line}")
    ok = all(c.passed for c in checks)
    verdict = "PASS" if ok else "FAIL"
    print(f"\n  {verdict}: {run_a['pipeline']} vs {run_b['pipeline']}\n")
    return ok


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check that two pipelines stored identical results in PostgreSQL.",
        epilog="Exit status: 0 = all PASS, 1 = a FAIL, 2 = missing run / database.",
    )
    parser.add_argument(
        "--engine",
        default="hive",
        type=canonical_engine_name,
        metavar="{mongodb,hive,pig,mapreduce,all}",
        help="pipeline to check (default: hive), or 'all' for every pipeline that has a "
        "run. Aliases: mongo, mr.",
    )
    parser.add_argument(
        "--reference",
        default=DEFAULT_REFERENCE,
        type=canonical_engine_name,
        metavar="PIPELINE",
        help=f"pipeline to compare against (default: {DEFAULT_REFERENCE}).",
    )
    parser.add_argument("--run-a", type=int, help="explicit run id for side A (with --run-b).")
    parser.add_argument("--run-b", type=int, help="explicit run id for side B (with --run-a).")
    parser.add_argument(
        "--batch-size", type=int, help="only consider runs of --engine with this batch size."
    )
    parser.add_argument(
        "--any-input",
        action="store_true",
        help="also consider runs on other input files (default: the two official NASA files).",
    )
    parser.add_argument(
        "--max-diffs", type=int, default=5, help="differences to print per check (default 5)."
    )
    args = parser.parse_args(argv)
    if (args.run_a is None) != (args.run_b is None):
        parser.error("--run-a and --run-b must be given together")
    if args.engine != "all" and args.engine not in ENGINES:
        parser.error(f"--engine must be one of {', '.join(ENGINES)} or all")
    return args


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")  # Latin-1 resource paths on a cp1252 console

    try:
        conn = reader.connect()
    except psycopg.OperationalError as exc:
        print(f"ERROR: cannot reach PostgreSQL: {exc}", file=sys.stderr)
        return 2

    with conn:
        pairs: list[tuple[Row, Row]] = []
        skipped: list[str] = []
        try:
            if args.run_a is not None:
                run_a, run_b = reader.get_run(conn, args.run_a), reader.get_run(conn, args.run_b)
                for rid, run in ((args.run_a, run_a), (args.run_b, run_b)):
                    if run is None or run["status"] != "succeeded":
                        raise MissingRun(f"run {rid} does not exist or did not succeed")
                assert run_a is not None and run_b is not None
                pairs.append((run_a, run_b))
            elif args.engine == "all":
                for engine in ENGINES:
                    if engine == args.reference:
                        continue
                    try:
                        pairs.append(
                            pick_pair(conn, engine, args.reference, args.batch_size, args.any_input)
                        )
                    except MissingRun as exc:
                        skipped.append(f"{engine}: {exc}")
                if not pairs:
                    raise MissingRun("no pipeline has a succeeded run to compare yet")
            else:
                pairs.append(
                    pick_pair(conn, args.engine, args.reference, args.batch_size, args.any_input)
                )
        except MissingRun as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2

        outcomes: list[tuple[str, bool]] = []
        for run_a, run_b in pairs:
            checks = compare_runs(conn, run_a, run_b, args.max_diffs)
            outcomes.append((run_a["pipeline"], print_comparison(run_a, run_b, checks)))

    if len(outcomes) > 1 or skipped:
        print("Summary")
        rows: list[list[Any]] = [
            [name, args.reference, "PASS" if ok else "FAIL"] for name, ok in outcomes
        ]
        rows += [[s.split(":")[0], args.reference, "SKIPPED"] for s in skipped]
        print(tabulate(rows, headers=["pipeline", "vs", "result"], tablefmt="simple"))
        for s in skipped:
            print(f"  (skipped) {s}")
        print()
    return 0 if all(ok for _, ok in outcomes) else 1


if __name__ == "__main__":
    sys.exit(main())
