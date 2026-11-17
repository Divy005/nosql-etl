"""Reporting module: shows the stored results of a run together with its execution metadata.

It only reads PostgreSQL (nothing is re-computed) and runs outside the timed ETL span.

    python report.py                         # latest succeeded run
    python report.py --pipeline hive         # latest succeeded run of one pipeline
    python report.py --run-id 3              # one specific run
    python report.py --full                  # print every result row (default: first rows)
    python report.py --export docs/sample_outputs   # also write the run's results as CSV
    python report.py --compare               # runtime comparison of all pipelines
    python report.py --compare --batch-size 100000

For the selected run it prints, as the project statement requires: the pipeline, query name,
runtime, batch ID, batch size and average batch size, then the three result sets. ``--compare``
puts the latest full-data run of every pipeline (and batch size) side by side.
"""

from __future__ import annotations

import argparse
import csv
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
from tabulate import tabulate

from db import reader
from db.reader import QUERY_TITLES, RESULT_TABLES, Row
from pipelines import canonical_engine_name

DEFAULT_LIMIT = 10  # rows of the long results (Q1: 295, Q3: 1,369) shown unless --full
BAR = "=" * 78


def _sec(value: Any) -> str:
    return "-" if value is None else f"{float(value):,.3f}"


def _int(value: Any) -> str:
    return "-" if value is None else f"{int(value):,}"


def _cell(value: Any) -> Any:
    """Plain, aligned values for tabulate: dates as ISO text, rates with 4 places."""
    if isinstance(value, Decimal):
        return f"{value:.4f}"
    return value if value is None or isinstance(value, int | str) else str(value)


def _is_numeric_text(value: Any) -> bool:
    return isinstance(value, int) or (
        isinstance(value, str) and value.replace(".", "", 1).isdigit()
    )


def _query_times(conn: psycopg.Connection[Row], run_id: int) -> dict[str, Any]:
    return {q["query_id"]: q["runtime_sec"] for q in reader.get_query_runs(conn, run_id)}


def _aligns(rows: list[list[Any]], width: int) -> tuple[str, ...]:
    """Numbers right-aligned, text left-aligned (tabulate's own detection is off, see below)."""
    sample = rows[0] if rows else [""] * width
    return tuple("right" if _is_numeric_text(v) else "left" for v in sample)


def _title(text: str) -> None:
    print(f"\n{text}\n{'-' * len(text)}")


def _truncated(rows: list[Row], limit: int | None) -> tuple[list[Row], int]:
    if limit is None or len(rows) <= limit:
        return rows, 0
    return rows[:limit], len(rows) - limit


# ------------------------------------------------------------------------------------------
# one run
# ------------------------------------------------------------------------------------------
def print_run_report(
    conn: psycopg.Connection[Row], run: Row, limit: int | None, all_batches: bool
) -> None:
    run_id = run["run_id"]
    batches = reader.get_batches(conn, run_id)
    qruns = {q["query_id"]: q for q in reader.get_query_runs(conn, run_id)}
    last_batch = reader.result_batch_id(conn, run_id)

    print(
        f"\n{BAR}\n  ETL RUN REPORT   pipeline: {run['pipeline'].upper()}   run id: {run_id}\n{BAR}"
    )

    _title("Run metadata")
    meta = [
        ["Pipeline", run["pipeline"]],
        ["Run ID", run_id],
        ["Status", run["status"]],
        ["Input files", ", ".join(reader.input_names(run))],
        ["Batch size (records per batch)", _int(run["batch_size"])],
        ["Number of batches", _int(run["num_batches"])],
        ["Batch ID of the results", f"{last_batch}  (the queries cover batches 1..{last_batch})"],
        [
            "Average batch size",
            f"{float(run['avg_batch_size']):,.2f}  (records / non-empty batches)",
        ],
        ["Records: total", _int(run["total_records"])],
        ["Records: parsed", _int(run["parsed_records"])],
        ["Records: malformed", _int(run["malformed_records"])],
        ["Runtime: total", f"{_sec(run['runtime_sec'])} s   (first read -> results committed)"],
        ["  ingest (all batches)", f"{_sec(run['ingest_runtime_sec'])} s"],
        ["  aggregate (Q1+Q2+Q3)", f"{_sec(run['aggregate_runtime_sec'])} s"],
        ["  load into PostgreSQL", f"{_sec(run['load_runtime_sec'])} s"],
        ["Started", run["started_at"].strftime("%Y-%m-%d %H:%M:%S %Z")],
    ]
    print(tabulate(meta, tablefmt="plain"))
    if run["status"] != "succeeded":
        print(f"\n  !! run did not succeed: {run['error_message']}")

    _title("Queries: execution metadata")
    rows = [
        [
            QUERY_TITLES[qid],
            run["pipeline"],
            _sec(qruns[qid]["runtime_sec"]) if qid in qruns else "-",
            last_batch if last_batch is not None else "-",
            _int(run["batch_size"]),
            f"{float(run['avg_batch_size']):,.2f}",
            _int(qruns[qid]["row_count"]) if qid in qruns else "-",
        ]
        for qid in RESULT_TABLES
    ]
    print(
        tabulate(
            rows,
            headers=[
                "Query",
                "Pipeline",
                "Runtime (s)",
                "Batch ID",
                "Batch size",
                "Avg batch size",
                "Rows",
            ],
            tablefmt="psql",
            disable_numparse=True,
            colalign=("left", "left", "right", "right", "right", "right", "right"),
        )
    )

    for qid, (_, columns, _) in RESULT_TABLES.items():
        results = reader.get_results(conn, run_id, qid)
        shown, hidden = _truncated(results, None if qid == "q2" else limit)
        q = qruns.get(qid)
        _title(
            f"{QUERY_TITLES[qid]}  [{run['pipeline']}, run {run_id}, "
            f"runtime {_sec(q['runtime_sec']) if q else '-'} s, {len(results):,} rows]"
        )
        table = [[_cell(r[c]) for c in columns] for r in shown]
        print(
            tabulate(
                table,
                headers=columns,
                tablefmt="psql",
                disable_numparse=True,
                colalign=_aligns(table, len(columns)),
            )
        )
        if hidden:
            print(f"  ... {hidden:,} more rows not shown (use --full, or --export DIR for CSV)")

    _title("Batches")
    shown_batches, hidden_b = _truncated(batches, None if all_batches else 5)
    btable = [
        [
            b["batch_id"],
            _int(b["first_line_no"]),
            _int(b["record_count"]),
            _int(b["parsed_count"]),
            _int(b["malformed_count"]),
            _sec(b["runtime_sec"]),
        ]
        for b in shown_batches
    ]
    print(
        tabulate(
            btable,
            headers=["Batch ID", "First line", "Records", "Parsed", "Malformed", "Runtime (s)"],
            tablefmt="psql",
            disable_numparse=True,
            colalign=("right",) * 6,
        )
    )
    if hidden_b:
        print(f"  ... {hidden_b:,} more batches (use --batches to list all)")

    samples = reader.get_malformed_samples(conn, run_id)
    _title(f"Malformed records ({_int(run['malformed_records'])} counted, never dropped)")
    if samples:
        stable = [[s["batch_id"], f"{s['line_no']:,}", repr(s["raw_line"])] for s in samples[:20]]
        print(
            tabulate(
                stable,
                headers=["Batch", "Line no", "Raw line"],
                tablefmt="psql",
                disable_numparse=True,
                colalign=("right", "right", "left"),
            )
        )
    else:
        print("  none")
    print()


def export_run(conn: psycopg.Connection[Row], run: Row, directory: Path) -> list[Path]:
    """Write the run's three result sets and its metadata row as CSV (UTF-8)."""
    directory.mkdir(parents=True, exist_ok=True)
    written = []
    names = {"q1": "q1_daily_traffic", "q2": "q2_top_resources", "q3": "q3_hourly_errors"}
    for qid, (_, columns, _) in RESULT_TABLES.items():
        path = directory / f"{run['pipeline']}_{names[qid]}.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, lineterminator="\n")
            writer.writerow(columns)
            for r in reader.get_results(conn, run["run_id"], qid):
                writer.writerow([_cell(r[c]) for c in columns])
        written.append(path)
    path = directory / f"{run['pipeline']}_runs.csv"
    fields = [
        "run_id", "pipeline", "batch_size", "num_batches", "avg_batch_size", "total_records",
        "parsed_records", "malformed_records", "runtime_sec", "ingest_runtime_sec",
        "aggregate_runtime_sec", "load_runtime_sec",
    ]  # fmt: skip
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(fields)
        writer.writerow([run[c] for c in fields])
    written.append(path)
    return written


# ------------------------------------------------------------------------------------------
# --compare: runtimes of all pipelines side by side
# ------------------------------------------------------------------------------------------
def print_runtime_comparison(
    conn: psycopg.Connection[Row], batch_size: int | None, any_input: bool, all_runs: bool
) -> bool:
    runs = reader.list_runs(
        conn, batch_size=batch_size, inputs=None if any_input else reader.FULL_DATA_INPUT
    )
    if not all_runs:  # latest run per (pipeline, batch size)
        seen: set[tuple[str, int]] = set()
        latest = []
        for r in runs:  # newest first
            key = (r["pipeline"], r["batch_size"])
            if key not in seen:
                seen.add(key)
                latest.append(r)
        runs = latest
    if not runs:
        what = "" if any_input else " on the full NASA data"
        print(f"No succeeded runs{what} found. Run: python -m controller.main --pipeline <name>")
        return False

    qtimes = {r["run_id"]: _query_times(conn, r["run_id"]) for r in runs}
    fastest: dict[int, float] = {}
    for r in runs:
        size = r["batch_size"]
        fastest[size] = min(fastest.get(size, float("inf")), float(r["runtime_sec"]))

    runs.sort(key=lambda r: (r["batch_size"], float(r["runtime_sec"])))
    table = []
    for r in runs:
        qt = qtimes[r["run_id"]]
        total = float(r["runtime_sec"])
        table.append(
            [
                _int(r["batch_size"]),
                r["pipeline"],
                r["run_id"],
                _int(r["num_batches"]),
                _sec(total),
                _sec(r["ingest_runtime_sec"]),
                _sec(r["aggregate_runtime_sec"]),
                _sec(r["load_runtime_sec"]),
                _sec(qt.get("q1")),
                _sec(qt.get("q2")),
                _sec(qt.get("q3")),
                f"{total / fastest[r['batch_size']]:.2f}x",
            ]
        )
    print(f"\n{BAR}\n  RUNTIME COMPARISON (seconds)\n{BAR}\n")
    print(
        tabulate(
            table,
            headers=[
                "Batch size", "Pipeline", "Run", "Batches", "Total", "Ingest",
                "Aggregate", "Load", "Q1", "Q2", "Q3", "vs fastest",
            ],
            tablefmt="psql",
            disable_numparse=True,
            colalign=("right", "left") + ("right",) * 10,
        )
    )  # fmt: skip
    print(
        "\n  Total = first read -> results committed in PostgreSQL (download, install and"
        "\n  report rendering excluded). Q1/Q2/Q3 = in-engine query time, part of Aggregate."
        "\n  'vs fastest' compares runs with the same batch size. Latest run per pipeline"
        f"\n  and batch size{'' if all_runs else ' (use --all-runs for every run)'}."
        "\n  Equivalence of the answers: python compare.py --engine all\n"
    )
    return True


# ------------------------------------------------------------------------------------------
def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show the stored results and execution metadata of an ETL run.",
    )
    pick = parser.add_argument_group("which run")
    pick.add_argument("--run-id", type=int, help="report this run id.")
    pick.add_argument(
        "--pipeline",
        type=canonical_engine_name,
        metavar="{mongodb,hive,pig,mapreduce}",
        help="latest succeeded run of this pipeline (aliases: mongo, mr).",
    )
    pick.add_argument("--batch-size", type=int, help="restrict the choice to this batch size.")
    show = parser.add_argument_group("what to show")
    show.add_argument("--full", action="store_true", help="print every result row.")
    show.add_argument(
        "--limit", type=int, default=DEFAULT_LIMIT, help="rows of Q1/Q3 to print (default 10)."
    )
    show.add_argument("--batches", action="store_true", help="list every batch.")
    show.add_argument("--export", type=Path, metavar="DIR", help="also write the results as CSV.")
    cmp_ = parser.add_argument_group("runtime comparison")
    cmp_.add_argument(
        "--compare", action="store_true", help="compare the runtimes of all pipelines."
    )
    cmp_.add_argument("--all-runs", action="store_true", help="with --compare: list every run.")
    cmp_.add_argument(
        "--any-input",
        action="store_true",
        help="include runs on other input files (default: the two official NASA files).",
    )
    return parser.parse_args(argv)


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
        if args.compare:
            ok = print_runtime_comparison(conn, args.batch_size, args.any_input, args.all_runs)
            return 0 if ok else 1

        if args.run_id is not None:
            run = reader.get_run(conn, args.run_id)
            if run is None:
                print(f"ERROR: run {args.run_id} does not exist", file=sys.stderr)
                return 1
        else:
            run = reader.latest_run(conn, pipeline=args.pipeline, batch_size=args.batch_size)
            if run is None:
                who = f" of pipeline '{args.pipeline}'" if args.pipeline else ""
                print(
                    f"ERROR: no succeeded run{who} found; run the controller first", file=sys.stderr
                )
                return 1

        print_run_report(conn, run, None if args.full else args.limit, args.batches)
        if args.export:
            for path in export_run(conn, run, args.export):
                print(f"  wrote {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
