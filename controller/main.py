"""CLI entry point for the NASA ETL controller.

Usage:
    python -m controller.main --pipeline mongodb --batch-size 100000
    python -m controller.main                      # interactive menu
"""

import argparse
import sys
import traceback
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from tabulate import tabulate

from controller.batcher import read_batches
from controller.config import get_pg_dsn
from controller.timing import Timer, TimingBreakdown
from db.loader import create_run, fail_run, finish_run, load_results, record_batch
from pipelines import get_engine, list_available_engines

MENU_ENGINES = ["mongodb", "hive", "pig", "mapreduce"]


def _interactive_menu() -> tuple[str, int]:
    """Show a numbered menu and prompt for engine + batch size."""
    print("\n=== NASA ETL Pipeline Engine Selection ===\n")
    for i, name in enumerate(MENU_ENGINES, start=1):
        print(f"  {i}. {name.title()}")
    print()

    while True:
        choice = input("Select engine [1-4]: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(MENU_ENGINES):
            engine_name = MENU_ENGINES[int(choice) - 1]
            break
        print("Invalid choice. Enter a number between 1 and 4.")

    batch_input = input("Batch size [default 100000]: ").strip()
    batch_size = int(batch_input) if batch_input else 100000

    return engine_name, batch_size


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="NASA HTTP Log ETL Controller",
        prog="python -m controller.main",
    )
    parser.add_argument(
        "--pipeline",
        type=str,
        default=None,
        choices=list_available_engines(),
        help="Engine to use for processing.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Number of lines per batch (default: 100000).",
    )
    parser.add_argument(
        "--input",
        nargs="+",
        type=Path,
        default=None,
        dest="input_files",
        help="Input log file paths. Default: both NASA files in DATA_DIR.",
    )
    parser.add_argument(
        "--keep-data",
        action="store_true",
        default=False,
        help="Do not drop engine-side temporary data after the run.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    # Resolve engine name and batch size
    if args.pipeline is None:
        engine_name, batch_size = _interactive_menu()
    else:
        engine_name = args.pipeline
        batch_size = args.batch_size or 100000

    # Resolve input files
    if args.input_files:
        input_files = args.input_files
    else:
        from controller.config import default_input_files

        input_files = default_input_files()

    # Validate input files exist
    for f in input_files:
        if not f.exists():
            print(f"ERROR: Input file not found: {f}", file=sys.stderr)
            sys.exit(1)

    # Instantiate engine
    try:
        engine = get_engine(engine_name)
    except (NotImplementedError, ValueError, ImportError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"\n{'='*60}")
    print("  NASA ETL Pipeline Run")
    print(f"  Engine:     {engine.name}")
    print(f"  Batch size: {batch_size:,}")
    print(f"  Input:      {', '.join(str(f) for f in input_files)}")
    print(f"{'='*60}\n")

    # Connect to PostgreSQL
    conn = psycopg.connect(get_pg_dsn(), autocommit=False)
    run_id: int | None = None

    try:
        # Create run row
        run_id = create_run(
            conn,
            pipeline=engine.name,
            batch_size=batch_size,
            input_files=[str(f) for f in input_files],
        )
        print(f"[run_id={run_id}] Created run row (status=running)")

        # Setup engine
        engine.setup(run_id)
        print(f"[run_id={run_id}] Engine setup complete")

        timing = TimingBreakdown()
        total_timer = Timer()
        total_records = 0
        parsed_records = 0
        malformed_records = 0
        num_batches = 0
        last_batch_id = 0

        # --- TIMER START ---
        total_timer.start()

        for batch_id, first_line_no, lines in read_batches(input_files, batch_size):
            batch_timer = Timer().start()
            batch_started_at = datetime.now(UTC)

            stats = engine.process_batch(run_id, batch_id, lines, first_line_no)

            batch_elapsed = batch_timer.stop()
            batch_finished_at = datetime.now(UTC)

            timing.add_ingest(batch_elapsed)

            total_records += stats.record_count
            parsed_records += stats.parsed_count
            malformed_records += stats.malformed_count
            num_batches += 1
            last_batch_id = batch_id

            # Record batch metadata in Postgres
            record_batch(
                conn,
                run_id=run_id,
                batch_id=batch_id,
                stats=stats,
                first_line_no=first_line_no,
                started_at=batch_started_at,
                finished_at=batch_finished_at,
                runtime_sec=batch_elapsed,
            )

            if num_batches % 5 == 0 or len(lines) < batch_size:
                print(
                    f"  Batch {batch_id:>4d}: {stats.record_count:>8,} lines "
                    f"({stats.parsed_count} parsed, {stats.malformed_count} malformed) "
                    f"[{batch_elapsed:.3f}s]"
                )

        print(f"\n[run_id={run_id}] Ingestion complete: {num_batches} batches")

        # --- FINALIZE (aggregate queries) ---
        agg_timer = Timer().start()
        result = engine.finalize(run_id)
        timing.aggregate = agg_timer.stop()

        print(
            f"[run_id={run_id}] Aggregation complete: "
            f"Q1={len(result.q1.rows)} rows ({result.q1.runtime_sec:.3f}s), "
            f"Q2={len(result.q2.rows)} rows ({result.q2.runtime_sec:.3f}s), "
            f"Q3={len(result.q3.rows)} rows ({result.q3.runtime_sec:.3f}s)"
        )

        # --- LOAD results to PostgreSQL ---
        load_timer = Timer().start()
        executed_at = datetime.now(UTC)

        load_results(
            conn,
            run_id=run_id,
            pipeline=engine.name,
            last_batch_id=last_batch_id,
            result=result,
            executed_at=executed_at,
        )

        finish_run(
            conn,
            run_id=run_id,
            total_records=total_records,
            parsed_records=parsed_records,
            malformed_records=malformed_records,
            num_batches=num_batches,
            runtime_sec=total_timer.elapsed,
            ingest_runtime_sec=timing.ingest,
            aggregate_runtime_sec=timing.aggregate,
            load_runtime_sec=load_timer.elapsed,
        )
        timing.load = load_timer.stop()

        # --- TIMER STOP ---
        timing.total = total_timer.stop()

        # --- Console summary ---
        avg_batch = total_records / num_batches if num_batches else 0
        summary_data = [
            ["Run ID", run_id],
            ["Pipeline", engine.name],
            ["Batches", num_batches],
            ["Avg Batch Size", f"{avg_batch:,.1f}"],
            ["Total Records", f"{total_records:,}"],
            ["Parsed Records", f"{parsed_records:,}"],
            ["Malformed Records", f"{malformed_records:,}"],
            ["", ""],
            ["Total Runtime", f"{timing.total:.4f}s"],
            ["  Ingest", f"{timing.ingest:.4f}s"],
            ["  Aggregate", f"{timing.aggregate:.4f}s"],
            ["  Load", f"{timing.load:.4f}s"],
        ]

        print(f"\n{'='*60}")
        print("  Run Summary")
        print(f"{'='*60}")
        print(tabulate(summary_data, tablefmt="plain"))
        print(f"{'='*60}\n")

    except Exception:
        if run_id is not None:
            try:
                fail_run(conn, run_id, traceback.format_exc()[-500:])
            except Exception:
                pass  # Best effort
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Cleanup engine-side temporary data
        if run_id is not None and not args.keep_data:
            try:
                engine.cleanup(run_id)
            except Exception:
                pass
        conn.close()


if __name__ == "__main__":
    main()
