"""Hive pipeline engine (Contract 1): parsing and all three queries run inside Hive.

Python's only jobs (Hard Rule 3): stage the raw lines of a batch in a text file that
HiveServer2 can read, send the HiveQL from ``hiveql.py``, and read counts, up to 5
malformed samples and the final query rows back. No regex, filtering or grouping in Python.

Flow per run::

    setup()          CREATE DATABASE/TABLEs IF NOT EXISTS, drop stale partitions of run_id
    process_batch()  write lines -> LOAD DATA (into HDFS, raw_logs partition)
                     -> one multi-insert (parse in Hive: parsed_logs + malformed_logs)
                     -> count both -> fetch <=5 malformed -> drop the raw partition
    finalize()       Q1, Q2, Q3 over every batch of the run, each timed
    cleanup()        drop the run's partitions (unless --keep-data), close the connection

Connection settings come from the environment / ``.env`` (see ``.env.example``):
``HIVE_HOST``, ``HIVE_PORT``, ``HIVE_DATABASE``, ``HIVE_STAGING_DIR`` (host folder that is
mounted into the hive-server container as ``HIVE_STAGING_MOUNT``).
"""

from __future__ import annotations

import os
import socket
import time
from pathlib import Path
from typing import Any

import controller.config  # noqa: F401  (loads .env before the os.getenv calls below)
from pipelines.base import BatchStats, Engine, FinalResult, QueryResult
from pipelines.hive import hiveql

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _hive_host() -> str:
    return os.getenv("HIVE_HOST", "localhost")


def _hive_port() -> int:
    return int(os.getenv("HIVE_PORT", "10000"))


def _hive_database() -> str:
    return os.getenv("HIVE_DATABASE", "nasa_etl")


def _staging_dir() -> Path:
    """Host folder the controller writes batch files to (mounted into hive-server)."""
    path = Path(os.getenv("HIVE_STAGING_DIR", ".hive_staging"))
    return path if path.is_absolute() else _PROJECT_ROOT / path


def _staging_mount() -> str:
    """Where that folder appears inside the hive-server container."""
    return os.getenv("HIVE_STAGING_MOUNT", "/staging").rstrip("/")


class HiveEngine(Engine):
    """Apache Hive (HiveQL on HDFS) implementation of the NASA log ETL engine."""

    name = "hive"

    def __init__(self) -> None:
        self.host = _hive_host()
        self.port = _hive_port()
        self.database = _hive_database()
        self._conn: Any = None
        self._cursor: Any = None

    # ------------------------------------------------------------------ connection
    def _open(self, database: str = "default", timeout_sec: float | None = None) -> Any:
        """Open a HiveServer2 connection (NOSASL, no native 'sasl' package needed).

        A buffered binary transport is exactly what ``auth="NOSASL"`` builds; passing the
        transport ourselves is what lets the availability check use a short socket timeout.
        """
        # Imported lazily so that importing the engine registry works without PyHive/thrift.
        from pyhive import hive
        from thrift.transport import TSocket, TTransport

        sock = TSocket.TSocket(self.host, self.port)
        if timeout_sec is not None:
            sock.setTimeout(int(timeout_sec * 1000))
        transport = TTransport.TBufferedTransport(sock)
        return hive.connect(thrift_transport=transport, database=database)

    def is_available(self) -> bool:
        """True when HiveServer2 answers; False (not an error) when Hadoop/Hive is not running.

        The contract test calls this and *skips* instead of failing, which keeps CI green
        on machines without Hadoop.
        """
        try:
            with socket.create_connection((self.host, self.port), timeout=2):
                pass
        except OSError:
            return False
        try:
            conn = self._open(timeout_sec=15)
            try:
                cur = conn.cursor()
                cur.execute("SHOW DATABASES")
                cur.fetchall()
            finally:
                conn.close()
            return True
        except Exception:
            return False

    def _cur(self) -> Any:
        if self._cursor is None:
            self._conn = self._open()
            self._cursor = self._conn.cursor()
            self._cursor.execute(hiveql.create_database(self.database))
            self._cursor.execute(f"USE {self.database}")
        return self._cursor

    def _execute(self, sql: str) -> None:
        self._cur().execute(sql)

    def _query(self, sql: str) -> list[tuple]:
        cur = self._cur()
        cur.execute(sql)
        return list(cur.fetchall())

    def _close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
        self._conn = None
        self._cursor = None

    # ------------------------------------------------------------------ Contract 1
    def setup(self, run_id: int) -> None:
        """Create the three partitioned tables (once) and clear leftovers of this run_id."""
        _staging_dir().mkdir(parents=True, exist_ok=True)
        for ddl in hiveql.create_tables():
            self._execute(ddl)
        for stmt in hiveql.drop_run_partitions(run_id):
            self._execute(stmt)

    def process_batch(
        self,
        run_id: int,
        batch_id: int,
        lines: list[str],
        first_line_no: int,
    ) -> BatchStats:
        """Stage the batch, load it into HDFS and parse it with HiveQL."""
        if not lines:
            return BatchStats(record_count=0, parsed_count=0, malformed_count=0)

        # 1. Raw lines -> a UTF-8 text file in the folder shared with the hive-server container.
        #    Hive reads text as UTF-8; the Latin-1 characters survive the round trip. Each line
        #    gets a "<line_no><TAB>" prefix (framing only) so malformed lines keep their number.
        staged = _staging_dir() / f"run{run_id}_batch{batch_id}.txt"
        with open(staged, "w", encoding="utf-8", newline="\n") as f:
            f.writelines(f"{first_line_no + i}\t{line}\n" for i, line in enumerate(lines))
        try:
            # 2. LOAD DATA copies the file into HDFS under the raw_logs partition.
            self._execute(hiveql.load_batch(f"{_staging_mount()}/{staged.name}", run_id, batch_id))
        finally:
            staged.unlink(missing_ok=True)

        # 3. Parse inside Hive: one scan, parsed rows and malformed rows written to ORC.
        self._execute(hiveql.parse_batch(run_id, batch_id))

        # 4. Read the counts back from the tables Hive wrote.
        counts = {int(k): int(n) for k, n in self._query(hiveql.count_batch(run_id, batch_id))}
        parsed_count = counts.get(1, 0)
        malformed_count = counts.get(0, 0)
        if parsed_count + malformed_count != len(lines):
            raise RuntimeError(
                f"Hive batch {batch_id}: parsed ({parsed_count}) + malformed ({malformed_count}) "
                f"!= lines sent ({len(lines)}). A line was split or lost in transport "
                "(Hadoop also splits records on a lone carriage return)."
            )

        # 5. Up to 5 malformed samples (line_no, raw_line), lowest line numbers first.
        samples: list[tuple[int, str]] = []
        if malformed_count:
            rows = self._query(hiveql.malformed_samples(run_id, batch_id, limit=5))
            samples = sorted((int(no), str(line)) for no, line in rows)

        # 5b. The raw copy is no longer needed.
        self._execute(hiveql.drop_raw_partition(run_id, batch_id))

        return BatchStats(
            record_count=len(lines),
            parsed_count=parsed_count,
            malformed_count=malformed_count,
            malformed_samples=samples,
        )

    def _timed(self, sql: str) -> tuple[list[tuple], float]:
        t0 = time.perf_counter()
        rows = self._query(sql)
        return rows, time.perf_counter() - t0

    def finalize(self, run_id: int) -> FinalResult:
        """Run Q1, Q2 and Q3 in Hive over all batches of the run; each query is timed."""
        r1, t1 = self._timed(hiveql.q1(run_id))
        q1_rows = [
            {
                "log_date": str(d),
                "status_code": int(s),
                "request_count": int(c),
                "total_bytes": int(b),
            }
            for d, s, c, b in r1
        ]

        r2, t2 = self._timed(hiveql.q2(run_id))
        q2_rows = [
            {
                "rank": rank,
                "resource_path": str(p),
                "request_count": int(c),
                "total_bytes": int(b),
                "distinct_host_count": int(h),
            }
            for rank, (p, c, b, h) in enumerate(r2, start=1)
        ]

        r3, t3 = self._timed(hiveql.q3(run_id))
        q3_rows = [
            {
                "log_date": str(d),
                "log_hour": int(h),
                "error_request_count": int(e),
                "total_request_count": int(t),
                "error_rate": float(rate),
                "distinct_error_hosts": int(dh),
            }
            for d, h, e, t, rate, dh in r3
        ]

        return FinalResult(
            q1=QueryResult(query_id="q1", rows=q1_rows, runtime_sec=t1),
            q2=QueryResult(query_id="q2", rows=q2_rows, runtime_sec=t2),
            q3=QueryResult(query_id="q3", rows=q3_rows, runtime_sec=t3),
        )

    def cleanup(self, run_id: int) -> None:
        """Drop this run's partitions (and thus its HDFS files); always close the connection."""
        try:
            for stmt in hiveql.drop_run_partitions(run_id):
                self._execute(stmt)
        except Exception:
            pass
        finally:
            self._close()
            for stale in _staging_dir().glob(f"run{run_id}_batch*.txt"):
                stale.unlink(missing_ok=True)
