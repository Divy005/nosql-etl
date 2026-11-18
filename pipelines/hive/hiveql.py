"""HiveQL statements for the Hive pipeline.

Every piece of data processing the Hive engine does is written here as HiveQL, so this one
file is "the Hive pipeline" (parsing, cleaning, derived fields, the three queries). The
Python in ``engine.py`` only moves raw lines in, runs these statements and reads results back.

Layout inside the Hive database (``HIVE_DATABASE``, default ``nasa_etl``):

==================  ============================================  =========================
table               holds                                         storage
==================  ============================================  =========================
``raw_logs``        the raw lines of one batch (``line_no``,       TEXTFILE + RegexSerDe
                    ``line``), partitioned by (run_id, batch_id)
``parsed_logs``     one row per parsed line with all derived       ORC
                    fields, partitioned by (run_id, batch_id)
``malformed_logs``  one row per malformed line (``line_no``,       ORC
                    ``line``), partitioned by (run_id, batch_id)
==================  ============================================  =========================

Why these choices (each one was found on the real data / fixture):

* **RegexSerDe with ``(.*)``, not a plain text table.** Hive's default field delimiter is
  ``\\x01`` and 7 real log lines contain that byte, so a one-column text table would cut them
  short. The regex keeps the whole line.
* **A ``line_no<TAB>`` prefix on every transported line.** The engine needs the line number
  of malformed lines (spec: samples are ``(line_no, raw_line)``) and Hive has no per-line
  row number. The prefix is only framing; the raw line is everything after the first tab.
  ``RegexSerDe`` fills STRING columns only in Hive 2.3, hence ``line_no STRING`` here and a
  cast to BIGINT when parsing.
* **ORC for parsed/malformed.** A text table would corrupt values that contain ``\\x01``.
* **``(?d)`` in front of every regex.** Java's ``.`` and ``$`` also treat ``\\r``, U+0085,
  U+2028 and U+2029 as line terminators; Python and MongoDB (PCRE) only treat ``\\n`` as one.
  ``(?d)`` (UNIX_LINES) makes Java behave like them, so a line containing e.g. U+0085 is
  classified identically in every engine. ``\\s`` ``\\S`` ``\\d`` are ASCII in Java already.
* **Integer arithmetic for ``error_rate``.** ``ROUND()`` rounds a double and can disagree
  with half-away-from-zero (spec: 1/32 = 0.03125 must give 0.0313).
"""

from __future__ import annotations

# --- the two regexes of spec/parsing_spec.md, byte for byte (checked by a unit test) --------
LINE_REGEX = (
    r'^(\S+) \S+ \S+ \[(\d{2})/([A-Za-z]{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2}) ([+-]\d{4})\] "(.*)"'
    r" (\d{3}) (\d+|-)\s*$"
)
REQUEST_REGEX = r"^(\S+)\s+(.*?)(?:\s+(HTTP/\S+))?\s*$"

JAVA_FLAGS = "(?d)"  # UNIX_LINES: only \n is a line terminator, as in Python / PCRE
LINE_PREFIX_REGEX = JAVA_FLAGS + r"^(\d+)\t(.*)$"  # RegexSerDe: "<line_no><TAB><raw line>"

MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

TABLES = ("raw_logs", "parsed_logs", "malformed_logs")


def literal(text: str) -> str:
    """Quote ``text`` as a HiveQL string literal.

    Hive unescapes backslashes inside string literals, so every backslash is doubled
    (``\\S`` must reach the regex engine as ``\\S``, not ``S``), and ``'`` is escaped.
    """
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


_LINE_RE = literal(JAVA_FLAGS + LINE_REGEX)
_REQ_RE = literal(JAVA_FLAGS + REQUEST_REGEX)


def _month_number(col: str) -> str:
    cases = " ".join(f"WHEN '{m}' THEN '{i:02d}'" for i, m in enumerate(MONTHS, start=1))
    return f"CASE {col} {cases} END"


# ------------------------------------------------------------------------------------------
# setup / cleanup
# ------------------------------------------------------------------------------------------
def create_database(db: str) -> str:
    return f"CREATE DATABASE IF NOT EXISTS {db}"


def create_tables() -> list[str]:
    """DDL for the three run-partitioned tables (idempotent)."""
    partitioned = "PARTITIONED BY (run_id INT, batch_id INT)"
    return [
        f"""CREATE TABLE IF NOT EXISTS raw_logs (line_no STRING, line STRING)
{partitioned}
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES ('input.regex' = {literal(LINE_PREFIX_REGEX)})
STORED AS TEXTFILE""",
        f"""CREATE TABLE IF NOT EXISTS parsed_logs (
  line_no BIGINT, host STRING, log_timestamp STRING, log_date STRING, log_hour INT,
  http_method STRING, resource_path STRING, protocol_version STRING,
  status_code INT, bytes_transferred BIGINT)
{partitioned}
STORED AS ORC""",
        f"""CREATE TABLE IF NOT EXISTS malformed_logs (line_no BIGINT, line STRING)
{partitioned}
STORED AS ORC""",
    ]


def drop_run_partitions(run_id: int) -> list[str]:
    """Remove every partition (all batches) of one run from all three tables."""
    return [f"ALTER TABLE {t} DROP IF EXISTS PARTITION (run_id = {int(run_id)})" for t in TABLES]


# ------------------------------------------------------------------------------------------
# per batch
# ------------------------------------------------------------------------------------------
def load_batch(container_path: str, run_id: int, batch_id: int) -> str:
    """Copy one staged text file (inside the HiveServer2 container) into HDFS."""
    return (
        f"LOAD DATA LOCAL INPATH {literal(container_path)} OVERWRITE INTO TABLE raw_logs "
        f"PARTITION (run_id = {int(run_id)}, batch_id = {int(batch_id)})"
    )


def parse_batch(run_id: int, batch_id: int) -> str:
    """Parse one raw batch inside Hive and split it into parsed / malformed rows.

    One scan of the raw partition, one multi-insert. A line is *parsed* when the whole-line
    regex matches AND the month is one of Jan..Dec (case-sensitive); everything else is
    *malformed* and kept (never dropped). A request that does not split (e.g. ``""``) still
    gives a parsed line with method, path and protocol all ``''``.
    """
    part = f"run_id = {int(run_id)} AND batch_id = {int(batch_id)}"
    months = ", ".join(f"'{m}'" for m in MONTHS)
    return f"""FROM (
  SELECT line_no, line, host, mon, dd, yyyy, hh, mi, ss, req, st, nbytes,
         (matched AND mon IN ({months})) AS ok
  FROM (
    SELECT CAST(line_no AS BIGINT) AS line_no, line,
           (line RLIKE {_LINE_RE}) AS matched,
           regexp_extract(line, {_LINE_RE}, 1) AS host,
           regexp_extract(line, {_LINE_RE}, 2) AS dd,
           regexp_extract(line, {_LINE_RE}, 3) AS mon,
           regexp_extract(line, {_LINE_RE}, 4) AS yyyy,
           regexp_extract(line, {_LINE_RE}, 5) AS hh,
           regexp_extract(line, {_LINE_RE}, 6) AS mi,
           regexp_extract(line, {_LINE_RE}, 7) AS ss,
           regexp_extract(line, {_LINE_RE}, 9) AS req,
           regexp_extract(line, {_LINE_RE}, 10) AS st,
           regexp_extract(line, {_LINE_RE}, 11) AS nbytes
    FROM raw_logs WHERE {part}
  ) x
) y
INSERT OVERWRITE TABLE parsed_logs PARTITION (run_id = {int(run_id)}, batch_id = {int(batch_id)})
SELECT line_no,
       host,
       concat(yyyy, '-', {_month_number("mon")}, '-', dd, ' ', hh, ':', mi, ':', ss),
       concat(yyyy, '-', {_month_number("mon")}, '-', dd),
       CAST(hh AS INT),
       CASE WHEN req RLIKE {_REQ_RE}
            THEN COALESCE(regexp_extract(req, {_REQ_RE}, 1), '') ELSE '' END,
       CASE WHEN req RLIKE {_REQ_RE}
            THEN COALESCE(regexp_extract(req, {_REQ_RE}, 2), '') ELSE '' END,
       CASE WHEN req RLIKE {_REQ_RE}
            THEN COALESCE(regexp_extract(req, {_REQ_RE}, 3), '') ELSE '' END,
       CAST(st AS INT),
       CASE WHEN nbytes = '-' THEN CAST(0 AS BIGINT) ELSE CAST(nbytes AS BIGINT) END
WHERE ok
INSERT OVERWRITE TABLE malformed_logs PARTITION (run_id = {int(run_id)}, batch_id = {int(batch_id)})
SELECT line_no, line
WHERE NOT ok"""


def count_batch(run_id: int, batch_id: int) -> str:
    """parsed (k=1) and malformed (k=0) row counts of one batch, read back from the tables."""
    part = f"run_id = {int(run_id)} AND batch_id = {int(batch_id)}"
    return f"""SELECT k, COUNT(*) AS n FROM (
  SELECT 1 AS k FROM parsed_logs WHERE {part}
  UNION ALL
  SELECT 0 AS k FROM malformed_logs WHERE {part}
) u GROUP BY k"""


def malformed_samples(run_id: int, batch_id: int, limit: int = 5) -> str:
    """Up to ``limit`` malformed lines of a batch (a plain fetch, no MapReduce job)."""
    return (
        f"SELECT line_no, line FROM malformed_logs "
        f"WHERE run_id = {int(run_id)} AND batch_id = {int(batch_id)} LIMIT {int(limit)}"
    )


def drop_raw_partition(run_id: int, batch_id: int) -> str:
    """Delete the raw copy of a batch once it is parsed (keeps HDFS usage bounded)."""
    return (
        f"ALTER TABLE raw_logs DROP IF EXISTS "
        f"PARTITION (run_id = {int(run_id)}, batch_id = {int(batch_id)})"
    )


# ------------------------------------------------------------------------------------------
# the three mandatory queries, over ALL batches of the run
# ------------------------------------------------------------------------------------------
def q1(run_id: int) -> str:
    """Query 1: daily traffic summary."""
    return f"""SELECT log_date, status_code,
       COUNT(*) AS request_count,
       SUM(bytes_transferred) AS total_bytes
FROM parsed_logs
WHERE run_id = {int(run_id)}
GROUP BY log_date, status_code
ORDER BY log_date, status_code"""


def q2(run_id: int) -> str:
    """Query 2: top 20 resources; ties at the cut broken by resource_path (UTF-8 byte order)."""
    return f"""SELECT resource_path,
       COUNT(*) AS request_count,
       SUM(bytes_transferred) AS total_bytes,
       COUNT(DISTINCT host) AS distinct_host_count
FROM parsed_logs
WHERE run_id = {int(run_id)}
GROUP BY resource_path
ORDER BY request_count DESC, resource_path ASC
LIMIT 20"""


def q3(run_id: int) -> str:
    """Query 3: hourly error analysis (error = status 400..599).

    ``error_rate = round_half_away(err / total, 4)`` computed with integers:
    ``(err * 20000 + total) DIV (2 * total) / 10000``.
    """
    return f"""SELECT log_date, log_hour, error_request_count, total_request_count,
       ((error_request_count * 20000 + total_request_count) DIV (2 * total_request_count))
         / 10000.0 AS error_rate,
       distinct_error_hosts
FROM (
  SELECT log_date, log_hour,
         SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) AS error_request_count,
         COUNT(*) AS total_request_count,
         COUNT(DISTINCT CASE WHEN status_code BETWEEN 400 AND 599 THEN host END)
           AS distinct_error_hosts
  FROM parsed_logs
  WHERE run_id = {int(run_id)}
  GROUP BY log_date, log_hour
) h
ORDER BY log_date, log_hour"""
