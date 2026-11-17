"""Reference implementation of parsing_spec.md in pure Python.

Used ONLY to generate ground truth expected files (spec/expected/q1.csv,
q2.csv, q3.csv, expected_counts.json) and verify test fixtures.
This is never used as a production pipeline.
"""

import csv
import json
import re
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

# Line validation regex (anchored strictly from ^ to $)
LINE_REGEX = re.compile(
    r"^(\S+) \S+ \S+ \[(\d{2})/([A-Za-z]{3})/(\d{4}):"
    r"(\d{2}):(\d{2}):(\d{2}) ([+-]\d{4})\] \"(.*)\" (\d{3}) (\d+|-)\s*$"
)

# Secondary request splitting regex
REQUEST_REGEX = re.compile(r"^(\S+)\s+(.*?)(?:\s+(HTTP/\S+))?\s*$")

# Month token to 2-digit zero-padded string mapping
MONTH_MAP = {
    "Jan": "01",
    "Feb": "02",
    "Mar": "03",
    "Apr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dec": "12",
}


def round_half_away_from_zero(val: float, decimals: int = 4) -> float:
    """Perform exact round-half-away-from-zero rounding."""
    d = Decimal(str(val))
    target = Decimal("10") ** -decimals
    return float(d.quantize(target, rounding=ROUND_HALF_UP))


def parse_line(raw_line: str) -> dict[str, Any] | None:
    """Parse a single raw log line according to parsing_spec.md.

    Returns a dictionary of parsed fields if valid, or None if malformed.
    """
    # Strip only trailing line terminators
    line = raw_line.rstrip("\r\n")

    match = LINE_REGEX.match(line)
    if not match:
        return None

    (
        host,
        day,
        month,
        year,
        hour_str,
        minute_str,
        second_str,
        _tz,
        request_raw,
        status_str,
        bytes_str,
    ) = match.groups()

    if month not in MONTH_MAP:
        return None

    month_num = MONTH_MAP[month]
    log_date = f"{year}-{month_num}-{day}"
    log_hour = int(hour_str)
    timestamp = f"{log_date} {hour_str}:{minute_str}:{second_str}"
    status_code = int(status_str)
    bytes_transferred = 0 if bytes_str == "-" else int(bytes_str)

    # Request decomposition
    req_match = REQUEST_REGEX.match(request_raw)
    if req_match:
        http_method = req_match.group(1)
        resource_path = req_match.group(2)
        protocol_version = req_match.group(3) or ""
    else:
        http_method = ""
        resource_path = ""
        protocol_version = ""

    return {
        "host": host,
        "timestamp": timestamp,
        "log_date": log_date,
        "log_hour": log_hour,
        "http_method": http_method,
        "resource_path": resource_path,
        "protocol_version": protocol_version,
        "status_code": status_code,
        "bytes_transferred": bytes_transferred,
    }


def process_log_file(filepath: Path) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Read and parse an entire log file, preserving order and counting malformed lines."""
    parsed_records: list[dict[str, Any]] = []
    total = 0
    malformed = 0

    with open(filepath, "rb") as f:
        for raw_bytes in f:
            total += 1
            line = raw_bytes.decode("latin-1")
            parsed = parse_line(line)
            if parsed is not None:
                parsed_records.append(parsed)
            else:
                malformed += 1

    counts = {
        "total": total,
        "parsed": len(parsed_records),
        "malformed": malformed,
    }
    return parsed_records, counts


def compute_q1(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Query 1: Daily Traffic Summary.

    Group by (log_date, status_code) -> request_count, total_bytes
    """
    groups: dict[tuple[str, int], dict[str, int]] = {}
    for r in records:
        key = (r["log_date"], r["status_code"])
        if key not in groups:
            groups[key] = {"request_count": 0, "total_bytes": 0}
        groups[key]["request_count"] += 1
        groups[key]["total_bytes"] += r["bytes_transferred"]

    results = [
        {
            "log_date": key[0],
            "status_code": key[1],
            "request_count": metrics["request_count"],
            "total_bytes": metrics["total_bytes"],
        }
        for key, metrics in groups.items()
    ]
    # Standard deterministic ordering
    results.sort(key=lambda x: (x["log_date"], x["status_code"]))
    return results


def compute_q2(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Query 2: Top Requested Resources.

    Group by resource_path -> request_count, total_bytes, distinct_host_count.
    Order by request_count DESC, resource_path ASC (byte order).
    Limit top 20, rank 1..20.
    """
    groups: dict[str, dict[str, Any]] = {}
    for r in records:
        path = r["resource_path"]
        if path not in groups:
            groups[path] = {"request_count": 0, "total_bytes": 0, "hosts": set()}
        groups[path]["request_count"] += 1
        groups[path]["total_bytes"] += r["bytes_transferred"]
        groups[path]["hosts"].add(r["host"])

    raw_list = [
        {
            "resource_path": path,
            "request_count": data["request_count"],
            "total_bytes": data["total_bytes"],
            "distinct_host_count": len(data["hosts"]),
        }
        for path, data in groups.items()
    ]

    # Deterministic sort: request_count DESC, resource_path ASC
    raw_list.sort(key=lambda x: (-x["request_count"], x["resource_path"]))

    top_20 = raw_list[:20]
    for idx, item in enumerate(top_20, start=1):
        item["rank"] = idx

    # Ensure key order matches Contract 3
    return [
        {
            "rank": item["rank"],
            "resource_path": item["resource_path"],
            "request_count": item["request_count"],
            "total_bytes": item["total_bytes"],
            "distinct_host_count": item["distinct_host_count"],
        }
        for item in top_20
    ]


def compute_q3(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Query 3: Hourly Error Analysis.

    Group by (log_date, log_hour) for all date/hour combinations with >= 1 request.
    Error defined as 400 <= status_code <= 599.
    error_rate = round(error / total, 4) using round-half-away-from-zero.
    """
    groups: dict[tuple[str, int], dict[str, Any]] = {}
    for r in records:
        key = (r["log_date"], r["log_hour"])
        if key not in groups:
            groups[key] = {
                "total_request_count": 0,
                "error_request_count": 0,
                "error_hosts": set(),
            }
        groups[key]["total_request_count"] += 1
        if 400 <= r["status_code"] <= 599:
            groups[key]["error_request_count"] += 1
            groups[key]["error_hosts"].add(r["host"])

    results = []
    for (log_date, log_hour), data in groups.items():
        total = data["total_request_count"]
        error = data["error_request_count"]
        error_rate = round_half_away_from_zero(error / total, 4) if total > 0 else 0.0
        results.append(
            {
                "log_date": log_date,
                "log_hour": log_hour,
                "error_request_count": error,
                "total_request_count": total,
                "error_rate": error_rate,
                "distinct_error_hosts": len(data["error_hosts"]),
            }
        )

    results.sort(key=lambda x: (x["log_date"], x["log_hour"]))
    return results


def write_csv(filepath: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    """Write rows to a CSV file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_reference(log_path: Path, output_dir: Path) -> dict[str, int]:
    """Execute reference implementation on a log file and save expected outputs."""
    records, counts = process_log_file(log_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    q1_rows = compute_q1(records)
    write_csv(
        output_dir / "q1.csv",
        q1_rows,
        ["log_date", "status_code", "request_count", "total_bytes"],
    )

    q2_rows = compute_q2(records)
    write_csv(
        output_dir / "q2.csv",
        q2_rows,
        ["rank", "resource_path", "request_count", "total_bytes", "distinct_host_count"],
    )

    q3_rows = compute_q3(records)
    write_csv(
        output_dir / "q3.csv",
        q3_rows,
        [
            "log_date",
            "log_hour",
            "error_request_count",
            "total_request_count",
            "error_rate",
            "distinct_error_hosts",
        ],
    )

    with open(output_dir / "expected_counts.json", "w", encoding="utf-8") as f:
        json.dump(counts, f, indent=2)

    return counts


if __name__ == "__main__":
    import sys

    log_file = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("spec/test.log")
    out_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("spec/expected")
    counts = run_reference(log_file, out_dir)
    print(f"Generated expected files in {out_dir}. Counts: {counts}")
