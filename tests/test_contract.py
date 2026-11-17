"""Contract tests: run any engine on spec/test.log and compare against spec/expected/.

Usage:
    pytest tests/test_contract.py --engine dummy
    pytest tests/test_contract.py --engine mongodb
"""

import csv
import json
from pathlib import Path

import pytest

from controller.batcher import read_batches
from pipelines import get_engine

SPEC_DIR = Path(__file__).resolve().parent.parent / "spec"
TEST_LOG = SPEC_DIR / "test.log"
EXPECTED_DIR = SPEC_DIR / "expected"

CONTRACT_BATCH_SIZE = 7  # Small batch size to exercise multiple batches + partial


def _load_expected_csv(filename: str) -> list[dict]:
    """Load a CSV from spec/expected/ and return a list of dicts with typed values."""
    rows = []
    with open(EXPECTED_DIR / filename, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def _load_expected_counts() -> dict:
    with open(EXPECTED_DIR / "expected_counts.json", encoding="utf-8") as f:
        return json.load(f)


def _skip_if_unavailable(engine_name: str) -> None:
    """Skip the test if the engine's backing service is unavailable."""
    if engine_name == "mongodb":
        try:
            from pymongo import MongoClient

            from controller.config import get_mongo_uri

            client = MongoClient(get_mongo_uri(), serverSelectionTimeoutMS=2000)
            client.admin.command("ping")
            client.close()
        except Exception:
            pytest.skip("MongoDB service unavailable")
    elif engine_name in ("hive", "pig", "mapreduce"):
        try:
            engine = get_engine(engine_name)  # noqa: F841
        except (NotImplementedError, ValueError, ImportError):
            pytest.skip(f"Engine '{engine_name}' is not implemented yet")


class TestContractCompliance:
    """Verify that an engine produces results matching the spec/expected/ ground truth."""

    @pytest.fixture(autouse=True)
    def _run_engine(self, engine_name: str) -> None:
        """Run the engine pipeline on spec/test.log and store results."""
        _skip_if_unavailable(engine_name)

        self.engine = get_engine(engine_name)
        run_id = 999  # Arbitrary test run_id

        self.engine.setup(run_id)

        total_records = 0
        total_parsed = 0
        total_malformed = 0

        for batch_id, first_line_no, lines in read_batches(
            [TEST_LOG], CONTRACT_BATCH_SIZE
        ):
            stats = self.engine.process_batch(run_id, batch_id, lines, first_line_no)
            total_records += stats.record_count
            total_parsed += stats.parsed_count
            total_malformed += stats.malformed_count
            assert stats.parsed_count + stats.malformed_count == stats.record_count

        self.total_records = total_records
        self.total_parsed = total_parsed
        self.total_malformed = total_malformed
        self.result = self.engine.finalize(run_id)
        self.engine.cleanup(run_id)

    def test_record_counts(self, engine_name: str) -> None:
        if engine_name == "dummy":
            # Dummy engine treats all lines as parsed (no real parsing)
            assert self.total_parsed + self.total_malformed == self.total_records
            return

        expected = _load_expected_counts()
        assert self.total_records == expected["total"]
        assert self.total_parsed == expected["parsed"]
        assert self.total_malformed == expected["malformed"]
        assert self.total_parsed + self.total_malformed == self.total_records

    def test_q1_daily_traffic(self, engine_name: str) -> None:
        if engine_name == "dummy":
            pytest.skip("Dummy engine returns mock data")

        expected_rows = _load_expected_csv("q1.csv")
        actual_rows = self.result.q1.rows

        assert len(actual_rows) == len(expected_rows), (
            f"Q1 row count mismatch: got {len(actual_rows)}, "
            f"expected {len(expected_rows)}"
        )

        for i, (actual, expected) in enumerate(zip(actual_rows, expected_rows, strict=True)):
            assert actual["log_date"] == expected["log_date"], f"Q1 row {i}: log_date"
            assert actual["status_code"] == int(expected["status_code"]), (
                f"Q1 row {i}: status_code"
            )
            assert actual["request_count"] == int(expected["request_count"]), (
                f"Q1 row {i}: request_count"
            )
            assert actual["total_bytes"] == int(expected["total_bytes"]), (
                f"Q1 row {i}: total_bytes"
            )

    def test_q2_top_resources(self, engine_name: str) -> None:
        if engine_name == "dummy":
            pytest.skip("Dummy engine returns mock data")

        expected_rows = _load_expected_csv("q2.csv")
        actual_rows = self.result.q2.rows

        assert len(actual_rows) == len(expected_rows), (
            f"Q2 row count mismatch: got {len(actual_rows)}, "
            f"expected {len(expected_rows)}"
        )

        for i, (actual, expected) in enumerate(zip(actual_rows, expected_rows, strict=True)):
            assert actual["rank"] == int(expected["rank"]), f"Q2 row {i}: rank"
            assert actual["resource_path"] == expected["resource_path"], (
                f"Q2 row {i}: resource_path: "
                f"got {actual['resource_path']!r}, "
                f"expected {expected['resource_path']!r}"
            )
            assert actual["request_count"] == int(expected["request_count"]), (
                f"Q2 row {i}: request_count"
            )
            assert actual["total_bytes"] == int(expected["total_bytes"]), (
                f"Q2 row {i}: total_bytes"
            )
            assert actual["distinct_host_count"] == int(
                expected["distinct_host_count"]
            ), f"Q2 row {i}: distinct_host_count"

    def test_q3_hourly_errors(self, engine_name: str) -> None:
        if engine_name == "dummy":
            pytest.skip("Dummy engine returns mock data")

        expected_rows = _load_expected_csv("q3.csv")
        actual_rows = self.result.q3.rows

        assert len(actual_rows) == len(expected_rows), (
            f"Q3 row count mismatch: got {len(actual_rows)}, "
            f"expected {len(expected_rows)}"
        )

        for i, (actual, expected) in enumerate(zip(actual_rows, expected_rows, strict=True)):
            assert actual["log_date"] == expected["log_date"], f"Q3 row {i}: log_date"
            assert actual["log_hour"] == int(expected["log_hour"]), (
                f"Q3 row {i}: log_hour"
            )
            assert actual["error_request_count"] == int(
                expected["error_request_count"]
            ), f"Q3 row {i}: error_request_count"
            assert actual["total_request_count"] == int(
                expected["total_request_count"]
            ), f"Q3 row {i}: total_request_count"
            assert round(actual["error_rate"], 4) == float(expected["error_rate"]), (
                f"Q3 row {i}: error_rate: "
                f"got {actual['error_rate']}, expected {expected['error_rate']}"
            )
            assert actual["distinct_error_hosts"] == int(
                expected["distinct_error_hosts"]
            ), f"Q3 row {i}: distinct_error_hosts"
