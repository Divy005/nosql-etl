"""MongoDB pipeline execution engine conforming to Contract 1."""

import time
from typing import Any

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from controller.config import get_mongo_db, get_mongo_uri
from pipelines.base import BatchStats, Engine, FinalResult, QueryResult
from pipelines.mongodb.aggregations import (
    build_parsing_pipeline,
    build_q1_pipeline,
    build_q2_pipeline,
    build_q3_pipeline,
)


class MongoDBEngine(Engine):
    """MongoDB implementation of the NASA log ETL engine."""

    name = "mongodb"

    def __init__(self) -> None:
        self.uri: str = get_mongo_uri()
        self.db_name: str = get_mongo_db()
        self._client: MongoClient | None = None
        self._db: Database | None = None

    @property
    def db(self) -> Database:
        if self._db is None or self._client is None:
            self._client = MongoClient(self.uri)
            self._db = self._client[self.db_name]
        return self._db

    def _raw_col(self, run_id: int) -> Collection:
        return self.db[f"raw_r{run_id}"]

    def _parsed_col(self, run_id: int) -> Collection:
        return self.db[f"parsed_r{run_id}"]

    def setup(self, run_id: int) -> None:
        """Initialize run-scoped collections and create necessary indexes."""
        raw_col = self._raw_col(run_id)
        parsed_col = self._parsed_col(run_id)

        # Drop existing collections if any
        raw_col.drop()
        parsed_col.drop()

        # Create indexes on parsed collection for fast querying and batch filtering
        parsed_col.create_index([("batch_id", 1)])
        parsed_col.create_index([("valid", 1)])

    def process_batch(
        self,
        run_id: int,
        batch_id: int,
        lines: list[str],
        first_line_no: int,
    ) -> BatchStats:
        """Ingest, parse, and store a batch inside MongoDB.

        Hard Rule 3: Python only inserts raw lines into MongoDB and reads
        the final counts back. All regex parsing and derivation happens
        inside the MongoDB aggregation framework.
        """
        raw_col = self._raw_col(run_id)
        parsed_col = self._parsed_col(run_id)

        if not lines:
            return BatchStats(
                record_count=0,
                parsed_count=0,
                malformed_count=0,
                malformed_samples=[],
            )

        # 1. Insert raw lines
        raw_docs = [
            {"batch_id": batch_id, "line_no": first_line_no + idx, "line": line}
            for idx, line in enumerate(lines)
        ]
        raw_col.insert_many(raw_docs, ordered=False)

        # 2. Run in-engine parsing aggregation on this batch
        parse_pipeline = build_parsing_pipeline(batch_id, parsed_col.name)
        raw_col.aggregate(parse_pipeline)

        # 3. Compute parsed / malformed counts via aggregation
        count_pipeline: list[dict[str, Any]] = [
            {"$match": {"batch_id": batch_id}},
            {"$group": {"_id": "$valid", "count": {"$sum": 1}}},
        ]
        count_docs = list(parsed_col.aggregate(count_pipeline))

        parsed_count = 0
        malformed_count = 0
        for doc in count_docs:
            if doc["_id"] is True:
                parsed_count = doc["count"]
            elif doc["_id"] is False:
                malformed_count = doc["count"]

        # 4. Fetch up to 5 malformed samples for logging
        malformed_samples: list[tuple[int, str]] = []
        if malformed_count > 0:
            sample_cursor = parsed_col.find(
                {"batch_id": batch_id, "valid": False},
                {"line_no": 1, "line": 1, "_id": 0},
            ).limit(5)
            malformed_samples = [(doc["line_no"], doc["line"]) for doc in sample_cursor]

        # 5. Delete raw batch documents to keep raw storage bounded
        raw_col.delete_many({"batch_id": batch_id})

        return BatchStats(
            record_count=len(lines),
            parsed_count=parsed_count,
            malformed_count=malformed_count,
            malformed_samples=malformed_samples,
        )

    def finalize(self, run_id: int) -> FinalResult:
        """Execute Q1, Q2, and Q3 queries inside MongoDB over all accumulated batches."""
        parsed_col = self._parsed_col(run_id)

        # Query 1
        t0 = time.perf_counter()
        q1_docs = list(parsed_col.aggregate(build_q1_pipeline(), allowDiskUse=True))
        q1_time = time.perf_counter() - t0

        q1_rows = [
            {
                "log_date": doc["_id"]["log_date"],
                "status_code": int(doc["_id"]["status_code"]),
                "request_count": int(doc["request_count"]),
                "total_bytes": int(doc["total_bytes"]),
            }
            for doc in q1_docs
        ]

        # Query 2
        t0 = time.perf_counter()
        q2_docs = list(parsed_col.aggregate(build_q2_pipeline(), allowDiskUse=True))
        q2_time = time.perf_counter() - t0

        q2_rows = [
            {
                "rank": idx,
                "resource_path": str(doc["_id"]),
                "request_count": int(doc["request_count"]),
                "total_bytes": int(doc["total_bytes"]),
                "distinct_host_count": int(doc["distinct_host_count"]),
            }
            for idx, doc in enumerate(q2_docs, start=1)
        ]

        # Query 3
        t0 = time.perf_counter()
        q3_docs = list(parsed_col.aggregate(build_q3_pipeline(), allowDiskUse=True))
        q3_time = time.perf_counter() - t0

        q3_rows = [
            {
                "log_date": doc["_id"]["log_date"],
                "log_hour": int(doc["_id"]["log_hour"]),
                "error_request_count": int(doc["error_request_count"]),
                "total_request_count": int(doc["total_request_count"]),
                "error_rate": float(round(doc["error_rate"], 4)),
                "distinct_error_hosts": int(doc["distinct_error_hosts"]),
            }
            for doc in q3_docs
        ]

        return FinalResult(
            q1=QueryResult(query_id="q1", rows=q1_rows, runtime_sec=q1_time),
            q2=QueryResult(query_id="q2", rows=q2_rows, runtime_sec=q2_time),
            q3=QueryResult(query_id="q3", rows=q3_rows, runtime_sec=q3_time),
        )

    def cleanup(self, run_id: int) -> None:
        """Drop run-scoped collections."""
        try:
            self._raw_col(run_id).drop()
            self._parsed_col(run_id).drop()
        except Exception:
            pass
        finally:
            if self._client is not None:
                self._client.close()
                self._client = None
                self._db = None
