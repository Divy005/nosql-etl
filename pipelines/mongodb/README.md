# MongoDB Aggregation Pipeline

> **Owner:** Divy (Phase 1)  
> **Status:** Fully Implemented & Contract-Verified ✅

---

## 1. Architecture & Design Principles

The MongoDB pipeline adheres strictly to **Hard Rule 3**:
* **Zero client-side parsing:** Python code never uses regexes or performs grouping on log data. Python only reads raw lines from the disk, inserts them into a temporary raw collection (`raw_r{run_id}`), triggers an in-engine aggregation pipeline, and retrieves the final query outputs.
* **Run-Scoped Isolation:** Collections are created dynamically per run ID:
  - `raw_r{run_id}`: Staging collection holding raw lines for the current batch. Cleaned up immediately after the batch parsing aggregation completes.
  - `parsed_r{run_id}`: Destination collection holding all parsed records and flagged malformed records for the duration of the run.
* **Bounded Memory Usage:** Raw lines are removed from `raw_r{run_id}` after each batch is processed with `$merge`, keeping raw disk consumption minimal.

---

## 2. In-Engine Parsing Aggregation Pipeline

The batch parsing is executed entirely inside the database engine using MongoDB's `$regexFind` operator and pipeline stages:

```javascript
[
  { "$match": { "batch_id": batch_id } },
  {
    "$project": {
      "batch_id": 1, "line_no": 1, "line": 1,
      "match": { "$regexFind": { "input": "$line", "regex": LINE_REGEX_PATTERN } }
    }
  },
  // Extract month string and resolve index in ["Jan", "Feb", ... "Dec"]
  {
    "$project": {
      "batch_id": 1, "line_no": 1, "line": 1, "match": 1,
      "month_idx": { "$indexOfArray": [MONTHS, { "$arrayElemAt": ["$match.captures", 2] }] }
    }
  },
  // Document is valid if and only if match found AND month index >= 0
  {
    "$project": {
      "batch_id": 1, "line_no": 1, "line": 1,
      "valid": { "$and": [{ "$ne": ["$match", null] }, { "$gte": ["$month_idx", 0] }] },
      // Derive structured fields using $concat, $toInt, $toLong, and secondary $regexFind
      ...
    }
  },
  // Store all records (both valid and malformed)
  {
    "$merge": {
      "into": target_collection,
      "on": "_id",
      "whenMatched": "replace",
      "whenNotMatched": "insert"
    }
  }
]
```

---

## 3. Two-Stage Aggregations for Q2 and Q3

To process millions of log records efficiently without exceeding MongoDB's 16MB document limit or 100MB in-memory stage buffer:

### Query 2 (Top 20 Resources):
* **Naive approach avoided:** Grouping by `resource_path` and accumulating `$addToSet: "$host"` fails on high-traffic endpoints (e.g. root or logos) where distinct hosts number in the tens or hundreds of thousands, causing oversized documents.
* **Two-stage grouping implemented:**
  1. Stage 1 groups by compound key `(resource_path, host)` to aggregate metrics per distinct host.
  2. Stage 2 groups by `resource_path`, computing `distinct_host_count = $sum: 1` alongside total requests and bytes.
  3. Sorts by `{request_count: -1, resource_path: 1}` and limits to 20.

### Query 3 (Hourly Errors):
* **Two-stage grouping implemented:**
  1. Stage 1 groups by `(log_date, log_hour, host)`, calculating total requests and error requests per host.
  2. Stage 2 groups by `(log_date, log_hour)`, summing metrics and counting hosts where `error_count > 0` (`$distinct_error_hosts`).

---

## 4. Rounding Investigation: Built-in `$round` vs Spec

### Observation & Findings
Contract 2 and the project statement mandate **round-half-away-from-zero** (e.g., $0.00005 \to 0.0001$).
We tested MongoDB's built-in `$round` operator against test values:
```python
# Values tested on MongoDB 7.0:
0.5 -> $round gives 0.0  (rounded to nearest even)
1.5 -> $round gives 2.0  (rounded to nearest even)
2.5 -> $round gives 2.0  (rounded to nearest even)
3.5 -> $round gives 4.0  (rounded to nearest even)
4.5 -> $round gives 4.0  (rounded to nearest even)
0.12355 -> $round(val, 4) gives 0.1235
```

MongoDB implements **Banker's Rounding (half-to-even)** per IEEE 754. This differs from the specification's required **half-away-from-zero**.

### Solution Implemented
To guarantee mathematical compliance with the specification across all engines, Q3 implements exact round-half-away-from-zero in the aggregation using arithmetic:
```javascript
{
  "$divide": [
    {
      "$floor": {
        "$add": [
          { "$multiply": [{ "$divide": ["$error_request_count", "$total_request_count"] }, 10000] },
          0.5
        ]
      }
    },
    10000.0
  ]
}
```
This formula evaluates exact half values away from zero identically to Python's `ROUND_HALF_UP`, ensuring 100% equivalence with PostgreSQL and the reference test suite.
