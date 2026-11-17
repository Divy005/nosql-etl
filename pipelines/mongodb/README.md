# MongoDB Pipeline

> **Owner:** Divy (Phase 1) · **Status:** ✅ implemented, contract test passing, full-data verified

## 1. What it does

All parsing and all three queries run **inside MongoDB** (Hard Rule 3). Python only:

1. inserts the raw lines of a batch into `raw_r{run_id}` as `{batch_id, line_no, line}`,
2. starts aggregations, and
3. reads back counts, up to 5 malformed samples, and the final query results.

There is no Python-side regex, filtering or grouping anywhere in `pipelines/mongodb/`.

| Collection (database `nasa_etl`) | Holds | Lifetime |
| :--- | :--- | :--- |
| `raw_r{run_id}` | raw lines of the batch being processed | each batch's docs deleted right after parsing |
| `parsed_r{run_id}` | one doc per input line: parsed fields + `valid`; malformed docs keep `line`, `line_no` | dropped by `cleanup()` unless `--keep-data` |

Indexes on `parsed_r{run_id}` (created in `setup()`):

- `{batch_id: 1}`: per-batch parsed/malformed count.
- `{batch_id: 1, valid: 1}` **partial**, only over `valid: false` docs: malformed-sample lookup.
  See §4 for why this is partial instead of a plain `{valid: 1}` index.

## 2. Per batch: `process_batch()`

1. `insert_many(..., ordered=False)` the raw lines.
2. One aggregation on `raw_r{id}` (`build_parsing_pipeline` in `aggregations.py`):
   - `$regexFind` with the spec's line regex,
   - month validated with `$indexOfArray` on `["Jan", …, "Dec"]`,
     `valid = match found AND month index >= 0`,
   - for valid docs: zero-padded `log_date` / `timestamp` via `$concat`, `log_hour` and
     `status_code` via `$toInt`, `bytes_transferred` via `$toLong` (`-` → 0), and the request
     split by a second `$regexFind`; an unmatched request gives `''` for all three parts,
   - valid docs drop the raw `line`; malformed docs keep `line` and `line_no`,
   - `$merge` **every** doc (valid and malformed) into `parsed_r{id}`.
3. `$group` on `valid` for this `batch_id` → parsed / malformed counts.
4. `find({batch_id, valid: false}).limit(5)` → malformed samples.
5. `delete_many` this batch's raw docs.

## 3. After the last batch: `finalize()`

Three aggregations on `parsed_r{id}` with `{valid: true}` and `allowDiskUse=True`, each timed:

- **Q1**: `$group` by `(log_date, status_code)` → count, sum bytes; `$sort`.
- **Q2**: two-stage group (avoids huge `$addToSet` arrays):
  1. by `(resource_path, host)` → count, bytes,
  2. by `resource_path` → sum counts, sum bytes, `distinct_host_count = $sum: 1`;
  3. `$sort {request_count: -1, _id: 1}`, `$limit 20`. MongoDB compares strings by UTF-8
     bytes, which orders exactly like the spec's code-point / Latin-1 byte order.
- **Q3**: two-stage group:
  1. by `(log_date, log_hour, error_host)` where `error_host` = host for 4xx/5xx and `null`
     otherwise → total and error counts,
  2. by `(log_date, log_hour)` → sums; `distinct_error_hosts` = groups with `error_count > 0`;
  3. `error_rate` with exact half-away-from-zero rounding (§5).

## 4. Tuning: what was measured and changed

Machine: 2 vCPU, 7 GB RAM container, MongoDB 7.0.34, WiredTiger cache 3 GB.

| Change | Before | After | Why |
| :--- | ---: | ---: | :--- |
| `$merge whenMatched: "replace"` → `"fail"` (1M-line synthetic, batch 100k) | ingest 38.6 s | 27.1 s | every `_id` is a fresh ObjectId, so a match can only mean a batch was processed twice: failing is both correct and cheaper than a replace-capable upsert |
| plain `{valid: 1}` index → partial index on malformed docs (1M synthetic) | Q1/Q2/Q3 1.60 / 3.15 / 8.32 s | 1.02 / 2.58 / 7.52 s | `{valid: true}` matches ~100% of docs; the planner chose an index scan over everything instead of a collection scan |
| Q3 stage 1 keyed by `error_host` instead of `host` (1M synthetic) | Q3 7.45 s | 1.43 s | non-error requests collapse into one group per hour; output is identical |
| `internalDocumentSourceGroupMaxMemoryBytes` 100 MB → 1 GB (**full data**) | Q2 26.7 s | 13.5 s | Q2 stage 1 has 2,145,707 `(path, host)` groups and was spilling to disk. Set in `docker-compose.yml` |
| Valid docs no longer store the raw `line` | n/a | n/a | smaller `parsed_r` collection; the queries never read `line` |

Restructuring the parsing projection into fewer stages was also tried: 1.07 s vs 1.13 s per
100k lines, no gain, so the readable version stays.

## 5. Rounding: `$round` vs the spec

The spec requires **round-half-away-from-zero**. MongoDB's `$round` rounds half **to even**
(measured on 7.0.34):

| value | `$round` (place 0) | `$round` (place 4) | spec |
| ---: | ---: | ---: | ---: |
| 0.5 | 0 | | 1 |
| 1.5 | 2 | | 2 |
| 2.5 | 2 | | 3 |
| 0.03125 (= 1/32) | | **0.0312** | **0.0313** |
| 0.09375 (= 3/32) | | 0.0938 | 0.0938 |

A float formula such as `floor(rate * 10000 + 0.5)` is also unsafe: `rate * 10000` can land
just below `.5`. So Q3 uses exact integer arithmetic:

```
k          = floor((error * 20000 + total) / (2 * total))   # via $subtract + $mod: exact integers
error_rate = k / 10000
```

`spec/test.log` contains an hour with 32 requests and 1 error, so the contract test fails for
any engine that rounds half-to-even.

## 6. Full-data results (both NASA files)

`python -m controller.main --pipeline mongodb --batch-size <N>`, Q2 memory setting on:

| Batch size | Batches | Avg batch | Total (s) | Ingest (s) | Aggregate (s) | Load (s) | Q1 / Q2 / Q3 (s) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| 50,000 | 70 | 49,451.6 | 113.8 | 92.3 | 20.1 | 0.05 | 3.2 / 13.7 / 3.3 |
| 100,000 | 35 | 98,903.2 | 113.2 | 91.9 | 19.9 | 0.05 | 3.0 / 13.5 / 3.3 |
| 500,000 | 7 | 494,516.1 | 111.7 | 89.9 | 20.5 | 0.04 | 3.1 / 13.8 / 3.5 |
| 100,000 (default 100 MB `$group` limit) | 35 | 98,903.2 | 123.6 | 89.4 | 33.0 | 0.04 | 3.2 / 26.4 / 3.4 |

- **Records:** 3,461,613 total = 3,461,612 parsed + **1 malformed**. The one malformed line is
  the last line of `NASA_access_log_Jul95`, which is truncated in the official file
  (`alyssa.p`, no newline). That is also why `wc -l` reports 3,461,612: it counts newline
  characters, and that last physical line has none.
- **Batch size barely matters** (~2% spread). Ingest time is dominated by per-document
  parsing work inside MongoDB, not by per-batch round trips. 100,000 is kept as the default.
- **Correctness:** all runs give identical Q1/Q2/Q3 rows, and they match an independent
  streaming run of `tests/reference_impl.py` over the full data, row for row (295 Q1 rows,
  top-20 Q2, 1,369 Q3 rows). The Q1/Q2/Q3 CSVs are in `docs/sample_outputs/`.

## 7. Running

```bash
docker compose --profile mongo up -d
pytest tests/test_contract.py --engine mongodb
python -m controller.main --pipeline mongodb --batch-size 100000            # full data
python -m controller.main --pipeline mongodb --batch-size 100000 --keep-data # keep collections
```

Running MongoDB without the compose file (e.g. a local install)? Results are identical; Q2 is
just slower. To apply the setting to a running server:
`db.adminCommand({setParameter: 1, internalDocumentSourceGroupMaxMemoryBytes: 1073741824})`.
