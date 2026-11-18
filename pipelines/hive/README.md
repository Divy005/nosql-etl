# Apache Hive Pipeline

> **Owner:** Member C (Phase 1) · **Status:** ✅ implemented, contract test passing, full-data verified
> against MongoDB (`python compare.py` → PASS)

## 1. What it does

All parsing and all three queries run **inside Hive** (Hard Rule 3). The HiveQL is in
[`hiveql.py`](hiveql.py); `engine.py` only

1. writes the lines of a batch to a text file in a folder that the HiveServer2 container can read,
2. sends the HiveQL statements (`LOAD DATA`, parse, count, queries), and
3. reads counts, up to 5 malformed samples and the final query rows back.

There is no Python regex, filtering or grouping anywhere in `pipelines/hive/`.

| Table (database `nasa_etl`) | Holds | Storage | Lifetime |
| :--- | :--- | :--- | :--- |
| `raw_logs` | the raw lines of one batch: `line_no`, `line` | text + `RegexSerDe` | partition dropped right after the batch is parsed |
| `parsed_logs` | one row per parsed line: `host`, `log_timestamp`, `log_date`, `log_hour`, `http_method`, `resource_path`, `protocol_version`, `status_code`, `bytes_transferred` (+ `line_no`) | ORC | partitions dropped by `cleanup()` unless `--keep-data` |
| `malformed_logs` | one row per malformed line: `line_no`, `line` | ORC | same |

All three are partitioned by `(run_id, batch_id)`, so runs never mix and the queries read only
`WHERE run_id = N`. The files live in HDFS (`/user/hive/warehouse/nasa_etl.db/...`).

## 2. Per batch: `process_batch()`

1. Python writes `"<line_no>\t<raw line>"` per line (UTF-8) to `HIVE_STAGING_DIR`, which is mounted
   into the hive-server container as `/staging`.
2. `LOAD DATA LOCAL INPATH '/staging/...' OVERWRITE INTO TABLE raw_logs PARTITION (run_id, batch_id)`
   copies the file into **HDFS**.
3. **One multi-insert** parses the partition in a single scan:
   - `line RLIKE <line regex>` and `mon IN ('Jan', …, 'Dec')` decide parsed vs malformed,
   - `regexp_extract(line, <line regex>, n)` pulls the groups; `concat` + a `CASE` on the
     month build `log_date` / `log_timestamp`; `bytes = '-'` → `0`,
   - a second `RLIKE` / `regexp_extract` splits the request; an unsplittable request (e.g. `""`)
     gives `''` for method, path and protocol (still a *parsed* line),
   - parsed rows go to `parsed_logs`, everything else to `malformed_logs`. Nothing is dropped.
4. One `SELECT … UNION ALL … GROUP BY` counts both tables for the batch → `BatchStats`. The engine
   raises if `parsed + malformed != lines sent`, so a lost or split line can never go unnoticed.
5. Up to 5 malformed rows are fetched (`LIMIT 5`, plain fetch, no MapReduce job).
6. The raw partition is dropped (keeps HDFS usage bounded, as MongoDB deletes its raw batch).

## 3. After the last batch: `finalize()`

Three HiveQL queries over `parsed_logs WHERE run_id = N` (all batches), each timed:

- **Q1** `GROUP BY log_date, status_code` → `COUNT(*)`, `SUM(bytes_transferred)`; `ORDER BY` both keys.
- **Q2** `GROUP BY resource_path` → count, bytes, `COUNT(DISTINCT host)`;
  `ORDER BY request_count DESC, resource_path ASC LIMIT 20`. Hive compares strings by UTF-8 bytes,
  which orders exactly like the spec's byte order (the fixture's tie at rank 20 passes).
  `rank` is the row position in that result.
- **Q3** `GROUP BY log_date, log_hour` → `SUM(CASE WHEN status_code BETWEEN 400 AND 599 …)`,
  `COUNT(*)`, `COUNT(DISTINCT CASE WHEN … THEN host END)` (0 when there are no errors), and
  `error_rate = ((err * 20000 + total) DIV (2 * total)) / 10000.0`.

The queries run once over all batches because distinct-host counts and the top 20 cannot be
added up batch by batch.

## 4. Things the real data and the fixture forced (and why the code looks the way it does)

| Trap | Where it bites | What the engine does |
| :--- | :--- | :--- |
| 7 real lines contain `\x01`, Hive's default field delimiter | a one-column text table cuts the line short; a text-format result table corrupts values | `RegexSerDe` with `(.*)` keeps whole lines; `parsed_logs` / `malformed_logs` are **ORC**. Verified field by field on all 7 lines. |
| `RegexSerDe` returns **NULL for BIGINT columns** (Hive 2.3) | every row of the raw table comes back NULL | `line_no STRING` in `raw_logs`, cast to `BIGINT` while parsing |
| Hive unescapes backslashes in string literals | `'\S'` reaches the regex engine as `S` | `hiveql.literal()` doubles every backslash and escapes `'`; a unit test checks the round trip |
| Java's `.` and `$` treat `\r`, U+0085, U+2028, U+2029 as line terminators; Python and MongoDB treat only `\n` as one | the same line could be parsed in one engine and malformed in another | every regex starts with the Java flag `(?d)` (UNIX_LINES). The spec regexes themselves are unchanged (unit test compares them with `spec/parsing_spec.md`). No such byte occurs in the NASA files, so this is insurance, not a result change. |
| `ROUND()` on a double | 1/32 = 0.03125 must give 0.0313 (half away from zero) | integer arithmetic, see Q3 |
| Hadoop splits records on a lone `\r` | would turn one line into two | the NASA files contain none (checked); the count check in step 4 would raise if one ever appeared |
| Empty lines | must count as records and as malformed | they survive `LOAD DATA` and the `RegexSerDe` (the prefix makes them non-empty) |
| Hive text results are UTF-8 | the files are Latin-1 | the controller decodes Latin-1 and the engine writes UTF-8: code points are unchanged, and UTF-8 byte order equals code-point order, so Q2's tie-break is right |

## 5. Environment

```bash
docker compose --profile hadoop up -d        # namenode, datanode, metastore (PostgreSQL), metastore, HiveServer2
docker compose --profile hadoop ps           # hive-server turns "healthy" after ~1 min
python -m controller.main --pipeline hive --batch-size 100000
```

Hadoop 2.7.4 + Hive 2.3.2 (big-data-europe images, one consistent version set), local MapReduce
inside the HiveServer2 container (no YARN). About 4 GB of RAM; stop the `mongo` profile while
running Hive on a laptop. Web UI of HDFS: <http://localhost:50070>.

The Python client is PyHive with `auth=NOSASL` (`hive.server2.authentication=NOSASL` in
`docker/hadoop-hive.env`), so the native `sasl` package, which does not build on Windows, is not
needed. `HIVE_HOST`, `HIVE_PORT`, `HIVE_DATABASE`, `HIVE_STAGING_DIR`, `HIVE_STAGING_MOUNT` are
in `.env.example`.

If HiveServer2 is not reachable, `HiveEngine.is_available()` returns `False`, so
`pytest tests/test_contract.py --engine hive` **skips** instead of failing (CI has no Hadoop).

Reset the Hive side: `docker compose --profile hadoop down -v` (also deletes HDFS and the metastore).

## 6. Full-data results (this laptop, 16 logical cores, WSL2 with 12 GB)

Both official files, 3,461,613 records = 3,461,612 parsed + 1 malformed (the truncated last line
of the July file, batch 19 at batch size 100k, line 1,891,715).

| Batch size | Batches | Total | Ingest | Aggregate | Q1 / Q2 / Q3 | Load |
| ---: | ---: | ---: | ---: | ---: | :--- | ---: |
| 50,000 | 70 | 438.0 s | 410.4 s | 24.2 s | 3.4 / 16.4 / 4.4 s | 0.19 s |
| 100,000 | 35 | 305.5 s | 278.0 s | 24.1 s | 3.3 / 16.4 / 4.4 s | 0.22 s |
| 500,000 | 7 | 195.1 s | 167.7 s | 24.0 s | 3.3 / 16.4 / 4.3 s | 0.24 s |

Row counts 295 / 20 / 1,369. **`python compare.py` → PASS against the MongoDB run** (record counts and
every cell of Q1, Q2 and Q3; `error_rate` exactly). The result CSVs in
[`docs/sample_outputs/`](../../docs/sample_outputs/) are byte-identical to the MongoDB ones.

Observations (for the report's comparison section):

- Ingest dominates and **depends on the batch size**: Hive pays a fixed cost per statement
  (JVM start for each local MapReduce job, metastore calls, `LOAD DATA`), and each batch needs four
  statements, two of them MapReduce jobs (parse and count). Doubling the batches from 35 to 70
  added ~130 s; 7 batches took 195 s. MongoDB's ingest is per line, so its runtime is nearly flat
  (≈113 s at 50k, 100k and 500k in the MongoDB README; 116 s at 100k on this laptop).
- Aggregation does **not** depend on the batch size (≈24 s): the queries run once over all batches.
  Q2 (a `COUNT(DISTINCT host)` over ~2.1 million `(path, host)` pairs, one local reducer) is the
  slowest query.
- The load into PostgreSQL is negligible (< 0.3 s for ~1,700 rows).

## 7. Tests

```bash
pytest tests/test_hive_hiveql.py                 # no Hadoop needed: spec regexes, escaping, statement shape
pytest tests/test_contract.py --engine hive      # needs `docker compose --profile hadoop up -d`; ~3 min
python -m controller.main --pipeline hive --batch-size 100000
python compare.py                                # Hive vs MongoDB on the full data (needs both runs in PostgreSQL)
```
