# NASA Web Server Log Analytics: Multi-Pipeline ETL Framework

A comparative, multi-pipeline ETL and reporting framework designed for large-scale web server log analytics (NASA Kennedy Space Center HTTP access logs, July & August 1995). The tool provides a unified command-line controller and standardized contracts to execute the identical parsing, cleaning, and multi-query analytical workload across four distinct NoSQL and distributed processing engines: **MongoDB**, **Apache Hive**, **Apache Pig**, and **Hadoop MapReduce**. Results and execution metadata are systematically loaded into a relational **PostgreSQL** database for reporting and cross-engine performance benchmarks.

---

## Team & Responsibilities

| Member | Phase | Responsibility Area |
| :--- | :--- | :--- |
| **Divy** | Phase 1 | Repository & Docker setup, parsing specification, test data suite, CLI controller (batching & timing), MongoDB pipeline, presentation |
| **Member C** | Phase 1 | PostgreSQL relational schema & loader, Hadoop/Hive infrastructure, HiveQL pipeline, report script & validation checker |
| **Member A** | Phase 2 | Hadoop Streaming MapReduce pipeline, comparative scaling experiments across all 4 engines, report write-up |
| **Member B** | Phase 2 | Apache Pig pipeline, performance analysis, final PDF report compilation, demo video |

---

## Pipeline Status

| Engine | Technology | Status | Owner |
| :--- | :--- | :---: | :--- |
| **MongoDB** | Aggregation Pipeline (`$regexFind`, `$group`, `$merge`) | ✅ Done (contract test + full data) | Divy |
| **Apache Hive** | HiveQL (`RLIKE`, `regexp_extract`, partitioned ORC tables on HDFS) | ✅ Done (contract test + full data, matches MongoDB) | Member C |
| **Apache Pig** | Pig Latin (`REGEX_EXTRACT_ALL`) | ⏳ Queued (Phase 2) | Member B |
| **MapReduce** | Hadoop Streaming Python | ⏳ Queued (Phase 2) | Member A |

---

## Architecture

```
                       +-----------------------------+
                       |   Raw NASA HTTP Log Files   |
                       |  (Jul 1995 & Aug 1995 Logs) |
                       +--------------+--------------+
                                      |
                                      v
                       +-----------------------------+
                       |    CLI Engine Controller    |
                       |    (controller/main.py)     |
                       +--------------+--------------+
                                      |
                      Streams raw batches (N lines)
                                      |
         +----------------------------+----------------------------+
         |                            |                            |
         v                            v                            v
+------------------+         +------------------+         +------------------+
| MongoDB Pipeline |         |  Hive Pipeline   |         | Pig / MapReduce  |
| (In-engine parse |         | (HDFS / HiveQL   |         |  (Phase 2)       |
|  & aggregation)  |         |  aggregation)    |         |                  |
+--------+---------+         +--------+---------+         +--------+---------+
         |                            |                            |
         +----------------------------+----------------------------+
                                      |
                           Standardized Result Sets
                             (Q1, Q2, Q3 + Timings)
                                      |
                                      v
                       +-----------------------------+
                       |   PostgreSQL Data Loader    |
                       |      (db/loader.py)         |
                       +--------------+--------------+
                                      |
                                      v
                       +-----------------------------+
                       | PostgreSQL Relational Store |
                       | (runs, batches, q1, q2, q3) |
                       +-----------------------------+
```

---

## Prerequisites

- Python 3.11 or higher
- Docker & Docker Compose
- Git
- `bash` + `curl` or `wget` for the dataset script (on Windows use **Git Bash**)
- ~1.5 GB free disk (≈360 MB of logs plus the MongoDB working collections)

---

## Quick Start

### 1. Clone & configure
```bash
git clone <repo-url> nosql-etl
cd nosql-etl
cp .env.example .env          # edit ports/passwords here if 5432/27017 are taken
```

### 2. Start services
```bash
docker compose --profile mongo up -d     # PostgreSQL (schema auto-loaded) + MongoDB
docker compose ps                        # wait until both show "healthy"
```
`db/schema.sql` is loaded only when the PostgreSQL volume is first created. To reset the
database: `docker compose down -v` then `up -d` again.

For the **Hive** pipeline start the Hadoop profile instead (about 4 GB of RAM; stop `mongo`
first on a small machine):
```bash
docker compose --profile hadoop up -d    # HDFS (namenode + datanode), Hive metastore, HiveServer2
docker compose --profile hadoop ps       # hive-server shows "healthy" after about a minute
```

### 3. Install Python dependencies
```bash
python -m venv .venv
source .venv/bin/activate     # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 4. Get the dataset
```bash
bash scripts/get_data.sh      # downloads + gunzips the two official files into data/
```
If the download fails (the host `ita.ee.lbl.gov` is sometimes slow or blocked), download
[NASA_access_log_Jul95.gz](https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz) and
[NASA_access_log_Aug95.gz](https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz) in a browser,
put them in `data/`, and run the script again: it then only decompresses them. Decompression
is the only change ever made to the input files. `data/` is git-ignored; never commit it.

### 5. Run a pipeline
```bash
python -m controller.main --pipeline mongodb --batch-size 100000
python -m controller.main                  # interactive menu: engine + batch size
```
Engine names follow the project statement: `mongodb`, `hive`, `pig`, `mapreduce`. Shorthands
`mongo` and `mr` are accepted for `--pipeline` and `--engine` and are always recorded under the
full name. Options: `--input FILE ...` (default: both files in `DATA_DIR`), `--keep-data` (keep the
engine's working collections/tables after the run). Pig and MapReduce print a
"not implemented yet" message until their owners add them.

### 6. Look at the results
Every run writes to PostgreSQL: `etl_runs` (status, counts, runtimes), `etl_batches`,
`etl_malformed_samples`, `etl_query_runs`, and the result tables `q1_daily_traffic`,
`q2_top_resources`, `q3_hourly_errors` (each row carries `pipeline`, `run_id`, `batch_id`,
`executed_at`).

```bash
docker compose exec postgres psql -U postgres -d nasa_analytics \
  -c "SELECT run_id, pipeline, status, total_records, malformed_records, runtime_sec FROM etl_runs;"
```
#### Report: results + run metadata (`report.py`)
Reads only PostgreSQL, so it works after any run and is outside the timed span.
```bash
python report.py                      # latest succeeded run
python report.py --pipeline hive      # latest run of one pipeline (aliases: mongo, mr)
python report.py --run-id 3 --full    # one run, every result row
python report.py --export docs/sample_outputs     # also write the run's results as CSV
python report.py --compare            # runtimes of all pipelines side by side
```
It prints the pipeline, query name, runtime, batch ID, batch size and average batch size of the
run, the three result sets, the batches and the malformed lines. `--compare` shows the latest
full-data run of every pipeline and batch size (total, ingest, aggregate, load, Q1/Q2/Q3, and the
ratio to the fastest at that batch size).

#### Checker: do the pipelines agree? (`compare.py`)
```bash
python compare.py                     # Hive vs MongoDB
python compare.py --engine all        # every pipeline that has a run, each vs MongoDB
python compare.py --run-a 3 --run-b 2 # two explicit runs
```
It compares the record counts and every result cell (Q1 by `(log_date, status_code)`, Q2 by `rank`,
Q3 by `(log_date, log_hour)`, `error_rate` exactly), ignoring `id`, `run_id`, `batch_id`,
`pipeline` and `executed_at`, and prints PASS or FAIL per query. Exit status 0 = all PASS,
1 = a difference, 2 = a run is missing. It only compares runs on the same input files.

---

## MongoDB results (full data)

| Batch size | Total | Ingest | Aggregate | Records (parsed + malformed) |
| ---: | ---: | ---: | ---: | :--- |
| 50,000 | 113.8 s | 92.3 s | 20.1 s | 3,461,613 (3,461,612 + 1) |
| 100,000 | 113.2 s | 91.9 s | 19.9 s | 3,461,613 (3,461,612 + 1) |
| 500,000 | 111.7 s | 89.9 s | 20.5 s | 3,461,613 (3,461,612 + 1) |

The single malformed record is the truncated last line of the official July file. Details,
tuning measurements and the rounding investigation are in
[`pipelines/mongodb/README.md`](pipelines/mongodb/README.md); result CSVs in
[`docs/sample_outputs/`](docs/sample_outputs/).

---

## Running Tests

```bash
ruff check . && ruff format --check .        # lint
pytest tests/test_batcher.py                  # batcher unit tests
pytest tests --ignore=tests/test_contract.py  # all unit tests (loader/report need PostgreSQL)
pytest tests/test_contract.py --engine dummy  # controller/framework smoke test
pytest tests/test_contract.py --engine mongodb   # needs MongoDB running
```

The contract test feeds `spec/test.log` (edge cases: empty/truncated lines, bad month, `-`
bytes, quotes in the request, case-different hosts, a Q2 tie at rank 20, an exact `.5`
rounding tie) in batches of 7 and compares every row with `spec/expected/`, which is
generated by `python -m tests.reference_impl`. CI runs all of the above on every push and PR
to `main`. See [CONTRIBUTING.md](CONTRIBUTING.md) for the team workflow and how to add an
engine.

---

## Repository Layout

```
nosql-etl/
├── README.md                     # Project documentation & team guide
├── CONTRIBUTING.md               # Contribution workflow & pipeline guidelines
├── .env.example                  # Environment configuration template
├── .gitignore                    # Git ignore definitions
├── .gitattributes                # Enforce LF line endings
├── pyproject.toml                # Ruff & Pytest configuration
├── .pre-commit-config.yaml       # Pre-commit hooks for formatting & large file checks
├── .github/
│   ├── workflows/ci.yml          # GitHub Actions CI matrix & service containers
│   └── pull_request_template.md  # Standard PR checklist
├── docker-compose.yml            # Multi-service setup (PostgreSQL, MongoDB, Hadoop placeholder)
├── requirements.txt              # Production & test dependencies
├── scripts/
│   └── get_data.sh               # Idempotent dataset downloader
├── spec/
│   ├── parsing_spec.md           # Authoritative parsing & aggregation rules
│   ├── test.log                  # Edge-case fixture log file
│   └── expected/                 # Ground-truth query outputs & counts
├── db/
│   ├── schema.sql                # PostgreSQL relational database contract
│   ├── loader.py                 # Standardized relational loader
│   └── reader.py                 # Read side shared by report.py and compare.py
├── report.py                     # Report: run metadata + results, --compare runtimes
├── compare.py                    # Checker: PASS/FAIL equivalence of two pipelines
├── controller/
│   ├── main.py                   # CLI entry point & menu
│   ├── batcher.py                # Pure line streaming & batch partitioning
│   ├── config.py                 # Configuration loader
│   └── timing.py                 # Performance measurement utilities
├── pipelines/
│   ├── base.py                   # Abstract engine contract (Engine, BatchStats, FinalResult)
│   ├── dummy/                    # Mock engine for framework testing
│   ├── mongodb/                  # MongoDB aggregation pipeline implementation
│   ├── hive/                     # Hive pipeline (Member C)
│   ├── pig/                      # Pig pipeline (Member B)
│   └── mapreduce/                # MapReduce pipeline (Member A)
├── tests/
│   ├── reference_impl.py         # Ground-truth parser to generate spec/expected/
│   ├── test_batcher.py           # Batching unit tests
│   ├── test_contract.py          # Strict engine contract compliance suite
│   └── conftest.py               # Pytest fixtures and options
└── docs/
    ├── project_statement.pdf     # Official DAS 839 project specification
    └── sample_outputs/           # Small result CSVs from full-data runs (< 1 MB each)
```
