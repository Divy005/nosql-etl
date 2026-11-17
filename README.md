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
| **MongoDB** | Aggregation Pipeline (`$regexFind`, `$group`, `$merge`) | 🚧 In Progress | Divy |
| **Apache Hive** | HiveQL (`regexp_extract`, partitioned tables) | 🚧 Planned (Phase 1) | Member C |
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
- `bash` / `curl` (for dataset download script)

---

## Quick Start

### 1. Clone & Setup Environment
```bash
git clone <repo-url> nosql-etl
cd nosql-etl

# Copy environment template
cp .env.example .env
```

### 2. Start Services
```bash
# Start PostgreSQL (core) and MongoDB services
docker compose --profile mongo up -d
```

### 3. Install Python Dependencies
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 4. Fetch Dataset
```bash
# Downloads and decompresses Jul95 and Aug95 logs into data/
bash scripts/get_data.sh
```

### 5. Run Pipeline
```bash
# Execute MongoDB pipeline with default batch size (100,000)
python -m controller.main --pipeline mongodb --batch-size 100000
```

---

## Running Tests

```bash
# Run batcher and unit tests
pytest tests/test_batcher.py

# Run contract tests against test fixtures
pytest tests/test_contract.py --engine dummy
pytest tests/test_contract.py --engine mongodb
```

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
│   └── loader.py                 # Standardized relational loader
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
    └── project_statement.pdf     # Official DAS 839 project specification
```
