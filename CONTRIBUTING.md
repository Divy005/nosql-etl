# Contributing

Four people push to this repository and it is submitted for grading at the end of Phase 2,
so please keep `main` clean and working at all times.

## Branches

Never commit directly to `main`. Branch from an up-to-date `main`:

| Kind of work | Branch name | Example |
| :--- | :--- | :--- |
| New feature / pipeline | `feat/<area>` | `feat/pig-pipeline`, `feat/report` |
| Bug fix | `fix/<area>` | `fix/loader-numeric` |

```bash
git checkout main && git pull
git checkout -b feat/<area>
```

## Commit messages

`<type>(<scope>): <what changed>`, short and in the imperative:

- `feat(mongo): run Q2 as a two-stage group`
- `fix(controller): mark run failed when engine setup raises`
- `test: add Q2 rank-20 tie case to fixture`
- `docs: add batch-size results to MongoDB README`

Types: `feat`, `fix`, `test`, `docs`, `chore`, `refactor`.

## Pull requests

1. Push your branch and open a pull request into `main`.
2. Fill in the PR template (it asks for your contract-test result).
3. **At least one teammate reviews and approves** before merging.
4. CI (lint, batcher tests, dummy + MongoDB contract tests, end-to-end smoke run) must be green.

## Contract files — change only with team agreement

These files are what lets four engines be compared fairly. Do not change them on your own;
raise it with the team first and mention the agreement in the PR:

- `pipelines/base.py` — the `Engine` interface and result dataclasses
- `spec/parsing_spec.md` — the parsing and query rules
- `db/schema.sql` — table and column names
- `db/loader.py` — function **signatures** (the body may be replaced by the loader owner)

`spec/test.log` and `spec/expected/*` are generated together: if you add a fixture line, run
`python -m tests.reference_impl` and commit the regenerated expected files in the same PR.

## Never commit

- The NASA data (`data/`, `*.gz`, the unzipped logs — ~350 MB; GitHub rejects files over 100 MB).
  `bash scripts/get_data.sh` recreates it.
- `.env` or any password / token / URI with credentials. Only `.env.example` with placeholders.
- Caches and environments (`__pycache__/`, `.venv/`, `.pytest_cache/`, `.ruff_cache/`), IDE folders.
- Large generated outputs. Small sample outputs for the report go in `docs/sample_outputs/`
  (each file < 1 MB).

Install the hooks once so these are caught automatically:

```bash
pip install pre-commit
pre-commit install
```

## Local checks before opening a PR

```bash
ruff check . && ruff format --check .
pytest tests/test_batcher.py
pytest tests/test_contract.py --engine <your-engine>
```

## How to add a new pipeline

1. Create `pipelines/<name>/engine.py` with a class implementing `pipelines.base.Engine`:
   - `name = "<name>"`
   - `setup(run_id)` — create run-scoped tables / collections / HDFS dirs (before the timer).
   - `process_batch(run_id, batch_id, lines, first_line_no) -> BatchStats` — load the raw lines
     **into the engine** and parse them there. Python must not parse, filter or regex the lines.
     `parsed_count + malformed_count == record_count`; return up to 5 malformed samples
     as `(line_no, raw_line)`.
   - `finalize(run_id) -> FinalResult` — run Q1, Q2, Q3 inside the engine over all batches,
     time each one, and return rows with exactly the keys in the spec (Contract 3).
   - `cleanup(run_id)` — optional, runs after the timer stops; must respect `--keep-data`.
2. Register it in `pipelines/__init__.py`:
   ```python
   from pipelines.<name>.engine import <Name>Engine
   _REGISTRY["<name>"] = <Name>Engine
   ```
   (or `register_engine("<name>", <Name>Engine)`).
3. If the engine needs external services (Hadoop, Hive server), give the class an optional
   `is_available(self) -> bool` method that returns `False` when they cannot be reached. The
   contract test then **skips** instead of failing, which keeps CI green without Hadoop.
4. Make the contract test pass:
   ```bash
   pytest tests/test_contract.py --engine <name>
   ```
   It feeds `spec/test.log` in batches of 7 (several batches plus a partial one) and compares
   every row against `spec/expected/` exactly (floats at 4 dp).
5. Run it on the full data and record the numbers in `pipelines/<name>/README.md`:
   ```bash
   python -m controller.main --pipeline <name> --batch-size 100000
   ```
6. Update the pipeline status table in the main `README.md`.

Things that commonly trip engines up (all covered by the fixture):

- Regex classes `\s`, `\S`, `\d` are **ASCII** (as in Java and MongoDB). In Python use `re.ASCII`.
- Hosts are case-sensitive (`HOST.nasa.gov` ≠ `host.nasa.gov`).
- An empty request `""` is still a *parsed* line with `resource_path = ''`.
- `error_rate` rounds **half away from zero** (1/32 = 0.03125 → 0.0313). Many built-in
  `round` functions are half-to-even; check yours.
- Q2 tie-break is `resource_path` ascending in plain byte order; the fixture has a tie at rank 20.
