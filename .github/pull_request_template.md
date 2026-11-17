## What changed

<!-- One or two sentences. Link the issue if there is one. -->

## Milestone / task

<!-- e.g. "Phase 2 – Pig pipeline: Q2" -->

## Contract test result

<!-- Paste the last line of: pytest tests/test_contract.py --engine <your-engine> -->

```
```

## Checklist

- [ ] No data files (`data/`, `*.gz`, NASA logs) or secrets (`.env`, passwords, URIs with credentials) are committed
- [ ] `ruff check .` and `ruff format --check .` pass
- [ ] `pytest tests/test_batcher.py` and the contract test for the engine I touched pass
- [ ] Contract files (`pipelines/base.py`, `spec/parsing_spec.md`, `db/schema.sql`, `db/loader.py` signatures) are unchanged, **or** the team agreed to the change (link the discussion)
- [ ] README / pipeline README updated if behaviour or run instructions changed
