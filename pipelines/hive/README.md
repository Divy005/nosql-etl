# Apache Hive Pipeline

> **Owner:** Member C (Phase 1)

## Overview
This directory will contain the Apache Hive pipeline for processing NASA HTTP server logs.

## Responsibilities
- Set up HDFS staging directory and Hive database.
- Create raw, parsed, and malformed tables partitioned by `run_id` and `batch_id`.
- `process_batch`: upload batch to HDFS, execute in-engine parsing via `regexp_extract` and `RLIKE`.
- `finalize`: execute the 3 analytical queries via HiveQL and return results conforming to Contract 3.
- Comply with `tests/test_contract.py --engine hive`.
