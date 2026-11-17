# MongoDB Aggregation Pipeline

> **Owner:** Divy (Phase 1)

## Overview
This directory implements the MongoDB pipeline for NASA HTTP log ETL and analytics.

## Requirements
- All parsing and query aggregations run inside MongoDB (aggregation framework: `$regexFind`, `$group`, `$merge`, etc.).
- Python only inserts raw lines and reads final query results.
- Run-scoped collections: `raw_r{run_id}`, `parsed_r{run_id}`.
- All docs (valid and malformed) stored in `parsed_r{run_id}`.
- Q1, Q2, and Q3 queries executed with `allowDiskUse=True`.
- Two-stage grouping for Q2 and Q3 to avoid memory constraints and oversized set accumulators.
