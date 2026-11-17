-- ==============================================================================
-- DAS 839 NoSQL Project: PostgreSQL Relational Database Schema Contract
-- Target Engine: PostgreSQL 16
-- ==============================================================================

-- Drop existing tables in reverse dependency order if recreating
DROP TABLE IF EXISTS q3_hourly_errors CASCADE;
DROP TABLE IF EXISTS q2_top_resources CASCADE;
DROP TABLE IF EXISTS q1_daily_traffic CASCADE;
DROP TABLE IF EXISTS etl_query_runs CASCADE;
DROP TABLE IF EXISTS etl_malformed_samples CASCADE;
DROP TABLE IF EXISTS etl_batches CASCADE;
DROP TABLE IF EXISTS etl_runs CASCADE;

-- ==============================================================================
-- Table: etl_runs
-- Master tracking table for all pipeline execution runs
-- ==============================================================================
CREATE TABLE etl_runs (
    run_id SERIAL PRIMARY KEY,
    pipeline VARCHAR(32) NOT NULL,
    batch_size INTEGER NOT NULL,
    input_files TEXT[] NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'running', -- 'running', 'succeeded', 'failed'
    total_records BIGINT DEFAULT 0,
    parsed_records BIGINT DEFAULT 0,
    malformed_records BIGINT DEFAULT 0,
    num_batches INTEGER DEFAULT 0,
    avg_batch_size NUMERIC(12, 2) DEFAULT 0.00,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    runtime_sec NUMERIC(10, 4),
    ingest_runtime_sec NUMERIC(10, 4),
    aggregate_runtime_sec NUMERIC(10, 4),
    load_runtime_sec NUMERIC(10, 4),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ==============================================================================
-- Table: etl_batches
-- Per-batch ingestion metadata and record counts
-- ==============================================================================
CREATE TABLE etl_batches (
    id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES etl_runs(run_id) ON DELETE CASCADE,
    batch_id INTEGER NOT NULL,
    record_count INTEGER NOT NULL,
    parsed_count INTEGER NOT NULL,
    malformed_count INTEGER NOT NULL,
    first_line_no BIGINT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    runtime_sec NUMERIC(10, 4) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_run_batch UNIQUE (run_id, batch_id)
);

-- ==============================================================================
-- Table: etl_malformed_samples
-- Sample raw lines that failed regex parsing (up to 5 per batch)
-- ==============================================================================
CREATE TABLE etl_malformed_samples (
    id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL,
    batch_id INTEGER NOT NULL,
    line_no BIGINT NOT NULL,
    raw_line TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_malformed_batch FOREIGN KEY (run_id, batch_id)
        REFERENCES etl_batches(run_id, batch_id) ON DELETE CASCADE
);

-- ==============================================================================
-- Table: etl_query_runs
-- In-engine execution time and row counts for each analytical query
-- ==============================================================================
CREATE TABLE etl_query_runs (
    id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES etl_runs(run_id) ON DELETE CASCADE,
    pipeline VARCHAR(32) NOT NULL,
    query_id VARCHAR(16) NOT NULL, -- 'q1', 'q2', 'q3'
    runtime_sec NUMERIC(10, 4) NOT NULL,
    row_count INTEGER NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL
);

-- ==============================================================================
-- Table: q1_daily_traffic
-- Query 1 Output: Traffic count and bytes transferred grouped by date & status code
-- ==============================================================================
CREATE TABLE q1_daily_traffic (
    id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL,
    batch_id INTEGER NOT NULL,
    pipeline VARCHAR(32) NOT NULL,
    log_date DATE NOT NULL,
    status_code INTEGER NOT NULL,
    request_count BIGINT NOT NULL,
    total_bytes BIGINT NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT fk_q1_batch FOREIGN KEY (run_id, batch_id)
        REFERENCES etl_batches(run_id, batch_id) ON DELETE CASCADE
);

-- ==============================================================================
-- Table: q2_top_resources
-- Query 2 Output: Top 20 resources by request count, with total bytes and distinct hosts
-- ==============================================================================
CREATE TABLE q2_top_resources (
    id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL,
    batch_id INTEGER NOT NULL,
    pipeline VARCHAR(32) NOT NULL,
    rank INTEGER NOT NULL,
    resource_path TEXT NOT NULL,
    request_count BIGINT NOT NULL,
    total_bytes BIGINT NOT NULL,
    distinct_host_count BIGINT NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT fk_q2_batch FOREIGN KEY (run_id, batch_id)
        REFERENCES etl_batches(run_id, batch_id) ON DELETE CASCADE
);

-- ==============================================================================
-- Table: q3_hourly_errors
-- Query 3 Output: Hourly error rate (4xx/5xx) and distinct error-generating hosts
-- ==============================================================================
CREATE TABLE q3_hourly_errors (
    id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL,
    batch_id INTEGER NOT NULL,
    pipeline VARCHAR(32) NOT NULL,
    log_date DATE NOT NULL,
    log_hour INTEGER NOT NULL CHECK (log_hour >= 0 AND log_hour <= 23),
    error_request_count BIGINT NOT NULL,
    total_request_count BIGINT NOT NULL,
    error_rate NUMERIC(7, 4) NOT NULL,
    distinct_error_hosts BIGINT NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT fk_q3_batch FOREIGN KEY (run_id, batch_id)
        REFERENCES etl_batches(run_id, batch_id) ON DELETE CASCADE
);

-- ==============================================================================
-- Indexes for efficient reporting and cross-engine comparison
-- ==============================================================================
CREATE INDEX idx_runs_pipeline ON etl_runs(pipeline);
CREATE INDEX idx_runs_status ON etl_runs(status);
CREATE INDEX idx_batches_run ON etl_batches(run_id);
CREATE INDEX idx_query_runs_run ON etl_query_runs(run_id, query_id);
CREATE INDEX idx_q1_run ON q1_daily_traffic(run_id);
CREATE INDEX idx_q2_run ON q2_top_resources(run_id);
CREATE INDEX idx_q3_run ON q3_hourly_errors(run_id);
