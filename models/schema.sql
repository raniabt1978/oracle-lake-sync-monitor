-- app/models/schema.sql
-- SQLite schema simulating Hive/Data Lake tables
-- Mirrors Oracle HR schema with partition structure

-- Performance settings for SQLite
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

-- ============================================
-- EMPLOYEES TABLE (Main fact table)
-- ============================================
CREATE TABLE IF NOT EXISTS employees_partitioned (
    -- Core employee data (matching HR.EMPLOYEES)
    employee_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    phone_number TEXT,
    hire_date TEXT,              -- ISO format: YYYY-MM-DD
    job_id TEXT,
    salary REAL,
    commission_pct REAL,
    manager_id INTEGER,
    department_id INTEGER,
    
    -- Hive-style partition columns
    year INTEGER,
    month INTEGER,
    day INTEGER,
    
    -- Data quality tracking
    load_timestamp TEXT,         -- When loaded to lake
    source_system TEXT DEFAULT 'ORACLE_HR',
    etl_batch_id TEXT,
    
    -- For tracking issues
    is_duplicate INTEGER DEFAULT 0,
    has_quality_issue INTEGER DEFAULT 0
);

-- ============================================
-- DEPARTMENTS TABLE (Dimension)
-- ============================================
CREATE TABLE IF NOT EXISTS departments (
    department_id INTEGER PRIMARY KEY,
    department_name TEXT,
    manager_id INTEGER,
    location_id INTEGER,
    last_updated TEXT
);

-- ============================================
-- AUDIT_RUNS TABLE (Track monitoring history)
-- ============================================
CREATE TABLE IF NOT EXISTS audit_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_timestamp TEXT NOT NULL,
    oracle_count INTEGER,
    hive_count INTEGER,
    sync_gap INTEGER,
    gap_percentage REAL,
    freshness_lag_hours REAL,
    missing_partitions_count INTEGER,
    data_quality_score REAL,
    severity TEXT,
    triage_summary TEXT,
    execution_time_seconds REAL
);

-- ============================================
-- DATA_QUALITY_ISSUES TABLE (Track specific issues)
-- ============================================
CREATE TABLE IF NOT EXISTS data_quality_issues (
    issue_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER,
    issue_type TEXT,             -- 'NULL_VALUE', 'ORPHAN', 'DUPLICATE', etc.
    table_name TEXT,
    record_identifier TEXT,
    issue_details TEXT,
    detected_timestamp TEXT,
    FOREIGN KEY (run_id) REFERENCES audit_runs(run_id)
);

-- ============================================
-- INDEXES for performance
-- ============================================
-- Partition queries
CREATE INDEX IF NOT EXISTS idx_emp_partition 
ON employees_partitioned(year, month, day);

-- Date range queries
CREATE INDEX IF NOT EXISTS idx_emp_hire_date 
ON employees_partitioned(hire_date);

-- Data quality queries
CREATE INDEX IF NOT EXISTS idx_emp_quality 
ON employees_partitioned(has_quality_issue);

-- Audit queries
CREATE INDEX IF NOT EXISTS idx_audit_timestamp 
ON audit_runs(run_timestamp);

CREATE INDEX IF NOT EXISTS idx_quality_issues_run 
ON data_quality_issues(run_id);

-- ============================================
-- VIEWS for easier querying
-- ============================================
-- Latest partition status
CREATE VIEW IF NOT EXISTS v_partition_status AS
SELECT 
    year,
    month,
    day,
    COUNT(*) as record_count,
    MAX(load_timestamp) as last_load_time
FROM employees_partitioned
GROUP BY year, month, day;

-- Current data quality summary
CREATE VIEW IF NOT EXISTS v_quality_summary AS
SELECT 
    COUNT(*) as total_records,
    SUM(CASE WHEN salary IS NULL THEN 1 ELSE 0 END) as null_salaries,
    SUM(CASE WHEN department_id NOT IN (SELECT department_id FROM departments) THEN 1 ELSE 0 END) as orphan_departments,
    SUM(is_duplicate) as duplicate_records
FROM employees_partitioned;