-- ================================================================
--  DuckDB Native Sampling Workflow
--
--  Builds histogram fingerprints from TABLESAMPLE data for any
--  DuckDB-accessible source (native tables, Parquet, CSV, DataFrames).
--
--  Pattern:
--    1. Sample the table once with TABLESAMPLE BERNOULLI
--    2. UNPIVOT all columns into (column_name, value) rows
--    3. Aggregate frequencies per (column_name, value)
--    4. Build histogram fingerprint per column via bf_build_histogram
--    5. Inject shape metrics via bf_histogram_set_shape
--    6. Compare against domain fingerprints
--
--  Prerequisites:
--    - roaring.duckdb_extension loaded
--    - domain_fingerprints table with (domain_name, fingerprint) columns
--
--  Usage:
--    SET VARIABLE target_table = 'my_schema.my_table';
--    SET VARIABLE sample_pct = 1.0;
--    .read sql/sampling/workflow_duckdb.sql
-- ================================================================

-- Step 1: Sample the target table
CREATE OR REPLACE TEMP TABLE raw_sample AS
SELECT * FROM query_table(getvariable('target_table'))
    TABLESAMPLE BERNOULLI(CAST(getvariable('sample_pct') AS DOUBLE) PERCENT);

-- Cast all columns to VARCHAR for UNPIVOT compatibility
CREATE OR REPLACE TEMP TABLE raw_sample_text AS
SELECT COLUMNS(*)::VARCHAR FROM raw_sample;

-- Step 2-3: UNPIVOT all columns, compute value frequencies
CREATE OR REPLACE TEMP TABLE sampled_freq AS
WITH UNPIVOTED AS (
    UNPIVOT raw_sample_text ON COLUMNS(*) INTO
        NAME column_name VALUE value
),
FREQ AS (
    SELECT
        column_name,
        CAST(value AS VARCHAR) AS value,
        COUNT(*)::DOUBLE AS freq
    FROM UNPIVOTED
    GROUP BY column_name, value
)
SELECT * FROM FREQ;

-- Step 4-5: Build histogram fingerprints with shape metrics
CREATE OR REPLACE TEMP TABLE sampled_histograms AS
WITH COLUMN_STATS AS (
    SELECT
        column_name,
        COUNT(DISTINCT value) AS n_distinct,
        SUM(freq) AS total_rows,
        AVG(freq) AS avg_freq,
        bf_build_histogram(value, freq) AS hist_fp
    FROM sampled_freq
    GROUP BY column_name
)
SELECT
    column_name,
    n_distinct,
    total_rows,
    bf_histogram_set_shape(
        hist_fp,
        '{"cardinality_ratio":' || (n_distinct::DOUBLE / total_rows) || ','
        || '"repeatability":' || avg_freq || ','
        || '"discreteness":1.0,'
        || '"range_density":0.0,'
        || '"sample_pct":' || getvariable('sample_pct') || ','
        || '"source":"TABLESAMPLE BERNOULLI"}'
    ) AS hist_fp
FROM COLUMN_STATS;

-- Step 6: Compare against domain fingerprints (if table exists)
SELECT
    h.column_name,
    h.n_distinct,
    h.total_rows::INTEGER AS sample_rows,
    d.domain_name,
    bf_histogram_containment(h.hist_fp, d.fingerprint)   AS weighted_containment,
    bf_containment(bf_histogram_bitmap(h.hist_fp),
                        d.fingerprint)                        AS unweighted_containment,
    bf_histogram_shape(h.hist_fp)                        AS shape
FROM sampled_histograms AS h
CROSS JOIN domain_fingerprints AS d
WHERE bf_intersection_card(
    bf_histogram_bitmap(h.hist_fp), d.fingerprint) > 0
ORDER BY h.column_name, weighted_containment DESC;
