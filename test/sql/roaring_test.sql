-- Test script for DuckDB Roaring Bitmap Extension
-- Usage: Run in DuckDB after loading the extension

-- Load the extension (adjust path as needed)
-- LOAD 'build/roaring_extension.duckdb_extension';

-- ============================================================
-- Test 1: Basic roaring_build and roaring_cardinality
-- ============================================================

-- Create test table with integers
CREATE OR REPLACE TABLE test_integers AS
SELECT * FROM range(1, 1001) t(id);

-- Build a roaring bitmap from integers
SELECT roaring_cardinality(roaring_build(id)) AS cardinality
FROM test_integers;
-- Expected: 1000

-- ============================================================
-- Test 2: Build from strings
-- ============================================================

CREATE OR REPLACE TABLE test_strings AS
SELECT 'user_' || i AS username
FROM range(1, 501) t(i);

SELECT roaring_cardinality(roaring_build(username)) AS cardinality
FROM test_strings;
-- Expected: 500

-- ============================================================
-- Test 3: Intersection cardinality
-- ============================================================

-- Create two overlapping sets
CREATE OR REPLACE TABLE set_a AS SELECT * FROM range(1, 101) t(val);    -- 1-100
CREATE OR REPLACE TABLE set_b AS SELECT * FROM range(51, 151) t(val);   -- 51-150

-- Build bitmaps
CREATE OR REPLACE TABLE bitmap_a AS
SELECT roaring_build(val) AS bm FROM set_a;

CREATE OR REPLACE TABLE bitmap_b AS
SELECT roaring_build(val) AS bm FROM set_b;

-- Test intersection cardinality (should be 50: values 51-100)
SELECT roaring_intersection_card(a.bm, b.bm) AS intersection_count
FROM bitmap_a a, bitmap_b b;
-- Expected: 50

-- ============================================================
-- Test 4: Containment
-- ============================================================

-- set_a has 100 elements, 50 of which are in set_b
-- containment(a, b) = |A ∩ B| / |A| = 50/100 = 0.5
SELECT roaring_containment(a.bm, b.bm) AS containment_a_in_b
FROM bitmap_a a, bitmap_b b;
-- Expected: 0.5

-- containment(b, a) = |A ∩ B| / |B| = 50/100 = 0.5
SELECT roaring_containment(b.bm, a.bm) AS containment_b_in_a
FROM bitmap_a a, bitmap_b b;
-- Expected: 0.5

-- ============================================================
-- Test 5: Jaccard similarity
-- ============================================================

-- |A| = 100, |B| = 100, |A ∩ B| = 50
-- |A ∪ B| = 100 + 100 - 50 = 150
-- Jaccard = 50/150 = 0.333...
SELECT roaring_jaccard(a.bm, b.bm) AS jaccard_similarity
FROM bitmap_a a, bitmap_b b;
-- Expected: ~0.333

-- ============================================================
-- Test 6: Simulate column fingerprint matching
-- ============================================================

-- Create a "stored fingerprints" table (like column_fingerprints in the plan)
CREATE OR REPLACE TABLE fingerprints AS
SELECT
    'db1' AS source_db,
    'users' AS source_table,
    'user_id' AS source_column,
    roaring_build(id) AS fingerprint,
    COUNT(*) AS row_count
FROM range(1, 10001) t(id)
UNION ALL
SELECT
    'db1', 'orders', 'order_id',
    roaring_build(id),
    COUNT(*)
FROM range(1, 5001) t(id)
UNION ALL
SELECT
    'db1', 'products', 'product_id',
    roaring_build(id),
    COUNT(*)
FROM range(1, 1001) t(id);

-- Simulate an input column (subset of user_ids)
CREATE OR REPLACE TABLE input_column AS
SELECT * FROM range(500, 1500) t(val);  -- 1000 values, 500 overlap with user_id range

-- Build input fingerprint
CREATE OR REPLACE TABLE input_fp AS
SELECT roaring_build(val) AS fp FROM input_column;

-- Find best matches
SELECT
    f.source_table,
    f.source_column,
    roaring_cardinality(f.fingerprint) AS stored_card,
    roaring_intersection_card(f.fingerprint, i.fp) AS overlap,
    roaring_containment(i.fp, f.fingerprint) AS containment,
    roaring_jaccard(f.fingerprint, i.fp) AS jaccard
FROM fingerprints f, input_fp i
ORDER BY containment DESC;

-- ============================================================
-- Test 7: NULL handling
-- ============================================================

CREATE OR REPLACE TABLE test_nulls AS
SELECT CASE WHEN i % 3 = 0 THEN NULL ELSE i END AS val
FROM range(1, 101) t(i);

-- Should only count non-NULL values (~66)
SELECT roaring_cardinality(roaring_build(val)) AS cardinality
FROM test_nulls;

-- ============================================================
-- Test 8: Serialization round-trip
-- ============================================================

-- Store serialized bitmap
CREATE OR REPLACE TABLE stored_bitmap AS
SELECT roaring_build(id) AS serialized
FROM range(1, 1001) t(id);

-- Verify it round-trips correctly
SELECT roaring_cardinality(serialized) AS cardinality
FROM stored_bitmap;
-- Expected: 1000

-- ============================================================
-- Test 9: Histogram fingerprint — build and compare
-- ============================================================

-- Simulate a histogram from sys.dm_db_stats_histogram
-- Low-cardinality dimension: state_code with 5 steps, all discrete
CREATE OR REPLACE TABLE test_histogram AS
SELECT * FROM (VALUES
    ('AL', 10000.0, 0.0, 0.0, 0.0),
    ('CA', 50000.0, 0.0, 0.0, 0.0),
    ('NY', 40000.0, 0.0, 0.0, 0.0),
    ('TX', 35000.0, 0.0, 0.0, 0.0),
    ('FL', 25000.0, 0.0, 0.0, 0.0)
) AS t(range_high_key, equal_rows, range_rows, distinct_range_rows, average_range_rows);

-- Build histogram fingerprint
CREATE OR REPLACE TABLE test_hist_fp AS
SELECT roaring_build_histogram(
    range_high_key, equal_rows, range_rows,
    distinct_range_rows, average_range_rows
) AS hist_fp
FROM test_histogram;

SELECT 'Histogram fingerprint built' AS status;
SELECT hist_fp FROM test_hist_fp;

-- Extract shape metrics
SELECT roaring_histogram_shape(hist_fp) AS shape FROM test_hist_fp;

-- Extract bitmap and check cardinality
SELECT roaring_cardinality(roaring_histogram_bitmap(hist_fp)) AS bitmap_cardinality
FROM test_hist_fp;
-- Expected: 5

-- ============================================================
-- Test 10: Histogram vs extensional domain comparison
-- ============================================================

-- Build extensional domain containing AL, CA, NY, TX, FL + more states
CREATE OR REPLACE TABLE test_states_domain AS
SELECT roaring_build(state) AS domain FROM (VALUES
    ('AL'),('CA'),('NY'),('TX'),('FL'),
    ('OH'),('PA'),('IL'),('GA'),('NC')
) AS t(state);

-- Weighted containment: all histogram keys are in the domain
-- (10000+50000+40000+35000+25000) / total = 1.0
SELECT roaring_histogram_containment(h.hist_fp, d.domain) AS weighted_containment
FROM test_hist_fp h, test_states_domain d;
-- Expected: 1.0 (all histogram keys match)

-- Unweighted bitmap containment
SELECT roaring_containment(roaring_histogram_bitmap(h.hist_fp), d.domain) AS unweighted_containment
FROM test_hist_fp h, test_states_domain d;
-- Expected: 1.0

-- ============================================================
-- Test 11: Partial match — some histogram keys not in domain
-- ============================================================

-- Domain with only AL and CA
CREATE OR REPLACE TABLE test_partial_domain AS
SELECT roaring_build(state) AS domain FROM (VALUES
    ('AL'),('CA')
) AS t(state);

-- Weighted: (10000+50000) / 160000 = 0.375
SELECT roaring_histogram_containment(h.hist_fp, d.domain) AS weighted_containment
FROM test_hist_fp h, test_partial_domain d;

-- Unweighted: 2/5 = 0.4
SELECT roaring_containment(roaring_histogram_bitmap(h.hist_fp), d.domain) AS unweighted_containment
FROM test_hist_fp h, test_partial_domain d;

-- ============================================================
-- Test 12: Shape similarity between histograms
-- ============================================================

-- Build a continuous histogram (measures-like)
CREATE OR REPLACE TABLE test_continuous_histogram AS
SELECT * FROM (VALUES
    ('100.00',  1.0, 5000.0, 500.0, 10.0),
    ('200.00',  1.0, 5000.0, 500.0, 10.0),
    ('300.00',  1.0, 5000.0, 500.0, 10.0),
    ('400.00',  1.0, 5000.0, 500.0, 10.0),
    ('500.00',  1.0, 5000.0, 500.0, 10.0)
) AS t(range_high_key, equal_rows, range_rows, distinct_range_rows, average_range_rows);

CREATE OR REPLACE TABLE test_cont_fp AS
SELECT roaring_build_histogram(
    range_high_key, equal_rows, range_rows,
    distinct_range_rows, average_range_rows
) AS hist_fp
FROM test_continuous_histogram;

-- Shape of continuous histogram (low discreteness, low repeatability)
SELECT roaring_histogram_shape(hist_fp) AS continuous_shape FROM test_cont_fp;

-- Similarity: discrete vs continuous should be high (different)
SELECT roaring_histogram_similarity(d.hist_fp, c.hist_fp) AS shape_distance
FROM test_hist_fp d, test_cont_fp c;
-- Expected: > 0.1 (significantly different shapes)

-- Self-similarity should be 0
SELECT roaring_histogram_similarity(d.hist_fp, d.hist_fp) AS self_similarity
FROM test_hist_fp d;
-- Expected: 0.0

-- ============================================================
-- Test 13: Source-agnostic 2-arg histogram (key, weight)
-- ============================================================

-- Simulate TABLESAMPLE: SELECT state, COUNT(*) ... GROUP BY state
CREATE OR REPLACE TABLE test_sample AS
SELECT * FROM (VALUES
    ('AL', 100.0),
    ('CA', 500.0),
    ('NY', 400.0),
    ('TX', 350.0),
    ('FL', 250.0)
) AS t(state_value, sample_count);

-- Build with 2-arg form
CREATE OR REPLACE TABLE test_sample_fp AS
SELECT roaring_build_histogram(state_value, sample_count) AS hist_fp
FROM test_sample;

SELECT roaring_cardinality(roaring_histogram_bitmap(hist_fp)) AS bitmap_cardinality
FROM test_sample_fp;
-- Expected: 5

-- Weighted containment against known domain
SELECT roaring_histogram_containment(h.hist_fp, d.domain) AS weighted_containment
FROM test_sample_fp AS h, test_states_domain AS d;
-- Expected: 1.0

-- ============================================================
-- Test 14: set_shape — inject externally-computed shape
-- ============================================================

CREATE OR REPLACE TABLE test_shaped_fp AS
SELECT roaring_histogram_set_shape(
    hist_fp,
    '{"cardinality_ratio":0.05,"repeatability":320.0,"discreteness":1.0,"range_density":0.0,"source_query":"TABLESAMPLE"}'
) AS hist_fp
FROM test_sample_fp;

-- Verify injected shape
SELECT roaring_histogram_shape(hist_fp) AS shape FROM test_shaped_fp;
-- Should include cardinality_ratio, repeatability, discreteness AND source_query

-- Shape similarity: sample vs SQL Server histogram of same column
SELECT roaring_histogram_similarity(a.hist_fp, b.hist_fp) AS sample_vs_histogram
FROM test_shaped_fp AS a, test_hist_fp AS b;
-- Expected: small distance (similar shapes)

-- ============================================================
-- Cleanup
-- ============================================================

DROP TABLE IF EXISTS test_integers;
DROP TABLE IF EXISTS test_strings;
DROP TABLE IF EXISTS set_a;
DROP TABLE IF EXISTS set_b;
DROP TABLE IF EXISTS bitmap_a;
DROP TABLE IF EXISTS bitmap_b;
DROP TABLE IF EXISTS fingerprints;
DROP TABLE IF EXISTS input_column;
DROP TABLE IF EXISTS input_fp;
DROP TABLE IF EXISTS test_nulls;
DROP TABLE IF EXISTS stored_bitmap;

SELECT 'All tests completed!' AS status;
