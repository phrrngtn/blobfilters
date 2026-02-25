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
