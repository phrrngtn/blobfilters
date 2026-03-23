-- ================================================================
--  DuckDB Domain Fingerprint Demo
--
--  Mirrors sqlite_demo.c: creates domain tables with real symbol
--  values, builds fingerprints into a unified table, then probes
--  "wild" collections of symbols against all domains.
--
--  Build the DuckDB extension first:
--    git submodule update --init --recursive
--    make
--
--  Usage (from project root):
--    duckdb -unsigned < demo/duckdb_demo.sql
-- ================================================================

LOAD 'build/roaring.duckdb_extension';

-- =================================================================
-- STEP 1: Create domain tables with real symbol values
-- =================================================================

CREATE TABLE domain_us_states (symbol VARCHAR);
INSERT INTO domain_us_states VALUES
    ('Alabama'),('Alaska'),('Arizona'),('Arkansas'),('California'),
    ('Colorado'),('Connecticut'),('Delaware'),('Florida'),('Georgia'),
    ('Hawaii'),('Idaho'),('Illinois'),('Indiana'),('Iowa'),
    ('Kansas'),('Kentucky'),('Louisiana'),('Maine'),('Maryland'),
    ('Massachusetts'),('Michigan'),('Minnesota'),('Mississippi'),('Missouri'),
    ('Montana'),('Nebraska'),('Nevada'),('New Hampshire'),('New Jersey'),
    ('New Mexico'),('New York'),('North Carolina'),('North Dakota'),('Ohio'),
    ('Oklahoma'),('Oregon'),('Pennsylvania'),('Rhode Island'),('South Carolina'),
    ('South Dakota'),('Tennessee'),('Texas'),('Utah'),('Vermont'),
    ('Virginia'),('Washington'),('West Virginia'),('Wisconsin'),('Wyoming');

CREATE TABLE domain_currencies (symbol VARCHAR);
INSERT INTO domain_currencies VALUES
    ('USD'),('EUR'),('GBP'),('JPY'),('CHF'),('CAD'),('AUD'),('NZD'),
    ('CNY'),('HKD'),('SGD'),('KRW'),('INR'),('BRL'),('MXN'),('ZAR'),
    ('SEK'),('NOK'),('DKK'),('PLN'),('CZK'),('HUF'),('TRY'),('RUB'),
    ('THB'),('MYR'),('IDR'),('PHP'),('TWD'),('ILS'),('AED'),('SAR'),
    ('CLP'),('COP'),('PEN'),('ARS'),('EGP'),('NGN'),('KES'),('GHS');

CREATE TABLE domain_http_status (symbol VARCHAR);
INSERT INTO domain_http_status VALUES
    ('200 OK'),('201 Created'),('204 No Content'),
    ('301 Moved Permanently'),('302 Found'),('304 Not Modified'),
    ('400 Bad Request'),('401 Unauthorized'),('403 Forbidden'),
    ('404 Not Found'),('405 Method Not Allowed'),('408 Request Timeout'),
    ('409 Conflict'),('413 Payload Too Large'),('415 Unsupported Media Type'),
    ('429 Too Many Requests'),
    ('500 Internal Server Error'),('502 Bad Gateway'),
    ('503 Service Unavailable'),('504 Gateway Timeout');

CREATE TABLE domain_mime_types (symbol VARCHAR);
INSERT INTO domain_mime_types VALUES
    ('text/html'),('text/css'),('text/plain'),('text/csv'),
    ('application/json'),('application/xml'),('application/pdf'),
    ('application/zip'),('application/gzip'),
    ('application/octet-stream'),('application/javascript'),
    ('image/png'),('image/jpeg'),('image/gif'),('image/svg+xml'),
    ('image/webp'),('audio/mpeg'),('audio/ogg'),('video/mp4'),
    ('video/webm'),('multipart/form-data'),
    ('application/x-www-form-urlencoded');

CREATE TABLE domain_languages (symbol VARCHAR);
INSERT INTO domain_languages VALUES
    ('Python'),('JavaScript'),('TypeScript'),('Java'),('C'),('C++'),
    ('C#'),('Go'),('Rust'),('Swift'),('Kotlin'),('Ruby'),('PHP'),
    ('Scala'),('Haskell'),('Elixir'),('Clojure'),('Lua'),('R'),
    ('Julia'),('Dart'),('Perl'),('MATLAB'),('SQL'),('Shell'),
    ('Assembly'),('Fortran'),('COBOL'),('Erlang'),('OCaml');

CREATE TABLE domain_elements (symbol VARCHAR);
INSERT INTO domain_elements VALUES
    ('Hydrogen'),('Helium'),('Lithium'),('Beryllium'),('Boron'),
    ('Carbon'),('Nitrogen'),('Oxygen'),('Fluorine'),('Neon'),
    ('Sodium'),('Magnesium'),('Aluminium'),('Silicon'),('Phosphorus'),
    ('Sulfur'),('Chlorine'),('Argon'),('Potassium'),('Calcium'),
    ('Scandium'),('Titanium'),('Vanadium'),('Chromium'),('Manganese'),
    ('Iron'),('Cobalt'),('Nickel'),('Copper'),('Zinc');

SELECT '>>> STEP 1: Domain sizes' AS info;
SELECT 'us_states' AS domain, COUNT(*) AS n FROM domain_us_states
UNION ALL SELECT 'currencies', COUNT(*) FROM domain_currencies
UNION ALL SELECT 'http_status', COUNT(*) FROM domain_http_status
UNION ALL SELECT 'mime_types', COUNT(*) FROM domain_mime_types
UNION ALL SELECT 'languages', COUNT(*) FROM domain_languages
UNION ALL SELECT 'elements', COUNT(*) FROM domain_elements;

-- =================================================================
-- STEP 2: Build fingerprints into a unified table
--
-- DuckDB advantage: bf_build() is a native aggregate function,
-- so we can build directly from column values — no JSON intermediate.
-- =================================================================

SELECT '>>> STEP 2: Build domain fingerprints' AS info;

CREATE TABLE domain_fingerprints (
    domain_name  VARCHAR PRIMARY KEY,
    symbol_count INTEGER,
    fingerprint  BLOB
);

INSERT INTO domain_fingerprints
SELECT 'us_states', COUNT(*), bf_build(symbol) FROM domain_us_states;

INSERT INTO domain_fingerprints
SELECT 'currencies', COUNT(*), bf_build(symbol) FROM domain_currencies;

INSERT INTO domain_fingerprints
SELECT 'http_status', COUNT(*), bf_build(symbol) FROM domain_http_status;

INSERT INTO domain_fingerprints
SELECT 'mime_types', COUNT(*), bf_build(symbol) FROM domain_mime_types;

INSERT INTO domain_fingerprints
SELECT 'languages', COUNT(*), bf_build(symbol) FROM domain_languages;

INSERT INTO domain_fingerprints
SELECT 'elements', COUNT(*), bf_build(symbol) FROM domain_elements;

SELECT domain_name,
       symbol_count,
       bf_cardinality(fingerprint) AS fp_cardinality,
       octet_length(fingerprint) AS blob_bytes
FROM domain_fingerprints
ORDER BY symbol_count DESC;

-- =================================================================
-- STEP 3: Probe "wild" symbols against all domain fingerprints
--
-- 8 US states + 5 currencies + 3 HTTP codes + 2 languages
-- + 4 garbage = 22 symbols
-- =================================================================

SELECT '>>> STEP 3: Probe wild symbols via CROSS JOIN' AS info;

.timer on
WITH probe AS (
    SELECT bf_build_json(
        '["California","Texas","New York","Florida","Ohio",
          "Illinois","Georgia","Oregon",
          "USD","EUR","GBP","JPY","CHF",
          "404 Not Found","500 Internal Server Error","200 OK",
          "Python","Rust",
          "FooBar","DEADBEEF","xyzzy","42"]'
    ) AS fp
)
SELECT
    d.domain_name,
    d.symbol_count                                     AS domain_size,
    bf_cardinality(probe.fp)                      AS probe_size,
    bf_intersection_card(probe.fp, d.fingerprint) AS est_hits,
    bf_containment(probe.fp, d.fingerprint)       AS containment,
    bf_jaccard(probe.fp, d.fingerprint)           AS jaccard
FROM domain_fingerprints d, probe
ORDER BY containment DESC;

-- =================================================================
-- STEP 4: Same query using bf_containment_json (no CTE needed)
-- =================================================================

SELECT '>>> STEP 4: Direct JSON probe via bf_containment_json' AS info;

SELECT
    domain_name,
    symbol_count                                       AS domain_size,
    bf_containment_json(
        '["California","Texas","New York","Florida","Ohio",
          "Illinois","Georgia","Oregon",
          "USD","EUR","GBP","JPY","CHF",
          "404 Not Found","500 Internal Server Error","200 OK",
          "Python","Rust",
          "FooBar","DEADBEEF","xyzzy","42"]',
        fingerprint
    )                                                  AS containment
FROM domain_fingerprints
ORDER BY containment DESC;

-- =================================================================
-- STEP 5: Second probe — pure currency codes (should be 100%)
-- =================================================================

SELECT '>>> STEP 5: Pure currency probe' AS info;

WITH probe AS (
    SELECT bf_build_json(
        '["USD","EUR","GBP","JPY","CHF","CAD","AUD","NZD"]'
    ) AS fp
)
SELECT
    d.domain_name,
    d.symbol_count                                     AS domain_size,
    bf_intersection_card(probe.fp, d.fingerprint) AS est_hits,
    bf_containment(probe.fp, d.fingerprint)       AS containment
FROM domain_fingerprints d, probe
ORDER BY containment DESC;

-- =================================================================
-- STEP 6: Third probe — pure noise (should match nothing)
-- =================================================================

SELECT '>>> STEP 6: Pure noise probe' AS info;

WITH probe AS (
    SELECT bf_build_json(
        '["xyzzy","plugh","42","DEADBEEF","asdf"]'
    ) AS fp
)
SELECT
    d.domain_name,
    bf_intersection_card(probe.fp, d.fingerprint) AS est_hits,
    bf_containment(probe.fp, d.fingerprint)       AS containment
FROM domain_fingerprints d, probe
ORDER BY containment DESC;

-- =================================================================
-- STEP 7 (DuckDB bonus): Build fingerprint directly from aggregate
--
-- DuckDB's bf_build() aggregate means you can build a domain
-- fingerprint in a single query without JSON intermediaries.
-- =================================================================

SELECT '>>> STEP 7: Direct aggregate build + probe in one query' AS info;

WITH all_domains AS (
    SELECT 'us_states' AS domain_name, symbol FROM domain_us_states
    UNION ALL SELECT 'currencies', symbol FROM domain_currencies
    UNION ALL SELECT 'http_status', symbol FROM domain_http_status
    UNION ALL SELECT 'mime_types', symbol FROM domain_mime_types
    UNION ALL SELECT 'languages', symbol FROM domain_languages
    UNION ALL SELECT 'elements', symbol FROM domain_elements
),
domain_fps AS (
    SELECT domain_name,
           COUNT(*) AS symbol_count,
           bf_build(symbol) AS fingerprint
    FROM all_domains
    GROUP BY domain_name
),
probe AS (
    SELECT bf_build_json(
        '["California","Texas","USD","EUR","404 Not Found","Python"]'
    ) AS fp
)
SELECT
    d.domain_name,
    d.symbol_count,
    bf_containment(probe.fp, d.fingerprint) AS containment
FROM domain_fps d, probe
WHERE bf_intersection_card(probe.fp, d.fingerprint) > 0
ORDER BY containment DESC;

-- =================================================================
-- STEP 8: Histogram fingerprint — metadata-only domain detection
--
-- Simulates what you'd get from sys.dm_db_stats_histogram:
-- boundary values + frequency counts, no data access needed.
-- =================================================================

SELECT '>>> STEP 8: Build histogram fingerprint from simulated metadata' AS info;

-- Simulate a SQL Server histogram for a state_code column
-- (low cardinality, fully discrete, high repeatability)
CREATE TABLE hist_state_code AS
SELECT * FROM (VALUES
    ('Alabama',      28000.0, 0.0, 0.0, 0.0),
    ('Alaska',        5000.0, 0.0, 0.0, 0.0),
    ('Arizona',      22000.0, 0.0, 0.0, 0.0),
    ('California',  120000.0, 0.0, 0.0, 0.0),
    ('Colorado',     18000.0, 0.0, 0.0, 0.0),
    ('Florida',      65000.0, 0.0, 0.0, 0.0),
    ('Georgia',      32000.0, 0.0, 0.0, 0.0),
    ('New York',     58000.0, 0.0, 0.0, 0.0),
    ('Ohio',         35000.0, 0.0, 0.0, 0.0),
    ('Texas',        85000.0, 0.0, 0.0, 0.0)
) AS t(range_high_key, equal_rows, range_rows, distinct_range_rows, average_range_rows);

-- Build histogram fingerprint
CREATE TABLE hist_fp_state AS
SELECT bf_build_histogram(
    range_high_key, equal_rows, range_rows,
    distinct_range_rows, average_range_rows
) AS hist_fp
FROM hist_state_code;

-- Show shape metrics (expect: fully discrete, high repeatability)
SELECT '>>> Shape metrics for state_code histogram:' AS info;
SELECT bf_histogram_shape(hist_fp) AS shape FROM hist_fp_state;

-- =================================================================
-- STEP 9: Compare histogram fingerprint against extensional domains
-- =================================================================

SELECT '>>> STEP 9: Weighted containment — histogram vs domain fingerprints' AS info;

SELECT
    d.domain_name,
    d.symbol_count                                            AS domain_size,
    bf_cardinality(bf_histogram_bitmap(h.hist_fp))  AS hist_keys,
    bf_histogram_containment(h.hist_fp, d.fingerprint)   AS weighted_containment,
    bf_containment(bf_histogram_bitmap(h.hist_fp),
                        d.fingerprint)                        AS unweighted_containment
FROM domain_fingerprints AS d, hist_fp_state AS h
ORDER BY weighted_containment DESC;

-- =================================================================
-- STEP 10: Contrast with a continuous/measure-like histogram
-- =================================================================

SELECT '>>> STEP 10: Continuous histogram (simulated amount column)' AS info;

CREATE TABLE hist_amount AS
SELECT * FROM (VALUES
    ('100.00',   1.0, 5000.0,  500.0, 10.0),
    ('500.00',   2.0, 8000.0,  800.0, 10.0),
    ('1000.00',  1.0, 6000.0,  600.0, 10.0),
    ('5000.00',  1.0, 4000.0,  400.0, 10.0),
    ('10000.00', 1.0, 3000.0,  300.0, 10.0)
) AS t(range_high_key, equal_rows, range_rows, distinct_range_rows, average_range_rows);

CREATE TABLE hist_fp_amount AS
SELECT bf_build_histogram(
    range_high_key, equal_rows, range_rows,
    distinct_range_rows, average_range_rows
) AS hist_fp
FROM hist_amount;

-- Compare shapes: state_code (dimension) vs amount (measure)
SELECT
    'state_code (dimension)' AS column_type,
    bf_histogram_shape(hist_fp) AS shape
FROM hist_fp_state
UNION ALL
SELECT
    'amount (measure)',
    bf_histogram_shape(hist_fp)
FROM hist_fp_amount;

-- Shape similarity distance (0 = identical, higher = more different)
SELECT bf_histogram_similarity(s.hist_fp, a.hist_fp) AS shape_distance
FROM hist_fp_state AS s, hist_fp_amount AS a;

-- =================================================================
-- STEP 11: Source-agnostic histogram — 2-arg (key, weight)
--
-- Universal path: works with TABLESAMPLE, pg_stats, blobboxes,
-- Pandas value_counts(), or any (value, frequency) pairs.
-- =================================================================

SELECT '>>> STEP 11: Source-agnostic histogram from simulated TABLESAMPLE' AS info;

-- Simulate: SELECT state, COUNT(*) FROM orders TABLESAMPLE BERNOULLI(1) GROUP BY state
CREATE TABLE sample_state AS
SELECT * FROM (VALUES
    ('Alabama',     280),
    ('Alaska',       50),
    ('Arizona',     220),
    ('California', 1200),
    ('Colorado',    180),
    ('Florida',     650),
    ('Georgia',     320),
    ('New York',    580),
    ('Ohio',        350),
    ('Texas',       850)
) AS t(state_value, sample_count);

-- Build with 2-arg form: just key + weight
CREATE TABLE sample_fp_state AS
SELECT bf_build_histogram(state_value, sample_count::DOUBLE) AS hist_fp
FROM sample_state;

-- Inject shape computed in SQL (the SQL layer knows the source semantics)
CREATE TABLE sample_fp_shaped AS
SELECT bf_histogram_set_shape(
    hist_fp,
    '{"cardinality_ratio":' || (10.0 / 4680) || ','
    || '"repeatability":' || (4680.0 / 10) || ','
    || '"discreteness":1.0,'
    || '"range_density":0.0,'
    || '"sample_pct":1.0,'
    || '"source_query":"TABLESAMPLE BERNOULLI(1)"}'
) AS hist_fp
FROM sample_fp_state;

-- Show shape (includes custom fields)
SELECT bf_histogram_shape(hist_fp) AS shape FROM sample_fp_shaped;

-- Compare against extensional domains — should match us_states
SELECT
    d.domain_name,
    bf_histogram_containment(h.hist_fp, d.fingerprint)   AS weighted_containment,
    bf_containment(bf_histogram_bitmap(h.hist_fp),
                        d.fingerprint)                        AS unweighted_containment
FROM domain_fingerprints AS d, sample_fp_shaped AS h
ORDER BY weighted_containment DESC;

-- =================================================================
-- STEP 12: Cross-source shape comparison
--
-- Compare histogram shapes from different sources:
-- SQL Server statistics vs TABLESAMPLE — same column, similar shape.
-- =================================================================

SELECT '>>> STEP 12: Cross-source shape comparison' AS info;

SELECT
    'sqlserver_histogram' AS source,
    bf_histogram_shape(hist_fp) AS shape
FROM hist_fp_state
UNION ALL
SELECT
    'tablesample',
    bf_histogram_shape(hist_fp)
FROM sample_fp_shaped
UNION ALL
SELECT
    'sqlserver_amount',
    bf_histogram_shape(hist_fp)
FROM hist_fp_amount;

-- Same column from different sources: should be similar
SELECT bf_histogram_similarity(a.hist_fp, b.hist_fp) AS state_vs_sample
FROM hist_fp_state AS a, sample_fp_shaped AS b;

-- Different columns: should be very different
SELECT bf_histogram_similarity(a.hist_fp, b.hist_fp) AS state_vs_amount
FROM sample_fp_shaped AS a, hist_fp_amount AS b;

SELECT '>>> Demo complete.' AS info;
