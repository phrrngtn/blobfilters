-- ================================================================
--  Sampling Demo: TABLESAMPLE → UNPIVOT → Histogram Fingerprints
--
--  Self-contained demo that:
--    1. Creates domain fingerprints (us_states, currencies)
--    2. Creates a synthetic "orders" table (100K rows)
--    3. Samples at 1% via TABLESAMPLE BERNOULLI
--    4. UNPIVOTs, builds histogram fingerprints per column
--    5. Compares against domains and shows shape metrics
--
--  Usage (from project root):
--    duckdb -unsigned < demo/sampling_demo.sql
-- ================================================================

LOAD 'build/roaring.duckdb_extension';

-- =================================================================
-- STEP 1: Build domain fingerprints
-- =================================================================

SELECT '>>> STEP 1: Build domain fingerprints' AS info;

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

CREATE TABLE domain_fingerprints (
    domain_name  VARCHAR PRIMARY KEY,
    symbol_count INTEGER,
    fingerprint  BLOB
);

INSERT INTO domain_fingerprints
SELECT 'us_states', COUNT(*), roaring_build(symbol) FROM domain_us_states;

INSERT INTO domain_fingerprints
SELECT 'currencies', COUNT(*), roaring_build(symbol) FROM domain_currencies;

SELECT domain_name, symbol_count,
       roaring_cardinality(fingerprint) AS fp_cardinality
FROM domain_fingerprints;

-- =================================================================
-- STEP 2: Create synthetic "orders" table
--
-- 100K rows with:
--   state_code  — 10 US states (dimension, should match us_states domain)
--   currency    — 5 currency codes (dimension, should match currencies domain)
--   amount      — continuous decimal (measure, should match nothing)
--   customer_id — high cardinality varchar (no domain match, ambiguous shape)
-- =================================================================

SELECT '>>> STEP 2: Create synthetic orders table (100K rows)' AS info;

CREATE TABLE orders AS
SELECT
    (['Alabama','Alaska','Arizona','California','Colorado',
      'Florida','Georgia','New York','Ohio','Texas'])[
        (i % 10) + 1] AS state_code,
    (['USD','EUR','GBP','JPY','CHF'])[
        (i % 5) + 1] AS currency,
    (RANDOM() * 10000)::DECIMAL(10,2) AS amount,
    'CUST-' || ((i % 5000) + 1)::VARCHAR AS customer_id
FROM range(1, 100001) AS t(i);

SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT state_code) AS distinct_states,
       COUNT(DISTINCT currency) AS distinct_currencies,
       COUNT(DISTINCT customer_id) AS distinct_customers
FROM orders;

-- =================================================================
-- STEP 3: Sample at 1%
-- =================================================================

SELECT '>>> STEP 3: Sample at 1% via TABLESAMPLE BERNOULLI' AS info;

CREATE TEMP TABLE raw_sample AS
SELECT * FROM orders TABLESAMPLE BERNOULLI(1 PERCENT);

SELECT COUNT(*) AS sample_rows FROM raw_sample;

-- =================================================================
-- STEP 4: UNPIVOT → frequency counts → histogram fingerprints
-- =================================================================

SELECT '>>> STEP 4: UNPIVOT and build histogram fingerprints' AS info;

CREATE TEMP TABLE raw_sample_text AS
SELECT COLUMNS(*)::VARCHAR FROM raw_sample;

CREATE TEMP TABLE sampled_freq AS
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

-- Show frequency distribution summary per column
SELECT
    column_name,
    COUNT(*) AS distinct_values,
    SUM(freq)::INTEGER AS total_rows,
    MIN(freq)::INTEGER AS min_freq,
    MAX(freq)::INTEGER AS max_freq,
    AVG(freq)::DECIMAL(10,1) AS avg_freq
FROM sampled_freq
GROUP BY column_name;

-- =================================================================
-- STEP 5: Build histograms + inject shape metrics
-- =================================================================

SELECT '>>> STEP 5: Build histograms with shape metrics' AS info;

CREATE TEMP TABLE sampled_histograms AS
WITH COLUMN_STATS AS (
    SELECT
        column_name,
        COUNT(DISTINCT value) AS n_distinct,
        SUM(freq) AS total_rows,
        AVG(freq) AS avg_freq,
        roaring_build_histogram(value, freq) AS hist_fp
    FROM sampled_freq
    GROUP BY column_name
)
SELECT
    column_name,
    n_distinct,
    total_rows,
    roaring_histogram_set_shape(
        hist_fp,
        '{"cardinality_ratio":' || (n_distinct::DOUBLE / total_rows) || ','
        || '"repeatability":' || avg_freq || ','
        || '"discreteness":1.0,'
        || '"range_density":0.0,'
        || '"sample_pct":1.0,'
        || '"source":"TABLESAMPLE BERNOULLI(1%)"}'
    ) AS hist_fp
FROM COLUMN_STATS;

-- Show shape metrics per column
SELECT
    column_name,
    n_distinct,
    total_rows::INTEGER AS sample_rows,
    roaring_histogram_shape(hist_fp) AS shape
FROM sampled_histograms;

-- =================================================================
-- STEP 6: Compare sampled histograms against domain fingerprints
-- =================================================================

SELECT '>>> STEP 6: Domain matching — sampled histograms vs domain fingerprints' AS info;

SELECT
    h.column_name,
    d.domain_name,
    d.symbol_count AS domain_size,
    h.n_distinct AS sample_distinct,
    roaring_histogram_containment(h.hist_fp, d.fingerprint)   AS weighted_containment,
    roaring_containment(roaring_histogram_bitmap(h.hist_fp),
                        d.fingerprint)                        AS unweighted_containment
FROM sampled_histograms AS h
CROSS JOIN domain_fingerprints AS d
ORDER BY h.column_name, weighted_containment DESC;

-- =================================================================
-- STEP 7: Classification summary
--
-- Combine shape + domain matching to classify each column
-- =================================================================

SELECT '>>> STEP 7: Classification summary' AS info;

WITH BEST_MATCH AS (
    SELECT
        h.column_name,
        h.n_distinct,
        h.total_rows,
        d.domain_name,
        roaring_histogram_containment(h.hist_fp, d.fingerprint) AS best_containment,
        ROW_NUMBER() OVER (
            PARTITION BY h.column_name
            ORDER BY roaring_histogram_containment(h.hist_fp, d.fingerprint) DESC
        ) AS rn
    FROM sampled_histograms AS h
    CROSS JOIN domain_fingerprints AS d
)
SELECT
    column_name,
    n_distinct,
    total_rows::INTEGER AS sample_rows,
    (n_distinct::DOUBLE / total_rows)::DECIMAL(10,6) AS cardinality_ratio,
    domain_name AS best_domain,
    best_containment::DECIMAL(10,4) AS containment,
    CASE
        WHEN best_containment > 0.8 THEN 'dimension (domain: ' || domain_name || ')'
        WHEN n_distinct::DOUBLE / total_rows > 0.9 THEN 'measure (high cardinality)'
        WHEN n_distinct::DOUBLE / total_rows < 0.05 THEN 'dimension (low cardinality)'
        ELSE 'ambiguous'
    END AS classification
FROM BEST_MATCH
WHERE rn = 1
ORDER BY column_name;

-- =================================================================
-- STEP 8: Stability check — re-sample and compare shapes
--
-- Sampling is stochastic; shapes should be similar across runs.
-- =================================================================

SELECT '>>> STEP 8: Stability check — second sample' AS info;

CREATE TEMP TABLE raw_sample_2 AS
SELECT * FROM orders TABLESAMPLE BERNOULLI(1 PERCENT);

CREATE TEMP TABLE raw_sample_2_text AS
SELECT COLUMNS(*)::VARCHAR FROM raw_sample_2;

CREATE TEMP TABLE sampled_histograms_2 AS
WITH UNPIVOTED AS (
    UNPIVOT raw_sample_2_text ON COLUMNS(*) INTO
        NAME column_name VALUE value
),
FREQ AS (
    SELECT column_name, CAST(value AS VARCHAR) AS value,
           COUNT(*)::DOUBLE AS freq
    FROM UNPIVOTED GROUP BY column_name, value
),
COLUMN_STATS AS (
    SELECT
        column_name,
        COUNT(DISTINCT value) AS n_distinct,
        SUM(freq) AS total_rows,
        AVG(freq) AS avg_freq,
        roaring_build_histogram(value, freq) AS hist_fp
    FROM FREQ
    GROUP BY column_name
)
SELECT
    column_name,
    roaring_histogram_set_shape(
        hist_fp,
        '{"cardinality_ratio":' || (n_distinct::DOUBLE / total_rows) || ','
        || '"repeatability":' || avg_freq || ','
        || '"discreteness":1.0,'
        || '"range_density":0.0,'
        || '"sample_pct":1.0,'
        || '"source":"TABLESAMPLE BERNOULLI(1%) run 2"}'
    ) AS hist_fp
FROM COLUMN_STATS;

-- Shape similarity between two independent samples (lower = more similar)
SELECT
    a.column_name,
    roaring_histogram_similarity(a.hist_fp, b.hist_fp) AS shape_distance
FROM sampled_histograms AS a
JOIN sampled_histograms_2 AS b USING (column_name)
ORDER BY a.column_name;

SELECT '>>> Demo complete.' AS info;
