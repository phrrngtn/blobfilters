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
-- DuckDB advantage: roaring_build() is a native aggregate function,
-- so we can build directly from column values — no JSON intermediate.
-- =================================================================

SELECT '>>> STEP 2: Build domain fingerprints' AS info;

CREATE TABLE domain_fingerprints (
    domain_name  VARCHAR PRIMARY KEY,
    symbol_count INTEGER,
    fingerprint  BLOB
);

INSERT INTO domain_fingerprints
SELECT 'us_states', COUNT(*), roaring_build(symbol) FROM domain_us_states;

INSERT INTO domain_fingerprints
SELECT 'currencies', COUNT(*), roaring_build(symbol) FROM domain_currencies;

INSERT INTO domain_fingerprints
SELECT 'http_status', COUNT(*), roaring_build(symbol) FROM domain_http_status;

INSERT INTO domain_fingerprints
SELECT 'mime_types', COUNT(*), roaring_build(symbol) FROM domain_mime_types;

INSERT INTO domain_fingerprints
SELECT 'languages', COUNT(*), roaring_build(symbol) FROM domain_languages;

INSERT INTO domain_fingerprints
SELECT 'elements', COUNT(*), roaring_build(symbol) FROM domain_elements;

SELECT domain_name,
       symbol_count,
       roaring_cardinality(fingerprint) AS fp_cardinality,
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

WITH probe AS (
    SELECT roaring_build_json(
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
    roaring_cardinality(probe.fp)                      AS probe_size,
    roaring_intersection_card(probe.fp, d.fingerprint) AS est_hits,
    roaring_containment(probe.fp, d.fingerprint)       AS containment,
    roaring_jaccard(probe.fp, d.fingerprint)           AS jaccard
FROM domain_fingerprints d, probe
ORDER BY containment DESC;

-- =================================================================
-- STEP 4: Same query using roaring_containment_json (no CTE needed)
-- =================================================================

SELECT '>>> STEP 4: Direct JSON probe via roaring_containment_json' AS info;

SELECT
    domain_name,
    symbol_count                                       AS domain_size,
    roaring_containment_json(
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
    SELECT roaring_build_json(
        '["USD","EUR","GBP","JPY","CHF","CAD","AUD","NZD"]'
    ) AS fp
)
SELECT
    d.domain_name,
    d.symbol_count                                     AS domain_size,
    roaring_intersection_card(probe.fp, d.fingerprint) AS est_hits,
    roaring_containment(probe.fp, d.fingerprint)       AS containment
FROM domain_fingerprints d, probe
ORDER BY containment DESC;

-- =================================================================
-- STEP 6: Third probe — pure noise (should match nothing)
-- =================================================================

SELECT '>>> STEP 6: Pure noise probe' AS info;

WITH probe AS (
    SELECT roaring_build_json(
        '["xyzzy","plugh","42","DEADBEEF","asdf"]'
    ) AS fp
)
SELECT
    d.domain_name,
    roaring_intersection_card(probe.fp, d.fingerprint) AS est_hits,
    roaring_containment(probe.fp, d.fingerprint)       AS containment
FROM domain_fingerprints d, probe
ORDER BY containment DESC;

-- =================================================================
-- STEP 7 (DuckDB bonus): Build fingerprint directly from aggregate
--
-- DuckDB's roaring_build() aggregate means you can build a domain
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
           roaring_build(symbol) AS fingerprint
    FROM all_domains
    GROUP BY domain_name
),
probe AS (
    SELECT roaring_build_json(
        '["California","Texas","USD","EUR","404 Not Found","Python"]'
    ) AS fp
)
SELECT
    d.domain_name,
    d.symbol_count,
    roaring_containment(probe.fp, d.fingerprint) AS containment
FROM domain_fps d, probe
WHERE roaring_intersection_card(probe.fp, d.fingerprint) > 0
ORDER BY containment DESC;

SELECT '>>> Demo complete.' AS info;
