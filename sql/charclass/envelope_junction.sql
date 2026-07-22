-- FK-target architecture: C vocabulary = the dimension (FK target); user-land
-- domain_features = the FK references; two envelope paths cross-check each other.
LOAD '/Users/paulharrington/checkouts/blobfilters/build/duckdb/blobfilters.duckdb_extension';

-- FK TARGET (from the one C source): the feature vocabulary dimension.
CREATE TABLE cc_features AS
  SELECT * FROM (SELECT unnest(from_json(json_extract(bf_cc_features_json(),'$.features'),
    '[{"bit":"INT","name":"VARCHAR","regexp":"VARCHAR","expr":"VARCHAR"}]'), recursive := true));

-- USER-LAND JUNCTION: a curator DECLARES each domain's necessary features (FK -> cc_features.name).
CREATE TABLE domain_features AS SELECT * FROM (VALUES
  ('us_state','all_upper'), ('us_state','len_2'), ('us_state','single_token'),
  ('us_state','title_case'),            -- OVER-declared: states are UPPER, never title-case
  ('us_state','is_all_caps'),           -- BROKEN FK: name not in the vocabulary
  ('small_int','all_digits'), ('small_int','single_token')
) AS t(domain, feature_name);

.print '=== FK integrity: declared feature_names that do not resolve to the vocabulary ==='
SELECT df.domain, df.feature_name AS broken_fk
FROM domain_features df LEFT JOIN cc_features f ON f.name = df.feature_name
WHERE f.name IS NULL;

-- DECLARED necessary mask = OR of the (valid) declared features' bits.
CREATE TABLE declared AS
SELECT df.domain, bit_or((1::UBIGINT) << f.bit) AS necessary
FROM domain_features df JOIN cc_features f ON f.name = df.feature_name
GROUP BY df.domain;

-- COMPILED necessary mask = bit_and over the full-enumeration member signatures.
CREATE TABLE members AS
      SELECT 'us_state' d, upper(Abbreviation) m FROM read_csv('us_states.csv')
UNION ALL SELECT 'small_int', (i)::varchar m FROM range(1,101) t(i);
CREATE TABLE compiled AS
SELECT d AS domain, bit_and(bf_cc_signature(m)) AS necessary FROM members GROUP BY d;

.print ''
.print '=== compiled(enumeration) vs declared(curator) cross-check ==='
SELECT c.domain,
  (SELECT string_agg(f.name, ',' ORDER BY f.bit) FROM cc_features f
     WHERE ((c.necessary >> f.bit)&1)=1 AND ((coalesce(d.necessary,0) >> f.bit)&1)=0)
    AS underdeclared_missing_from_curator,
  (SELECT string_agg(f.name, ',' ORDER BY f.bit) FROM cc_features f
     WHERE ((coalesce(d.necessary,0) >> f.bit)&1)=1 AND ((c.necessary >> f.bit)&1)=0)
    AS OVERDECLARED_unsupported_by_enum
FROM compiled c LEFT JOIN declared d ON d.domain = c.domain
ORDER BY c.domain;
