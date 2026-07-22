-- Interactive feature-vector inspection, SQL-only (regexps from the C vocabulary).
LOAD 'blobfilters';   -- adjust to your built extension path
CREATE OR REPLACE TABLE cc_features AS SELECT * FROM (
  SELECT unnest(from_json(json_extract(bf_cc_features_json(),'$.features'),
    '[{"bit":"INT","name":"VARCHAR","regexp":"VARCHAR","expr":"VARCHAR"}]'), recursive := true));

CREATE OR REPLACE TABLE sym AS
  SELECT unnest(['42','CA','$1,234.56','2024-01-15','a@b.com','INV-0001']) AS v;

-- feature vector per symbol (regexp-driven == bf_cc_signature); tune WHERE to drop noise
SELECT s.v AS symbol, string_agg(f.name, ', ' ORDER BY f.bit) AS features
FROM sym s JOIN cc_features f ON f.regexp IS NOT NULL AND regexp_matches(s.v, f.regexp)
GROUP BY s.v ORDER BY s.v;
