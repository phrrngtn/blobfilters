-- Type a few Chinook relvars into {D}->{C} arrows.
-- rule4 role  = catalog (PK => determinant {D}; FK => which target key-domain a col references).
-- blob role   = values (validate each declared FK by CONTAINMENT; type the rest measure/text/temporal).
INSTALL sqlite; LOAD sqlite;
ATTACH 'chinook.sqlite' AS ch (TYPE sqlite);

CREATE TABLE cols AS SELECT * FROM read_csv('catalog_columns.csv', header=true);
CREATE TABLE fks  AS SELECT * FROM read_csv('catalog_fks.csv', header=true);

-- Values, all-varchar, long form (only the tables we type + their FK targets).
CREATE TABLE cell AS
  SELECT 'Album'       AS tbl, col, val FROM (UNPIVOT (SELECT COLUMNS(*)::VARCHAR FROM ch.Album)       ON COLUMNS(*) INTO NAME col VALUE val)
  UNION ALL SELECT 'Artist',      col, val FROM (UNPIVOT (SELECT COLUMNS(*)::VARCHAR FROM ch.Artist)      ON COLUMNS(*) INTO NAME col VALUE val)
  UNION ALL SELECT 'Track',       col, val FROM (UNPIVOT (SELECT COLUMNS(*)::VARCHAR FROM ch.Track)       ON COLUMNS(*) INTO NAME col VALUE val)
  UNION ALL SELECT 'Genre',       col, val FROM (UNPIVOT (SELECT COLUMNS(*)::VARCHAR FROM ch.Genre)       ON COLUMNS(*) INTO NAME col VALUE val)
  UNION ALL SELECT 'MediaType',   col, val FROM (UNPIVOT (SELECT COLUMNS(*)::VARCHAR FROM ch.MediaType)   ON COLUMNS(*) INTO NAME col VALUE val)
  UNION ALL SELECT 'Invoice',     col, val FROM (UNPIVOT (SELECT COLUMNS(*)::VARCHAR FROM ch.Invoice)     ON COLUMNS(*) INTO NAME col VALUE val)
  UNION ALL SELECT 'InvoiceLine', col, val FROM (UNPIVOT (SELECT COLUMNS(*)::VARCHAR FROM ch.InvoiceLine) ON COLUMNS(*) INTO NAME col VALUE val)
  UNION ALL SELECT 'Customer',    col, val FROM (UNPIVOT (SELECT COLUMNS(*)::VARCHAR FROM ch.Customer)    ON COLUMNS(*) INTO NAME col VALUE val)
  UNION ALL SELECT 'Employee',    col, val FROM (UNPIVOT (SELECT COLUMNS(*)::VARCHAR FROM ch.Employee)    ON COLUMNS(*) INTO NAME col VALUE val);

CREATE TABLE cset AS SELECT DISTINCT tbl, col, val FROM cell WHERE val IS NOT NULL;

-- Single-column PKs define the key-domains (labelled by their table).
CREATE TABLE pk AS
  SELECT tbl,
         max(col) FILTER (WHERE pk_ord = 1)  AS pk_col,
         count(*) FILTER (WHERE pk_ord > 0)  AS pk_arity
  FROM cols GROUP BY tbl;

CREATE TABLE keydom AS
  SELECT p.tbl AS domain_tbl, s.val
  FROM pk AS p
  JOIN cset AS s ON s.tbl = p.tbl AND s.col = p.pk_col
  WHERE p.pk_arity = 1;

-- Validate each declared FK: containment C(fk distinct values, target key-domain).
CREATE TABLE fk_valid AS
WITH FKV AS (
  SELECT f.tbl, f.from_col, f.target_tbl, s.val
  FROM fks AS f JOIN cset AS s ON s.tbl = f.tbl AND s.col = f.from_col
),
SZ AS (SELECT tbl, from_col, count(DISTINCT val) AS n FROM FKV GROUP BY tbl, from_col),
HIT AS (
  SELECT v.tbl, v.from_col, v.target_tbl, count(*) AS inter
  FROM FKV AS v JOIN keydom AS k ON k.domain_tbl = v.target_tbl AND k.val = v.val
  GROUP BY v.tbl, v.from_col, v.target_tbl
)
SELECT h.tbl, h.from_col, h.target_tbl, sz.n AS fk_card, h.inter,
       round(h.inter::double / sz.n, 3) AS containment
FROM HIT AS h JOIN SZ AS sz ON sz.tbl = h.tbl AND sz.from_col = h.from_col;

-- Type every column: determinant, FK domain-reference, measure, text, temporal.
CREATE TABLE typed AS
SELECT c.tbl, c.col, c.ord, c.pk_ord,
  CASE
    WHEN c.pk_ord > 0                 THEN 'DET domain:' || c.tbl
    WHEN f.target_tbl IS NOT NULL     THEN 'domain:' || f.target_tbl
    WHEN regexp_matches(upper(c.decl_type), 'INT|NUMERIC|REAL|DEC|DOUBLE|FLOAT') THEN 'measure'
    WHEN regexp_matches(upper(c.decl_type), 'DATE|TIME')                         THEN 'temporal'
    ELSE 'text'
  END AS type_tag
FROM cols AS c
LEFT JOIN fks AS f ON f.tbl = c.tbl AND f.from_col = c.col;

.print '=== FK validation: declared FK  ⊆  target key-domain (blob confirms the catalog) ==='
SELECT tbl, from_col, target_tbl, fk_card, inter, containment
FROM fk_valid ORDER BY containment, tbl;

.print ''
.print '=== {D} -> {C} arrows (determinant PK -> typed co-domain) ==='
SELECT t.tbl AS relvar,
       '{ ' || string_agg(CASE WHEN t.pk_ord > 0 THEN t.col END, ', ') || ' }' AS D,
       '{ ' || string_agg(CASE WHEN t.pk_ord = 0 THEN t.col || ':' || t.type_tag END, ', ' ORDER BY t.ord) || ' }' AS C
FROM typed AS t
WHERE t.tbl IN ('Artist','Album','Genre','MediaType','Track','Invoice','InvoiceLine')
GROUP BY t.tbl
ORDER BY t.tbl;

.print ''
.print '=== arrow COMPOSITION: InvoiceLine feeds Track feeds Album feeds Artist ==='
.print '(each hop: a co-domain domain:X matches the determinant domain of relvar X)'
SELECT f.tbl AS consumer, f.from_col AS via_col, f.target_tbl AS producer,
       v.containment AS hop_confidence
FROM fks AS f JOIN fk_valid AS v ON v.tbl = f.tbl AND v.from_col = f.from_col
WHERE (f.tbl='InvoiceLine' AND f.target_tbl='Track')
   OR (f.tbl='Track'       AND f.target_tbl='Album')
   OR (f.tbl='Album'       AND f.target_tbl='Artist')
ORDER BY consumer;
