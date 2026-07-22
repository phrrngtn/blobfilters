-- blobfilters Stage-0 proof: induced domains + argmax-matching over Socrata columns.
-- The "filter" here is just an exact interned value-set (Roaring is the later storage form);
-- this validates the SEMANTICS: intern -> set -> asymmetric containment -> arg_max.

-- 1. Long (dataset, column, value), canonicalized (trim, drop blanks). all_varchar so UNPIVOT is uniform.
CREATE TABLE CELL_VALUES AS
WITH RAW AS (
  SELECT 'crimes' AS ds, col, trim(val) AS val
    FROM (UNPIVOT (FROM read_csv('crimes.csv',   all_varchar=true)) ON COLUMNS(*) INTO NAME col VALUE val)
  UNION ALL
  SELECT 'licenses' AS ds, col, trim(val) AS val
    FROM (UNPIVOT (FROM read_csv('licenses.csv', all_varchar=true)) ON COLUMNS(*) INTO NAME col VALUE val)
  UNION ALL
  SELECT 'permits' AS ds, col, trim(val) AS val
    FROM (UNPIVOT (FROM read_csv('permits.csv',  all_varchar=true)) ON COLUMNS(*) INTO NAME col VALUE val)
)
SELECT ds || '.' || col AS cid, val
  FROM RAW
 WHERE val IS NOT NULL AND val <> '';

-- 2. The interned value-SET per column (the "blobfilter" content).
CREATE TABLE COL_SET AS SELECT DISTINCT cid, val FROM CELL_VALUES;
CREATE TABLE COL_SIZE AS SELECT cid, count(*) AS n FROM COL_SET GROUP BY cid;

-- 3. Pairwise asymmetric containment  C(A,B) = |A n B| / |A|.
CREATE TABLE CONTAINMENT AS
WITH ISECT AS (
  SELECT a.cid AS ca, b.cid AS cb, count(*) AS inter
    FROM COL_SET AS a JOIN COL_SET AS b USING (val)
   WHERE a.cid <> b.cid
   GROUP BY a.cid, b.cid
)
SELECT i.ca, i.cb, i.inter, sa.n AS na,
       round(i.inter::double / sa.n, 3) AS containment
  FROM ISECT AS i
  JOIN COL_SIZE AS sa ON sa.cid = i.ca;

.print '=== column sizes (|distinct values|) ==='
SELECT cid, n FROM COL_SIZE ORDER BY cid;

.print ''
.print '=== argmax-matching: each column -> the column it is most CONTAINED in ==='
.print '(high score + cross-dataset match = same induced domain)'
SELECT ca                              AS source_column,
       arg_max(cb, containment)        AS best_match,
       round(max(containment), 3)      AS score
  FROM CONTAINMENT
 GROUP BY ca
 ORDER BY ca;

.print ''
.print '=== REFINED: argmax on MUTUAL containment  MC(A,B) = min(C(A,B), C(B,A)) ==='
.print '(same-domain = symmetric ~1.0; nested-subset = one-way, so MC drops -> collision resolved)'
WITH MUTUAL AS (
  SELECT c1.ca, c1.cb, least(c1.containment, c2.containment) AS mc
    FROM CONTAINMENT AS c1
    JOIN CONTAINMENT AS c2 ON c1.ca = c2.cb AND c1.cb = c2.ca
)
SELECT ca                        AS source_column,
       arg_max(cb, mc)           AS best_match,
       round(max(mc), 3)         AS mutual_score
  FROM MUTUAL
 GROUP BY ca
 ORDER BY ca;

.print ''
.print '=== the small-integer COLLISION (why fingerprint is a sieve, not a classifier) ==='
.print 'ward(1-50) is ~fully contained in community_area(1-77) though they are DIFFERENT domains:'
SELECT ca AS source_column, cb AS candidate, na AS source_card, inter, containment
  FROM CONTAINMENT
 WHERE ca IN ('crimes.ward','licenses.community_area')
   AND cb IN ('crimes.community_area','licenses.ward','permits.community_area','permits.ward')
 ORDER BY ca, containment DESC;
