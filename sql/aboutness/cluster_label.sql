-- blobfilters Stage-0b: induced-domain CLUSTERING (connected components on mutual
-- containment) + authority-file LABELING. Also demonstrates the canonicalization crux.

-- Long (cid, raw_val). all_varchar so UNPIVOT is uniform.
CREATE TABLE CELL_VALUES AS
WITH RAW AS (
  SELECT 'crimes'   AS ds, col, trim(val) AS val FROM (UNPIVOT (FROM read_csv('crimes.csv',   all_varchar=true)) ON COLUMNS(*) INTO NAME col VALUE val)
  UNION ALL
  SELECT 'licenses' AS ds, col, trim(val) AS val FROM (UNPIVOT (FROM read_csv('licenses.csv', all_varchar=true)) ON COLUMNS(*) INTO NAME col VALUE val)
  UNION ALL
  SELECT 'permits'  AS ds, col, trim(val) AS val FROM (UNPIVOT (FROM read_csv('permits.csv',  all_varchar=true)) ON COLUMNS(*) INTO NAME col VALUE val)
)
SELECT ds || '.' || col AS cid, val AS raw_val,
       -- CANON: all-digit values lose leading zeros ("008"->"8"); else upper. This is the
       -- shared-canonical-value-space (interning) boundary in miniature.
       CASE WHEN regexp_full_match(val, '[0-9]+') THEN CAST(CAST(val AS BIGINT) AS VARCHAR)
            ELSE upper(val) END AS canon_val
  FROM RAW
 WHERE val IS NOT NULL AND val <> '';

-- Reusable: connected components over mutual containment for a chosen value column.
-- (Built twice, once per canonicalization, so we can compare.)

-- ---------- pass over a value-set: returns component assignment ----------
CREATE MACRO build_sets(valcol) AS TABLE
  SELECT DISTINCT cid, valcol AS val FROM CELL_VALUES;

-- RAW sets
CREATE TABLE SET_RAW   AS SELECT DISTINCT cid, raw_val   AS val FROM CELL_VALUES;
CREATE TABLE SET_CANON AS SELECT DISTINCT cid, canon_val AS val FROM CELL_VALUES;

-- mutual containment for a given set table
CREATE TABLE MC_RAW AS
WITH SZ AS (SELECT cid, count(*) n FROM SET_RAW GROUP BY cid),
     I AS (SELECT a.cid ca, b.cid cb, count(*) inter FROM SET_RAW a JOIN SET_RAW b USING(val) WHERE a.cid<>b.cid GROUP BY 1,2),
     C AS (SELECT i.ca, i.cb, i.inter::double/sa.n AS cont FROM I i JOIN SZ sa ON sa.cid=i.ca)
SELECT c1.ca, c1.cb, least(c1.cont, c2.cont) AS mc
  FROM C c1 JOIN C c2 ON c1.ca=c2.cb AND c1.cb=c2.ca;

CREATE TABLE MC_CANON AS
WITH SZ AS (SELECT cid, count(*) n FROM SET_CANON GROUP BY cid),
     I AS (SELECT a.cid ca, b.cid cb, count(*) inter FROM SET_CANON a JOIN SET_CANON b USING(val) WHERE a.cid<>b.cid GROUP BY 1,2),
     C AS (SELECT i.ca, i.cb, i.inter::double/sa.n AS cont FROM I i JOIN SZ sa ON sa.cid=i.ca)
SELECT c1.ca, c1.cb, least(c1.cont, c2.cont) AS mc
  FROM C c1 JOIN C c2 ON c1.ca=c2.cb AND c1.cb=c2.ca;

-- connected components (min-reachable-cid label) at MC >= 0.70
CREATE TABLE COMP_RAW AS
WITH RECURSIVE NODES AS (SELECT DISTINCT cid FROM SET_RAW),
E AS (SELECT ca, cb FROM MC_RAW WHERE mc >= 0.70),
R(node, reach) AS (
  SELECT cid, cid FROM NODES
  UNION
  SELECT r.node, e.cb FROM R r JOIN E e ON r.reach = e.ca
)
SELECT node AS cid, min(reach) AS component FROM R GROUP BY node;

CREATE TABLE COMP_CANON AS
WITH RECURSIVE NODES AS (SELECT DISTINCT cid FROM SET_CANON),
E AS (SELECT ca, cb FROM MC_CANON WHERE mc >= 0.70),
R(node, reach) AS (
  SELECT cid, cid FROM NODES
  UNION
  SELECT r.node, e.cb FROM R r JOIN E e ON r.reach = e.ca
)
SELECT node AS cid, min(reach) AS component FROM R GROUP BY node;

.print '=== induced domains, RAW canonicalization (trim only) ==='
.print '(district and police_district DO NOT merge: "008" vs "8" never intersect)'
SELECT component AS induced_domain, count(*) AS n_cols, string_agg(cid, ', ' ORDER BY cid) AS columns
  FROM COMP_RAW GROUP BY component HAVING count(*) > 1 ORDER BY n_cols DESC, component;

.print ''
.print '=== induced domains, CANON (strip leading zeros) ==='
.print '(police-district domain now merges across datasets once encodings are canonicalized)'
SELECT component AS induced_domain, count(*) AS n_cols, string_agg(cid, ', ' ORDER BY cid) AS columns
  FROM COMP_CANON GROUP BY component HAVING count(*) > 1 ORDER BY n_cols DESC, component;

-- ---------- authority-file labeling (asymmetric containment: cluster ⊆ master) ----------
CREATE TABLE AUTHORITY AS
  SELECT 'US_STATES' AS auth, upper(trim(Abbreviation)) AS val FROM read_csv('us_states.csv');

CREATE TABLE CLUSTER_VAL AS
  SELECT DISTINCT c.component, s.val FROM COMP_CANON c JOIN SET_CANON s ON s.cid = c.cid;

.print ''
.print '=== authority labeling: each induced domain -> best authority file (containment >= 0.9) ==='
WITH CSZ AS (SELECT component, count(*) n FROM CLUSTER_VAL GROUP BY component),
HIT AS (SELECT cv.component, a.auth, count(*) inter FROM CLUSTER_VAL cv JOIN AUTHORITY a USING(val) GROUP BY 1,2)
SELECT csz.component AS induced_domain, csz.n AS cluster_card,
       coalesce(arg_max(h.auth, h.inter::double/csz.n), '(no authority)') AS best_authority,
       round(coalesce(max(h.inter::double/csz.n), 0), 3) AS containment,
       CASE WHEN max(h.inter::double/csz.n) >= 0.9 THEN 'LABELED' ELSE 'unlabeled (needs its own authority file)' END AS verdict
  FROM CSZ csz LEFT JOIN HIT h ON h.component = csz.component
 GROUP BY csz.component, csz.n
 ORDER BY containment DESC, cluster_card DESC;
