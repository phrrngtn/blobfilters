-- Composite registry (user-land) over the char-class vocabulary.
--
-- The split that dissolves "user freedom vs. clean centralized surrogates":
--   * users own the DEFINITION -- (name, expr) over vocabulary features AND/OR other
--     composites (a DAG); arbitrary boolean shapes are fine.
--   * the system DERIVES identity + integrity and NEVER lets a user write a surrogate.
--
-- Canonical stored form = the DAG fully inlined to a primitive-only expr (representation-
-- neutral); the surrogate = content-hash of that canonical form (=> semantically-identical
-- composites dedup automatically, and garbage surrogates are impossible because the id is a
-- pure function of the meaning). bf_cc_eval is the reference evaluator that any downstream
-- mask / flattened-SSA / Roaring compiler must agree with.
--
-- Adjust the LOAD path to your built extension.
LOAD 'blobfilters';
SET lambda_syntax='ENABLE_SINGLE_ARROW';

-- FK-target vocabulary, from the one C source.
CREATE TABLE cc_features AS SELECT * FROM (SELECT unnest(from_json(
  json_extract(bf_cc_features_json(),'$.features'),
  '[{"bit":"INT","name":"VARCHAR","regexp":"VARCHAR","expr":"VARCHAR"}]'), recursive:=true));

-- USER-LAND: the composite DAG. Refs to other composites are fine; the last three rows are
-- deliberately bad to exercise integrity (a structural duplicate, a typo, and a cycle).
CREATE TABLE composite(name, expr) AS SELECT * FROM (VALUES
  ('money',        'has_dollar & has_digit'),
  ('clean_zip',    'all_digits & len_5'),
  ('zip_no_dirt',  'len_5 & !(has_upper)'),               -- a straggler-refined class
  ('money_or_zip', 'money | clean_zip'),                  -- DAG: refs two composites
  ('flagged',      'money_or_zip & !has_space'),          -- DAG depth 2
  ('postal5',      'all_digits & len_5'),                 -- structural DUPLICATE of clean_zip
  ('typo',         'all_digits & len_55'),                -- BROKEN: len_55 not in vocabulary
  ('a',            'b & has_digit'),                       -- CYCLE with b
  ('b',            'a | has_upper')
) AS t(name, expr);

-- Inline the DAG to a primitive-only expr, to fixpoint. One list_reduce pass replaces every
-- composite name once (names packed "name<US>expr"); iterate until no composite name survives.
-- The step cap is the cycle guard: a cycle never converges and stays flagged below.
CREATE TABLE expanded AS
WITH RECURSIVE
  PACK(cs) AS (SELECT list(name || chr(30) || expr) FROM composite),
  EXP AS (
     SELECT name, expr AS cur, 0 AS step FROM composite
     UNION ALL
     SELECT e.name,
            list_reduce((SELECT cs FROM PACK),
              (acc,c) -> regexp_replace(acc,'\b'||split_part(c,chr(30),1)||'\b',
                                        '('||split_part(c,chr(30),2)||')','g'),
              e.cur),
            e.step+1
     FROM EXP e
     WHERE e.step < 8
       AND EXISTS (SELECT 1 FROM composite c WHERE regexp_matches(e.cur,'\b'||c.name||'\b'))
  )
SELECT name, cur AS expanded_expr, step AS depth,
       NOT EXISTS (SELECT 1 FROM composite c WHERE regexp_matches(cur,'\b'||c.name||'\b')) AS converged
FROM EXP QUALIFY row_number() OVER (PARTITION BY name ORDER BY step DESC) = 1;

-- Every identifier in the inlined expr, to classify: vocabulary / unresolved-composite (cycle
-- residue) / undefined (typo or missing definition).
CREATE TABLE reg_ids AS
  SELECT e.name, unnest(regexp_extract_all(e.expanded_expr,'[A-Za-z_][A-Za-z0-9_]*')) AS id
  FROM expanded e;

-- THE REGISTRY: identity + integrity, all system-derived.
--   surrogate           = content-hash of the canonical inlined form (dedup + garbage-proof)
--   participation_mask  = OR of the primitive feature bits the composite reads (a concrete
--                         "expanded mask": the footprint, usable for cheap prefiltering)
--   undefined/unresolved refs = the integrity findings
CREATE TABLE registry AS
SELECT e.name, e.expanded_expr, e.depth, e.converged,
       list_distinct(list(i.id) FILTER (WHERE cf.name IS NULL AND cp.name IS NOT NULL)) AS unresolved_refs,
       list_distinct(list(i.id) FILTER (WHERE cf.name IS NULL AND cp.name IS NULL))     AS undefined_refs,
       bit_or((1::UBIGINT) << cf.bit)                                                   AS participation_mask,
       bf_sha256(regexp_replace(e.expanded_expr,'\s+','','g')::BLOB)                    AS surrogate
FROM expanded e
JOIN reg_ids i          ON i.name = e.name
LEFT JOIN cc_features cf ON cf.name = i.id
LEFT JOIN composite  cp  ON cp.name = i.id
GROUP BY e.name, e.expanded_expr, e.depth, e.converged;

-- A single validity predicate reused below.
CREATE MACRO is_valid(conv, undef) AS (conv AND coalesce(len(undef),0)=0);

.mode box
.print '=== the registry: identity (surrogate) + integrity, all system-derived ==='
SELECT name,
       CASE WHEN length(expanded_expr)>60 THEN substr(expanded_expr,1,57)||'...' ELSE expanded_expr END AS expanded_expr,
       is_valid(converged, undefined_refs) AS valid,
       CASE WHEN NOT converged THEN 'CYCLE'
            WHEN coalesce(len(undefined_refs),0)>0 THEN 'undefined:'||undefined_refs::VARCHAR END AS problem,
       substr(surrogate,1,12) AS surrogate12
FROM registry ORDER BY name;

.print ''
.print '=== content-addressed DEDUP: independently-named, same canonical form => one identity ==='
SELECT list(name) AS same_surrogate, any_value(expanded_expr) AS expr, substr(surrogate,1,12) AS surrogate12
FROM registry WHERE is_valid(converged, undefined_refs)
GROUP BY surrogate HAVING count(*) > 1;

.print ''
.print '=== live evaluation of VALID composites (self-contained symbol set incl. real dirty zips) ==='
CREATE TABLE SYMS(sym) AS SELECT * FROM (VALUES
  ('60661'),('60614'),('90210'),('10001'),   -- clean US zips
  ('INDIA'),('H3P3C'),('M1B5N'),('JMCJS'),   -- the real dirt from the licenses zip column
  ('$9.50'),('CHICAGO')) AS t(sym);
SELECT r.name, r.expanded_expr,
       string_agg(s.sym, ', ') FILTER (WHERE bf_cc_eval(bf_cc_signature(s.sym), r.expanded_expr)) AS matched
FROM registry r CROSS JOIN SYMS s
WHERE is_valid(r.converged, r.undefined_refs)
GROUP BY r.name, r.expanded_expr ORDER BY r.name;
