-- Char-class profiling from a DATABASE STATISTICS HISTOGRAM -- no table scan.
--
-- A statistics histogram (e.g. SQL Server sys.dm_db_stats_histogram) is a free,
-- pre-computed per-column sketch: ~200 boundary values (RANGE_HI_KEY = real column
-- values) with frequency weights (EQ_ROWS). Feed those boundary values to the SAME
-- cc_signature/cc_eval -- the source doesn't matter to the profiler. Great for coarse
-- whole-table shape fingerprinting (weak for interior singleton dirt, which isn't a
-- boundary; but extreme-valued dirt IS, since it sorts to the ends).
--
-- Input: histograms.csv = one row per (col, boundary value, weight), e.g. exported as
--   SELECT c.name col, CAST(range_high_key AS NVARCHAR(200)) range_high_key, equal_rows
--   FROM sys.dm_db_stats_histogram(OBJECT_ID('dbo.<table>'), <stats_id>) ...
LOAD 'blobfilters';   -- adjust to your built extension path

CREATE TABLE hist AS SELECT * FROM read_csv('histograms.csv');

-- each boundary value -> char-class signature, weighted by frequency
CREATE TABLE H AS
  SELECT col, range_high_key AS v, equal_rows AS w, bf_cc_signature(range_high_key) AS sig
  FROM hist WHERE range_high_key IS NOT NULL AND range_high_key <> '';

-- General, deliberately OVERLAPPING shapes -- an abstract domain with multi-membership.
-- A value can satisfy several; that is a feature, not a bug.
CREATE TABLE SHAPES(name, expr) AS SELECT * FROM (VALUES
  ('email','email_shaped'), ('date','date_shaped'), ('decimal','decimal_shaped'),
  ('all_digits','all_digits'), ('code','code_shaped'), ('all_upper','all_upper'),
  ('all_alpha','all_alpha'), ('has_letters','has_upper | has_lower'),
  ('multi_token','multi_token'), ('has_paren','has_paren'));

CREATE TABLE COV AS
  SELECT h.col, s.name AS shape,
         sum(h.w) FILTER (WHERE bf_cc_eval(h.sig, s.expr)) AS wmatch,
         sum(h.w) AS wtot
  FROM H h CROSS JOIN SHAPES s GROUP BY h.col, s.name;

-- residue: weighted % of symbols firing NO shape at all
CREATE TABLE NONE AS
  SELECT h.col, CAST(round(100.0 * sum(h.w) FILTER (
      WHERE (SELECT count(*) FROM SHAPES s WHERE bf_cc_eval(h.sig, s.expr)) = 0) / sum(h.w)) AS INT) AS none_pct
  FROM H h GROUP BY h.col;

.mode box
.print '=== per-column rule coverage: % of (frequency-weighted) symbols firing each rule ==='
.print '    (sum > 100% = overlapping shapes; none: = residue that matches nothing)'
SELECT c.col AS col_name,
   string_agg(c.shape || ':' || CAST(round(100.0*c.wmatch/c.wtot) AS INT), ' ' ORDER BY c.wmatch/c.wtot DESC)
     FILTER (WHERE c.wmatch > 0) AS rule_coverage_pct,
   n.none_pct AS none
FROM COV c JOIN NONE n ON n.col = c.col
GROUP BY c.col, n.none_pct ORDER BY col_name;
