-- Straggler-removal workflow: tighten a loose probe by subtracting a holdout.
--
-- Pattern:  profile -> cast a loose probe -> DIFF the fingerprints of the matched
--           keepers vs. stragglers to find a discriminating feature -> author
--           new = old & !(holdout) -> verify with two zero-columns (no collateral damage).
--
-- The data below is a small stand-in for a real column; on a real bag of symbols the
-- profile's high/low bands and the discriminator ranking are what guide authoring.
-- (Demonstrated live on Chicago licenses.zip_code: 615 distinct, 4 real impostors
--  INDIA/H3P3C/M1B5N/JMCJS, all length-5 so only a CONTENT feature separates them.)
LOAD 'blobfilters';   -- adjust to your built extension path

CREATE TABLE cc_features AS SELECT * FROM (SELECT unnest(from_json(
  json_extract(bf_cc_features_json(),'$.features'),
  '[{"bit":"INT","name":"VARCHAR","regexp":"VARCHAR","expr":"VARCHAR"}]'), recursive:=true));

-- A "zip" column that is supposed to be 5-digit US zips but carries foreign postal codes.
CREATE TABLE SYMS AS
  SELECT sym, bf_cc_signature(sym) AS sig, cnt AS freq FROM (VALUES
    ('60661',9000),('60614',6000),('90210',3000),('10001',1987),('33101',400),
    ('INDIA',1),('H3P3C',1),('M1B5N',1),('JMCJS',1)     -- the impostors (all length 5)
  ) AS t(sym, cnt);

.mode box
.print '=== (1) profile: which probes describe the column? (consensus vs. anomalous tail) ==='
SELECT f.name AS probe,
       count(*) FILTER (WHERE ((s.sig >> f.bit) & 1) = 1) AS n_syms,
       round(100.0 * count(*) FILTER (WHERE ((s.sig >> f.bit) & 1) = 1) / count(*), 1) AS pct
FROM SYMS s CROSS JOIN cc_features f
GROUP BY f.name, f.bit HAVING pct > 0 ORDER BY pct DESC, f.bit LIMIT 14;

.print ''
.print '=== (2) loose probe = len_5. The stragglers it wrongly admits: ==='
SELECT sym, freq FROM SYMS
WHERE bf_cc_eval(sig,'len_5') AND NOT bf_cc_eval(sig,'all_digits') ORDER BY sym;

.print ''
.print '=== (3) GUIDANCE: diff the fingerprints. A (100, 0) row is a perfect holdout term. ==='
WITH M AS (SELECT sig, bf_cc_eval(sig,'all_digits') AS keep FROM SYMS WHERE bf_cc_eval(sig,'len_5'))
SELECT f.name AS candidate_holdout_term,
       round(100.0*avg(CASE WHEN NOT keep THEN ((sig>>f.bit)&1) END),1) AS pct_in_stragglers,
       round(100.0*avg(CASE WHEN     keep THEN ((sig>>f.bit)&1) END),1) AS pct_in_keepers
FROM M CROSS JOIN cc_features f
GROUP BY f.name, f.bit
HAVING coalesce(pct_in_stragglers,0) > coalesce(pct_in_keepers,0)
ORDER BY (coalesce(pct_in_stragglers,0) - coalesce(pct_in_keepers,0)) DESC LIMIT 6;

.print ''
.print '=== (4) author new = len_5 & !(has_upper); verify (both wrong-columns must be 0) ==='
WITH R AS (
  SELECT sym, bf_cc_eval(sig,'len_5') AS old_m,
              bf_cc_eval(sig,'len_5 & !(has_upper)') AS new_m,
              bf_cc_eval(sig,'all_digits') AS is_clean
  FROM SYMS)
SELECT count(*) FILTER (WHERE old_m)                            AS old_syms,
       count(*) FILTER (WHERE new_m)                            AS new_syms,
       count(*) FILTER (WHERE old_m AND NOT new_m)              AS dropped,
       count(*) FILTER (WHERE new_m AND NOT is_clean)           AS dirt_wrongly_kept,
       count(*) FILTER (WHERE old_m AND NOT new_m AND is_clean) AS clean_wrongly_dropped
FROM R;
