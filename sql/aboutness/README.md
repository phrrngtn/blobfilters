# sql/aboutness — evidence generation: induced domains, authority labeling

Working DuckDB recipes that validate the domain-detection loop end-to-end over
**public Socrata open-data**, and that motivate the design refinements recorded
in [`DESIGN.md` → *Empirical Refinements*](../../DESIGN.md#empirical-refinements-socrata-validation-2026-07-22).

These recipes **generate evidence**, they do not decide. Each probe contributes
`⟨probe, domain, canon_level, grain, metric, value⟩` rows to be combined / ranked /
filtered downstream; `arg_max` appears here only as *one terminal reducer for
display*. The sieve is coarse→fine on purpose, and the stage at which a symbol drops
out is itself signal.

```bash
./fetch_socrata.sh                 # public data (City of Chicago + USPS states)
duckdb < argmax_match.sql          # containment + mutual-containment argmax
duckdb < cluster_label.sql         # connected-components clustering + authority labeling
```

## What these recipes are

The "filter" here is an **exact interned value-set** computed in plain SQL — the
*ground-truth* semantics that the shipped `bf_*` functions approximate. Mapping:

| recipe SQL (exact) | blobfilters function (hashed) |
|---|---|
| `SELECT DISTINCT cid, val` (the set) | `bf_build(value)` aggregate → roaring of FNV-1a hashes |
| `|A∩B| / |A|` via join + count | `bf_containment(probe, ref)` |
| `least(C(A,B), C(B,A))` | two `bf_containment` calls, `least(...)` |

Kept in exact SQL on purpose: it is decodable and false-positive-free, so it is
the reference the hashed layer is checked against (see refinement #1).

## What they establish (all measured, not asserted)

- **Induced domains need no external catalog.** Connected components over mutual
  containment recover the `community_area` and `ward` domains across three
  independent datasets from the data alone.
- **Canonicalization is load-bearing.** `crimes.district` is zero-padded
  (`"008"`) and `licenses.police_district` is not (`"8"`); they are the *same*
  domain but only merge after numeric canonicalization — Unicode casefold does
  not fix it. (refinement #2)
- **One-way containment under-determines.** `ward{1..50} ⊆ community_area{1..77}`
  ties at containment 1.0 across several wrong candidates; `arg_max` breaks the
  tie arbitrarily. **Mutual** containment resolves it, and the twinless column
  (`police_district`) self-rejects at a low score. (refinement #3)
- **Authority labeling** via asymmetric containment: `licenses.state ⊆ US_STATES`
  at 0.946 → labeled; geographic-integer domains score 0.0 → correctly unlabeled.
- **The residue is signal.** The two uncovered `state` values are `ON`/`QC`
  (Ontario/Quebec) — domain-boundary discovery, not noise. (refinement #4)

`argmax_match.sql` = the containment/mutual-containment argmax. `cluster_label.sql`
= connected-components clustering + authority-file labeling + the canonicalization
comparison. Fetched CSVs are git-ignored; re-run `fetch_socrata.sh` to reproduce.
