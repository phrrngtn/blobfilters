# Domain-Typed Morphisms — the `{D}→{C}` arrow over spreadsheets, tables, and relvars

**The bet:** every artifact that *computes* or *tabulates* a function — a spreadsheet
formula DAG, a document table, a database relvar — reduces to the **same compact,
manifestation-invariant type: a domain→co-domain arrow `{D}→{C}`**, where the domains
are blobfilter sketches. Once everything is typed into that one arrow-space, matching,
classification, provenance, and verification are all bitmap operations over sketches —
no embeddings, no model, microsecond ops, and the one-sided disjointness guarantee at
every level.

This is the bridge between [blobboxes](../../blobboxes) (structure), blobfilters
(domains — see [`DESIGN.md`](../DESIGN.md) for the compact-value/evidence primitives),
rule4 (catalog), and the lift/oracle vision. Companion to blobboxes'
*Application Architecture — Documents as a Table of Boxes*.

---

## 1. The ladder

| level | unit | how its domain(s) are obtained | sketch |
|---|---|---|---|
| L0 | value | hash → first-order fingerprint | bitmap over **values** |
| L1 | box | probe value vs catalog **and** propagate through the blocky DAG | domain-bitfield |
| L2 | column | **consensus** of box-domains (row-major + redundancy) | domain-bitfield |
| L3 | table | **composite** of column-domains | **bitmap over domain-ids** (§2, §3) |
| L4 | cross-source | probe table-fingerprints against each other | — |
| L5 | whole DAG / relvar | estimate the artifact as a morphism | **`{D}→{C}` arrow** (§4, §5) |

Each level composes into the next by Roaring `OR`; the compact-value primitive and its
**sound-pruning guarantee — *disjoint ⟹ definitely unrelated*** — hold unchanged at
every rung (see DESIGN.md → *Empirical Refinements*).

## 2. The second-order sketch: a bitmap over *domain-ids*

The first-order fingerprint is a bitmap over hashed **values** ("which known domain is
this column?"). The second-order fingerprint is a bitmap over **domain-ids** ("which
domains does this *table* reference?"). A table stops being a bag of values and becomes
a small dense bitfield — `{customer_id, state, product_code, amount}`. The identical
Roaring machinery applies one level up, for free: `bf_intersection_card` = shared-domain
count, containment/Jaccard = structural similarity, and **disjoint domain-bitfields ⟹
the tables are definitely unrelated**.

## 3. Composite table representation: set-bitfield (coarse) + ordered-tuple (fine)

A table's composite domain is carried at two fidelities — the coarse→fine cascade, one
level up:

- **Coarse — the set-bitfield.** An unordered `OR` of the column domain-ids. Order- and
  transpose-invariant, so it survives column reordering and row/column transposition.
  This is the **primary key** and the fast prune: *disjoint set-bitfields ⟹ different
  table, drop it* (sound — never discards a real match).
- **Fine — the ordered tuple.** The sequence `⟨domain(col₁), domain(col₂), …⟩`. This is
  the schema signature; it is the **confirm** step for survivors of the coarse prune,
  matched column-by-column (the alignment / SIFT-Hough step from
  *latent-tables-and-stemmatics*). Multiplicity lives here too (repeated column domains).

Coarse blocks candidates at bitmap speed; fine aligns only the survivors.

## 4. Spreadsheets: estimate the arrow by abstract interpretation

The blocky DAG (coarsen the cell-DAG to a region-DAG) **is** a morphism `{D}→{C}`, and
recovering it is **abstract interpretation**:

- **Abstract domain (lattice)** = the domain-bitfield, ordered by `⊆`, join = Roaring
  `OR`, bottom = `∅`, finite (bounded by the number of known domains). Roaring gives the
  lattice operations for free. (The pun is load-bearing: the *domain of discourse* is the
  analysis's *abstract domain*.)
- **Transfer functions** = per-function domain→codomain rules: `SUM(range) → {measure}`,
  `VLOOKUP(k, tbl, i) → domain(tbl.colᵢ)`, `A&B → join(domain(A), domain(B))`,
  `IF(c,a,b) → join(domain(a), domain(b))`.
- **"Fixpoint"** = a topological sweep in dependency order (spreadsheets are usually
  acyclic; a true fixpoint is needed only for iterative/circular calc). Two loops:
  *inner* = the dataflow sweep over a fixed catalog; *outer* = catalog growth — promote
  high-consensus columns (L2) to **new induced domains**, which adds lattice elements,
  then re-sweep.
- **Soundness** = over-approximation: a box's estimate ⊇ its true domains, so *disjoint
  estimates ⟹ truly disjoint* — the same one-sidedness as the hash-collision guarantee,
  from a second independent direction; they compound safely.

The workbook's type is then `(join of input-region domains) → (join of output-region
co-domains)`. Structure × content = the structural skeleton × the domain-bitfield.

## 5. Databases: the opposite-of-lift — relvars as extensional functions

A **relvar is the *extensional* representation of a function**: the graph of
`key-attributes ↦ dependent-attributes`, tabulated as a set of tuples (Codd: a relation
is the extension of a predicate). Where a spreadsheet DAG is *intensional* (rules) and
needs abstract interpretation (§4) to *recover* its arrow, a relvar already **declares**
its structure. So typing a database table is the **opposite of lift** — no imputation:

- **blob (values)** → each column's domain-bitfield, by probing values against the
  catalog (blobfilters). For a live database this can come straight from the **stats
  histogram** (`sys.dm_db_stats_histogram`, `bf_build_histogram`) — cheap metadata, no
  table scan — the histogram-triage path already in DESIGN.md.
- **rule4 (catalog)** → PK / FK / functional dependencies name the **determinants** (the
  `{D}`) and the **dependents** (the `{C}`). The arrow's *partition* is given, not
  inferred.

Combine them and every relvar yields its morphism `{determinant-domains} → {dependent-
co-domains}` directly, and **FKs compose relvars** — a foreign key from `A` into `B`'s
key means `A` *consumes* `B`'s key-domain, chaining the arrows.

**The unification:** intensional (spreadsheet / code) and extensional (relvar / table)
representations of a function reduce to the *same* `{D}→{C}` type. *Lift* turns an
intensional artifact into code; a relvar is already the extensional graph; both land in
one typed space. And the oracle's domain/codomain **envelope truth-table** is itself an
extensional relvar — so *an oracle envelope and a database table are the same kind of
object*, which is why the same arrow can be checked, matched, or composed across all of
them.

## 6. What the shared arrow-space buys

- **Cross-source matching & provenance at the arrow level.** DB tables, Excel ranges,
  PDF tables, and web tables are all typed the same, so you can find *the same function*
  across manifestations. Lineage upgrades from "similar structure" to **typed dataflow**:
  `A:{D}→{C}` and `B:{C}→{E}` *chain* when `A`'s output co-domains match `B`'s input
  domains — inclusion-dependency / FK-discovery **at the arrow level** (which artifact
  feeds which — *latent-tables-and-stemmatics*).
- **Functional classification.** Cluster and label whole models by their arrow —
  `{product, region, date, qty} → {revenue}` = the sales models;
  `{principal, rate, term} → {schedule}` = the amortizations.
- **Verification.** Two artifacts claiming the same arrow can be differentially checked
  on the shared domain/codomain envelope — the oracle from the lift work, now keyed by
  the `{D}→{C}` type.

## 7. Build posture & open items

Relational-first, per the house bias: SQL over the box tables + the rule4 catalog +
blobfilters `bf_*` functions. The second-order sketch is just `bf_build` over domain-ids
— the primitive is reused, not rebuilt. The one piece of real, Excel-specific work is
the **per-function transfer-rule table** (§4): start with the high-frequency functions
(`VLOOKUP`/`INDEX-MATCH`, arithmetic, `IF`, text-join, aggregates), or drive it from the
lifter's envelope truth-tables. Clean-IP: catalog anchoring and every reference domain
must be **public** authority files / sample databases, never a private system-of-record.
