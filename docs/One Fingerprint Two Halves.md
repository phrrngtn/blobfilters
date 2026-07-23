# One Fingerprint, Two Halves

*Fingerprinting the extension (data domains) and the intension (computation) with
a single primitive, supervised by the catalog.*

This captures an arc: a value-set fingerprint (blobfilters roaring bitmaps) turns
out to recognize, relate, and de-duplicate not just **data domains** but also
**computations** — because both reduce to a feature multiset, and similarity is
set overlap. The catalog ([[Catalog As Data]], [[Data As Control Plane]]) supplies
the labels. Sits under [[Rule4 For Functions]] and [[Blobfilter Domain Probing]].

Built so far (runnable sketches, both smokes pass):
`column_role/value_fingerprint.py` — extension half: recognition, de-facto FK,
union-cover, cross-substrate dedup. `column_role/formula_fingerprint.py` —
intension half: LET/LAMBDA lift → WL clone/idiom fingerprints, plus both feedback
loops (shape-guided cutting from a popularity index; data-typed holes that prune
idiom matches by inferred domain signature). Remaining stubs: live-source value
sampling (extension) and a real Excel parser — pycel/xlcalculator (intension).

## The premise: the fingerprint is substrate-blind

Everything reduces to a **feature multiset**, and similarity is overlap:

- **containment** `|probe ∩ ref| / |probe|` — subset-ness (asymmetric)
- **Jaccard** `|A ∩ B| / |A ∪ B|` — equivalence (symmetric)

blobfilters computes both over roaring bitmaps of hashed features in microseconds,
precomputed once per column/artifact. *What* the features mean — currency codes,
formula tokens, AST subtrees — is invisible to the overlap. That is why it "can't
not work": set overlap doesn't know what it's overlapping.

## Supervised-unsupervised: the catalog is the label set

Recognizing an anonymous slab (headerless result set, names stripped) is nominally
**unsupervised**. But the catalog's declared **PK/FK constraints are free labels**:
a fingerprint of a known PK domain *is* a label ("this value-set is
`dim_currency.code`"). So the unsupervised problem becomes **nearest-fingerprint
classification** against a labeled library — **distant / weak supervision** (the
knowledge-base-as-labels move; Mintz, Snorkel). The labels were never annotated for
this task; they were harvested from constraints a DBA already declared.

The supervision is **one-sided**, which is why **dimensions are recognizable and
measures are not**: dimensions have a catalog label (a domain) to match; measures
(aggregated, no stable domain) can only be the unlabeled residual. The asymmetry
isn't incidental — it's the shape of where the supervision reaches.

## The duality: extension and intension

Data is the **extension** (the enumerated graph of a function); computation is the
**intension** (the rule). A relvar keyed by K is the function K → rest, tabulated;
a formula / cut / LAMBDA is the same function, as a rule. The fingerprint works on
both halves — the whole point of this document.

## Extension side: five operations, one primitive

| operation | question | primitive |
|---|---|---|
| **Recognition** | which domain is this slab column? | nearest containment vs the label library |
| **De-facto FK (IND)** | is this column a subset of a key domain? | `containment(child, parent) → ~1.0` |
| **Untagged union** | is this column *one of* several domains? | greedy **set-cover** over the library |
| **Redundancy / unification** | which columns are the same domain? | corpus **clustering** by Jaccard |
| **Popularity** | which tables hold the needles? | FK in-degree (declared) / cover in-degree (discovered) |

- **De-facto FK** = inclusion-dependency (IND) discovery. A declared FK is a
  declared IND; an undeclared one is discovered from data. Cheap (a bitmap AND) and
  — the payoff — works **across databases**, where you cannot JOIN but can compare
  fingerprints.
- **Untagged union** = a column whose values come from `A ∪ B ∪ C` with the
  discriminator dropped (the spreadsheet antipattern). No single domain covers it;
  a minimal set of domains does. Recover the discriminator by attributing each value
  to the domain that contains it.
- **Unification** clusters DB *and* spreadsheet columns (blobboxes lands XLSX cells
  relationally → same grain) by mutual Jaccard. Each cluster is one domain with N
  redundant copies; canonical = the hub; **Jaccard < 1 = same domain modulo dirt**
  (a data-quality signal for free). Redundancy reduction = "copy → reference".

## Intension side: computation is fingerprintable too

The surprise that isn't: **code fingerprinting is a mature field.** The canonical
instance is **Moss** (winnowing over k-gram token hashes) — literally
containment/Jaccard over token fingerprints. The clone-detection taxonomy maps onto
our normalizers:

| level | features | clone type | normalizer |
|---|---|---|---|
| textual | chars/lines | Type-1 | — |
| token | alpha-renamed tokens | Type-2 | R1C1 |
| structural | AST / PDG subtrees | Type-3 | LET/LAMBDA + De Bruijn |
| semantic | I/O behavior | Type-4 | (undecidable) |

**Lift a cell + its antecedent cone to `LET` + `LAMBDA`.** LET = A-normal form
(every intermediate is a binding → the dependency DAG becomes a linear term);
LAMBDA abstracts the cut's inputs as parameters (**literally reifying the cut as a
function**). Alpha-normalize the bindings (De Bruijn indices) and **positional
noise vanishes** — a formula filled down a column and the same computation in a
different sheet collapse to one term. Fingerprint that (token bag or Weisfeiler–
Lehman subtree hashes) and structural clones collide regardless of location.
Recurring LAMBDA fingerprint = a **factor-out / shared-function** target → promote
the cut into the function catalog ([[Rule4 For Functions]]). "It works for
subgraphs" is **PDG-based clone detection**; it's a real subfield.

Pieces exist: pycel / xlcalculator / formulas build the dependency graph + AST;
blobboxes supplies `text` + `formula` per cell.

## Choosing the frontier: from clones to polymorphic idioms

Fingerprinting an AST forces a choice the value tier never faced: **where is the
frontier?** Which nodes are *roots* (outputs), how much subtree to include, and —
the key knob — **which nodes to abstract into leaves (holes)**. That choice is the
same "cut" as everywhere, now controlling the *generality* of what you recognize:

- Concrete leaves → an **exact clone** (this formula, filled down).
- Leaves abstracted into typed holes → a **polymorphic idiom**: "sort, then take
  top N" is one pattern regardless of the column or its type. Formally this is
  **anti-unification** (least general generalization, Plotkin/Reynolds): the most
  specific template generalizing several concrete ASTs, differing subterms replaced
  by variables — `TAKE(?n, SORT(?rows, ?key))`.

So a *spectrum* of fingerprints per computation, frontier as the dial. WL hashing
gives it cheaply: vary the subtree radius and mask leaf labels to slide from clone
to idiom. Matching at the abstract end finds "same idea, zero shared tokens" —
**idiom mining** (Allamanis' probabilistic tree-substitution grammars; frequent-
subtree mining; e-graphs for equivalent forms).

**The two halves cooperate — and this is where the original type-inference thread
lands.** An idiom is polymorphic (`Ord k => …`); Excel won't type the hole, but the
*values* flowing into that leaf get recognized as a domain by the extension
fingerprint. So you **recognize the structure with the intension fingerprint and
type its holes with the extension fingerprint** — the pattern's principal type is
inferred from the frontier plus the leaf domains. Recognizing an unknown computation
as "instance of idiom X, holes Y,Z" is supervised-unsupervised once more: the idiom
vocabulary is the label set, anti-unification distance the classifier.

## Self-revealing idioms: scoped popularity

The idiom vocabulary needn't be declared — it **self-reveals by frequency**. This is
frequent-subtree / motif mining: an idiom is whatever recurs above a support
threshold, so **frequency is the label** — the *unsupervised* end of the axis. The
catalog *accelerates* recognition; recurrence alone *bootstraps* the vocabulary with
no catalog. Same self-revealing as FK-target hubs (in-degree) and de-facto FKs
(containment), now for computation.

The signal is **scoped** — worksheet ⊂ workbook ⊂ directory ⊂ global is a scope
tree, and an idiom's significance is **TF-IDF over that tree**:
- frequent *within a workbook*, rare *globally* → a **local convention / signature**
  (distinctive to an author or team);
- frequent *everywhere* → a **universal idiom** (boilerplate to factor out).

What it buys:
- **Stylometry / provenance / lineage.** High-TF-IDF stanzas fingerprint *who built*
  a workbook; workbooks sharing *rare* idioms are copy-descendants of a common
  ancestor — lineage without declared history.
- **Refactoring priority.** Globally popular → promote to a shared LAMBDA; locally-
  popular-globally-rare → standardize, or preserve as a domain idiom.
- **Anomaly vs norm.** A near-match to a popular idiom that deviates = the divergent-
  cell bug signal at idiom scope.
- **A self-documenting DSL.** The popular idioms *are* the implicit computational
  vocabulary users invented in Excel — surfacing them documents the corpus's
  ubiquitous language ([[ubiquitous-language]]) with nobody writing it down.

## The pejorative is a policy overlay: measurement vs judgment

**Plagiarism detection is similarity search under an adversarial framing.** The
mechanism — Moss, fingerprint overlap — is value-neutral; "plagiarism" is a
*normative label* on a *neutral fact*. Measurement and judgment are separate layers,
and this whole document has been measurement. One similarity fact, many readings,
across both halves:

| high overlap | framing |
|---|---|
| plagiarism | adversarial / illicit |
| reuse, DRY | refactor opportunity |
| lineage, provenance | neutral descent |
| idiom, convention | shared vocabulary (good) |
| citation, derivation | attributed reuse |
| redundancy | waste to consolidate |
| data leak (extension side) | policy violation |
| foreign key / master data | intended, canonical |

So there are **two supervision layers**: identity ("what is this?" — the domain/idiom
label) and **policy** ("is this copy sanctioned?"). The fingerprint is **evidence**
([[Facts As Evidence]]); the verdict is a rule applied to evidence — mechanism
proposes, context disposes, exactly as IND ≠ FK and clone ≠ intent, now at the
normative level.

**Rarity is evidentiary weight.** Scoped popularity (the TF-IDF above) is what turns
a similarity fact into evidence: copying a *universal* idiom is innocuous (everyone
writes it); copying a *rare* signature stanza is strong evidence of descent. Same
number — weaponized or not depending on framing.

The bound that makes the pejorative framing *dangerous*, not just impolite: the
signal **cannot distinguish plagiarism from sanctioned reuse from independent
convergence.** That needs provenance, intent, and authorization — all outside the
similarity measure. Reading similarity *as guilt* is the canonical harm (flagging
shared boilerplate; two people independently writing the obvious idiom). The neutral
mechanism cannot justify the loaded verdict alone.

## The theory, as a map (so the intuition has names)

fingerprint/winnowing → **Moss**; token/AST/PDG clones → **CCFinder, Deckard,
SourcererCC, Komondoor–Horwitz**; scalable similarity → **LSH / MinHash / SimHash**
(roaring-Jaccard is their exact ground truth); "column ⊆ column" → **inclusion
dependency discovery**; catalog-as-labels → **distant supervision / Snorkel**;
graph fingerprint → **Weisfeiler–Lehman hashing**; LET-lift → **A-normal form**;
position-free names → **De Bruijn indices**; dimension/measure → **FD & key
discovery (TANE/HyFD)**.

## Where the intuition is WRONG or bounded

The honest half. "It can't not work" is true of the **candidate-generation** step
and false of nearly everything downstream. Each of these is a place a deep, correct
instinct quietly over-reaches:

1. **Structural similarity ≠ semantic equivalence (Rice's theorem).** Same
   fingerprint can mean different computations; *different* fingerprints (constant-
   folding, commutativity, associativity) can mean the *same* function. Clone
   fingerprints are candidates, never proofs — confirm by running both on sampled
   inputs. This is the hard ceiling; the Type-4 row is undecidable, full stop.
2. **Inclusion dependency ≠ foreign key.** Containment is *necessary, not
   sufficient*. You also need the parent to be a key and the reference to be
   intentional; coincidental containment is common. "Contained ⇒ it's an FK"
   over-claims — it's a hypothesis to confirm.
3. **Small / dense domains manufacture false positives.** Booleans, status codes,
   sequential ints: everything is "contained by" them. The instinct that shared
   values imply a relationship fails hardest exactly where domains are small — guard
   by cardinality / entropy (the sketch's `min_ref_distinct`). Classic IND pitfall.
4. **Containment is directional; "they share values" is not the same question.**
   FK is `child ⊆ parent` (a subset), not symmetric overlap. Use *containment* for
   subset questions and *Jaccard* for equivalence — mixing them inverts FKs or hides
   real ones (Jaccard penalizes a small child against a large parent).
5. **Sampling breaks the clean ~1.0.** A genuine FK can show containment well below
   1.0 because the parent *sample* missed values. So low containment does **not**
   refute an FK — thresholds must live below 1, and absence of evidence isn't
   evidence of absence.
6. **Hash collisions inflate overlap at scale.** Roaring over hashed values: enough
   distinct values and the birthday bound produces spurious intersection → false
   containment. "Distinct values ⇒ distinct bits" is false; there's a real false-
   positive rate that grows with cardinality (blobfilters' uint32 space).
7. **Over-normalization merges genuinely-distinct things.** Casefold/NFKD (data) and
   De Bruijn / constant-fold (code) collapse more matches *and* manufacture false
   positives. The "right" normalization is task-dependent; there is no free, always-
   correct canonical form.
8. **Popularity conflates "hub" with "junk drawer."** High FK in-degree flags the
   interesting dimension hubs — and *also* the generic `status`/`type` lookup every
   table references, which is often the least interesting domain. Most-referenced ≠
   most-meaningful.
9. **Greedy set-cover isn't optimal, and overlapping domains fake unions.** A single
   messy domain can look like a union of two overlapping reference domains; greedy
   can pick a spurious multi-domain cover. Discriminator recovery is ambiguous
   exactly when the covering domains overlap.
10. **Cross-DB "same domain" hides representation drift.** The same domain encoded
    differently across systems (leading zeros, casing, surrogate vs natural key,
    units) yields *non-matching* fingerprints though it's the same domain — and no
    value-normalization fixes a surrogate-key remapping. "Same domain ⇒ same
    fingerprint" fails silently here.
11. **Measures aren't uniformly unrecognizable.** The one-sidedness is *mostly*
    right, but bounded/coded measures (a 1–5 rating, a known unit) do have domains;
    treating every numeric as residual throws away signal.
12. **The generalization can be too general, and the frontier space is exponential.**
    The trivial anti-unifier is a single hole matching everything; the frontier is a
    bias/variance trade with no canonical answer. You must cut at principled seams
    (type edges, LAMBDA boundaries, high-fanout nodes), not enumerate subgraphs. With
    commutative/associative operators anti-unification is equational and its LGG is
    **non-unique** (incomparable idioms). "Same idiom" still isn't "same intent."
13. **Popular ≠ correct, and scope is a modeling choice.** Frequency self-reveals
    *convention*, not *quality* — a widely-copied buggy idiom is popular (the junk-
    drawer-hub pitfall, one level up). TF-IDF is sensitive to how the scope
    boundaries are drawn; a bad scope tree manufactures spurious "local" idioms.
    Shared rarity suggests copy-descent but cannot rule out independent convergence
    on the obvious solution. And popularity drifts over time — a snapshot misleads
    (track it via the same TTST temporal capture that started this thread).

## Where the intuition is RIGHT (validated)

Substrate-blindness of the primitive; the extension/intension unity; the catalog as
a free label source; near-miss overlap as a "modulo one edit / modulo dirt" anomaly
signal; and the **generate-and-test discipline** — fingerprints propose, cheap
confirmation disposes — as the correct posture everywhere. "It can't not work" holds
for *proposing candidates*. It never held for *confirming* them, and shouldn't.

## The payoff: a self-documenting worksheet

Everything above is recognition — anonymous cells into structured facts. The capstone
*consumes* those facts to write prose: **summarize a worksheet in natural language
from its labels, formulas, data, and workbook/sheet names.** The pipeline supplies
the evidence; a template or LLM narrates it:

- **labels** (header/text cells, via xl_refract's tokenizer) — the human names;
- **data** → recognized **domains + dimension/measure roles** (extension tier);
- **formulas** → recognized **idioms + dependency structure** (intension tier +
  xl_refract's antecedent graph);
- **names** (workbook/sheet) — weak topical context.

These assemble into a structured **worksheet profile** (title; per-column
label + domain + role + idiom + precedents; de-facto FKs; notable idioms; anomalies),
rendered to grounded prose — blobtemplates for deterministic output, or an LLM seeded
by the profile for richer text. The profile is the control plane, the prose a
rendering: [[Facts As Evidence]] → narration.

The **label × data cross-check** is where it earns trust: label "CCY" + values that
fingerprint as ISO 4217 = *confirmed*; label "Date" + currency values = *flagged
mislabel*. It reports agreement and disagreement instead of parroting either signal —
recognition supervising the prose, which is what keeps an LLM honest. Bound: separate
**observed facts** (domains/idioms/deps — grounded) from **inferred purpose**
(speculative), and trust neither labels nor sheet names blindly — they are the noise
the fingerprints exist to see through.

### Redundancy across the corpus

Scale the same clustering to whole worksheets and workbooks, across **four layers**:
**structure** (column layout + dependency-graph shape), **data** (union of a sheet's
column domain fingerprints), **formulas** (bag of idiom fingerprints), and **intent**
(the prose/profile rollup — emergent, least grounded). A worksheet is just a bigger
artifact reduced to a feature-set (`domains ∪ idioms ∪ structure ∪ intent`); cluster
by each layer, scoped worksheet ⊂ workbook ⊂ directory ⊂ global with the TF-IDF
weighting from above.

The discriminator that makes it useful, not noisy: **same structure + same data =
copy-paste rot** (consolidate) vs **same structure + *different* data = a healthy
template** (a region/month instance — parameterized reuse, not waste). Structure
alone can't separate them — which is exactly why all four layers are needed;
"some layer matches" ≠ "redundant worksheet." Findings: dedup duplicated reports and
copied lookups (copy → reference / proxy-lift); discover templates + copy-descent
lineage (rare shared structure+formulas); flag **divergence** — drifted near-
duplicates, where the question is *which copy is canonical*. Intent-redundancy is
highest-value / lowest-confidence → route to human review. Scale via the blocking key
+ LSH, never all-pairs.

**Intension substrate — xl_refract** (renamed from `xl_refract`). One Rust parser core
in **two slim builds** — the same portable-vs-native tier seam that runs through this
whole document:
- **WASM** (`wasm32-unknown-unknown`, 0 imports, JSON ABI) — the safe/portable tier for
  cross-platform **Office.js** Excel add-ins (Win/Mac/web); the Python host wraps this.
- **C-ABI** — linked into a **native DuckDB C extension** (`xl_refract.duckdb_extension`):
  `xl_refract_references` / `xl_refract_functions` / `xlr_as_r1c1` as vectorized scalar
  functions, no Python, no JSON-over-WASM.

**Built now — this half is real, not designed.** Reference extraction (`xlr_references`,
native + Python) *and* the region-aware **antecedent graph**: `queries/region_graph.sql`
composes blobboxes `bb_xlsx` (cells, drag-fills coalesced into region boxes) +
`xlr_references` (swept across each region's extent) into region→region edges, entirely
in DuckDB ("tooling = queries", no Python — the recursive-CTE precedent closure the
design called for). `queries/skeleton.sql` computes a workbook **structural signature**
(FAST cell-typing → a model fingerprint) = *our* worksheet-structure layer, already real;
`rules/fast_standard.yaml` is a FAST-standard linter ruleset. So the transitive
antecedent CTE, the R1C1 region-coalescing (our clone-normalizer, reused), and structural
fingerprints are all **built**. Convergence: `formula_fingerprint` adds the finer
per-cut idiom/clone/anti-unification layer on top; `value_fingerprint` types the holes.
WASM : C-ABI :: pip-quickjs : arrow-udf-js :: this doc's Python fingerprint stand-in :
blobfilters.

**Deployment topology, and why one core is a correctness requirement.** WASM runs the
parser at the **edge** — inside cross-platform **Office.js add-ins** (Excel Win/Mac/
web, the lowest-common-denominator zero-native extension model) — so recognition
happens *as the user types* (fill anomalies, idiom hints, label↔data mismatches,
refactor-to-LAMBDA). C-ABI runs it at the **center** — batch corpus analysis in
DuckDB. One core is not just DRY: it guarantees the edge and the center compute
**bit-identical** R1C1 / AST / references, so a fingerprint made in the add-in
*equals* one in the corpus index. Without that, the killer edge feature — "does this
cell match a known idiom / an existing LAMBDA / a corpus domain?" — silently breaks.
Cross-tier fingerprint consistency is correctness, and only a single core delivers it.

## Same computation, different substrate — a confidence gradient

xl_refract's own build architecture (one `formualizer` core → WASM + C-ABI) is an
instance of the theme this project keeps circling: **the same computation on a
different substrate.** The Rust source is the IR; `wasm32` and the native target are
two substrates; the compiler is the lift/lower. The tool that analyzes the pattern is
built out of the pattern. That reframes the whole arc as one relation — "is this the
same computation?" — held at three levels of confidence:

| level | how sameness is established | guarantee | confirm by |
|---|---|---|---|
| **compiler** | one source, two compile targets (xl_refract WASM / C-ABI) | provable, bit-identical | nothing — by construction |
| **translation** | one IR → different dialects (Substrait → SQL Server / Postgres via blobtemplates) | usually, modulo dialect quirks | spot-check |
| **fingerprint** | recover sameness from structure, no shared source | abductive (Rice) | I/O test |

The punchline that unifies the two halves of this document: **fingerprinting is how
you recover "same computation, different substrate" when you lack the provenance that
would prove it.** With the source, sameness is *guaranteed* (xl_refract's two builds);
without it, *inferred* (an anonymous idiom in a stranger's workbook that happens to
match your index). Proof vs evidence — the supervised/unsupervised and measurement/
judgment split one more time, now over computations. The lift/lower ladder this sits
on is [[Rule4 For Functions]].

## Links
- [[Rule4 For Functions]]
- [[Catalog As Data]]
- [[Data As Control Plane]]
- [[Blobfilter Domain Probing]]
- [[Blobrule4 Project]]
