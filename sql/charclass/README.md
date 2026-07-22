# char-class structural-type layer — the FK-target pattern

The C library owns the **feature vocabulary** (primitive char-class bits + named
composites-as-logical-operators) — the canonical, versioned, cross-surface-identical
**FK target**. `bf_cc_features_json()` dumps it as `{version, features:[{bit,name,
regexp,expr}]}`; `bf_cc_signature(text)` is the one-pass feature bitmask; `bf_cc_feature_bit(name)`
is the FK resolver (NULL = a broken reference / a source's diverged name).

User-land tables hold the **domain ↔ feature mappings** (the FK *references*) and the
per-domain envelopes. Two ways to populate an envelope, and they cross-check:
- **compile** from a full enumeration: `bit_and`(necessary) / `bit_or`(envelope) over member signatures;
- **declare** in a `domain_features` junction; a curator says "us_state ⟵ all_upper, len_2".

## composites-as-data

`bf_cc_eval(sig, expr)` evaluates a boolean feature-expression (`& | ^ ! ~ ()` over
vocabulary names) against a precomputed signature — the runtime for user composites.
Users define composites as **data**; they can reference other composites (a DAG).

`composite_registry.sql` — the registry pattern. Users own the *definition* (`name, expr`);
the system *derives* identity + integrity and never lets a user write a surrogate:
- the DAG is inlined to a canonical primitive-only expr;
- `surrogate = bf_sha256(canonical)` → semantically-identical composites **dedup**
  automatically and garbage surrogates are impossible (the id is a pure function of meaning);
- integrity findings: **broken references** (a name in no vocabulary) and **cycles**
  (a definition that never converges) are flagged;
- `participation_mask` = OR of the primitive bits the composite reads (a concrete
  "expanded mask" footprint for cheap prefiltering).
`bf_cc_eval` is the reference evaluator any downstream mask / flattened-SSA / Roaring
compiler must agree with.

## recipes

`feature_vector.sql` — cross-join symbols × the vocabulary's regexps to inspect/tune the
vector in pure SQL (verified identical to `bf_cc_signature`).
`envelope_junction.sql` — materialize the FK target, the junction, FK-integrity check, and
the compiled-vs-declared cross-check (under-/over-declaration findings).
`straggler_workflow.sql` — tighten a loose probe: profile → diff the keeper/straggler
fingerprints to find a discriminating feature → `new = old & !(holdout)` → verify with two
zero-columns (no collateral damage).
`composite_registry.sql` — the user-land composite registry described above.

Adjust the `LOAD` path to your built `blobfilters.duckdb_extension`.
