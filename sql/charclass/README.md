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

`feature_vector.sql` — cross-join symbols × the vocabulary's regexps to inspect/tune the
vector in pure SQL (verified identical to `bf_cc_signature`).
`envelope_junction.sql` — materialize the FK target, the junction, FK-integrity check, and
the compiled-vs-declared cross-check (under-/over-declaration findings).

Adjust the `LOAD` path to your built `blobfilters.duckdb_extension`.
