# Blobfilters

> **Note:** This code is almost entirely AI-authored (Claude, Anthropic), albeit under close human supervision, and is for research and experimentation purposes. Successful experiments may be re-implemented in a more coordinated and curated manner.

A lightweight library that fingerprints the *values* in any data column — from a database, spreadsheet, PDF, or API — into compact roaring bitmaps. Given a "wild" table you've never seen before, it can tell you in microseconds which columns are US states, which are currency codes, which are customer IDs — by probing the actual data against a catalog of known domains. No ML, no training data, no GPU — just hash, intersect, and count. It runs as a SQLite extension, a DuckDB extension, or a Python library, and the entire classifier is a single portable database file.

The endgame: **query inversion** — reconstructing the probable `SELECT ... FROM ... JOIN ...` that produced any table you encounter in the wild.

## How It Works

Each **domain** (the set of values a column can take) is fingerprinted by hashing its symbols via FNV-1a into a [roaring bitmap](https://roaringbitmap.org/). To classify a wild column, hash its distinct values into a probe bitmap and compare:

- **Containment** = |probe ∩ domain| / |probe| — "what fraction of my values belong to this domain?"
- **Jaccard** = |probe ∩ domain| / |probe ∪ domain| — "how similar are the two sets?"

This runs in microseconds per comparison. A 1000-symbol probe against 500 stored domains completes in well under 1ms.

## Quick Start

```bash
# Build core library + SQLite extension + demos
cmake -B build -DBUILD_SQLITE_EXTENSION=ON
cmake --build build

# Run the SQLite demo
./build/sqlite_demo

# Run tests
./build/test_roaring
```

## Functions

Available in both SQLite and DuckDB extensions:

| Function | Type | Description |
|---|---|---|
| `roaring_build(value)` | aggregate | Hash each value, build bitmap |
| `roaring_build_json(json_array)` | scalar | Build bitmap from JSON array of strings |
| `roaring_cardinality(blob)` | scalar | Number of elements in bitmap |
| `roaring_intersection_card(a, b)` | scalar | \|A ∩ B\| |
| `roaring_containment(probe, ref)` | scalar | \|probe ∩ ref\| / \|probe\| |
| `roaring_jaccard(a, b)` | scalar | \|A ∩ B\| / \|A ∪ B\| |
| `roaring_containment_json(json, ref)` | scalar | Build probe from JSON, return containment |
| `roaring_to_base64(blob)` | scalar | Serialize bitmap to base64 text |
| `roaring_from_base64(text)` | scalar | Deserialize bitmap from base64 |

## Example: Domain Detection

```sql
-- Build domain fingerprints
INSERT INTO domain_fingerprints
SELECT 'us_states', COUNT(*), roaring_build(state_name)
FROM us_states;

-- Probe a wild column against all domains
WITH probe AS (
    SELECT roaring_build(value) AS fp FROM wild_column
)
SELECT d.domain_name,
       roaring_containment(probe.fp, d.fingerprint) AS containment
FROM domain_fingerprints d, probe
ORDER BY containment DESC;
```

## Architecture

```
blobfilters/
├── include/roaring_fp.h          # Core C API
├── src/roaring_fp.cpp            # Core implementation (CRoaring + nlohmann/json)
├── sqlite_ext/                   # SQLite loadable extension
├── duckdb_ext/                   # DuckDB extension
├── python/                       # Python bindings (nanobind)
├── third_party/roaring/          # CRoaring amalgamation
├── demo/
│   ├── sqlite_demo.c             # SQLite demo (statically linked)
│   └── duckdb_demo.sql           # DuckDB demo (SQL script)
└── test/
    └── test_roaring.c            # Core library tests
```

## Build Options

```bash
cmake -B build \
  -DBUILD_SQLITE_EXTENSION=ON \   # SQLite extension + demo
  -DBUILD_DUCKDB_EXTENSION=ON \   # DuckDB extension (requires: brew install duckdb)
  -DBUILD_PYTHON_BINDINGS=ON \    # Python nanobind module
  -DBUILD_TESTS=ON                # Tests + fingerprint demo
```

## Design

See [DESIGN.md](DESIGN.md) for the full vision: universal catalog schema, page classification pipeline, histogram triage, FK discovery, and query inversion.

## Origins

Based on [oldmoe/roaringlite](https://github.com/oldmoe/roaringlite), a SQLite extension for CRoaring bitmap operations. blobfilters adds domain fingerprinting (containment/Jaccard probing), the blob extension build pattern (shared C core + DuckDB and Python bindings), and the [rbloom](https://github.com/KenanHanke/rbloom) Bloom filter library was a reference point during early evaluation of probabilistic set membership approaches.

## License

- Extension code: MIT
- CRoaring: Apache 2.0 / MIT dual license
