# Browser-Side Domain Matching via WASM

Run blobfilters' roaring bitmap domain classifiers in the browser,
matching text bounding boxes against domain filters in real time.

## Motivation

When browsing a web page that contains tabular data (pricing tables,
financial data, product specs), we want to find regions that match
schemas of interest — e.g., "show me the table that looks like the
named ranges in my Excel workbook."

The database (DuckDB/PostgreSQL via blobfilters) has domain classifiers
built from known data: roaring bitmaps where each set bit represents a
token hash associated with a domain (e.g., `price_per_mtok`,
`model_identifier`, `currency_code`). These classifiers need to run
in the browser, against text extracted from the rendered DOM.

## Architecture

```
Database (blobfilters)              Browser (roaring-wasm)
┌─────────────────────┐            ┌──────────────────────────────┐
│ bf_roaring_create()  │            │  CDP Isolated World          │
│ bf_roaring_add()     │  portable  │                              │
│ bf_roaring_serialize │ ──bytes──→ │  RoaringBitmap32.deserialize │
│   ("portable")       │            │                              │
└─────────────────────┘            │  MutationObserver            │
                                   │    → TreeWalker              │
                                   │    → Range.getClientRects()  │
                                   │    → tokenize text           │
                                   │    → build bitmap            │
                                   │    → andCardinality(filter)  │
                                   │    → highlight if match      │
                                   └──────────────────────────────┘
```

## Key insight: portable serialization as interop bridge

CRoaring (the C library that blobfilters wraps) and
[roaring-wasm](https://github.com/SalvatorePreviti/roaring-wasm)
(the WASM port) share the same portable serialization format. Bitmaps
created in DuckDB via `bf_roaring_serialize(bitmap, 'portable')` can
be deserialized in the browser via `RoaringBitmap32.deserialize("portable", bytes)`.

No conversion, no translation — same bytes, different runtimes.

## Browser-side flow

```javascript
// 1. Initialize roaring-wasm
import { RoaringBitmap32, roaringLibraryInitialize } from "roaring-wasm";
await roaringLibraryInitialize();

// 2. Deserialize domain filters (sent from Python controller)
//    Each filter is a roaring bitmap of token hashes for one domain
const filters = domainFilters.map(f => ({
    domain: f.name,       // e.g. "price_per_mtok"
    bitmap: RoaringBitmap32.deserialize("portable", f.bytes),
}));

// 3. For each text bbox from the MutationObserver:
function classifyBBox(bbox) {
    // Tokenize and hash (same hash function as blobfilters uses)
    const textBitmap = new RoaringBitmap32();
    for (const token of tokenize(bbox.text)) {
        textBitmap.add(fnv1a(token));
    }

    // Probe each domain filter
    const matches = [];
    for (const f of filters) {
        const overlap = f.bitmap.andCardinality(textBitmap);
        if (overlap > 0) {
            matches.push({
                domain: f.domain,
                score: overlap / textBitmap.size,  // Jaccard-ish
            });
        }
    }

    textBitmap.dispose();  // free WASM memory
    return matches;
}
```

## Performance expectations

- `andCardinality` on roaring bitmaps: O(min(n,m)), microseconds
- WASM execution: within 1.5-2x of native C
- A typical page: ~500 text bboxes × ~20 domain filters = ~10,000 probes
- Total time: well under 10ms — imperceptible

The bottleneck is DOM traversal (TreeWalker + getClientRects), not
bitmap operations. The WASM cost is noise.

## Injection controllers

The same JS extraction + classification code runs in both environments:

| Controller | Injection | Isolated world | JS→Python callback |
|---|---|---|---|
| **Playwright** (headless) | `page.evaluate()` | CDP `createIsolatedWorld` | `expose_function()` |
| **PySide6** (interactive) | `runJavaScript(js, worldId)` | `ApplicationWorld` (1-256) | `QWebChannel` |

Playwright for headless scraping/CI. PySide6 for interactive use
(Excel CTPs, development, "browsing while hunting" mode).

## The "hunting" use case

A user is browsing the web with a PySide6 CTP (Content Task Pane)
connected to their Excel workbook. The CTP:

1. Reads the named ranges and ListObjects from the active workbook
2. Extracts column schemas → domain labels (via blobfilters)
3. Serializes the domain filters as portable roaring bitmaps
4. Injects them into the browser's isolated world
5. The MutationObserver + bbox classifier runs continuously
6. When the user navigates to a page with matching tables, the
   matching bboxes are highlighted — "here's a pricing table that
   matches the columns in your spreadsheet"

The user doesn't search for the data — the data finds them.

## Hash function compatibility

The token hash function must be identical between blobfilters (C) and
the browser (JS). blobfilters uses FNV-1a (32-bit). The JS equivalent:

```javascript
function fnv1a(str) {
    let hash = 0x811c9dc5;  // FNV offset basis
    for (let i = 0; i < str.length; i++) {
        hash ^= str.charCodeAt(i);
        hash = Math.imul(hash, 0x01000193);  // FNV prime
    }
    return hash >>> 0;  // unsigned 32-bit
}
```

`Math.imul` is critical — it does 32-bit integer multiplication
(JS numbers are floats by default). The `>>> 0` ensures unsigned.

## Project structure

This capability would live in blobfilters as a new wrapper tier:

```
blobfilters/
├── src/                    # Core C API (roaring bitmaps, domain filters)
├── duckdb_ext/             # DuckDB wrapper
├── sqlite_ext/             # SQLite wrapper
├── python/                 # Python wrapper
└── wasm/                   # Browser wrapper (NEW)
    ├── src/
    │   ├── bbox_observer.js    # TreeWalker + MutationObserver
    │   ├── domain_matcher.js   # Roaring bitmap probing
    │   └── index.js            # Entry point + initialization
    ├── package.json            # depends on roaring-wasm
    └── README.md
```

The `wasm/` directory is a standalone npm package that depends on
`roaring-wasm` and provides the bbox observer + domain matching
pipeline as a single injectable script. The Python controllers
(Playwright, PySide6) load this script and inject it into the
browser's isolated world.

## Cross-references

- **blobapi**: `bbox_extract.py` — working Playwright prototype of the
  TreeWalker + getClientRects extraction
- **blobapi**: `docs/schema-driven-table-extraction.md` — the broader
  vision for variable-resolution schema matching
- **blobapi**: `pyside6_bbox_demo.py` — PySide6 equivalent
- **blobboxes**: spatial layout analysis for PDFs (future: share the
  bbox data structure between browser and PDF extraction)
