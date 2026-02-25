# Blobfilters — Design Notes

## Vision

Every table encountered "in the wild" (Excel, PDF, CSV, API response) is the
materialized result of some query. Given a wild table with columns {c}, there
exists a "query inversion" that attributes each column to its source table and
infers the join paths that produced the result — assuming equijoins and
reasonable simplifications.

Blobfilters provides the foundational layer for this: fast, exact domain
detection via roaring bitmap fingerprints, stored in a self-describing catalog
that follows Codd's Rule 4 (the catalog is itself a collection of tables,
queryable with the same SQL used on the data it describes).

## Core Mechanism

Each **domain** (the set of values a column can take) is fingerprinted by
hashing its symbols via FNV-1a into a roaring bitmap. To classify a wild
column, its distinct values are hashed into a probe bitmap and compared
against stored domain fingerprints using intersection cardinality:

- **Containment** = |probe ∩ domain| / |probe| — "what fraction of my values
  belong to this domain?"
- **Jaccard** = |probe ∩ domain| / |probe ∪ domain| — "how similar are the
  two sets?"

This runs in microseconds per comparison. A 1000-symbol probe against 500
stored domains completes in well under 1ms.

### Collision Characteristics

FNV-1a produces uint32 hashes. Roaring bitmaps store these exactly (no
additional lossy compression).

**Within-domain collisions** (birthday problem — causes cardinality undercount):

| Domain size | P(any collision) |
|-------------|------------------|
| 1,000       | 0.01%            |
| 10,000      | 1%               |
| 77,000      | 50%              |

**Cross-domain false positives** (a probe symbol accidentally matches a domain
it doesn't belong to). Expected false hits = `probe_size × domain_size / 2^32`:

| Probe × Domain | Expected false hits |
|-----------------|---------------------|
| 1K × 1K        | 0.0002              |
| 1K × 100K      | 0.023               |
| 1K × 10M       | 2.3                 |

For domains up to low millions, false positives are negligible. If needed,
dual uint32 hashing (stored as uint64 in Roaring64) pushes the threshold to
2^64.

### Serialized Sizes

| Domain symbols | BLOB bytes | Bytes/symbol |
|----------------|------------|--------------|
| 50             | 508        | 10.2         |
| 1,000          | 10,008     | 10.0         |
| 10,000         | 95,760     | 9.6          |
| 100,000        | 614,608    | 6.1          |
| 1,000,000      | 2,524,296  | 2.5          |

Compression improves at higher density as roaring switches from sorted arrays
to bitset containers.

## Universal Catalog Schema

The structure of *any* tabular data source can be captured in 7 tables,
modeled after SQL Server's `sys.tables`, `sys.columns`, `sys.types`, etc.
The catalog is stored in SQLite and is portable, shareable, and versionable.

```sql
CREATE TABLE servers (
    server_id       INTEGER PRIMARY KEY,
    server_name     TEXT NOT NULL,
    server_type     TEXT NOT NULL,  -- 'sql_server','postgres','excel','csv','pdf','api'
    connection_info TEXT
);

CREATE TABLE catalogs (
    catalog_id      INTEGER PRIMARY KEY,
    server_id       INTEGER REFERENCES servers,
    catalog_name    TEXT NOT NULL   -- database name, filename, workbook name
);

CREATE TABLE schemas (
    schema_id       INTEGER PRIMARY KEY,
    catalog_id      INTEGER REFERENCES catalogs,
    schema_name     TEXT             -- 'dbo','public','Sheet1', NULL for flat files
);

CREATE TABLE tables (
    table_id        INTEGER PRIMARY KEY,
    schema_id       INTEGER REFERENCES schemas,
    table_name      TEXT NOT NULL,
    table_type      TEXT NOT NULL,  -- 'TABLE','VIEW','EXCEL_RANGE','CSV','PDF_EXTRACT'
    row_count       INTEGER
);

CREATE TABLE columns (
    column_id       INTEGER PRIMARY KEY,
    table_id        INTEGER REFERENCES tables,
    column_name     TEXT,           -- may be NULL for headerless sources
    ordinal_position INTEGER NOT NULL,
    data_type       TEXT,           -- logical: 'string','integer','decimal','date',...
    is_nullable     BOOLEAN
);

CREATE TABLE domains (
    domain_id       INTEGER PRIMARY KEY,
    domain_name     TEXT NOT NULL,
    description     TEXT,
    fingerprint     BLOB,           -- roaring bitmap
    symbol_count    INTEGER
);

-- Junction table: columns to domains (many-to-many with scores)
CREATE TABLE column_domains (
    column_id       INTEGER REFERENCES columns,
    domain_id       INTEGER REFERENCES domains,
    containment     REAL,           -- probe score: |col ∩ domain| / |col|
    confidence      REAL,           -- combined/weighted score
    PRIMARY KEY (column_id, domain_id)
);

-- Known column header names / aliases for each domain
CREATE TABLE domain_aliases (
    domain_id       INTEGER REFERENCES domains,
    alias           TEXT NOT NULL,  -- 'State', 'ST', 'state_code', 'ship_state'
    alias_type      TEXT,           -- 'exact', 'abbreviation', 'translation'
    PRIMARY KEY (domain_id, alias)
);
```

### Domain Alias Bootstrapping

The `domain_aliases` table can be populated automatically from the catalog
itself. When crawling databases, every `column_name` that has been attributed
to a domain (via `column_domains` with high containment) is an observed alias
for that domain. After crawling 50 databases, the `us_states` domain might
have accumulated aliases like "State", "STATE", "st", "state_code",
"customer_state", "ship_state", "billing_state" — all discovered from real
column names pointing to the same domain.

```sql
-- Bootstrap aliases from observed column names
INSERT OR IGNORE INTO domain_aliases (domain_id, alias, alias_type)
SELECT cd.domain_id, c.column_name, 'exact'
FROM column_domains cd
JOIN columns c USING (column_id)
WHERE cd.containment > 0.8
  AND c.column_name IS NOT NULL;
```

This turns the alias table into a crowdsourced thesaurus of column naming
conventions, grounded in actual data rather than manual curation. When
classifying a wild table, unmatched bounding box text (potential column
headers) can be looked up against this table. If a header matches multiple
domain aliases, the value-based containment score from the body of the
column breaks the tie.

### Source Mapping

| Source       | server_type | catalog    | schema  | table            | column      |
|--------------|-------------|------------|---------|------------------|-------------|
| SQL Server   | sql_server  | database   | dbo     | table            | column      |
| Postgres     | postgres    | database   | public  | table            | column      |
| Excel        | excel       | workbook   | sheet   | named range/area | header cell |
| CSV          | csv         | file       | NULL    | the file itself  | header      |
| PDF extract  | pdf_extract | file       | page    | detected table   | header      |
| REST API     | api         | base URL   | endpoint| response array   | JSON key    |

### Key Queries Against the Catalog

**Which database columns share a domain?** (implicit FK discovery)
```sql
SELECT d.domain_name,
       c1.column_name AS col_a, t1.table_name AS table_a,
       c2.column_name AS col_b, t2.table_name AS table_b
FROM column_domains cd1
JOIN column_domains cd2 USING (domain_id)
JOIN columns c1 ON cd1.column_id = c1.column_id
JOIN columns c2 ON cd2.column_id = c2.column_id
JOIN tables t1 ON c1.table_id = t1.table_id
JOIN tables t2 ON c2.table_id = t2.table_id
JOIN domains d ON cd1.domain_id = d.domain_id
WHERE cd1.column_id < cd2.column_id
  AND cd1.containment > 0.5 AND cd2.containment > 0.5;
```

**Classify a wild column against all domains:**
```sql
WITH probe AS (
    SELECT roaring_build_json('["California","Texas","USD","unknown"]') AS fp
)
SELECT d.domain_name, d.symbol_count,
       roaring_intersection_card(probe.fp, d.fingerprint) AS hits,
       roaring_containment(probe.fp, d.fingerprint)       AS containment
FROM domains d, probe
WHERE containment > 0
ORDER BY containment DESC;
```

**Infer join graph from wild table column attributions:**
```sql
-- Given that wild columns have been matched to domains, and domains
-- are linked to source columns, find the probable join paths.
SELECT DISTINCT
    t1.table_name  AS left_table,
    t2.table_name  AS right_table,
    c1.column_name AS join_column
FROM column_domains cd1
JOIN columns c1 ON cd1.column_id = c1.column_id
JOIN tables  t1 ON c1.table_id  = t1.table_id
JOIN column_domains cd2 ON cd1.domain_id = cd2.domain_id
JOIN columns c2 ON cd2.column_id = c2.column_id
JOIN tables  t2 ON c2.table_id  = t2.table_id
WHERE t1.table_id != t2.table_id
  AND cd1.containment > 0.7
  AND cd2.containment > 0.7;
```

## Page Classification Pipeline

Documents (PDF, Excel, Word) are tokenized into bounding boxes by the
companion [blobboxes](../blobboxes) library, which provides a SQLite virtual
table `bboxes` with columns `(page_id, x, y, w, h, text, style_id, ...)`.
Each text fragment is a token. For Excel this is one cell per bbox; for PDF
it is a run of characters with consistent style and position.

The classification pipeline is pure SQL using both extensions (`bboxes` for
extraction, `roaring` for domain probing). No ML, no training data.

### Per-Page Domain Detection

```sql
WITH page_tokens AS (
    SELECT page_id,
           json_group_array(text) AS symbols_json
    FROM bboxes
    WHERE file_path = 'report.pdf'
      AND text != ''
    GROUP BY page_id
),
page_probes AS (
    SELECT page_id,
           roaring_build_json(symbols_json) AS fp
    FROM page_tokens
),
page_matches AS (
    SELECT p.page_id,
           d.domain_name,
           d.symbol_count                                  AS domain_size,
           roaring_cardinality(p.fp)                       AS probe_size,
           roaring_intersection_card(p.fp, d.fingerprint)  AS est_hits,
           roaring_containment(p.fp, d.fingerprint)        AS containment,
           roaring_jaccard(p.fp, d.fingerprint)            AS jaccard
    FROM page_probes p, domain_fingerprints d
)
SELECT page_id,
       domain_name,
       domain_size,
       probe_size,
       est_hits,
       containment,
       jaccard
FROM page_matches
WHERE est_hits > 0
ORDER BY page_id, containment DESC;
```

Each page gets its own probe bitmap (built once in `page_probes`), then
cross-joined against all domain fingerprints. The `WHERE est_hits > 0`
filters to domains with at least one symbol present on that page.

### Page Run Detection (Gaps-and-Islands)

Pages with the same domain signature are likely part of the same logical
table spanning multiple pages. The classic gaps-and-islands technique groups
consecutive pages that share a signature:

```sql
WITH page_tokens AS (
    SELECT page_id,
           json_group_array(text) AS symbols_json
    FROM bboxes
    WHERE file_path = 'report.pdf'
      AND text != ''
    GROUP BY page_id
),
page_probes AS (
    SELECT page_id,
           roaring_build_json(symbols_json) AS fp
    FROM page_tokens
),
page_matches AS (
    SELECT p.page_id,
           d.domain_name,
           roaring_containment(p.fp, d.fingerprint) AS containment
    FROM page_probes p, domain_fingerprints d
),
page_signatures AS (
    SELECT page_id,
           GROUP_CONCAT(domain_name, '|') AS signature
    FROM page_matches
    WHERE containment > 0.05
    ORDER BY page_id, domain_name
),
runs AS (
    SELECT page_id,
           signature,
           page_id - ROW_NUMBER() OVER (PARTITION BY signature ORDER BY page_id) AS run_id
    FROM page_signatures
)
SELECT MIN(page_id) AS start_page,
       MAX(page_id) AS end_page,
       COUNT(*)     AS page_count,
       signature
FROM runs
GROUP BY signature, run_id
ORDER BY start_page;
```

A 200-page PDF where pages 1–2 are title/TOC, pages 3–45 are a customer
listing, pages 46–120 are order details, and pages 121–200 are an appendix
would produce exactly those 4 runs — each identified by its domain signature.

### Properties

This approach has several desirable properties:

- **No training data** — domains are defined by their actual values. Add a
  domain by inserting its symbols. No retraining, no embeddings to regenerate.
- **Interpretable** — output is "8 of your 22 values are literally in this
  domain." Not a probability from a black box.
- **Compositional** — the whole pipeline is CTEs. Wrap it, filter it, join it
  with other queries. Run detection is just a `GROUP BY` on top of
  classification. Join path inference is another query on top of that.
- **Incremental** — scan a new database, `INSERT INTO domains` its column
  fingerprints, and every future probe benefits immediately.
- **Portable** — the entire classifier is a SQLite file (the domain catalog)
  plus two loadable extensions. No Python runtime, no model weights, no GPU.
- **Fast** — microsecond bitmap intersections mean thousands of domains can be
  probed per page, processing hundreds of pages per second. The bottleneck is
  document parsing (blobboxes), not classification.

## Complementary Signals (Future)

Roaring bitmap probing is the fast, exact foundation. Additional signals can
be layered on top for cases where exact matching is insufficient:

### Histogram Probing

For large database columns where full symbol extraction is impractical,
SQL Server's `sys.dm_db_stats_histogram` provides a 200-step summary.
A probe can binary-search histogram boundaries for exact hits (symbol equals
a `range_high_key`) and in-range hits (symbol falls within a bucket with
`distinct_range_rows > 0`). Accuracy degrades with domain size — at 200 steps
over 100K values, 99.8% of the domain is not directly visible.

### Semantic Embedding

For matching across naming conventions, languages, or abbreviations (e.g.,
"CA" vs "California", "État" vs "State"), column header embeddings and/or
domain description embeddings can provide a semantic similarity score.
Embeddings trade false negatives for false positives compared to exact
hashing, so they complement rather than replace the roaring approach.

### Combined Scoring

| Signal              | Method          | Strength                                    |
|---------------------|-----------------|---------------------------------------------|
| Exact membership    | Roaring bitmap  | Definitive, microsecond, no ML              |
| Approx. membership  | Histogram probe | Works without full domain, lossy             |
| Semantic similarity | Embeddings      | Catches synonyms/translations, needs model   |

## Query Inversion

The end goal: given a wild table, reconstruct the probable query that produced
it.

1. **Column-to-source attribution** — probe each wild column against domain
   catalog, rank candidate source columns.
2. **Join path inference** — given attributed columns spanning multiple source
   tables, find FK/domain paths connecting them in the schema graph.
3. **Derived column detection** — columns with no domain match may be computed
   (concatenations, arithmetic, aggregates). Heuristics or LLM assistance.
4. **Query assembly** — emit `SELECT ... FROM ... JOIN ...` from the inferred
   attribution and join graph.

Steps 1–2 are tractable with the catalog as described. Steps 3–4 are harder
and may require interactive/LLM-assisted refinement.
