# CLAUDE.md

## Project Context

Blobfilters is a **roaring bitmap fingerprint engine** for domain detection and column classification. It provides the core computation layer: hash values into bitmaps, compare them (containment, Jaccard), build histogram fingerprints with shape metrics. It is backend-agnostic and has no opinion about where data comes from or where classification results go.

### What belongs here
- C library (`libroaring_fp`) and its extensions (DuckDB, SQLite, Python)
- Domain fingerprint building and comparison primitives
- Histogram fingerprint aggregates and shape metrics
- Demo/recipe SQL showing how to use the primitives (e.g., `demo/sampling_demo.sql`)

### What belongs elsewhere (rule4 or similar)
- Schema scraping: catalog queries against `sys.columns`, `sys.tables`, `sys.stats`, `sys.dm_db_stats_histogram`, `pg_stats`, etc.
- ODBC orchestration: `odbc_query()` workflows that extract metadata from remote databases
- Classification write-back: writing results to SQL Server extended properties via `RULE4.extended_property`
- PK/FK catalog analysis, schema topology time-series
- Classification *rules* and *thresholds* (the policy layer that interprets shape metrics)

### The contract between blobfilters and its consumers

Blobfilters' job ends at producing: a histogram fingerprint (JSON), shape metrics, containment scores, and classification signals. The output shape is something like `(column_name, classification, best_domain, containment, cardinality_ratio, ...)`. The consuming workflow (rule4) picks that up and handles catalog writes, extended property plumbing, and schema-level reasoning.

## SQL Style Guidelines

- **CTE naming**: Use UPPER_CASE_SNAKE_CASE for CTE names (e.g., `SINGLE_COL_STATS`).
- **Table aliasing**: Always use explicit `AS` between a table/CTE reference and its alias (e.g., `FROM sys.stats AS s`, not `FROM sys.stats s`).
- **Prefer CTEs over correlated subqueries**: Factor out filtering logic into CTEs rather than using `NOT EXISTS` or correlated subqueries in `WHERE` clauses.
- **Prefer window functions over GROUP BY**: When a CTE needs to compute aggregates for filtering, use windowing functions (e.g., `COUNT(*) OVER (PARTITION BY ...)`) rather than `GROUP BY` / `HAVING`.
- **No unnecessary ORDER BY**: Omit `ORDER BY` unless the consumer requires ordered output.

## Project Dependencies

- CRoaring (roaring bitmap library)
- nlohmann/json
- DuckDB (for extension, linked against installed headers)
- SQLite3 (for extension)
