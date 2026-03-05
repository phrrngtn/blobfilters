# CLAUDE.md

## Project Context

This project implements automated column classification (dimension vs measure) for database tables using statistics histograms and catalog metadata. The primary workflow is:

1. Extract histogram and catalog data from SQL Server (and PostgreSQL) via DuckDB's nanodbc ODBC extension
2. Compute classification features (cardinality ratio, repeatability, discreteness) in DuckDB
3. Write results back to SQL Server as extended properties via the `RULE4.extended_property` view

The target databases are SQL Server, PostgreSQL, DuckDB, and SQLite. Each has its own catalog query dialect — do not attempt to abstract across them.

## SQL Style Guidelines

- **CTE naming**: Use UPPER_CASE_SNAKE_CASE for CTE names (e.g., `SINGLE_COL_STATS`).
- **Table aliasing**: Always use explicit `AS` between a table/CTE reference and its alias (e.g., `FROM sys.stats AS s`, not `FROM sys.stats s`).
- **Prefer CTEs over correlated subqueries**: Factor out filtering logic into CTEs rather than using `NOT EXISTS` or correlated subqueries in `WHERE` clauses.
- **Prefer window functions over GROUP BY**: When a CTE needs to compute aggregates for filtering, use windowing functions (e.g., `COUNT(*) OVER (PARTITION BY ...)`) rather than `GROUP BY` / `HAVING`.
- **Prefer PIVOT over self-joins or conditional aggregation**: When pivoting rows to columns, use the `PIVOT` operator rather than multiple self-joins or `MAX(CASE WHEN ...)` patterns.
- **Output raw IDs for programmatic use**: When results will be consumed by automated processing, prefer raw identifiers (`object_id`, `stats_id`, `column_id`) over resolved names. Do not wrap IDs in helper functions like `OBJECT_NAME()` or `COL_NAME()` unless explicitly requested or needed for debugging.
- **No unnecessary ORDER BY**: Omit `ORDER BY` unless the consumer requires ordered output. When ordering is needed and the query runs via `odbc_query`, apply `ORDER BY` on the DuckDB side rather than the SQL Server side to avoid query plan regressions.
- **NVARCHAR(MAX) column positioning**: When querying SQL Server via ODBC, place `NVARCHAR(MAX)` and other LOB columns last in the SELECT list. The ODBC driver requires large object columns to be fetched after all fixed-width columns (`Invalid Descriptor Index` error otherwise).
- **SQL_VARIANT handling**: Use `SQL_VARIANT_PROPERTY(value, 'BaseType')` to inspect the underlying type of variant values. For binary/varbinary/image/timestamp base types, convert through `VARBINARY` with style 1 to produce hex strings safe for UTF-8 transit.

## DuckDB via ODBC Conventions

- Queries to SQL Server are executed via `odbc_query('dsn_name', '...')`.
- Single quotes inside the SQL string must be escaped as `''` (DuckDB string escaping).
- DuckDB can resolve column names from `odbc_query` result sets at planning time — unlike SQL Server's `OPENQUERY`, you can reference column names in outer queries without prior declaration.
- Sorting, filtering, and joining against `odbc_query` results should be done on the DuckDB side when possible for performance.

## Extended Properties Architecture

- The `RULE4.extended_property` view provides a relational facade over `sys.extended_properties` with INSTEAD OF INSERT/UPDATE/DELETE triggers.
- MERGE statements work against this view (all three INSTEAD OF triggers are present, which is required).
- Security trimming is achieved by transferring view ownership to a no-login user (`break_ownership_user`), breaking the ownership chain so that metadata visibility rules apply per-row.
- Property names use dotted prefix conventions to define clusters: `survey.classification`, `survey.cardinality_ratio`, `fingerprint.histogram`, etc.
- The cardinality of distinct property names is expected to be small (< 100). Each cluster can be exposed as a PIVOT view.
- Extended property values are `sql_variant` (max 7,500 bytes) — sufficient for classification labels, numeric features, and compact JSON fingerprint vectors.

## Classification Signals (strongest to weakest)

1. **Foreign key membership** — column is a dimension key, full stop
2. **Primary key structure** — single-column PK = surrogate key; composite PK member without FK = degenerate dimension
3. **Histogram shape** — cardinality ratio, repeatability (avg equal_rows), discreteness (fraction of zero-range steps), range density
4. **Data type** — money/float/decimal almost always measures; varchar with low cardinality almost always dimensions; date/datetime are special cases (dimensions in DW sense but continuous histograms)

## Project Dependencies

- DuckDB with nanodbc community extension
- SQL Server 2016 SP1+ (for `sys.dm_db_stats_histogram`)
- `RULE4` schema objects from https://github.com/phrrngtn/rule4
