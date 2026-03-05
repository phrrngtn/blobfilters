# Column Classification via Database Statistics Histograms

Automated dimension/measure classification of database columns using statistics histograms, catalog metadata, and extended properties as a security-trimmed metadata store.

## Motivation

In data warehousing and data discovery, understanding whether a column is a **dimension** (categorical, groupable) or a **measure** (numeric, aggregatable) is fundamental. This project infers that classification automatically by analyzing:

1. **Statistics histograms** — the shape of the value distribution (cardinality, repeatability, discreteness)
2. **Primary key participation** — surrogate keys, composite keys, degenerate dimensions
3. **Foreign key relationships** — the strongest signal; an FK column is a dimension key by definition

Results are written back as **SQL Server extended properties** via the [RULE4.extended_property](https://github.com/phrrngtn/rule4) view, which provides:

- A relational facade over `sp_addextendedproperty` / `sp_updateextendedproperty` / `sp_dropextendedproperty`
- **Automatic security trimming** via a deliberate ownership chain break — users only see properties on objects they have access to
- INSTEAD OF INSERT/UPDATE/DELETE triggers enabling standard DML (including MERGE) against extended properties

## Architecture

```
┌─────────────────────┐
│   SQL Server        │
│                     │
│  sys.stats          │
│  sys.stats_columns  │
│  sys.dm_db_stats_   │
│    histogram()      │──── ODBC/nanodbc ────┐
│  sys.index_columns  │                      │
│  sys.foreign_key_   │                      │
│    columns          │                      ▼
│                     │            ┌──────────────────┐
│  RULE4.extended_    │            │   DuckDB         │
│    property (VIEW)  │◄───────────│   (per-person    │
│    + INSTEAD OF     │  writeback │    search index) │
│      triggers       │            └──────────────────┘
└─────────────────────┘
```

### Key Architectural Principles

- **Searchable index per person**: Each user gets their own DuckDB database. Security and visibility are handled by the index boundary itself — no need for row-level security on the search index because the index only contains what the user was authorized to see at extraction time.

- **Extended properties as metadata cache**: Expensive survey computations (histogram analysis, fingerprinting) are materialized as extended properties on the source objects. `sys.extended_properties` respects SQL Server's metadata visibility rules, so downstream consumers get security trimming for free.

- **Dialect-specific catalog queries**: Rather than abstracting across database backends, we write explicit catalog queries for each target (SQL Server, PostgreSQL, DuckDB, SQLite). The set of targets is small enough that this is simpler than any abstraction layer.

## SQL Server: Histogram Extraction

Extract all single-column statistics histograms from user tables via DuckDB's nanodbc extension:

```sql
odbc_query('your_dsn', '
WITH SINGLE_COL_STATS AS (
    SELECT object_id, stats_id, column_id,
           COUNT(*) OVER (PARTITION BY object_id, stats_id) AS col_count
    FROM sys.stats_columns
)
SELECT
    s.object_id,
    s.stats_id,
    scs.column_id,
    h.step_number,
    h.range_rows,
    h.equal_rows,
    h.distinct_range_rows,
    h.average_range_rows,
    CASE
        WHEN SQL_VARIANT_PROPERTY(h.range_high_key, ''BaseType'')
             IN (''binary'', ''varbinary'', ''image'', ''timestamp'')
            THEN CONVERT(NVARCHAR(MAX), CONVERT(VARBINARY(8000), h.range_high_key), 1)
        ELSE CONVERT(NVARCHAR(MAX), h.range_high_key)
    END AS range_high_key
FROM sys.stats AS s
INNER JOIN SINGLE_COL_STATS AS scs
    ON s.object_id = scs.object_id
    AND s.stats_id = scs.stats_id
    AND scs.col_count = 1
CROSS APPLY sys.dm_db_stats_histogram(s.object_id, s.stats_id) AS h
WHERE OBJECTPROPERTY(s.object_id, ''IsUserTable'') = 1
')
```

### Important notes

- **NVARCHAR(MAX) column ordering**: The ODBC driver (Microsoft ODBC Driver 17 for SQL Server) requires LOB columns (`NVARCHAR(MAX)`) to be last in the SELECT list. Otherwise you get `Invalid Descriptor Index` errors. Place `range_high_key` at the end.

- **UTF-8 encoding issues**: `range_high_key` is `SQL_VARIANT` and can contain byte sequences invalid in UTF-8 (for binary/varbinary columns). Use `SQL_VARIANT_PROPERTY(h.range_high_key, 'BaseType')` to detect the underlying type and hex-encode binary values.

- **ORDER BY performance**: Adding `ORDER BY` to the SQL Server query can cause plan regressions (observed: 18s → 30s for 13k rows). Do the sort on the DuckDB side instead — sorting 13k rows in DuckDB is essentially free.

## SQL Server: Classification Query

The full classification query with histogram features, PK/FK metadata, and heuristic classification:

```sql
odbc_query('your_dsn', '
WITH SINGLE_COL_STATS AS (
    SELECT object_id, stats_id, column_id,
           COUNT(*) OVER (PARTITION BY object_id, stats_id) AS col_count
    FROM sys.stats_columns
),
PK_COLUMNS AS (
    SELECT
        ic.object_id,
        ic.column_id,
        COUNT(*) OVER (PARTITION BY ic.object_id, ic.index_id) AS pk_column_count
    FROM sys.index_columns AS ic
    INNER JOIN sys.indexes AS i
        ON ic.object_id = i.object_id
        AND ic.index_id = i.index_id
    WHERE i.is_primary_key = 1
),
FK_COLUMNS AS (
    SELECT DISTINCT
        parent_object_id AS object_id,
        parent_column_id AS column_id
    FROM sys.foreign_key_columns
),
HISTOGRAM_FEATURES AS (
    SELECT
        s.object_id,
        s.stats_id,
        scs.column_id,
        COUNT(*) AS step_count,
        SUM(h.equal_rows) AS total_equal_rows,
        SUM(h.range_rows) AS total_range_rows,
        SUM(h.distinct_range_rows) AS total_distinct_range,
        SUM(h.equal_rows) + SUM(h.range_rows) AS total_rows,
        AVG(h.equal_rows) AS avg_equal_rows,
        AVG(h.average_range_rows) AS avg_avg_range_rows,
        COUNT(CASE WHEN h.range_rows = 0 THEN 1 END) AS zero_range_steps
    FROM sys.stats AS s
    INNER JOIN SINGLE_COL_STATS AS scs
        ON s.object_id = scs.object_id
        AND s.stats_id = scs.stats_id
        AND scs.col_count = 1
    CROSS APPLY sys.dm_db_stats_histogram(s.object_id, s.stats_id) AS h
    WHERE OBJECTPROPERTY(s.object_id, ''IsUserTable'') = 1
    GROUP BY s.object_id, s.stats_id, scs.column_id
)
SELECT
    OBJECT_SCHEMA_NAME(hf.object_id) AS schema_name,
    OBJECT_NAME(hf.object_id) AS table_name,
    COL_NAME(hf.object_id, hf.column_id) AS column_name,
    CASE
        WHEN fk.column_id IS NOT NULL THEN ''dimension key''
        WHEN pk.column_id IS NOT NULL AND pk.pk_column_count > 1 AND fk.column_id IS NULL
            THEN ''degenerate dimension''
        WHEN pk.column_id IS NOT NULL AND pk.pk_column_count = 1
            THEN ''surrogate key''
        WHEN hf.total_rows > 0
             AND CAST((hf.step_count + hf.total_distinct_range) AS FLOAT) / hf.total_rows < 0.1
             AND hf.avg_equal_rows > 100
            THEN ''low-cardinality dimension''
        WHEN hf.total_rows > 0
             AND CAST(hf.zero_range_steps AS FLOAT) / hf.step_count > 0.5
             AND hf.avg_equal_rows > 5
            THEN ''dimension''
        WHEN hf.total_rows > 0
             AND CAST((hf.step_count + hf.total_distinct_range) AS FLOAT) / hf.total_rows > 0.5
             AND hf.avg_equal_rows < 2
            THEN ''measure''
        ELSE ''ambiguous''
    END AS classification,
    hf.object_id,
    hf.stats_id,
    hf.column_id,
    hf.step_count,
    hf.total_rows,
    CASE WHEN hf.total_rows > 0
        THEN CAST((hf.step_count + hf.total_distinct_range) AS FLOAT) / hf.total_rows
        ELSE 0
    END AS cardinality_ratio,
    hf.avg_equal_rows AS repeatability,
    CASE WHEN hf.step_count > 0
        THEN CAST(hf.zero_range_steps AS FLOAT) / hf.step_count
        ELSE 0
    END AS discreteness,
    hf.avg_avg_range_rows AS range_density,
    CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_pk,
    ISNULL(pk.pk_column_count, 0) AS pk_column_count,
    CASE WHEN fk.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_fk
FROM HISTOGRAM_FEATURES AS hf
LEFT JOIN PK_COLUMNS AS pk
    ON hf.object_id = pk.object_id
    AND hf.column_id = pk.column_id
LEFT JOIN FK_COLUMNS AS fk
    ON hf.object_id = fk.object_id
    AND hf.column_id = fk.column_id
')
ORDER BY schema_name, table_name, column_name;
```

### Classification heuristics

| Classification | Signal |
|---|---|
| **dimension key** | Column is a foreign key (strongest signal) |
| **degenerate dimension** | In composite PK but not an FK |
| **surrogate key** | Sole column in PK |
| **low-cardinality dimension** | < 10% cardinality ratio, avg equal_rows > 100 |
| **dimension** | > 50% zero-range histogram steps, avg equal_rows > 5 |
| **measure** | > 50% cardinality ratio, avg equal_rows < 2 |
| **ambiguous** | None of the above |

These thresholds need tuning against real data. The FK-based classifications should be reliable; the histogram-based ones are heuristic starting points.

### Multi-column statistics

SQL Server only auto-creates multi-column statistics for composite indexes. Auto-stats created by the query optimizer (`_WA_Sys_` prefix) are always single-column. So single-column histogram analysis covers all auto-generated statistics. The density vector on composite index statistics could provide additional signal but requires `DBCC SHOW_STATISTICS ... WITH DENSITY_VECTOR` which has no clean DMV equivalent.

## Survey Writeback via Extended Properties

Write classification results back to SQL Server as extended properties using MERGE against `RULE4.extended_property`:

```sql
MERGE RULE4.extended_property AS tgt
USING (
    SELECT
        OBJECT_SCHEMA_NAME(object_id) AS object_schema,
        OBJECT_NAME(object_id) AS object_name,
        COL_NAME(object_id, column_id) AS column_name,
        'survey.classification' AS property_name,
        CAST(classification AS SQL_VARIANT) AS property_value
    FROM #classification_results
) AS src
ON tgt.object_schema = src.object_schema
    AND tgt.object_name = src.object_name
    AND ISNULL(tgt.column_name, '') = ISNULL(src.column_name, '')
    AND tgt.property_name = src.property_name
WHEN MATCHED THEN
    UPDATE SET property_value = src.property_value
WHEN NOT MATCHED THEN
    INSERT (object_schema, object_name, column_name, property_name, property_value)
    VALUES (src.object_schema, src.object_name, src.column_name, src.property_name, src.property_value);
```

MERGE works with INSTEAD OF triggers provided all three triggers (INSERT, UPDATE, DELETE) are enabled — which `RULE4.extended_property` satisfies. The triggers use cursors internally but performance is acceptable for thousands of objects.

### Pivot views over survey properties

Create faceted views that pivot property clusters into columns:

```sql
CREATE VIEW RULE4.column_classification AS
SELECT
    object_schema,
    object_name,
    column_name,
    [survey.classification] AS classification,
    [survey.cardinality_ratio] AS cardinality_ratio,
    [survey.discreteness] AS discreteness,
    [survey.repeatability] AS repeatability
FROM (
    SELECT object_schema, object_name, column_name, property_name, property_value
    FROM RULE4.extended_property
    WHERE property_name LIKE 'survey.%'
        AND column_name IS NOT NULL
) AS src
PIVOT (
    MAX(property_value)
    FOR property_name IN (
        [survey.classification],
        [survey.cardinality_ratio],
        [survey.discreteness],
        [survey.repeatability]
    )
) AS pvt;
```

The subquery controls which columns participate in the implicit GROUP BY. Property name prefixes (`survey.`, `fingerprint.`, `lineage.`) define natural clusters for separate pivot views.

## PostgreSQL: Equivalent Catalog Query

PostgreSQL uses `pg_stats` with pre-computed histogram and frequency data rather than raw histogram steps:

```sql
WITH STATS_FEATURES AS (
    SELECT
        c.oid AS object_id,
        s.schemaname AS schema_name,
        s.tablename AS table_name,
        s.attname AS column_name,
        a.attnum AS column_id,
        c.reltuples AS total_rows,
        s.n_distinct,
        s.null_frac,
        s.correlation,
        CASE
            WHEN s.n_distinct < 0 THEN -s.n_distinct * c.reltuples
            ELSE s.n_distinct
        END AS estimated_distinct,
        array_length(s.most_common_vals::text[], 1) AS mcv_count,
        s.most_common_freqs[1] AS top_freq,
        array_length(s.histogram_bounds::text[], 1) AS histogram_bucket_count
    FROM pg_stats AS s
    INNER JOIN pg_class AS c
        ON s.tablename = c.relname
    INNER JOIN pg_namespace AS n
        ON c.relnamespace = n.oid
        AND s.schemaname = n.nspname
    INNER JOIN pg_attribute AS a
        ON a.attrelid = c.oid
        AND a.attname = s.attname
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
        AND c.relkind = 'r'
),
PK_COLUMNS AS (
    SELECT
        con.conrelid AS object_id,
        cols.attnum AS column_id,
        array_length(con.conkey, 1) AS pk_column_count
    FROM pg_constraint AS con
    CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS cols(attnum, ord)
    WHERE con.contype = 'p'
),
FK_COLUMNS AS (
    SELECT DISTINCT
        con.conrelid AS object_id,
        cols.attnum AS column_id
    FROM pg_constraint AS con
    CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS cols(attnum, ord)
    WHERE con.contype = 'f'
)
SELECT
    sf.schema_name,
    sf.table_name,
    sf.column_name,
    CASE
        WHEN fk.column_id IS NOT NULL THEN 'dimension key'
        WHEN pk.column_id IS NOT NULL AND pk.pk_column_count > 1 AND fk.column_id IS NULL
            THEN 'degenerate dimension'
        WHEN pk.column_id IS NOT NULL AND pk.pk_column_count = 1
            THEN 'surrogate key'
        WHEN sf.total_rows > 0 AND sf.estimated_distinct / sf.total_rows < 0.1
             AND sf.top_freq > 0.01
            THEN 'low-cardinality dimension'
        WHEN sf.total_rows > 0 AND sf.mcv_count IS NOT NULL
             AND sf.estimated_distinct < 1000 AND sf.top_freq > 0.005
            THEN 'dimension'
        WHEN sf.total_rows > 0 AND sf.estimated_distinct / sf.total_rows > 0.5
             AND (sf.top_freq IS NULL OR sf.top_freq < 0.001)
            THEN 'measure'
        ELSE 'ambiguous'
    END AS classification,
    sf.object_id,
    sf.column_id,
    sf.total_rows,
    sf.estimated_distinct,
    sf.estimated_distinct / NULLIF(sf.total_rows, 0) AS cardinality_ratio,
    sf.top_freq,
    sf.mcv_count,
    sf.histogram_bucket_count,
    sf.null_frac,
    sf.correlation
FROM STATS_FEATURES AS sf
LEFT JOIN PK_COLUMNS AS pk
    ON sf.object_id = pk.object_id
    AND sf.column_id = pk.column_id
LEFT JOIN FK_COLUMNS AS fk
    ON sf.object_id = fk.object_id
    AND sf.column_id = fk.column_id
ORDER BY sf.schema_name, sf.table_name, sf.column_name;
```

### PostgreSQL-specific notes

- `n_distinct`: negative means fraction of rows (e.g. -1.0 = unique), positive means estimated count
- `most_common_vals` / `most_common_freqs`: parallel arrays of frequent values and their frequencies
- `histogram_bounds`: equi-depth boundaries for values not in the MCV list
- `correlation`: physical-to-logical ordering correlation — useful additional signal (low correlation on a dimension key in a fact table suggests facts arrive in time order, not dimension order)

## Fingerprinting

Histograms serve as column fingerprints — a lossy compression of the value distribution. Two columns with similar distribution shapes are likely the same data, regardless of column naming. This enables:

- **Workbook matching**: A broker sends an Excel file; match its columns against known database columns by comparing sampled distributions against stored histogram fingerprints
- **Cross-source discovery**: Find columns across different databases that contain the same kind of data
- **Stability**: Fingerprints are invariant to formatting changes, column reordering, row reordering
- **Graceful degradation with sampling**: A subset of rows is sufficient to approximate the distribution shape

Fingerprint vectors can be stored as JSON in extended property values (`sql_variant` supports up to 7,500 bytes).

## Dependencies

- [DuckDB](https://duckdb.org/) with the [nanodbc community extension](https://community-extensions.duckdb.org/) for ODBC access
- [RULE4.extended_property](https://github.com/phrrngtn/rule4/blob/main/sql/extended_properties.sql) view and triggers
- SQL Server 2016 SP1+ (for `sys.dm_db_stats_histogram`)
