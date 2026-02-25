# DuckDB Roaring Bitmap Extension

High-performance set operations using [Roaring Bitmaps](https://roaringbitmap.org/) for column fingerprinting and domain inference in DuckDB.

## Overview

This extension provides functions to:
- Build compact bitmap fingerprints from column values
- Compute set intersection, containment, and Jaccard similarity
- Enable fast column matching for schema inference on unlabeled data

## Quick Start

```bash
# Build
mkdir build && cd build
cmake ..
cmake --build .

# Run the demo
./fingerprint_demo

# Or run tests
./test_roaring
```

## Prerequisites

- **CMake** 3.14+
- **C compiler** (gcc, clang, or MSVC)
- **DuckDB** installed with development headers
  - macOS: `brew install duckdb`
  - Linux: Download from [DuckDB releases](https://github.com/duckdb/duckdb/releases)

## Building

### Standard Build

```bash
git clone <this-repo>
cd duckdb_bit_filter

mkdir build && cd build
cmake ..
cmake --build .
```

This produces:
- `libroaring_extension.duckdb_extension` - The extension library
- `test_roaring` - Unit test executable
- `fingerprint_demo` - Interactive demo

### Build Options

```bash
# Debug build
cmake -DCMAKE_BUILD_TYPE=Debug ..

# Specify DuckDB location
cmake -DDUCKDB_ROOT=/path/to/duckdb ..

# Link against DuckDB for development
cmake -DLINK_DUCKDB=ON ..
```

## Usage

### Method 1: Linked Executable (Recommended)

Due to DuckDB v1.4+ requiring signed extension metadata, the simplest approach is to link your application directly:

```c
#include "duckdb.h"

// Declare the init function
extern void roaring_extension_init(duckdb_connection conn);

int main() {
    duckdb_database db;
    duckdb_connection conn;

    duckdb_open(NULL, &db);
    duckdb_connect(db, &conn);

    // Register extension functions
    roaring_extension_init(conn);

    // Now use the functions in SQL
    duckdb_query(conn, "SELECT roaring_cardinality(roaring_build(i)) FROM range(1,1001) t(i)", ...);

    duckdb_disconnect(&conn);
    duckdb_close(&db);
}
```

Link with: `-lduckdb -L/path/to/build -lroaring_extension`

### Method 2: Dynamic Loading (Requires Extension Signing)

For production dynamic loading, build using [DuckDB's extension template](https://github.com/duckdb/extension-template) which handles signing and metadata:

```sql
-- If using a properly signed extension:
LOAD 'roaring';

SELECT roaring_cardinality(roaring_build(id)) FROM my_table;
```

## Functions

### `roaring_build(column) → BLOB`

Aggregate function that builds a Roaring Bitmap from column values.

```sql
-- Build fingerprint from integer column
SELECT roaring_build(user_id) AS fp FROM users;

-- Build fingerprint from string column (values are hashed)
SELECT roaring_build(email) AS fp FROM users;

-- Store fingerprints in a table
CREATE TABLE fingerprints AS
SELECT 'users' AS tbl, 'email' AS col, roaring_build(email) AS fp
FROM users;
```

**Supported types**: All integer types, VARCHAR, BLOB

### `roaring_cardinality(bitmap) → UBIGINT`

Returns the number of elements in the bitmap.

```sql
SELECT roaring_cardinality(roaring_build(id)) AS distinct_count
FROM orders;
```

### `roaring_intersection_card(a, b) → UBIGINT`

Returns |A ∩ B| - the count of elements in both bitmaps.

```sql
WITH a AS (SELECT roaring_build(i) AS bm FROM range(1,101) t(i)),
     b AS (SELECT roaring_build(i) AS bm FROM range(51,151) t(i))
SELECT roaring_intersection_card(a.bm, b.bm) AS overlap
FROM a, b;
-- Returns: 50
```

### `roaring_containment(probe, reference) → DOUBLE`

Returns |probe ∩ reference| / |probe| - what fraction of probe values exist in reference.

```sql
-- "What fraction of my input values match the stored column?"
SELECT roaring_containment(input_fp, stored_fp) AS match_ratio
FROM ...
```

### `roaring_jaccard(a, b) → DOUBLE`

Returns |A ∩ B| / |A ∪ B| - Jaccard similarity coefficient.

```sql
SELECT roaring_jaccard(fp1, fp2) AS similarity
FROM ...
```

## Example: Column Fingerprint Matching

```sql
-- Step 1: Build fingerprints from known database columns
CREATE TABLE column_fingerprints AS
SELECT
    'customers' AS source_table,
    'customer_id' AS source_column,
    roaring_build(customer_id) AS fingerprint
FROM customers
UNION ALL
SELECT 'products', 'sku', roaring_build(sku) FROM products
UNION ALL
SELECT 'orders', 'order_id', roaring_build(order_id) FROM orders;

-- Step 2: Given unlabeled input data, build its fingerprint
CREATE TABLE input_data AS
SELECT * FROM read_csv('unknown_column.csv');

-- Step 3: Find best matches
WITH input_fp AS (
    SELECT roaring_build(value) AS fp,
           roaring_cardinality(roaring_build(value)) AS card
    FROM input_data
)
SELECT
    f.source_table,
    f.source_column,
    roaring_intersection_card(i.fp, f.fingerprint) AS overlap,
    roaring_containment(i.fp, f.fingerprint) AS containment
FROM column_fingerprints f, input_fp i
WHERE roaring_cardinality(f.fingerprint) BETWEEN i.card * 0.1 AND i.card * 10  -- Cardinality pruning
ORDER BY containment DESC
LIMIT 5;
```

## Python Integration

The serialized bitmaps are compatible with [pyroaring](https://github.com/Ezibenroc/PyRoaringBitMap):

```python
from pyroaring import BitMap
import duckdb

# Build bitmap in Python
bm = BitMap()
for val in my_values:
    bm.add(hash(val) & 0xFFFFFFFF)  # 32-bit hash

# Use in DuckDB
conn = duckdb.connect()
conn.execute("SELECT roaring_containment(?, stored_fp) FROM fingerprints", [bm.serialize()])

# Or retrieve and use in Python
result = conn.execute("SELECT fingerprint FROM fingerprints WHERE ...").fetchone()
stored_bm = BitMap.deserialize(result[0])
print(f"Jaccard: {len(bm & stored_bm) / len(bm | stored_bm)}")
```

## Performance Notes

- **Cardinality pruning**: Filter candidates by cardinality ratio before computing similarity
- **Roaring compression**: Bitmaps automatically compress runs and sparse regions
- **SIMD acceleration**: CRoaring uses AVX2/SSE4 when available
- **No false positives**: Unlike Bloom filters, intersection counts are exact

## Project Structure

```
duckdb_bit_filter/
├── CMakeLists.txt              # Build configuration
├── README.md                   # This file
├── run_demo.sh                 # Demo script
├── src/
│   ├── roaring_extension.c     # Main extension source
│   └── include/
│       └── roaring_extension.h
├── third_party/roaring/        # CRoaring amalgamation
│   ├── roaring.c
│   └── roaring.h
├── test/
│   ├── test_roaring.c          # Unit tests
│   └── sql/roaring_test.sql    # SQL test script
├── demo/
│   └── fingerprint_demo.c      # Interactive demo
└── build/                      # Build output
```

## License

- Extension code: MIT
- CRoaring: Apache 2.0 / MIT dual license

## References

- [Roaring Bitmaps](https://roaringbitmap.org/)
- [CRoaring](https://github.com/RoaringBitmap/CRoaring)
- [DuckDB Extension Development](https://duckdb.org/docs/extensions/overview)
