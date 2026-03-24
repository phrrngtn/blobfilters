"""Smoke test: load the blobfilters DuckDB extension and verify functions exist."""
import duckdb
from blobfilters_duckdb import extension_path

EXPECTED_FUNCTIONS = [
    "bf_cardinality",
    "bf_to_base64",
    "bf_from_base64",
    "bf_jaccard",
    "bf_containment",
    "bf_intersection_card",
]

conn = duckdb.connect()
conn.execute("SET allow_unsigned_extensions = true")
conn.execute(f"LOAD '{extension_path()}'")

registered = set(
    row[0]
    for row in conn.execute(
        "SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_name LIKE 'bf_%'"
    ).fetchall()
)

missing = [f for f in EXPECTED_FUNCTIONS if f not in registered]
if missing:
    raise AssertionError(f"Missing functions: {missing}")

print(f"OK: {len(registered)} bf_* functions registered, all {len(EXPECTED_FUNCTIONS)} expected functions present")
