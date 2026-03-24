"""Smoke test: load the blobfilters SQLite extension and verify functions exist."""
import sqlite3
from blobfilters_sqlite import extension_path

EXPECTED_FUNCTIONS = [
    "bf_cardinality",
    "bf_to_base64",
    "bf_from_base64",
    "bf_jaccard",
    "bf_containment",
    "bf_intersection_card",
]

conn = sqlite3.connect(":memory:")
conn.enable_load_extension(True)
conn.load_extension(extension_path())

for func in EXPECTED_FUNCTIONS:
    try:
        conn.execute(f"SELECT {func}(x'00')")
    except sqlite3.OperationalError as e:
        if "no such function" in str(e).lower():
            raise AssertionError(f"Function {func} not registered") from e

print(f"OK: all {len(EXPECTED_FUNCTIONS)} expected functions present")
