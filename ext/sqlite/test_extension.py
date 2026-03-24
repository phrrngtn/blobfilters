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

registered = set(
    row[0]
    for row in conn.execute("SELECT name FROM pragma_function_list").fetchall()
)

missing = [f for f in EXPECTED_FUNCTIONS if f not in registered]
if missing:
    raise AssertionError(f"Missing functions: {missing}")

print(f"OK: all {len(EXPECTED_FUNCTIONS)} expected functions present in pragma_function_list")
