#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Building core library + SQLite extension ==="
cmake -B build -DBUILD_SQLITE_EXTENSION=ON -DBUILD_TESTS=ON
cmake --build build -j "$(nproc 2>/dev/null || sysctl -n hw.ncpu)"

echo ""
echo "=== Running core library tests ==="
./build/test_roaring

echo ""
echo "=== Building DuckDB extension ==="
if [ -d extension-ci-tools ] && [ -d duckdb ]; then
    make release
else
    echo "SKIP: duckdb/ and extension-ci-tools/ submodules not initialized."
    echo "  Run: git submodule update --init --recursive"
fi

echo ""
echo "=== Building Python wheel ==="
if command -v pip >/dev/null 2>&1; then
    pip install -e .
else
    echo "SKIP: pip not found"
fi

echo ""
echo "=== Done ==="
