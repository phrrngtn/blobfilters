#!/bin/bash
#
# Roaring Bitmap Extension Demo Script
#
# Demonstrates column fingerprinting and matching using Roaring Bitmaps
#

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"

echo "======================================================="
echo "  Roaring Bitmap Extension - Demo Script"
echo "======================================================="
echo ""

# Build if needed
if [ ! -f "$BUILD_DIR/fingerprint_demo" ]; then
    echo "Building extension and demo..."
    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR"
    cmake ..
    cmake --build .
    cd "$SCRIPT_DIR"
    echo ""
fi

# Run the fingerprint demo
"$BUILD_DIR/fingerprint_demo"

echo ""
echo "======================================================="
echo "  Additional Examples"
echo "======================================================="
echo ""
echo "Run the unit tests:"
echo "  $BUILD_DIR/test_roaring"
echo ""
echo "To use in your own C code:"
echo "  #include \"duckdb.h\""
echo "  extern void roaring_extension_init(duckdb_connection conn);"
echo ""
echo "  // After connecting to DuckDB:"
echo "  roaring_extension_init(conn);"
echo ""
echo "  // Then use SQL:"
echo "  // SELECT roaring_cardinality(roaring_build(column)) FROM table;"
echo ""
