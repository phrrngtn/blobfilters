"""Packaging wrapper for the blobfilters DuckDB extension."""

import pathlib

_HERE = pathlib.Path(__file__).parent


def extension_path() -> str:
    """Return the absolute path to the blobfilters DuckDB extension.

    Usage:
        LOAD '<path>';  -- in DuckDB with allow_unsigned_extensions
    """
    ext = _HERE / "blobfilters.duckdb_extension"
    if not ext.exists():
        raise FileNotFoundError(f"Extension not found at {ext}")
    return str(ext)
