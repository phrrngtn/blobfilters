"""Packaging wrapper for the blobfilters SQLite extension."""

import pathlib

_HERE = pathlib.Path(__file__).parent


def extension_path() -> str:
    """Return the absolute path to the blobfilters SQLite extension (without suffix).

    SQLite's .load command does not want the file extension:
        .load <path>
    """
    base = _HERE / "blobfilters"
    for suffix in (".so", ".dylib", ".dll"):
        if (base.parent / f"blobfilters{suffix}").exists():
            return str(base)
    raise FileNotFoundError(f"Extension not found at {base}.*")
