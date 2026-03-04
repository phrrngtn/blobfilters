#!/usr/bin/env python3
"""Append DuckDB extension metadata footer to a shared library.

DuckDB requires a 512-byte footer on extension files:
  - 256 bytes: 8 x 32-byte metadata fields (stored in reverse order)
  - 256 bytes: signature (all zeros for unsigned)

Usage:
    python3 scripts/append_duckdb_metadata.py build/roaring.duckdb_extension [platform] [duckdb_version]
"""
import struct
import subprocess
import sys
import os

FOOTER_SIZE = 512
FIELD_SIZE = 32
SIGNATURE_SIZE = 256

def get_duckdb_version():
    try:
        out = subprocess.check_output(["duckdb", "--version"], text=True).strip()
        # e.g. "v1.4.3 (Andium) d1dc88f950" -> "v1.4.3"
        return out.split()[0]
    except Exception:
        return "v0.0.0"

def get_platform():
    import platform
    machine = platform.machine()
    system = platform.system()
    if system == "Darwin":
        os_name = "osx"
    elif system == "Linux":
        os_name = "linux"
    elif system == "Windows":
        os_name = "windows"
    else:
        os_name = system.lower()
    if machine == "arm64" or machine == "aarch64":
        arch = "arm64"
    elif machine == "x86_64" or machine == "AMD64":
        arch = "amd64"
    else:
        arch = machine
    return f"{os_name}_{arch}"

def pad_field(s: str) -> bytes:
    b = s.encode("utf-8")[:FIELD_SIZE]
    return b.ljust(FIELD_SIZE, b"\x00")

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <extension_file> [platform] [duckdb_version]")
        sys.exit(1)

    ext_file = sys.argv[1]
    plat = sys.argv[2] if len(sys.argv) > 2 else get_platform()
    duckdb_ver = sys.argv[3] if len(sys.argv) > 3 else get_duckdb_version()

    # Check if footer already present
    with open(ext_file, "rb") as f:
        f.seek(0, 2)
        size = f.tell()
        if size > FOOTER_SIZE:
            f.seek(size - FOOTER_SIZE)
            existing = f.read(FOOTER_SIZE)
            # Check for magic value "4" at the end of the metadata section
            # Metadata is stored reversed, so field 1 (magic "4") is at offset 224
            if existing[224:225] == b"4":
                print(f"Footer already present in {ext_file}, skipping")
                return

    # Build metadata fields (will be stored in reverse order)
    fields = [
        pad_field("4"),           # 1: magic value
        pad_field(plat),          # 2: platform
        pad_field(duckdb_ver),    # 3: duckdb version
        pad_field("v0.1.0"),      # 4: extension version
        pad_field("CPP"),         # 5: ABI type
        pad_field(""),            # 6: unused
        pad_field(""),            # 7: unused
        pad_field(""),            # 8: unused
    ]

    # Reverse and concatenate
    metadata = b"".join(reversed(fields))
    assert len(metadata) == FIELD_SIZE * 8  # 256 bytes

    # Signature: all zeros (unsigned)
    signature = b"\x00" * SIGNATURE_SIZE

    footer = metadata + signature
    assert len(footer) == FOOTER_SIZE

    with open(ext_file, "ab") as f:
        f.write(footer)

    print(f"Appended DuckDB metadata footer to {ext_file}")
    print(f"  Platform: {plat}")
    print(f"  DuckDB version: {duckdb_ver}")
    print(f"  ABI: CPP")

if __name__ == "__main__":
    main()
