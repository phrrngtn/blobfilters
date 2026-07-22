#!/usr/bin/env bash
# Fetch the public Chinook sample DB and extract its catalog (the rule4 role).
# Clean-IP: public sample database, no credentials. Run from this directory.
set -euo pipefail

curl -sL --max-time 60 \
  "https://github.com/lerocha/chinook-database/raw/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite" \
  -o chinook.sqlite

# Determinant/dependent partition: PK flag per column.
sqlite3 -header -csv chinook.sqlite "
SELECT m.name AS tbl, ti.name AS col, ti.pk AS pk_ord, ti.type AS decl_type, ti.cid AS ord
FROM sqlite_master m JOIN pragma_table_info(m.name) ti
WHERE m.type='table' ORDER BY m.name, ti.cid;" > catalog_columns.csv

# Declared foreign keys: which column references which target key.
sqlite3 -header -csv chinook.sqlite "
SELECT m.name AS tbl, fk.\"from\" AS from_col, fk.\"table\" AS target_tbl, fk.\"to\" AS target_col
FROM sqlite_master m JOIN pragma_foreign_key_list(m.name) fk
WHERE m.type='table' ORDER BY m.name;" > catalog_fks.csv

echo "fetched: chinook.sqlite catalog_columns.csv catalog_fks.csv"
