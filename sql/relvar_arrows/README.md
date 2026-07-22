# sql/relvar_arrows — typing database relvars into `{D}→{C}` arrows

The **opposite-of-lift** corner of
[*Domain-Typed Morphisms*](../../docs/Domain-Typed%20Morphisms%20—%20the%20D-to-C%20Arrow%20over%20Spreadsheets%20and%20Databases.md):
a relvar is the *extensional* graph of a function, so — unlike a spreadsheet DAG —
its arrow needs no imputation. The **catalog** (rule4 role) declares the
determinant/dependent partition and the FK targets; the **blob values** validate each
FK by containment and type the remaining columns.

```bash
./fetch_chinook.sh                 # public Chinook sample DB + its catalog (PK/FK)
duckdb < chinook_arrows.sql
```

## What it does

- `fetch_chinook.sh` pulls the public Chinook SQLite DB and extracts two catalog CSVs
  via `PRAGMA table_info` (PK ⇒ determinant `{D}`) and `PRAGMA foreign_key_list`
  (FK ⇒ which target key-domain a column references).
- `chinook_arrows.sql` (DuckDB, reading the SQLite values directly): validates every
  declared FK as `containment(fk values, target key-domain)`, types each column
  (`domain:X` / `measure` / `temporal` / `text`), and emits one `{D}→{C}` arrow per
  relvar plus a composition chain.

## Result

Every declared FK validates at **containment 1.0** (blob confirms the catalog), giving
arrows such as:

```
Artist       { ArtistId }      → { Name:text }
Album        { AlbumId }       → { Title:text, ArtistId:domain:Artist }
Track        { TrackId }       → { Name:text, AlbumId:domain:Album, MediaTypeId:domain:MediaType,
                                   GenreId:domain:Genre, Composer:text,
                                   Milliseconds:measure, Bytes:measure, UnitPrice:measure }
InvoiceLine  { InvoiceLineId } → { InvoiceId:domain:Invoice, TrackId:domain:Track,
                                   UnitPrice:measure, Quantity:measure }
```

and a composed, typed dataflow (each hop's co-domain `domain:X` matches relvar `X`'s
determinant, all at confidence 1.0):

```
InvoiceLine —TrackId→ Track —AlbumId→ Album —ArtistId→ Artist
```

The containment check is exact-set SQL here (the ground truth the `bf_containment`
sketch approximates); a live database would source each column's domain sketch from its
stats histogram (`bf_build_histogram`) instead of scanning — cheap metadata.
