"""Value tier: sampled-value domain fingerprints for the column_role metacatalog.

column_role captures a source's *schema* as data (columns + pk/fk/index members,
per ``(dataserver, database, sample_time)``). This module adds the *value* tier
the recognizer needs: sample distinct values from the key/dimension columns those
schema rows point at, build a domain **fingerprint** per column, and store it on
the **same provenance grain** into a sibling DuckLake table
``lake.column_fingerprint`` (partitioned on ``(dataserver, database)``, exactly
like ``lake.column_role``). Then, given a headerless **slab** (a bare result set),
probe each slab column against the fingerprint library to recover candidate
source tables for the keys and to flag the measures.

Design, matching the rest of the family:
- **Metadata-first, then data.** Candidate selection is a ``select()`` over the
  catalog (``column_role`` for pk/fk roles, type families for representation
  compatibility). Value probing is the *final* ranker over the survivors only.
- **The fingerprinter is a seam.** ``HashSetFingerprint`` here is a faithful
  Python stand-in for blobfilters (containment / Jaccard over hashed value sets);
  the real ``blobfilters`` roaring-bitmap functions (``rfp_add_hash`` /
  ``rfp_to_base64`` / ``rfp_containment``) drop in behind the same ``Fingerprinter``
  protocol with no change to the storage grain or the probe pipeline.

Reads/writes go through ``ducklake_oob_writer`` just like ``registry.Registry``.
Sampling values needs a live SA connection to the *source* (the same connection
``capture()`` already uses); that door is marked and left to the caller.
"""
from __future__ import annotations

import base64
import hashlib
import os
import struct
from dataclasses import dataclass, field
from typing import Iterable, Protocol, Sequence

import yaml
from sqlalchemy import (BigInteger, Column, DateTime, MetaData, String, Table, and_, func,
                        select, text)

import ducklake_oob_writer as dl
from registry import _CR  # the real lake.column_role Core table — read against it directly

# --------------------------------------------------------------------------- #
# lake.column_fingerprint — value tier hung off the column_role provenance grain
# --------------------------------------------------------------------------- #
_LAKE = MetaData()
_FP = Table(
    "column_fingerprint", _LAKE,
    Column("dataserver", String), Column("database", String),
    Column("sample_time", DateTime), Column("schema_name", String),
    Column("object_name", String), Column("member_name", String),
    Column("grouping_kind", String), Column("data_type", String),
    Column("n_sampled", BigInteger), Column("n_distinct", BigInteger),
    Column("fingerprint", String),        # base64 of the hashed value set (raw)
    Column("fingerprint_norm", String),   # base64 of the normalized (casefold) set
    schema="lake",
)
_FP_DDL = [
    ("dataserver", "varchar"), ("database", "varchar"), ("sample_time", "timestamp"),
    ("schema_name", "varchar"), ("object_name", "varchar"), ("member_name", "varchar"),
    ("grouping_kind", "varchar"), ("data_type", "varchar"),
    ("n_sampled", "int64"), ("n_distinct", "int64"),
    ("fingerprint", "varchar"), ("fingerprint_norm", "varchar"),
]


# --------------------------------------------------------------------------- #
# The fingerprint seam — HashSetFingerprint is the blobfilters stand-in
# --------------------------------------------------------------------------- #
class Fingerprinter(Protocol):
    def build(self, values: Iterable[str]) -> str: ...
    def containment(self, probe_b64: str, ref_b64: str) -> float: ...
    def jaccard(self, a_b64: str, b_b64: str) -> float: ...


def _hash64(s: str) -> int:
    # stand-in for blobfilters' FNV-1a -> uint32; blake2b truncated to 64 bits here
    return struct.unpack("<Q", hashlib.blake2b(s.encode("utf-8"), digest_size=8).digest())[0]


class HashSetFingerprint:
    """A sorted set of 64-bit value hashes, base64-serialized. Containment and
    Jaccard match blobfilters' roaring semantics; swap in ``blobfilters`` for the
    real thing (and its serialized-roaring base64) behind this same interface."""

    def build(self, values: Iterable[str]) -> str:
        hs = sorted({_hash64(v) for v in values})
        return base64.b64encode(b"".join(struct.pack("<Q", h) for h in hs)).decode("ascii")

    @staticmethod
    def _load(b64: str) -> set[int]:
        raw = base64.b64decode(b64)
        return {struct.unpack_from("<Q", raw, i)[0] for i in range(0, len(raw), 8)}

    def containment(self, probe_b64: str, ref_b64: str) -> float:
        probe, ref = self._load(probe_b64), self._load(ref_b64)
        return (len(probe & ref) / len(probe)) if probe else 0.0

    def jaccard(self, a_b64: str, b_b64: str) -> float:
        a, b = self._load(a_b64), self._load(b_b64)
        u = len(a | b)
        return (len(a & b) / u) if u else 0.0


def _normalize(v) -> str:
    return "" if v is None else str(v).strip()


def _norm_fold(v) -> str:
    return _normalize(v).casefold()


# --------------------------------------------------------------------------- #
# type families (representation compatibility) — from src/rule4/type_map.yaml
# --------------------------------------------------------------------------- #
_TYPE_MAP = os.path.join(os.path.dirname(__file__), "..", "src", "rule4", "type_map.yaml")


def _load_families() -> dict[str, str]:
    """physical/SA type name (casefolded) -> family name, from type_map.yaml."""
    with open(_TYPE_MAP) as fh:
        fam = yaml.safe_load(fh).get("type_families", {})
    rev: dict[str, str] = {}
    for family, names in fam.items():
        for n in names:
            rev[str(n).casefold()] = family
    return rev


def type_family(data_type: str, families: dict[str, str] | None = None) -> str | None:
    fams = families if families is not None else _load_families()
    return fams.get(str(data_type).split("(")[0].strip().casefold())


def infer_family(values: Sequence, families: dict[str, str] | None = None) -> str:
    """Cheap representation-type guess for a headerless slab column (no catalog)."""
    vals = [_normalize(v) for v in values if v is not None and str(v).strip() != ""]
    if not vals:
        return "string"
    def all_ok(pred):
        return all(pred(v) for v in vals)
    def is_int(v):
        try: int(v); return True
        except ValueError: return False
    def is_float(v):
        try: float(v); return True
        except ValueError: return False
    if all_ok(is_int):
        return "integer"
    if all_ok(is_float):
        return "numeric"
    return "string"


# --------------------------------------------------------------------------- #
# Stage 1 (metadata-first): which columns are worth fingerprinting / probing
# --------------------------------------------------------------------------- #
@dataclass
class KeyMember:
    dataserver: str
    database: str
    schema_name: str
    object_name: str
    member_name: str
    grouping_kind: str          # 'pk' | 'fk' | 'uk'
    data_type: str | None
    referenced_object: str | None
    referenced_member: str | None


def key_members(catalog_path: str, data_path: str, dataserver: str, database: str,
                when) -> list[KeyMember]:
    """Metadata-first candidate set: the pk/fk/uk members of (dataserver, database)
    as of ``when`` — read straight off the real ``lake.column_role`` model. These
    are the columns whose *values* are worth fingerprinting (they carry domains).
    FK members additionally name their target domain (referenced_object/member)."""
    cr = _CR
    latest = (select(func.max(cr.c.sample_time)).where(
        and_(cr.c.dataserver == dataserver, cr.c.database == database,
             cr.c.sample_time <= when)).scalar_subquery())
    stmt = (select(cr.c.dataserver, cr.c.database, cr.c.schema_name, cr.c.object_name,
                   cr.c.member_name, cr.c.grouping_kind, cr.c.data_type,
                   cr.c.referenced_object, cr.c.referenced_member)
            .where(and_(cr.c.dataserver == dataserver, cr.c.database == database,
                        cr.c.sample_time == latest,
                        cr.c.grouping_kind.in_(("pk", "fk", "uk"))))
            .order_by(cr.c.object_name, cr.c.grouping_kind, cr.c.member_name))
    with dl.lake_reader(f"sqlite:{catalog_path}", data_path) as conn:
        return [KeyMember(*r) for r in conn.execute(stmt).fetchall()]


def fk_target_popularity(catalog_path: str, data_path: str, dataserver: str, database: str,
                         when) -> list[tuple]:
    """Declared FK-target popularity — the metadata-only 'popular tables' analysis:
    rank tables by how many FK columns point at them (in-degree in the FK graph).
    High in-degree = a hub dimension = where the needles live. Pure catalog, no data.
    (Its data-derived twin is discovered-IND in-degree; see ``defacto_fks``.)"""
    cr = _CR
    latest = (select(func.max(cr.c.sample_time)).where(
        and_(cr.c.dataserver == dataserver, cr.c.database == database,
             cr.c.sample_time <= when)).scalar_subquery())
    stmt = (select(cr.c.referenced_object, func.count().label("in_degree"))
            .where(and_(cr.c.dataserver == dataserver, cr.c.database == database,
                        cr.c.sample_time == latest, cr.c.grouping_kind == "fk",
                        cr.c.referenced_object.isnot(None)))
            .group_by(cr.c.referenced_object)
            .order_by(func.count().desc()))
    with dl.lake_reader(f"sqlite:{catalog_path}", data_path) as conn:
        return conn.execute(stmt).fetchall()


# --------------------------------------------------------------------------- #
# Stage 2: sample distinct values from the live source  (THE ONE LIVE DOOR)
# --------------------------------------------------------------------------- #
def _limit_clause(dialect_name: str, n: int) -> tuple[str, str]:
    """Return (prefix, suffix) row-limit clause for the source dialect."""
    if dialect_name.startswith("mssql"):
        return (f"TOP {int(n)} ", "")
    return ("", f" LIMIT {int(n)}")            # postgres/duckdb/sqlite/mysql


def sample_distinct(src_conn, schema_name: str | None, object_name: str,
                    member_name: str, *, limit: int = 4096) -> list[str]:
    """Pull up to ``limit`` DISTINCT values of one column from the LIVE source over
    a SQLAlchemy connection (the same kind ``registry.capture`` uses). Identifiers
    are quoted with the source dialect's preparer; values are stringified for the
    fingerprint. This is the only function that touches a source database."""
    prep = src_conn.dialect.identifier_preparer
    qcol = prep.quote(member_name)
    qtab = prep.quote(object_name)
    if schema_name:
        qtab = f"{prep.quote(schema_name)}.{qtab}"
    pre, suf = _limit_clause(src_conn.dialect.name, limit)
    sql = f"SELECT DISTINCT {pre}{qcol} AS v FROM {qtab} WHERE {qcol} IS NOT NULL{suf}"
    return [_normalize(r[0]) for r in src_conn.execute(text(sql)).fetchall()]


# --------------------------------------------------------------------------- #
# Build + store fingerprints (mirrors registry.Registry's DuckLake write path)
# --------------------------------------------------------------------------- #
class FingerprintStore:
    """The ``lake.column_fingerprint`` table in the *same* DuckLake as column_role."""

    def __init__(self, catalog_path: str, data_path: str, fp: Fingerprinter | None = None):
        from sqlalchemy import create_engine
        self.catalog_path, self.data_path = catalog_path, data_path
        self.fp = fp or HashSetFingerprint()
        self._eng = create_engine(f"sqlite:///{catalog_path}")
        dl.create_catalog(self._eng)
        w = dl.DuckLakeWriter(self._eng, dl.DUCKLAKE_METADATA)
        w.init_catalog(data_path=data_path)
        w.create_table("main", "column_fingerprint", _FP_DDL)
        w.set_partitioning("column_fingerprint", ["dataserver", "database"])
        os.makedirs(os.path.join(data_path, "main", "column_fingerprint"), exist_ok=True)
        self._w = w

    def build(self, src_conn, members: Sequence[KeyMember], sample_time, *, limit=4096):
        """For each key member: sample the live source, fingerprint, buffer a row."""
        rows = []
        for m in members:
            vals = sample_distinct(src_conn, m.schema_name, m.object_name, m.member_name,
                                   limit=limit)
            rows.append((
                m.dataserver, m.database, sample_time, m.schema_name, m.object_name,
                m.member_name, m.grouping_kind, m.data_type, len(vals), len(set(vals)),
                self.fp.build(vals), self.fp.build(_norm_fold(v) for v in vals),
            ))
        return rows

    def record(self, rows: list[tuple], sample_time):
        if not rows:
            return
        tag = f"{rows[0][0]}__{rows[0][1]}__{sample_time:%Y%m%dT%H%M%S}fp".replace("/", "_")
        pq = os.path.join(self.data_path, "main", "column_fingerprint", f"{tag}.parquet")
        dl.write_rows_parquet(_FP_DDL, rows, pq)
        self._w.register_parquet("column_fingerprint", pq, rel_path=f"{tag}.parquet",
                                 snapshot_time=sample_time)

    def dispose(self):
        self._eng.dispose()


# --------------------------------------------------------------------------- #
# Stage 3: probe a headerless slab against the fingerprint library
# --------------------------------------------------------------------------- #
@dataclass
class Ref:
    """One library entry (a stored column fingerprint)."""
    dataserver: str; database: str; schema_name: str; object_name: str
    member_name: str; grouping_kind: str; data_type: str | None
    n_distinct: int
    fingerprint: str; fingerprint_norm: str


def load_library(catalog_path: str, data_path: str, when=None) -> list[Ref]:
    """Read the fingerprint library (optionally as-of ``when``) from the lake."""
    fp = _FP
    conds = []
    if when is not None:
        conds.append(fp.c.sample_time <= when)
    stmt = select(fp.c.dataserver, fp.c.database, fp.c.schema_name, fp.c.object_name,
                  fp.c.member_name, fp.c.grouping_kind, fp.c.data_type, fp.c.n_distinct,
                  fp.c.fingerprint, fp.c.fingerprint_norm)
    if conds:
        stmt = stmt.where(and_(*conds))
    with dl.lake_reader(f"sqlite:{catalog_path}", data_path) as conn:
        return [Ref(*r) for r in conn.execute(stmt).fetchall()]


@dataclass
class ColumnVerdict:
    index: int
    inferred_family: str
    role: str                                   # 'key' | 'measure' | 'unknown'
    candidates: list[tuple] = field(default_factory=list)   # (Ref, containment) desc


def _orient(slab: Sequence[Sequence], row_major: bool) -> list[list]:
    """Return slab as a list of columns."""
    if not row_major:
        return [list(c) for c in slab]
    return [list(c) for c in zip(*slab)] if slab else []


def probe_slab(slab: Sequence[Sequence], library: Sequence[Ref], *, row_major=True,
               fp: Fingerprinter | None = None, threshold: float = 0.6,
               families: dict[str, str] | None = None) -> list[ColumnVerdict]:
    """Recognize a headerless slab. For each column:
       Stage A (metadata/type prune) — keep only library refs whose data_type family
         is compatible with the column's inferred family (no value work yet).
       Stage B (data probe) — fingerprint the column, containment vs survivors, rank.
       Stage C (classify) — a strong key-domain match => 'key' (=> candidate source
         table); a numeric column that matches nothing => 'measure' (probable
         aggregate); else 'unknown'.
    Aggregation confirmation (running the putative JOIN/WHERE/GROUP BY as a sampling
    query and scoring the match) is the deliberate next stage — left as a hook."""
    fp = fp or HashSetFingerprint()
    fams = families if families is not None else _load_families()
    cols = _orient(slab, row_major)
    verdicts: list[ColumnVerdict] = []
    for i, col in enumerate(cols):
        fam = infer_family(col, fams)
        # Stage A: cheap type-family prune over the library
        survivors = [r for r in library
                     if type_family(r.data_type or "", fams) in (fam, None)]
        # Stage B: value probe over survivors only
        probe = fp.build(_normalize(v) for v in col)
        scored = sorted(((r, fp.containment(probe, r.fingerprint)) for r in survivors),
                        key=lambda rc: rc[1], reverse=True)
        best = scored[0][1] if scored else 0.0
        # Stage C: classify
        if best >= threshold:
            role = "key"
        elif fam in ("integer", "numeric"):
            role = "measure"
        else:
            role = "unknown"
        verdicts.append(ColumnVerdict(i, fam, role,
                                      [(r, c) for r, c in scored if c > 0.0][:5]))
    return verdicts


def candidate_tables(verdicts: Sequence[ColumnVerdict]) -> dict[tuple, list[int]]:
    """Assemble table hypotheses: (dataserver, database, object_name) -> slab column
    indices whose best key match lands on that table. The generate-and-test seed for
    a putative JOIN; a sampling query over these ranks/prunes the survivors."""
    out: dict[tuple, list[int]] = {}
    for v in verdicts:
        if v.role == "key" and v.candidates:
            r = v.candidates[0][0]
            out.setdefault((r.dataserver, r.database, r.object_name), []).append(v.index)
    return out


# --------------------------------------------------------------------------- #
# The inverse: discover de-facto FKs (undeclared inclusion dependencies)
# --------------------------------------------------------------------------- #
# Recognition and FK-discovery are the SAME containment sweep — they differ only
# in what is being probed (an anonymous slab column vs a catalogued column). A
# foreign key IS an inclusion dependency (child values ⊆ parent key domain); a
# *de-facto* FK is that IND discovered from data when the schema never declared it:
#     containment(child, parent) = |child ∩ parent| / |child|  →  ~1.0  ⟹  child ⊆ parent
# Cheap (a bitmap AND, precomputed once) and — the payoff — works ACROSS databases,
# where you cannot JOIN but can compare fingerprints.

@dataclass
class IND:
    child: str            # "object.member" (the putative FK column)
    parent: str           # "object.member" (a PK/UK domain that contains it)
    containment: float
    parent_distinct: int


def contained_by(probe_b64: str, library: Sequence[Ref], fp: Fingerprinter, *,
                 threshold: float = 0.95, min_ref_distinct: int = 8) -> list[tuple]:
    """PK/UK domains that (nearly) contain the probe column, ranked. The cardinality
    guard drops tiny domains (booleans, status codes) that trivially contain
    everything — the classic spurious-IND trap."""
    out = []
    for r in library:
        if r.grouping_kind not in ("pk", "uk") or (r.n_distinct or 0) < min_ref_distinct:
            continue
        c = fp.containment(probe_b64, r.fingerprint)
        if c >= threshold:
            out.append((r, c))
    return sorted(out, key=lambda rc: rc[1], reverse=True)


def defacto_fks(putatives: Sequence[tuple], pk_library: Sequence[Ref], fp: Fingerprinter, *,
                threshold: float = 0.95, min_ref_distinct: int = 8) -> list[IND]:
    """Sweep putative columns ``(label, fingerprint)`` against PK/UK domains and emit the
    discovered inclusion dependencies — candidate undeclared FKs. Threshold < 1.0 by
    default because sampled fingerprints are partial; treat these as ranked candidates
    to confirm with a cheap spot-check ``SELECT`` (generate-and-test). Composite
    (multi-column) INDs need tuple-fingerprints — a later extension."""
    inds: list[IND] = []
    for label, pfp in putatives:
        for r, c in contained_by(pfp, pk_library, fp, threshold=threshold,
                                 min_ref_distinct=min_ref_distinct):
            parent = f"{r.object_name}.{r.member_name}"
            if parent == label:
                continue                       # a domain trivially contains itself
            inds.append(IND(label, parent, c, r.n_distinct or 0))
    return sorted(inds, key=lambda x: x.containment, reverse=True)


# --------------------------------------------------------------------------- #
# Untagged union types: cover a column with a SET of domains (no discriminator)
# --------------------------------------------------------------------------- #
# The antipattern: a column whose values come from *one of* several domains with
# the discriminator dropped (A ∪ B ∪ C). No single domain has high containment, so
# winner-take-all recognition fails; the answer is a minimal set-COVER over the
# library. de-facto FK = single-domain containment; de-facto union = multi-domain
# cover — same fingerprint algebra, set-valued answer.

@dataclass
class Cover:
    domains: list[tuple]   # (Ref, marginal_coverage), in greedy pick order
    coverage: float        # fraction of probe values covered by the union
    residual: float        # 1 - coverage: values in NO known domain (novel/dirty/measure)

    @property
    def is_union(self) -> bool:
        return len(self.domains) >= 2 and self.coverage >= 0.9


def domain_cover(probe_b64: str, library: Sequence[Ref], fp: Fingerprinter, *,
                 target: float = 0.98, max_domains: int = 4,
                 min_ref_distinct: int = 8) -> Cover:
    """Greedy set-cover of a probe column over the domain library — detect an untagged
    union type. Returns the covering domains (marginal-gain order), total union
    coverage, and the residual (uncovered → novel/dirty/measure). ``is_union`` flags a
    genuine ≥2-domain cover. Roaring's native OR/ANDNOT replace these python set ops
    for the real blobfilters; the discriminator is recovered by attributing each value
    to the domain(s) that contain it (disjoint → clean tag, overlap → ambiguous)."""
    probe = HashSetFingerprint._load(probe_b64)
    if not probe:
        return Cover([], 0.0, 1.0)
    total = len(probe)
    remaining = set(probe)
    cand = [r for r in library if (r.n_distinct or 0) >= min_ref_distinct]
    picks: list[tuple] = []
    while remaining and len(picks) < max_domains:
        best, gain = None, 0
        for r in cand:
            g = len(remaining & HashSetFingerprint._load(r.fingerprint))
            if g > gain:
                best, gain = r, g
        if best is None:
            break
        picks.append((best, gain / total))
        remaining -= HashSetFingerprint._load(best.fingerprint)
        cand.remove(best)
        if 1 - len(remaining) / total >= target:
            break
    return Cover(picks, 1 - len(remaining) / total, len(remaining) / total)


# --------------------------------------------------------------------------- #
# Substrate-agnostic corpus: spreadsheets in, redundancy/unification out
# --------------------------------------------------------------------------- #
# The fingerprint doesn't care about the substrate. A spreadsheet range is just
# another column: blobboxes lands XLSX cells relationally, we group them into
# columns and fingerprint onto the SAME grain (provenance = workbook/sheet/range).
# Then recognition / IND / union-cover all work unchanged, and clustering the whole
# corpus by mutual Jaccard is unification + redundancy reduction (MDM): each cluster
# is one domain with N materializations; the canonical is the hub, the rest become
# references. Jaccard < 1.0 to the canonical = "same domain modulo dirt".

def fingerprints_from_columns(columns: dict, fp: Fingerprinter, *, workbook: str,
                              sheet: str) -> list[Ref]:
    """Adapter: extracted spreadsheet columns ``{name: [values]}`` (e.g. blobboxes XLSX
    cell extraction, grouped by column) → Ref fingerprints on the shared grain.
    Spreadsheets carry no declared keys (grouping_kind='cell') and are dirtier, so the
    normalized fingerprint does more of the work."""
    out = []
    for name, vals in columns.items():
        v = [_normalize(x) for x in vals]
        out.append(Ref("workbook", workbook, sheet, sheet, name, "cell", None,
                       len(set(v)), fp.build(v), fp.build(_norm_fold(x) for x in v)))
    return out


@dataclass
class DomainCluster:
    canonical: Ref                 # the largest member — the master/hub candidate
    members: list[tuple]           # (Ref, jaccard_to_canonical), canonical first


def cluster_domains(library: Sequence[Ref], fp: Fingerprinter, *,
                    threshold: float = 0.8) -> list[DomainCluster]:
    """Corpus-wide unification: cluster columns (DB + spreadsheet) by mutual Jaccard;
    each cluster is one domain with N redundant materializations. Canonical = the
    member with the largest distinct set (proxy for the master/hub). Clusters with >1
    member are the redundancy findings; near-miss Jaccard is drift/dirt."""
    refs = list(library)
    order = sorted(range(len(refs)), key=lambda i: refs[i].n_distinct or 0, reverse=True)
    used: set[int] = set()
    clusters: list[DomainCluster] = []
    for i in order:
        if i in used:
            continue
        seed = refs[i]
        used.add(i)
        members = [(seed, 1.0)]
        for j in order:
            if j in used:
                continue
            jac = fp.jaccard(seed.fingerprint, refs[j].fingerprint)
            if jac >= threshold:
                members.append((refs[j], jac))
                used.add(j)
        clusters.append(DomainCluster(seed, members))
    return clusters


# --------------------------------------------------------------------------- #
# pure-logic smoke test (no lake, no live source): `uv run python value_fingerprint.py`
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    fp = HashSetFingerprint()
    fams = _load_families()

    # a pretend library: two dimension key domains + one that won't match
    lib = [
        Ref("srv", "shop", "dbo", "dim_region", "region_id", "pk", "int", 50,
            fp.build(str(i) for i in range(1, 51)), fp.build(str(i) for i in range(1, 51))),
        Ref("srv", "shop", "dbo", "dim_product", "product_id", "pk", "int", 100,
            fp.build(str(i) for i in range(1000, 1100)),
            fp.build(str(i) for i in range(1000, 1100))),
        Ref("srv", "shop", "dbo", "dim_currency", "code", "pk", "nvarchar", 4,
            fp.build(["USD", "GBP", "EUR", "JPY"]), fp.build(["usd", "gbp", "eur", "jpy"])),
    ]

    # a headerless slab = a GROUP BY over region x product with a SUM measure
    slab = [
        [5, 1002, 84213.5],
        [7, 1050, 1200.0],
        [5, 1099, 55.25],
        [42, 1002, 9000.0],   # 42 is a region id in-domain (<=50)
    ]
    verdicts = probe_slab(slab, lib, row_major=True, threshold=0.5, families=fams)
    for v in verdicts:
        top = v.candidates[0] if v.candidates else None
        tag = f"{top[0].object_name}.{top[0].member_name} c={top[1]:.2f}" if top else "-"
        print(f"col {v.index}: family={v.inferred_family:<8} role={v.role:<8} best={tag}")
    print("candidate source tables for keys:", dict(candidate_tables(verdicts)))

    # the inverse: an undeclared FK column whose values ⊆ dim_region.region_id.
    # dim_currency (4 distinct) is guarded out as too small to be a meaningful domain.
    putative = [("orders.ship_region", fp.build(str(i) for i in [3, 7, 12, 44, 7, 3])),
                ("orders.flag", fp.build(["0", "1", "0", "1"]))]
    print("de-facto FKs:", [(d.child, "->", d.parent, round(d.containment, 2))
                            for d in defacto_fks(putative, lib, fp, threshold=0.9)])

    # untagged union: a column mixing region ids and currency codes — covered by the
    # UNION of two domains, no single one (min_ref_distinct=1 to admit the tiny ccy set)
    cov = domain_cover(fp.build(["3", "7", "USD", "EUR"]), lib, fp,
                       min_ref_distinct=1, target=0.98)
    print(f"union cover: is_union={cov.is_union} coverage={cov.coverage:.2f} "
          f"residual={cov.residual:.2f} via="
          + str([f"{r.object_name}.{r.member_name}" for r, _ in cov.domains]))

    # substrate-agnostic: a dirty spreadsheet copy of the region domain unifies to the
    # DB canonical — redundancy reduction across DB + workbook (blobboxes would supply
    # the cells; here we hand them in directly)
    sheet = fingerprints_from_columns(
        {"A": [str(i) for i in range(1, 49)] + ["999"]}, fp,
        workbook="regions.xlsx", sheet="Sheet1")
    for cl in cluster_domains(lib + sheet, fp, threshold=0.8):
        if len(cl.members) > 1:
            print("redundant domain: canonical "
                  f"{cl.canonical.object_name}.{cl.canonical.member_name} <= "
                  + str([(f"{m.object_name}.{m.member_name}", round(j, 2))
                         for m, j in cl.members]))
