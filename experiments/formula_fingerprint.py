"""Intension tier: fingerprint computations (spreadsheet formulas / DAGs).

The extension tier (``value_fingerprint.py``) fingerprints *data domains*; this
fingerprints *computations*, with the same containment/Jaccard primitive. A cell +
its antecedent cone is lifted to a canonical, position-free term (inline the cone →
abstract leaves to De-Bruijn holes = LET/LAMBDA in A-normal form), then WL subtree-
hashed into a bitmap. Structural clones then collide regardless of where they live.

Two feedback loops (both requested, both what make it tractable):
- **Shape-guided cutting** — a ``PopularityIndex`` of skeleton hashes (self-revealing
  idioms) prunes which sub-DAGs are worth fingerprinting: cut where a known-popular
  shape sits, don't enumerate the exponential frontier. Biased toward the known
  vocabulary, so pair it with an unguided support-mining pass to grow the vocabulary.
- **Data-typed holes** — infer the domains of a cone's leaf/root *values* (extension
  tier) and carry them as a signature; match prunes to domain-compatible idioms
  BEFORE structural comparison. Recognize the structure, type the holes with the data.

Self-contained: a tiny formula parser + DAG so it runs with no deps (pycel /
xlcalculator / formulas are the real parser). The fingerprint helpers mirror
``value_fingerprint`` (could be shared later).
"""
from __future__ import annotations

import base64
import hashlib
import re
import struct
from collections import Counter
from dataclasses import dataclass, field

# --------------------------------------------------------------------------- #
# AST (frozen + tuple args so nodes are hashable / structurally comparable)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Lit:
    value: str

@dataclass(frozen=True)
class Cell:
    ref: str            # "A1"

@dataclass(frozen=True)
class Rng:
    start: str; end: str  # "A1:A10"

@dataclass(frozen=True)
class Call:
    op: str             # 'SUM', 'SORT', or '+','-','*','/'
    args: tuple

@dataclass(frozen=True)
class Hole:
    tag: str            # 'h0','h1',... (indexed) or '?' (flat)


# --------------------------------------------------------------------------- #
# Tiny formula parser (a subset: nums, strings, cells, ranges, calls, + - * /)
# --------------------------------------------------------------------------- #
_TOK = re.compile(r"""\s*(?:
    (?P<num>\d+(?:\.\d+)?) |
    (?P<str>"[^"]*") |
    (?P<cell>[A-Za-z]+\d+) |
    (?P<ident>[A-Za-z_][A-Za-z0-9_]*) |
    (?P<op>[+\-*/(),:]) )""", re.X)


def _tokens(s: str) -> list[tuple]:
    out, i = [], 0
    while i < len(s):
        m = _TOK.match(s, i)
        if not m or m.end() == i:
            if s[i:].strip() == "":
                break
            raise SyntaxError(f"bad token at {s[i:]!r}")
        i = m.end()
        for k in ("num", "str", "cell", "ident", "op"):
            if m.group(k) is not None:
                out.append((k, m.group(k)))
                break
    return out


class _Parser:
    def __init__(self, toks): self.t, self.i = toks, 0
    def peek(self): return self.t[self.i] if self.i < len(self.t) else (None, None)
    def take(self): tok = self.t[self.i]; self.i += 1; return tok

    def parse(self):
        node = self.expr()
        if self.i != len(self.t):
            raise SyntaxError("trailing tokens")
        return node

    def expr(self):
        node = self.term()
        while self.peek()[1] in ("+", "-"):
            op = self.take()[1]
            node = Call(op, (node, self.term()))
        return node

    def term(self):
        node = self.factor()
        while self.peek()[1] in ("*", "/"):
            op = self.take()[1]
            node = Call(op, (node, self.factor()))
        return node

    def factor(self):
        kind, val = self.take()
        if kind == "num" or kind == "str":
            return Lit(val)
        if kind == "op" and val == "(":
            node = self.expr(); self.take()  # ')'
            return node
        if kind == "cell":
            if self.peek()[1] == ":":
                self.take(); _, end = self.take()
                return Rng(val, end)
            return Cell(val)
        if kind == "ident":
            self.take()  # '('
            args = []
            if self.peek()[1] != ")":
                args.append(self.expr())
                while self.peek()[1] == ",":
                    self.take(); args.append(self.expr())
            self.take()  # ')'
            return Call(val.upper(), tuple(args))
        raise SyntaxError(f"unexpected {kind}:{val}")


def parse(formula: str):
    return _Parser(_tokens(formula)).parse()


# --------------------------------------------------------------------------- #
# Sheet + cone inlining + canonicalization (the LET/LAMBDA lift, for fingerprints)
# --------------------------------------------------------------------------- #
@dataclass
class Sheet:
    formulas: dict          # cell_ref -> AST  (computed cells)
    values: dict = field(default_factory=dict)   # cell_ref -> sample value (inputs/outputs)


def inline(sheet: Sheet, target: str):
    """Inline the target's antecedent cone into one AST; input (non-formula) refs
    remain as Cell/Rng leaves. (Assumes a DAG.)"""
    def subst(node):
        if isinstance(node, Cell):
            return subst(sheet.formulas[node.ref]) if node.ref in sheet.formulas else node
        if isinstance(node, Call):
            return Call(node.op, tuple(subst(a) for a in node.args))
        return node
    return subst(sheet.formulas[target])


def canonicalize(node, *, hole_mode: str = "indexed", abstract_lits: bool = False):
    """Position-free normal form. ``hole_mode``: 'concrete' keeps leaves; 'indexed'
    abstracts leaves to De-Bruijn holes h0,h1,… by first-use (coordinate-independent,
    for clone detection); 'flat' abstracts every leaf to '?' (skeleton, for idioms)."""
    order: dict = {}

    def leaf_key(n):
        if isinstance(n, Cell): return ("cell", n.ref)
        if isinstance(n, Rng): return ("rng", n.start, n.end)
        return None

    def go(n):
        if isinstance(n, Call):
            return Call(n.op, tuple(go(a) for a in n.args))
        if isinstance(n, Lit):
            if not abstract_lits:
                return n
            return Hole("?") if hole_mode == "flat" else Hole("lit")
        k = leaf_key(n)
        if k is None:
            return n
        if hole_mode == "concrete":
            return n
        if hole_mode == "flat":
            return Hole("?")
        order.setdefault(k, len(order))          # indexed / De-Bruijn by first use
        return Hole(f"h{order[k]}")
    return go(node)


def render(n) -> str:
    if isinstance(n, Call):
        if n.op in "+-*/":
            return "(" + f" {n.op} ".join(render(a) for a in n.args) + ")"
        return f"{n.op}(" + ", ".join(render(a) for a in n.args) + ")"
    if isinstance(n, Hole): return n.tag
    if isinstance(n, Lit): return n.value
    if isinstance(n, Cell): return n.ref
    if isinstance(n, Rng): return f"{n.start}:{n.end}"
    return "?"


# --------------------------------------------------------------------------- #
# WL subtree hashing + fingerprint (mirrors value_fingerprint's roaring stand-in)
# --------------------------------------------------------------------------- #
def _h64(s: str) -> int:
    return struct.unpack("<Q", hashlib.blake2b(s.encode(), digest_size=8).digest())[0]


def _node_label(n) -> str:
    if isinstance(n, Hole): return n.tag if n.tag != "lit" else "?lit"
    if isinstance(n, Lit): return "lit:" + n.value
    if isinstance(n, Cell): return "cell:" + n.ref
    if isinstance(n, Rng): return f"rng:{n.start}:{n.end}"
    return "?"


def wl_hashes(node) -> list[int]:
    """Multiset of Weisfeiler–Lehman subtree hashes (position-canonical)."""
    hs: list[int] = []
    def visit(n) -> int:
        if isinstance(n, Call):
            ch = [visit(a) for a in n.args]
            h = _h64(n.op + "(" + ",".join(map(str, ch)) + ")")
        else:
            h = _h64(_node_label(n))
        hs.append(h)
        return h
    visit(node)
    return hs


def root_hash(node) -> int:
    """The single hash of a whole (sub)tree — its skeleton identity."""
    if isinstance(node, Call):
        return _h64(node.op + "(" + ",".join(str(root_hash(a)) for a in node.args) + ")")
    return _h64(_node_label(node))


def fingerprint(canon) -> str:
    hs = sorted(set(wl_hashes(canon)))
    return base64.b64encode(b"".join(struct.pack("<Q", h) for h in hs)).decode("ascii")


def _load(b64: str) -> set:
    raw = base64.b64decode(b64)
    return {struct.unpack_from("<Q", raw, i)[0] for i in range(0, len(raw), 8)}


def jaccard(a: str, b: str) -> float:
    x, y = _load(a), _load(b)
    u = len(x | y)
    return len(x & y) / u if u else 0.0


def fp_cell(sheet: Sheet, cell: str, *, hole_mode="indexed", abstract_lits=False) -> str:
    return fingerprint(canonicalize(inline(sheet, cell),
                                    hole_mode=hole_mode, abstract_lits=abstract_lits))


# --------------------------------------------------------------------------- #
# Coarse shape signature: depth + arity as a cheap blocking key (front of cascade)
# --------------------------------------------------------------------------- #
def shape_signature(canon) -> dict:
    """Cheap coarse features — a Deckard-style characteristic vector, computed BEFORE
    the WL fingerprint. depth = AST height; arity = # distinct De-Bruijn holes (the
    function's true input width = its domain arity); nodes = size; ops = operator
    histogram. Alpha-invariant, but NOT invariant under AC-flattening / constant-fold
    — so it is a recall-favouring BLOCKING key (buckets candidates), never a
    classifier: a function and its refactored equivalent can land in different
    buckets, and many distinct functions share (depth, arity)."""
    ops: Counter = Counter()
    holes: set = set()
    nodes = 0

    def go(n) -> int:
        nonlocal nodes
        nodes += 1
        if isinstance(n, Call):
            ops[n.op] += 1
            return 1 + max((go(a) for a in n.args), default=1)
        if isinstance(n, Hole):
            holes.add(n.tag)
        return 1

    depth = go(canon)
    return {"depth": depth, "arity": len(holes), "nodes": nodes, "ops": dict(ops)}


def block_key(node) -> tuple:
    """(depth, arity) on the indexed canonical form — the coarse category / bucket."""
    sig = shape_signature(canonicalize(node, hole_mode="indexed", abstract_lits=True))
    return (sig["depth"], sig["arity"])


def bucket(named_nodes: dict) -> dict:
    """Partition functions by (depth, arity); only within-bucket pairs need the
    expensive WL/Jaccard compare — O(n^2) → O(sum bucket^2)."""
    out: dict = {}
    for name, node in named_nodes.items():
        out.setdefault(block_key(node), []).append(name)
    return out


# --------------------------------------------------------------------------- #
# Clone / fill-down detection: cells sharing an identical canonical fingerprint
# --------------------------------------------------------------------------- #
def clone_clusters(sheet: Sheet, cells, *, hole_mode="indexed", abstract_lits=False) -> dict:
    """Group cells by identical canonical fingerprint. The largest group over a
    contiguous run is a fill; singletons breaking the run are anomalies (a divergent
    formula in a filled range — the classic bug)."""
    groups: dict = {}
    for c in cells:
        groups.setdefault(fp_cell(sheet, c, hole_mode=hole_mode, abstract_lits=abstract_lits),
                           []).append(c)
    return groups


# --------------------------------------------------------------------------- #
# Feedback 1: shape-guided cutting via a self-revealing popularity index
# --------------------------------------------------------------------------- #
def subcalls(node):
    """Internal (Call) subtrees only — the meaningful cut candidates."""
    if isinstance(node, Call):
        yield node
        for a in node.args:
            yield from subcalls(a)


class PopularityIndex:
    """Skeleton-hash frequencies over a corpus (self-revealing idioms). Flat mode so
    idioms collapse across differing leaves."""
    def __init__(self, *, hole_mode="flat", abstract_lits=True):
        self.counts: Counter = Counter()
        self.opts = dict(hole_mode=hole_mode, abstract_lits=abstract_lits)

    def add(self, node):
        canon = canonicalize(node, **self.opts)
        for st in subcalls(canon):
            self.counts[root_hash(st)] += 1

    def popular(self, support: int) -> set:
        return {h for h, c in self.counts.items() if c >= support}


def guided_cuts(node, popular: set, *, hole_mode="flat", abstract_lits=True) -> list:
    """Only the sub-DAGs whose skeleton is already popular — 'don't build graphs that
    won't match'. Returns (rendered_skeleton, subtree) for each popular cut. Pair with
    an unguided support pass to keep discovering NEW idioms (this path is, by design,
    biased toward the known vocabulary)."""
    canon = canonicalize(node, hole_mode=hole_mode, abstract_lits=abstract_lits)
    return [(render(st), st) for st in subcalls(canon) if root_hash(st) in popular]


# --------------------------------------------------------------------------- #
# Feedback 2: data-typed holes — prune idiom matches by inferred domain signature
# --------------------------------------------------------------------------- #
def infer_domain(values) -> str:
    """Coarse domain/family of a value column (mirrors value_fingerprint.infer_family)."""
    vals = [str(v).strip() for v in values if str(v).strip() != ""]
    if not vals:
        return "empty"
    def all_ok(p):
        return all(p(v) for v in vals)
    def is_int(v):
        try: int(v); return True
        except ValueError: return False
    def is_float(v):
        try: float(v); return True
        except ValueError: return False
    if all_ok(is_int): return "integer"
    if all_ok(is_float): return "numeric"
    return "string"


@dataclass
class TypedFingerprint:
    label: str
    fp: str                 # structural fingerprint (idiom skeleton)
    in_domains: tuple       # inferred domains of the input leaves
    out_domain: str         # inferred domain of the result


def match(query: TypedFingerprint, library, *, sig_must_match=True, threshold=0.5):
    """Rank library idioms by structural Jaccard — but first PRUNE by domain signature
    (the data telling us which functions are even worth matching). Two idioms with the
    same skeleton but different domains no longer collide."""
    cands = [t for t in library
             if not sig_must_match
             or (t.in_domains == query.in_domains and t.out_domain == query.out_domain)]
    scored = sorted(((t, jaccard(query.fp, t.fp)) for t in cands),
                    key=lambda ts: ts[1], reverse=True)
    return [(t, s) for t, s in scored if s >= threshold]


# --------------------------------------------------------------------------- #
# smoke test: `uv run python column_role/formula_fingerprint.py`
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    print("== parse / lift / render ==")
    ast = inline(Sheet({"C2": parse("A2*B2")}), "C2")
    print(" A2*B2 canonical:", render(canonicalize(ast, hole_mode="indexed")))

    print("\n== fill-down clone + anomaly ==")
    sheet = Sheet({"C2": parse("A2*B2"), "C3": parse("A3*B3"),
                   "C4": parse("A4*B4"), "C5": parse("A5+B5")})  # C5 is the bug
    for fp, cells in clone_clusters(sheet, ["C2", "C3", "C4", "C5"]).items():
        kind = "fill" if len(cells) > 1 else "ANOMALY"
        print(f"  {kind}: {cells}")

    print("\n== idiom across different data (flat/abstract frontier) ==")
    f1 = canonicalize(parse("TAKE(3, SORT(A1:A10, B1:B10))"), hole_mode="flat", abstract_lits=True)
    f2 = canonicalize(parse("TAKE(5, SORT(X1:X20, Y1:Y20))"), hole_mode="flat", abstract_lits=True)
    print("  f1 idiom:", render(f1))
    print("  f2 idiom:", render(f2))
    print("  concrete jaccard:",
          round(jaccard(fp_cell(Sheet({"Z": parse("TAKE(3, SORT(A1:A10, B1:B10))")}), "Z"),
                        fp_cell(Sheet({"Z": parse("TAKE(5, SORT(X1:X20, Y1:Y20))")}), "Z")), 2),
          "| idiom jaccard:", round(jaccard(fingerprint(f1), fingerprint(f2)), 2))

    print("\n== feedback 1: shape-guided cutting ==")
    idx = PopularityIndex()
    corpus = ["TAKE(3, SORT(A1:A10, B1:B10))", "TAKE(1, SORT(C1:C9, D1:D9))",
              "SORT(E1:E5, F1:F5)", "SUM(A1:A10)"]
    for f in corpus:
        idx.add(parse(f))
    popular = idx.popular(support=2)
    print("  popular skeletons (support>=2):",
          [render(canonicalize(parse(f), hole_mode="flat", abstract_lits=True))
           for f in ("SORT(A1:A2,B1:B2)",) if root_hash(
               canonicalize(parse(f), hole_mode="flat", abstract_lits=True)) in popular])
    cuts = guided_cuts(parse("TAKE(9, SORT(P1:P3, Q1:Q3))"), popular)
    print("  guided cuts worth fingerprinting:", [r for r, _ in cuts])

    print("\n== feedback 2: data-typed holes prune idiom matches ==")
    skel = fingerprint(canonicalize(parse("TAKE(3, SORT(A1:A10, B1:B10))"),
                                    hole_mode="flat", abstract_lits=True))
    lib = [TypedFingerprint("top-N by money", skel, ("integer",), "numeric"),
           TypedFingerprint("top-N by date", skel, ("integer",), "string")]
    # read the cone's computed values -> infer domains -> type the holes
    q = TypedFingerprint("unknown top-N", skel,
                         (infer_domain(["3"]),), infer_domain(["84.50", "12.99", "9.75"]))
    print("  query signature:", q.in_domains, "->", q.out_domain)
    print("  matches (domain-pruned):", [(t.label, round(s, 2)) for t, s in match(q, lib)])
    print("  matches (structure only):",
          [(t.label, round(s, 2)) for t, s in match(q, lib, sig_must_match=False)])

    print("\n== depth+arity as a coarse category / blocking key ==")
    fns = {"A2*B2": parse("A2*B2"), "A5+B5": parse("A5+B5"),
           "SUM(A1:A10)": parse("SUM(A1:A10)"),
           "TAKE(3,SORT(..))": parse("TAKE(3, SORT(A1:A10, B1:B10))")}
    for key, names in sorted(bucket(fns).items()):
        print(f"  (depth={key[0]}, arity={key[1]}): {names}"
              + ("   ← coarse collision; WL separates * from +" if len(names) > 1 else ""))
