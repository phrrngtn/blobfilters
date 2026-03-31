"""MEP equipment domains: manufacturers, model numbers, and schedule vocabulary.

Sources:
  - NEEA residential HPWH qualified products list (PDF, shredded)
  - Manufacturer spec sheets (PDF, shredded)
  - SEC EDGAR SIC 3585 companies
  - Curated from corpus extraction

Three domains:
  mep_manufacturers     — HVAC/plumbing manufacturer names
  mep_model_numbers     — Equipment model numbers (high cardinality, high signal)
  mep_schedule_terms    — Engineering drawing schedule headers and MEP vocabulary
"""

import json
import re
import time
import duckdb

BLOBFILTERS_EXT = "/Users/paulharrington/checkouts/blobfilters/build/duckdb/blobfilters.duckdb_extension"
BHTTP_EXT = "/Users/paulharrington/checkouts/blobhttp/build/bhttp.duckdb_extension"
BBOXES_EXT = "/Users/paulharrington/checkouts/blobboxes/build/duckdb/bboxes.duckdb_extension"

NEEA_URL = "https://neea.org/wp-content/uploads/2025/03/residential-HPWH-qualified-products-list.pdf"


def fetch_neea_products(con):
    """Download and shred the NEEA qualified products list PDF.

    Returns dict of {manufacturer_name: [model_numbers]}.
    """
    print("  Fetching NEEA qualified products list...", flush=True)
    row = con.execute("""
        SELECT r.response_blob FROM (SELECT bh_http_get($1,
            headers := MAP {'User-Agent': 'Mozilla/5.0 (Macintosh)'}
        ) AS r)
    """, [NEEA_URL]).fetchone()

    blob = row[0]
    if not blob or blob[:4] != b'%PDF':
        print("  NEEA PDF fetch failed")
        return {}

    bboxes = json.loads(con.execute(
        "SELECT bb_objs_json($1::BLOB)", [blob]
    ).fetchone()[0])

    doc = json.loads(con.execute(
        "SELECT bb_objs_doc_json($1::BLOB)", [blob]
    ).fetchone()[0])
    print(f"  {doc['page_count']} pages, {len(bboxes)} bboxes")

    # Column layout: x≈28=manufacturer, x≈149=model
    from collections import defaultdict
    mfr_models = defaultdict(set)

    for b in bboxes:
        if b["page_id"] == 0:
            continue
        text = (b.get("text") or "").strip()
        if not text:
            continue
        x, y = b["x"], b["y"]

        if 140 < x < 350:
            # Model column — find manufacturer on same row
            mfr_candidates = [
                (bb.get("text") or "").strip()
                for bb in bboxes
                if bb["page_id"] == b["page_id"]
                and 20 < bb["x"] < 130
                and abs(bb["y"] - y) < 5
            ]
            if mfr_candidates:
                mfr = mfr_candidates[0]
                if mfr and not mfr[0].isdigit():
                    mfr_models[mfr].add(text)

    print(f"  {len(mfr_models)} manufacturers, "
          f"{sum(len(v) for v in mfr_models.values())} models")
    return {k: sorted(v) for k, v in mfr_models.items()}


def fetch_spec_sheet_models(con):
    """Shred manufacturer spec sheet PDFs for model numbers.

    Returns list of (manufacturer, model_number) tuples.
    """
    spec_sheets = [
        ("Caleffi",
         "https://www.caleffi.com/sites/default/files/media/external-file/01060_NA.pdf"),
        ("Mitsubishi",
         "https://www.acdirect.com/media/specs/Mitsubishi/M_SUBMITTAL_MSZ-FS12NA_MUZ-FS12NA_en.pdf"),
        ("Rheem",
         "https://media.rheem.com/blobazrheem/wp-content/uploads/sites/36/2024/12/Plug-in-Spec-Sheet.pdf"),
        ("Rheem",
         "https://media.rheem.com/blobazrheem/wp-content/uploads/sites/36/2024/12/Hybrid-Electric-HPWH-with-LeakGuard-Spec-Sheet.pdf"),
        ("Bradford White",
         "https://docs.bradfordwhite.com/Spec_Sheets/1903_Current.pdf"),
        ("Navien",
         "https://images.activeplumbing.com/specsheets/navien/296127-s.pdf"),
    ]

    results = []
    for mfr, url in spec_sheets:
        print(f"  Shredding {mfr} spec sheet...", end=" ", flush=True)
        row = con.execute("""
            SELECT r.response_status_code, r.response_blob
            FROM (SELECT bh_http_get($1,
                headers := MAP {'User-Agent': 'blobboxes/0.1 (MEP equipment research)'}
            ) AS r)
        """, [url]).fetchone()

        if row[0] != 200 or not row[1] or row[1][:4] != b'%PDF':
            print(f"HTTP {row[0]}, skipping")
            continue

        bboxes = json.loads(con.execute(
            "SELECT bb_objs_json($1::BLOB)", [row[1]]
        ).fetchone()[0])

        count = 0
        for b in bboxes:
            text = (b.get("text") or "").strip()
            if not text:
                continue
            # Model number patterns
            is_model = False
            if re.match(r'^M[SUX]Z-', text):
                is_model = True
            elif re.match(r'^5[0-9]{4,5}[A-Z]?$', text):
                is_model = True
            elif re.match(r'^[A-Z]{2,5}-[A-Z0-9]{2,}', text):
                is_model = True
            elif re.match(r'^(PAC|MAC|TAZ|MLS|DSD|SP\d|AP\d|AS\d|RE2H|NWP|NPE)-?', text):
                is_model = True
            elif re.match(r'^[A-Z]{1,3}\d{4,}[A-Z]*$', text) and len(text) > 5:
                is_model = True

            if is_model:
                results.append((mfr, text))
                count += 1

        print(f"{count} models")

    return results


def curated_manufacturers():
    """Curated MEP equipment manufacturer names with alt labels."""
    return {
        # Heat pumps & HVAC — Tier 1 (from corpus with specific models)
        "Mitsubishi Electric": ["MITSUBISHI", "MELCO", "Mitsubishi Electric Trane"],
        "Colmac": ["COLMAC", "Colmac WaterHeat", "Colmac Coil"],
        "Aermec": ["AERMEC"],
        "Daikin": ["DAIKIN", "Daikin Applied", "Daikin Comfort"],
        "LG Electronics HVAC": ["LG", "LG HVAC", "LG VRF"],
        "York": ["YORK", "Johnson Controls York"],
        "Carrier": ["CARRIER"],
        "Lennox": ["LENNOX"],
        "Trane": ["TRANE", "Trane Technologies"],
        "Fujitsu": ["FUJITSU", "Fujitsu General"],

        # Heat pump water heaters — Tier 1 (from NEEA + corpus)
        "Rheem": ["RHEEM", "Ruud", "Richmond"],
        "A.O. Smith": ["A.O. SMITH", "AO SMITH", "American", "State Water Heaters"],
        "Bradford White": ["BRADFORD WHITE"],
        "Sanden": ["SANDEN", "SANCO2", "QAHV"],
        "Navien": ["NAVIEN"],
        "Rinnai": ["RINNAI"],
        "Lochinvar": ["LOCHINVAR"],

        # Hydronic components — from corpus
        "Caleffi": ["CALEFFI"],
        "Taco Comfort Solutions": ["TACO"],
        "Grundfos": ["GRUNDFOS"],
        "Watts": ["WATTS", "Watts Water Technologies"],
        "Danfoss": ["DANFOSS"],
        "Tekmar": ["TEKMAR"],
        "Heat-Flo": ["HEAT-FLO", "HeatFlo"],
        "Bell & Gossett": ["BELL & GOSSETT", "B&G"],
        "Armstrong International": ["ARMSTRONG"],
        "Wilo": ["WILO"],
        "Belimo": ["BELIMO"],
        "Victaulic": ["VICTAULIC"],
        "Honeywell": ["HONEYWELL", "Resideo"],
        "Siemens Building Technologies": ["SIEMENS"],

        # NEEA-discovered manufacturers (not in corpus)
        "GE Appliances": ["GE"],
        "Bosch": ["BOSCH"],
        "Midea": ["MIDEA"],
        "Ariston": ["ARISTON"],
        "Friedrich": ["FRIEDRICH"],
        "American Standard": ["AMERICAN STANDARD"],
        "Eco-Logical": ["ECO-LOGICAL"],
        "Harvest Thermal": ["HARVEST THERMAL"],
        "Senville": ["SENVILLE"],
        "Vaughn Thermal": ["VAUGHN", "VAUGHN THERMAL"],

        # SEC EDGAR SIC 3585 / NAICS 333415
        "Johnson Controls": ["JCI", "JOHNSON CONTROLS"],
        "Weil-McLain": ["WEIL-MCLAIN"],
        "AERCO": ["AERCO"],
        "Cleaver-Brooks": ["CLEAVER BROOKS"],
        "Xylem": ["XYLEM"],
    }


def curated_schedule_terms():
    """MEP engineering drawing schedule headers and vocabulary."""
    return [
        # Schedule types (from corpus extraction)
        "HEAT PUMP SCHEDULE", "PUMP SCHEDULE", "PIPE INSULATION SCHEDULE",
        "BUFFER TANK SCHEDULE", "AIR SEPARATOR SCHEDULE",
        "HEAT EXCHANGER SCHEDULE", "EXPANSION TANK SCHEDULE",
        "MIXING VALVE SCHEDULE", "CONTROL VALVE SCHEDULE",
        "EQUIPMENT SCHEDULE", "PLUMBING FIXTURE SCHEDULE",
        "FAN COIL SCHEDULE", "AIR HANDLING UNIT SCHEDULE",
        "AHU SCHEDULE", "VAV BOX SCHEDULE", "DIFFUSER SCHEDULE",
        "WATER HEATER SCHEDULE", "BOILER SCHEDULE",
        "CHILLER SCHEDULE", "COOLING TOWER SCHEDULE",
        "VRF SCHEDULE", "MINI-SPLIT SCHEDULE",
        "DUCTWORK SCHEDULE", "DAMPER SCHEDULE",
        "ELECTRICAL PANEL SCHEDULE", "DISCONNECT SCHEDULE",

        # Drawing types
        "PIPING DIAGRAM", "FLOW DIAGRAM", "RISER DIAGRAM",
        "ONE-LINE DIAGRAM", "WIRING DIAGRAM", "CONTROL DIAGRAM",
        "MECHANICAL PLAN", "PLUMBING PLAN", "ELECTRICAL PLAN",
        "REFLECTED CEILING PLAN", "ROOF PLAN",
        "SECTION", "DETAIL", "ELEVATION", "ISOMETRIC",
        "INSTALLATION DETAIL", "MOUNTING DETAIL",
        "TYPICAL PIPING DETAIL", "VALVE DETAIL",

        # MEP specification vocabulary
        "GENERAL NOTES", "ABBREVIATIONS", "LEGEND", "SYMBOL LIST",
        "SCOPE OF WORK", "BASIS OF DESIGN",
        "DESIGN CONDITIONS", "DESIGN CRITERIA",
        "SEQUENCE OF OPERATIONS", "CONTROLS NARRATIVE",
        "COMMISSIONING", "TESTING AND BALANCING", "TAB",
        "SUBMITTAL", "SHOP DRAWING", "AS-BUILT",

        # Equipment descriptors
        "HEATING CAPACITY", "COOLING CAPACITY",
        "ENTERING WATER TEMP", "LEAVING WATER TEMP", "EWT", "LWT",
        "FLOW RATE", "HEAD LOSS", "PRESSURE DROP",
        "ELECTRICAL DATA", "VOLTAGE", "PHASE", "AMPERAGE",
        "MINIMUM CIRCUIT AMPACITY", "MCA",
        "MAXIMUM OVERCURRENT PROTECTION", "MOP", "MOCP",
        "REFRIGERANT", "R-410A", "R-32", "R-744", "CO2",
        "SOUND LEVEL", "SOUND POWER",
        "WEIGHT", "DIMENSIONS", "CLEARANCES",
        "ENERGY FACTOR", "UEF", "COP", "EER", "SEER", "HSPF",
        "FIRST HOUR RATING", "FHR",
        "RECOVERY RATE", "INPUT RATE",

        # Piping terms
        "SUPPLY", "RETURN", "DOMESTIC HOT WATER", "DHW",
        "DOMESTIC COLD WATER", "DCW", "RECIRCULATION", "RECIRC",
        "CONDENSATE", "DRAIN", "OVERFLOW", "RELIEF",
        "EXPANSION LOOP", "FLEXIBLE CONNECTOR",
        "ISOLATION VALVE", "BALL VALVE", "BUTTERFLY VALVE",
        "CHECK VALVE", "PRESSURE RELIEF VALVE", "PRV",
        "THERMOSTATIC MIXING VALVE", "TMV",
        "BALANCING VALVE", "CONTROL VALVE",
        "AIR SEPARATOR", "DIRT SEPARATOR",
        "STRAINER", "FILTER", "FLOW SWITCH",
        "PRESSURE GAUGE", "TEMPERATURE GAUGE",
        "THERMOWELL", "P&T GAUGE",
    ]


def main():
    duck = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    duck.execute(f"LOAD '{BLOBFILTERS_EXT}'")
    duck.execute(f"LOAD '{BHTTP_EXT}'")
    duck.execute(f"LOAD '{BBOXES_EXT}'")
    duck.execute("INSTALL postgres; LOAD postgres;")
    duck.execute("ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES)")

    existing = set(r[0] for r in duck.execute(
        "SELECT domain_name FROM pg.domain.enumeration"
    ).fetchall())
    print(f"Existing: {len(existing)} domains\n")

    total_new = 0

    # ── Domain 1: mep_manufacturers ──────────────────────────────────
    domain = "mep_manufacturers"
    if domain not in existing:
        print(f"Building {domain}...")
        curated = curated_manufacturers()

        # Enrich from NEEA
        neea = fetch_neea_products(duck)
        for mfr in neea:
            if mfr not in curated:
                curated[mfr] = [mfr.upper()]

        members = []
        for name, alts in curated.items():
            members.append(name)
            members.extend(alts)

        members = sorted(set(members))
        print(f"  {len(curated)} manufacturers, {len(members)} total labels (with alt names)\n")

        duck.execute(
            "INSERT INTO pg.domain.enumeration "
            "(domain_name, domain_label, source, member_count) "
            "VALUES (?, ?, 'curated:neea+edgar+corpus', ?)",
            [domain, "MEP equipment manufacturers (HVAC, plumbing, hydronic)", len(members)])

        duck.executemany(
            "INSERT INTO pg.domain.member (domain_name, label) VALUES (?, ?)",
            [(domain, m) for m in members])
        total_new += len(members)
    else:
        print(f"  SKIP {domain}")

    # ── Domain 2: mep_model_numbers ──────────────────────────────────
    domain = "mep_model_numbers"
    if domain not in existing:
        print(f"Building {domain}...")

        # Collect from NEEA
        neea = fetch_neea_products(duck)
        members = set()
        for mfr, models in neea.items():
            members.update(models)

        # Collect from spec sheets
        spec_models = fetch_spec_sheet_models(duck)
        for mfr, model in spec_models:
            members.add(model)

        # Curated from corpus (the ones we actually saw in RFPs)
        corpus_models = [
            "MSZ-FS12NA", "MXZ-2C20NAHZ", "MXZ-3C24NAHZ", "MXZ-3C30NAHZ",
            "MXZ-SM48NAMHZ", "SUZ-AA12NLHZ",
            "CXW-15",
            "NYG0500-G", "NYK500",
            "551006A",
            "QAHV-N136YAU-HPB", "QAHV-N136YAU-HPB-BS",
            "HF-22-BT",
        ]
        members.update(corpus_models)
        members = sorted(members)
        print(f"  {len(members)} unique model numbers\n")

        duck.execute(
            "INSERT INTO pg.domain.enumeration "
            "(domain_name, domain_label, source, member_count) "
            "VALUES (?, ?, 'curated:neea+spec_sheets+corpus', ?)",
            [domain, "MEP equipment model numbers (HPWH, heat pumps, hydronic)", len(members)])

        duck.executemany(
            "INSERT INTO pg.domain.member (domain_name, label) VALUES (?, ?)",
            [(domain, m) for m in members])
        total_new += len(members)
    else:
        print(f"  SKIP {domain}")

    # ── Domain 3: mep_schedule_terms ─────────────────────────────────
    domain = "mep_schedule_terms"
    if domain not in existing:
        print(f"Building {domain}...")
        members = curated_schedule_terms()
        members = sorted(set(members))
        print(f"  {len(members)} terms\n")

        duck.execute(
            "INSERT INTO pg.domain.enumeration "
            "(domain_name, domain_label, source, member_count) "
            "VALUES (?, ?, 'curated:mep_standards', ?)",
            [domain, "MEP engineering drawing schedule headers and specification vocabulary", len(members)])

        duck.executemany(
            "INSERT INTO pg.domain.member (domain_name, label) VALUES (?, ?)",
            [(domain, m) for m in members])
        total_new += len(members)
    else:
        print(f"  SKIP {domain}")

    # ── Rebuild filters ──────────────────────────────────────────────
    print(f"\nRebuilding normalized filters...")
    t0 = time.perf_counter()
    filters = duck.execute("""
        WITH DOMAIN_MEMBERS AS (
            SELECT domain_name, json_group_array(label) AS members_json
            FROM pg.domain.member GROUP BY domain_name
        )
        SELECT domain_name,
               bf_to_base64(bf_build_json_normalized(members_json)) AS filter_b64,
               bf_cardinality(bf_build_json_normalized(members_json)) AS cardinality
        FROM DOMAIN_MEMBERS
    """).fetchall()
    print(f"  {len(filters)} filters in {time.perf_counter()-t0:.2f}s")

    for dn, fb64, card in filters:
        duck.execute(
            "UPDATE pg.domain.enumeration "
            "SET filter_b64 = ?, member_count = ?, updated_at = NOW() "
            "WHERE domain_name = ?",
            [fb64, int(card), dn])

    final = duck.execute(
        "SELECT COUNT(DISTINCT domain_name), COUNT(*) FROM pg.domain.member"
    ).fetchone()
    print(f"\n{'='*60}")
    print(f"  {final[0]} domains, {final[1]} total members (+{total_new} new)")
    print(f"{'='*60}")

    # Show MEP domains
    mep = duck.execute("""
        SELECT domain_name, member_count, source
        FROM pg.domain.enumeration
        WHERE domain_name LIKE 'mep_%'
        ORDER BY domain_name
    """).fetchall()
    print(f"\n  MEP domains:")
    for dn, mc, src in mep:
        print(f"    {dn:<25} {mc:>6} members  ({src})")

    duck.close()


if __name__ == "__main__":
    main()
