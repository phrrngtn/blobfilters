"""Build materials-related blobfilter domains.

Domains created:
  - alloy_designations: from Wikidata (alloys + aliases)
  - plastics: from Wikidata (plastic types + aliases)
  - material_types: Wikidata material taxonomy (subclasses of material)
  - engineering_materials: curated common engineering material names
  - sustainability_terms: EPD/LCA vocabulary
  - hs_material_codes: HS codes chapters 25-83 descriptions
"""

import json
import time
import urllib.request
import urllib.parse
import psycopg2

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
PG_DSN = "dbname=rule4_test host=/tmp"


def sparql_query(query, delay=2.0):
    """Run a SPARQL query against Wikidata."""
    params = urllib.parse.urlencode({'query': query, 'format': 'json'})
    url = f"{WIKIDATA_ENDPOINT}?{params}"
    req = urllib.request.Request(url, headers={
        'User-Agent': 'blobfilters-domain-builder/1.0 (phrrngtn@gmail.com)',
        'Accept': 'application/sparql-results+json'
    })
    time.sleep(delay)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def extract_labels(results):
    """Extract labels and aliases from SPARQL results."""
    members = set()
    for row in results['results']['bindings']:
        label = row.get('itemLabel', {}).get('value', '')
        if label and not label.startswith('Q'):
            members.add(label)
        aliases = row.get('aliases', {}).get('value', '')
        if aliases:
            for a in aliases.split('|'):
                a = a.strip()
                if a and not a.startswith('Q'):
                    members.add(a)
    return sorted(members)


def upsert_domain(cur, domain_name, label, source, members):
    """Insert or update a domain in PG."""
    # Upsert enumeration
    cur.execute("""
        INSERT INTO domain.enumeration (domain_name, domain_label, source, member_count)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (domain_name) DO UPDATE
        SET domain_label = EXCLUDED.domain_label,
            source = EXCLUDED.source,
            member_count = EXCLUDED.member_count,
            updated_at = NOW()
    """, (domain_name, label, source, len(members)))

    # Clear old members and insert new
    cur.execute("DELETE FROM domain.member WHERE domain_name = %s", (domain_name,))
    for m in members:
        cur.execute(
            "INSERT INTO domain.member (domain_name, label) VALUES (%s, %s)",
            (domain_name, m)
        )
    print(f"  {domain_name}: {len(members)} members")


def build_alloys():
    """Wikidata alloys with designations and aliases."""
    print("Fetching alloys from Wikidata...")
    results = sparql_query("""
    SELECT DISTINCT ?item ?itemLabel
      (GROUP_CONCAT(DISTINCT ?altLabel; separator="|") AS ?aliases)
    WHERE {
      ?item wdt:P31/wdt:P279* wd:Q37756 .
      OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
    }
    GROUP BY ?item ?itemLabel
    LIMIT 2000
    """)
    return extract_labels(results)


def build_plastics():
    """Wikidata plastics."""
    print("Fetching plastics from Wikidata...")
    results = sparql_query("""
    SELECT DISTINCT ?item ?itemLabel
      (GROUP_CONCAT(DISTINCT ?altLabel; separator="|") AS ?aliases)
    WHERE {
      ?item wdt:P31/wdt:P279* wd:Q11474 .
      OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
    }
    GROUP BY ?item ?itemLabel
    LIMIT 2000
    """)
    return extract_labels(results)


def build_material_types():
    """Wikidata material taxonomy — subclasses of material."""
    print("Fetching material types from Wikidata...")
    results = sparql_query("""
    SELECT DISTINCT ?item ?itemLabel
      (GROUP_CONCAT(DISTINCT ?altLabel; separator="|") AS ?aliases)
    WHERE {
      ?item wdt:P279+ wd:Q214609 .
      OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
    }
    GROUP BY ?item ?itemLabel
    LIMIT 5000
    """)
    return extract_labels(results)


def build_engineering_materials():
    """Curated common engineering material names."""
    return sorted(set([
        # Metals
        "steel", "aluminum", "aluminium", "copper", "brass", "bronze",
        "titanium", "nickel", "zinc", "iron", "lead", "tin", "tungsten",
        "gold", "silver", "platinum", "chromium", "cobalt", "manganese",
        "molybdenum", "vanadium", "magnesium", "beryllium", "palladium",
        # Alloy families
        "stainless steel", "carbon steel", "tool steel", "cast iron",
        "wrought iron", "galvanized steel", "mild steel", "high-speed steel",
        "aluminum alloy", "copper alloy", "nickel alloy", "titanium alloy",
        "superalloy", "solder", "pewter",
        # Common designations
        "304", "316", "316L", "410", "430", "6061", "6061-T6", "7075",
        "7075-T6", "2024", "A36", "1018", "1020", "4140", "4340",
        "A2", "D2", "M2", "O1", "S7", "H13",
        # Polymers
        "polyethylene", "polypropylene", "polyvinyl chloride", "PVC",
        "nylon", "ABS", "polycarbonate", "PEEK", "PET", "HDPE", "LDPE",
        "LLDPE", "PTFE", "Teflon", "epoxy", "polyester", "polyurethane",
        "silicone", "acrylic", "polystyrene", "polyimide", "phenolic",
        "melamine", "vinyl", "vinyl ester", "urea formaldehyde",
        "polylactic acid", "PLA", "polyamide", "polyphenylene sulfide",
        "polysulfone", "polyetherimide", "UHMWPE",
        # Polymer abbreviations
        "PE", "PP", "PS", "PC", "PA", "POM", "PBT", "PPO", "PSU", "PEI",
        "PA6", "PA66", "PA12", "PMMA", "PPS", "TPU", "TPE", "EVA",
        # Ceramics
        "alumina", "silicon carbide", "silicon nitride", "zirconia",
        "porcelain", "glass", "borosilicate", "soda lime glass",
        "fused silica", "steatite", "cordierite", "mullite",
        "boron nitride", "tungsten carbide",
        # Composites
        "fiberglass", "carbon fiber", "GFRP", "CFRP", "Kevlar", "aramid",
        "glass fiber", "basalt fiber", "fiber reinforced polymer", "FRP",
        "SMC", "BMC", "prepreg", "laminate",
        # Construction
        "concrete", "cement", "Portland cement", "mortar", "rebar",
        "aggregate", "gravel", "sand", "asphalt", "bitumen",
        "plywood", "OSB", "MDF", "particleboard", "drywall", "gypsum",
        "insulation", "foam", "mineral wool", "fiberglass insulation",
        "XPS", "EPS", "polyisocyanurate", "spray foam",
        "brick", "block", "masonry", "tile", "granite", "marble",
        "limestone", "sandstone", "slate", "travertine",
        "structural steel", "rebar", "wire mesh", "geotextile",
        # Natural
        "wood", "timber", "lumber", "bamboo", "cotton", "wool",
        "leather", "rubber", "natural rubber", "latex", "cork",
        "hemp", "jute", "sisal", "linen", "silk",
        # Coatings/finishes
        "paint", "primer", "varnish", "lacquer", "powder coating",
        "galvanizing", "anodizing", "plating", "chrome plating",
        # Electronics
        "silicon", "germanium", "gallium arsenide", "FR-4",
        "copper clad laminate", "solder paste", "thermal paste",
        "conformal coating", "EMI shielding",
    ]))


def build_sustainability_terms():
    """EPD/LCA sustainability vocabulary."""
    return sorted(set([
        # Impact categories
        "GWP", "global warming potential", "carbon footprint",
        "embodied carbon", "embodied energy", "ozone depletion",
        "ozone depletion potential", "ODP", "acidification",
        "acidification potential", "AP", "eutrophication",
        "eutrophication potential", "EP", "photochemical ozone",
        "POCP", "abiotic depletion", "ADP", "resource depletion",
        # Energy
        "primary energy", "renewable energy", "non-renewable energy",
        "PERE", "PENRE", "PERT", "PENRT",
        "total primary energy", "energy recovery",
        # Materials
        "recycled content", "post-consumer", "pre-consumer",
        "recyclable", "biodegradable", "biobased", "bio-based",
        "virgin material", "secondary material", "reused material",
        "recycled material",
        # LCA phases
        "life cycle assessment", "LCA", "life cycle analysis",
        "EPD", "environmental product declaration",
        "declared unit", "functional unit",
        "cradle-to-gate", "cradle-to-grave", "cradle-to-cradle",
        "gate-to-gate", "end-of-life", "use phase",
        "production phase", "construction phase",
        "A1", "A2", "A3", "A1-A3", "A4", "A5",
        "B1", "B2", "B3", "B4", "B5", "B6", "B7",
        "C1", "C2", "C3", "C4", "D",
        # Waste
        "waste", "hazardous waste", "non-hazardous waste",
        "radioactive waste", "landfill", "incineration",
        "reuse", "recycling", "recovery", "disposal",
        # Standards
        "ISO 14025", "ISO 14040", "ISO 14044", "EN 15804",
        "EN 15978", "ISO 21930", "ISO 14067", "ISO 14064",
        "PCR", "product category rules",
        # Water
        "water use", "freshwater", "water consumption",
        "water footprint", "blue water", "grey water",
        # Other
        "carbon neutral", "carbon negative", "net zero",
        "greenhouse gas", "GHG", "CO2 equivalent", "CO2e",
        "kg CO2 eq", "MJ", "kWh",
        "climate change", "environmental impact",
        "sustainability", "circular economy",
        "product environmental footprint", "PEF",
    ]))


def build_hs_material_codes():
    """HS codes chapters 25-83 — material commodity descriptions."""
    print("Fetching HS codes from GitHub...")
    urls = [
        "https://raw.githubusercontent.com/datasets/harmonized-system/master/data/harmonized-system.csv",
        "https://raw.githubusercontent.com/warrantgroup/WCO-HS-Codes/main/data/hs-codes.csv",
    ]
    lines = None
    for url in urls:
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'blobfilters-domain-builder/1.0'
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                lines = resp.read().decode('utf-8').splitlines()
                print(f"  Downloaded from {url}: {len(lines)} lines")
                break
        except Exception as e:
            print(f"  Failed {url}: {e}")

    if not lines:
        print("  Could not download HS codes, using fallback")
        return []

    # Parse CSV — find description column
    import csv
    import io
    reader = csv.DictReader(io.StringIO('\n'.join(lines)))
    members = set()
    for row in reader:
        # Try different column names
        code = row.get('hscode', row.get('code', row.get('Code', '')))
        desc = row.get('description', row.get('Description', ''))
        if not code or not desc:
            continue
        # Filter to chapters 25-83
        try:
            chapter = int(str(code)[:2])
        except (ValueError, IndexError):
            continue
        if 25 <= chapter <= 83:
            # Clean up description
            desc = desc.strip().rstrip('.')
            if len(desc) > 3:
                members.add(desc)
    return sorted(members)


def main():
    con = psycopg2.connect(PG_DSN)
    con.autocommit = True
    cur = con.cursor()

    # Check existing domains
    cur.execute("SELECT domain_name FROM domain.enumeration")
    existing = {r[0] for r in cur.fetchall()}
    print(f"Existing domains: {len(existing)}")

    # 1. Wikidata alloys
    members = build_alloys()
    upsert_domain(cur, "alloy_designations",
                  "Alloy names and designations (Wikidata)",
                  "wikidata", members)

    # 2. Wikidata plastics
    members = build_plastics()
    upsert_domain(cur, "plastics",
                  "Plastic and polymer types (Wikidata)",
                  "wikidata", members)

    # 3. Wikidata material types
    members = build_material_types()
    upsert_domain(cur, "material_types",
                  "Material taxonomy — subclasses of material (Wikidata)",
                  "wikidata", members)

    # 4. Curated engineering materials
    members = build_engineering_materials()
    upsert_domain(cur, "engineering_materials",
                  "Common engineering material names (curated)",
                  "curated", members)

    # 5. Sustainability terms
    members = build_sustainability_terms()
    upsert_domain(cur, "sustainability_terms",
                  "EPD/LCA sustainability vocabulary (curated)",
                  "curated", members)

    # 6. HS material codes
    members = build_hs_material_codes()
    if members:
        upsert_domain(cur, "hs_material_codes",
                      "HS commodity codes chapters 25-83 (materials)",
                      "wco:hs", members)

    cur.close()
    con.close()
    print("\nDone. Run tools/build_normalized_filters.py to rebuild bitmaps.")


if __name__ == "__main__":
    main()
