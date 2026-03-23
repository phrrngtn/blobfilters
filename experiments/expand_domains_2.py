"""Second wave of Wikidata domain expansion.

Focus on high-frequency value types in documents:
- Named entities that appear in tables, forms, and reports
- Reference data that columns are commonly populated with
- Terms that appear as categorical values in structured data
"""
import json
import time
import urllib.request
import urllib.parse
import duckdb

BLOBFILTERS_EXT = "/Users/paulharrington/checkouts/blobfilters/build/duckdb/blobfilters.duckdb_extension"
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"

DOMAINS = {
    # ── High-frequency geographic ──
    "airports": {
        "description": "Airports (IATA codes + names)",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q1248784 .
              ?item wdt:P238 ?iata .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 8000
        """,
    },
    "us_counties": {
        "description": "US counties",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31 wd:Q47168 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            }
        """,
    },
    # ── People categories ──
    "nationalities": {
        "description": "Demonyms / nationality adjectives",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31 wd:Q231002 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            }
        """,
    },
    "given_names": {
        "description": "Common given/first names",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              { ?item wdt:P31 wd:Q202444 . }
              UNION
              { ?item wdt:P31 wd:Q11879590 . }
              ?item wdt:P2561 ?nativeName .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 10000
        """,
    },
    "family_names": {
        "description": "Common family/surnames",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31 wd:Q101352 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 10000
        """,
    },
    # ── Natural world ──
    "animals": {
        "description": "Common animals",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31 wd:Q16521 .
              ?item wdt:P1843 ?commonName . FILTER(LANG(?commonName) = "en")
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 5000
        """,
    },
    "plants": {
        "description": "Common plants and crops",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q756 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 5000
        """,
    },
    "colors": {
        "description": "Named colors",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31 wd:Q1075 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            }
        """,
    },
    "materials": {
        "description": "Materials and substances",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q214609 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 5000
        """,
    },
    # ── Business & finance ──
    "stock_exchanges": {
        "description": "Stock exchanges worldwide",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31 wd:Q11691 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            }
        """,
    },
    "legal_forms": {
        "description": "Business legal forms (LLC, GmbH, SA, etc.)",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31 wd:Q21980377 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            }
        """,
    },
    # ── Technology & standards ──
    "file_formats": {
        "description": "File formats and extensions",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q235557 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 3000
        """,
    },
    "software": {
        "description": "Notable software applications",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q7397 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 5000
        """,
    },
    # ── Government & law ──
    "government_agencies": {
        "description": "Government agencies (US + intl)",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q327333 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 5000
        """,
    },
    # ── Transport ──
    "vehicle_makes": {
        "description": "Automobile manufacturers and brands",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q3041792 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 3000
        """,
    },
    # ── Demographics ──
    "religions": {
        "description": "Religions and belief systems",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q9174 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 2000
        """,
    },
    "ethnic_groups": {
        "description": "Ethnic groups",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31 wd:Q41710 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 5000
        """,
    },
    # ── Infrastructure ──
    "building_types": {
        "description": "Types of buildings and structures",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P279 wd:Q41176 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            }
        """,
    },
    # ── Education ──
    "academic_degrees": {
        "description": "Academic degrees (BSc, PhD, MD, etc.)",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q189533 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            }
        """,
    },
    # ── Health ──
    "symptoms": {
        "description": "Medical symptoms and signs",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31 wd:Q169872 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 3000
        """,
    },
    "medical_tests": {
        "description": "Medical/diagnostic tests and procedures",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q796194 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 3000
        """,
    },
}


def query_wikidata(sparql):
    params = urllib.parse.urlencode({"query": sparql, "format": "json"})
    url = f"{WIKIDATA_ENDPOINT}?{params}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "blobboxes-domain-builder/0.1 (https://github.com/phrrngtn/blobboxes)",
        "Accept": "application/sparql-results+json",
    })
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = json.loads(resp.read())
    results = []
    for b in data["results"]["bindings"]:
        label = b.get("label", {}).get("value", "")
        alt = b.get("altLabel", {}).get("value", "")
        if label: results.append(label)
        if alt and alt != label: results.append(alt)
    return list(set(results))


def main():
    duck = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    duck.execute(f"LOAD '{BLOBFILTERS_EXT}'")
    duck.execute("INSTALL postgres; LOAD postgres;")
    duck.execute("ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES)")

    existing = set(r[0] for r in duck.execute(
        "SELECT domain_name FROM pg.domain.enumeration"
    ).fetchall())
    print(f"Existing: {len(existing)} domains\n")

    total_new = 0
    for domain_name, config in DOMAINS.items():
        if domain_name in existing:
            print(f"  SKIP {domain_name}")
            continue

        print(f"  {domain_name}: {config['description']}...", end=" ", flush=True)
        t0 = time.perf_counter()
        try:
            members = query_wikidata(config["sparql"])
            elapsed = time.perf_counter() - t0
            print(f"{len(members)} members ({elapsed:.1f}s)")

            if not members:
                continue

            duck.execute("""
                INSERT INTO pg.domain.enumeration (domain_name, domain_label, source, member_count)
                VALUES (?, ?, 'wikidata', ?)
            """, [domain_name, config["description"], len(members)])
            duck.executemany("""
                INSERT INTO pg.domain.member (domain_name, label) VALUES (?, ?)
            """, [(domain_name, m) for m in members])

            total_new += len(members)
        except Exception as e:
            print(f"ERROR: {e}")
            time.sleep(3)
            continue

        time.sleep(1)  # be polite to Wikidata

    # Rebuild all normalized filters
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
    print(f"  Built {len(filters)} filters in {time.perf_counter()-t0:.2f}s")

    for dn, fb64, card in filters:
        duck.execute("""
            UPDATE pg.domain.enumeration
            SET filter_b64 = ?, member_count = ?, updated_at = NOW()
            WHERE domain_name = ?
        """, [fb64, int(card), dn])

    final = duck.execute(
        "SELECT COUNT(DISTINCT domain_name), COUNT(*) FROM pg.domain.member"
    ).fetchone()
    print(f"\n{'='*50}")
    print(f"  {final[0]} domains, {final[1]} total members (+{total_new} new)")
    print(f"{'='*50}")
    duck.close()


if __name__ == "__main__":
    main()
