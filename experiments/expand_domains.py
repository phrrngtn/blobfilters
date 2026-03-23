"""Expand domain registry with Wikidata + other sources.

Queries Wikidata SPARQL for enumerable domains and inserts them into
PG domain.enumeration + domain.member. Each domain gets its members
plus alt_labels from Wikidata for better matching coverage.

Uses DuckDB + httpfs to query Wikidata SPARQL endpoint directly.
"""
import json
import time
import duckdb
import urllib.request
import urllib.parse

BLOBFILTERS_EXT = "/Users/paulharrington/checkouts/blobfilters/build/duckdb/blobfilters.duckdb_extension"

# Wikidata SPARQL queries for high-value domains
# Each returns ?item ?label ?altLabel
WIKIDATA_DOMAINS = {
    # ── Geographic ──
    "world_cities": {
        "description": "Cities with population > 100k",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q515 .
              ?item wdt:P1082 ?pop . FILTER(?pop > 100000)
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            }
        """,
    },
    "world_countries": {
        "description": "Sovereign states",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31 wd:Q3624078 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            }
        """,
    },
    "admin_regions": {
        "description": "First-level administrative divisions (states, provinces, etc.)",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q10864048 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 10000
        """,
    },
    # ── Organizations ──
    "universities": {
        "description": "Universities worldwide",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q3918 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 10000
        """,
    },
    "companies_sp500": {
        "description": "S&P 500 component companies",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P361 wd:Q242345 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            }
        """,
    },
    # ── Science ──
    "diseases": {
        "description": "Human diseases",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31 wd:Q12136 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 5000
        """,
    },
    "medications": {
        "description": "Medications / pharmaceutical drugs",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31 wd:Q12140 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 5000
        """,
    },
    # ── Units & measures ──
    "units_of_measurement": {
        "description": "Units of measurement",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q47574 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 5000
        """,
    },
    # ── Taxonomy ──
    "occupations": {
        "description": "Human occupations/professions",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31 wd:Q28640 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 5000
        """,
    },
    "sports": {
        "description": "Sports and athletic activities",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31 wd:Q349 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            }
        """,
    },
    "programming_languages": {
        "description": "Programming languages",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31 wd:Q9143 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            }
        """,
    },
    "food_items": {
        "description": "Food items and ingredients",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q2095 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 5000
        """,
    },
    "industries": {
        "description": "Industry sectors (NAICS/ISIC style)",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31 wd:Q268592 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            }
        """,
    },
    "medical_specialties": {
        "description": "Medical specialties",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31 wd:Q930752 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            }
        """,
    },
    "academic_disciplines": {
        "description": "Academic disciplines and fields of study",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31 wd:Q11862829 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            }
        """,
    },
}

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"


def query_wikidata(sparql: str) -> list:
    """Query Wikidata SPARQL endpoint, return list of (label, altLabel) pairs."""
    params = urllib.parse.urlencode({"query": sparql, "format": "json"})
    url = f"{WIKIDATA_ENDPOINT}?{params}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "blobboxes-domain-builder/0.1 (https://github.com/phrrngtn/blobboxes)",
        "Accept": "application/sparql-results+json",
    })

    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read())

    results = []
    for binding in data["results"]["bindings"]:
        label = binding.get("label", {}).get("value", "")
        alt = binding.get("altLabel", {}).get("value", "")
        if label:
            results.append(label)
        if alt and alt != label:
            results.append(alt)

    return list(set(results))  # deduplicate


def main():
    duck = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    duck.execute(f"LOAD '{BLOBFILTERS_EXT}'")
    duck.execute("INSTALL postgres; LOAD postgres;")
    duck.execute("ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES)")

    # Get existing domains to avoid duplicates
    existing = set(r[0] for r in duck.execute(
        "SELECT domain_name FROM pg.domain.enumeration"
    ).fetchall())
    print(f"Existing domains: {len(existing)}")

    total_new = 0
    errors = []

    for domain_name, config in WIKIDATA_DOMAINS.items():
        if domain_name in existing:
            print(f"  SKIP {domain_name} (already exists)")
            continue

        print(f"\n  Fetching {domain_name}: {config['description']}...")
        t0 = time.perf_counter()

        try:
            members = query_wikidata(config["sparql"])
            t1 = time.perf_counter()
            print(f"    {len(members)} members in {t1-t0:.1f}s")

            if len(members) == 0:
                print(f"    SKIP (no results)")
                continue

            # Insert into PG
            duck.execute("""
                INSERT INTO pg.domain.enumeration (domain_name, domain_label, source, member_count)
                VALUES (?, ?, 'wikidata', ?)
            """, [domain_name, config["description"], len(members)])

            # Batch insert members
            duck.executemany("""
                INSERT INTO pg.domain.member (domain_name, label)
                VALUES (?, ?)
            """, [(domain_name, m) for m in members])

            total_new += len(members)
            print(f"    Inserted {len(members)} members")

            # Show a few examples
            examples = members[:5]
            print(f"    Examples: {', '.join(examples)}")

        except Exception as e:
            print(f"    ERROR: {e}")
            errors.append((domain_name, str(e)))
            # Rate limit — Wikidata is generous but let's be polite
            time.sleep(2)
            continue

        # Small delay between queries
        time.sleep(1)

    # Summary
    final = duck.execute("""
        SELECT COUNT(DISTINCT domain_name) AS n_domains,
               COUNT(*) AS n_members
        FROM pg.domain.member
    """).fetchone()

    print(f"\n{'='*60}")
    print(f"  Done. {final[0]} domains, {final[1]} total members (+{total_new} new)")
    if errors:
        print(f"  {len(errors)} errors:")
        for dn, err in errors:
            print(f"    {dn}: {err}")
    print(f"{'='*60}")

    duck.close()


if __name__ == "__main__":
    main()
