"""Fetch enriched disaster records from Wikidata with structured fields.

Builds hierarchical embedding paths for each event:
  Natural Disaster > {peril_type} > {region} > {year} > {event_name}

Stores in PG domain.disaster_event + domain.member for blobfilter probing.
"""
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime

import duckdb
from sqlalchemy import (
    Column, String, Text, Integer, Float, Date, DateTime,
    create_engine, text as sa_text,
)
from sqlalchemy.orm import DeclarativeBase, Session, mapped_column, Mapped

BLOBFILTERS_EXT = "/Users/paulharrington/checkouts/blobfilters/build/duckdb/blobfilters.duckdb_extension"
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
UA = "blobboxes-domain-builder/0.1 (https://github.com/phrrngtn/blobboxes)"

# ── SQLAlchemy model ────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass

class DisasterEvent(Base):
    __tablename__ = "disaster_event"
    __table_args__ = {"schema": "domain"}

    wikidata_uri: Mapped[str] = mapped_column(String(200), primary_key=True)
    event_name: Mapped[str] = mapped_column(Text)
    event_date: Mapped[str | None] = mapped_column(String(20), nullable=True)
    peril_type: Mapped[str] = mapped_column(String(100))
    peril_subtype: Mapped[str | None] = mapped_column(String(200), nullable=True)
    country: Mapped[str | None] = mapped_column(String(200), nullable=True)
    region: Mapped[str | None] = mapped_column(String(200), nullable=True)
    deaths: Mapped[int | None] = mapped_column(Integer, nullable=True)
    damage_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    magnitude: Mapped[str | None] = mapped_column(String(50), nullable=True)
    hierarchical_path: Mapped[str] = mapped_column(Text)


# ── SPARQL queries per disaster type ────────────────────────────────

DISASTER_QUERIES = {
    "Earthquake": {
        "qid": "Q7944",
        "sparql": """
            SELECT ?item ?label ?altLabel ?date ?country ?countryLabel
                   ?location ?locationLabel ?deaths ?damage ?magnitude
            WHERE {{
              ?item wdt:P31/wdt:P279* wd:Q7944 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL {{ ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P585 ?date }}
              OPTIONAL {{ ?item wdt:P580 ?startDate . BIND(?startDate AS ?date) }}
              OPTIONAL {{ ?item wdt:P17 ?country . ?country rdfs:label ?countryLabel .
                          FILTER(LANG(?countryLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P276 ?location . ?location rdfs:label ?locationLabel .
                          FILTER(LANG(?locationLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P1120 ?deaths }}
              OPTIONAL {{ ?item wdt:P2284 ?damage }}
              OPTIONAL {{ ?item wdt:P2528 ?magnitude }}
            }} LIMIT 3000
        """,
    },
    "Tropical Cyclone": {
        "qid": "Q8092",
        "sparql": """
            SELECT ?item ?label ?altLabel ?date ?country ?countryLabel
                   ?location ?locationLabel ?deaths ?damage ?magnitude
            WHERE {{
              ?item wdt:P31/wdt:P279* wd:Q8092 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL {{ ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P585 ?date }}
              OPTIONAL {{ ?item wdt:P580 ?startDate . BIND(?startDate AS ?date) }}
              OPTIONAL {{ ?item wdt:P17 ?country . ?country rdfs:label ?countryLabel .
                          FILTER(LANG(?countryLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P276 ?location . ?location rdfs:label ?locationLabel .
                          FILTER(LANG(?locationLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P1120 ?deaths }}
              OPTIONAL {{ ?item wdt:P2284 ?damage }}
              OPTIONAL {{ ?item wdt:P4895 ?magnitude }}
            }} LIMIT 3000
        """,
    },
    "Flood": {
        "qid": "Q8068",
        "sparql": """
            SELECT ?item ?label ?altLabel ?date ?country ?countryLabel
                   ?location ?locationLabel ?deaths ?damage
            WHERE {{
              ?item wdt:P31/wdt:P279* wd:Q8068 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL {{ ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P585 ?date }}
              OPTIONAL {{ ?item wdt:P580 ?startDate . BIND(?startDate AS ?date) }}
              OPTIONAL {{ ?item wdt:P17 ?country . ?country rdfs:label ?countryLabel .
                          FILTER(LANG(?countryLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P276 ?location . ?location rdfs:label ?locationLabel .
                          FILTER(LANG(?locationLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P1120 ?deaths }}
              OPTIONAL {{ ?item wdt:P2284 ?damage }}
            }} LIMIT 3000
        """,
    },
    "Volcanic Eruption": {
        "qid": "Q7692360",
        "sparql": """
            SELECT ?item ?label ?altLabel ?date ?country ?countryLabel
                   ?location ?locationLabel ?deaths
            WHERE {{
              ?item wdt:P31/wdt:P279* wd:Q7692360 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL {{ ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P585 ?date }}
              OPTIONAL {{ ?item wdt:P580 ?startDate . BIND(?startDate AS ?date) }}
              OPTIONAL {{ ?item wdt:P17 ?country . ?country rdfs:label ?countryLabel .
                          FILTER(LANG(?countryLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P276 ?location . ?location rdfs:label ?locationLabel .
                          FILTER(LANG(?locationLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P1120 ?deaths }}
            }} LIMIT 2000
        """,
    },
    "Wildfire": {
        "qid": "Q169950",
        "sparql": """
            SELECT ?item ?label ?altLabel ?date ?country ?countryLabel
                   ?location ?locationLabel ?deaths ?damage
            WHERE {{
              ?item wdt:P31/wdt:P279* wd:Q169950 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL {{ ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P585 ?date }}
              OPTIONAL {{ ?item wdt:P580 ?startDate . BIND(?startDate AS ?date) }}
              OPTIONAL {{ ?item wdt:P17 ?country . ?country rdfs:label ?countryLabel .
                          FILTER(LANG(?countryLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P276 ?location . ?location rdfs:label ?locationLabel .
                          FILTER(LANG(?locationLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P1120 ?deaths }}
              OPTIONAL {{ ?item wdt:P2284 ?damage }}
            }} LIMIT 2000
        """,
    },
    "Tsunami": {
        "qid": "Q8070",
        "sparql": """
            SELECT ?item ?label ?altLabel ?date ?country ?countryLabel
                   ?location ?locationLabel ?deaths ?damage
            WHERE {{
              ?item wdt:P31/wdt:P279* wd:Q8070 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL {{ ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P585 ?date }}
              OPTIONAL {{ ?item wdt:P580 ?startDate . BIND(?startDate AS ?date) }}
              OPTIONAL {{ ?item wdt:P17 ?country . ?country rdfs:label ?countryLabel .
                          FILTER(LANG(?countryLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P276 ?location . ?location rdfs:label ?locationLabel .
                          FILTER(LANG(?locationLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P1120 ?deaths }}
              OPTIONAL {{ ?item wdt:P2284 ?damage }}
            }} LIMIT 2000
        """,
    },
    "Tornado": {
        "qid": "Q8081",
        "sparql": """
            SELECT ?item ?label ?altLabel ?date ?country ?countryLabel
                   ?location ?locationLabel ?deaths ?damage
            WHERE {{
              ?item wdt:P31/wdt:P279* wd:Q8081 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL {{ ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P585 ?date }}
              OPTIONAL {{ ?item wdt:P580 ?startDate . BIND(?startDate AS ?date) }}
              OPTIONAL {{ ?item wdt:P17 ?country . ?country rdfs:label ?countryLabel .
                          FILTER(LANG(?countryLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P276 ?location . ?location rdfs:label ?locationLabel .
                          FILTER(LANG(?locationLabel) = "en") }}
              OPTIONAL {{ ?item wdt:P1120 ?deaths }}
              OPTIONAL {{ ?item wdt:P2284 ?damage }}
            }} LIMIT 3000
        """,
    },
}


def query_wikidata(sparql):
    params = urllib.parse.urlencode({"query": sparql, "format": "json"})
    url = f"{WIKIDATA_ENDPOINT}?{params}"
    req = urllib.request.Request(url, headers={
        "User-Agent": UA, "Accept": "application/sparql-results+json",
    })
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())["results"]["bindings"]


def extract_value(binding, key):
    return binding.get(key, {}).get("value")


def build_hierarchical_path(peril_type, country, region, date_str, name):
    parts = ["Natural Disaster", peril_type]
    if country:
        parts.append(country)
    if region and region != country:
        parts.append(region)
    if date_str:
        try:
            year = date_str[:4]
            parts.append(year)
        except (ValueError, IndexError):
            pass
    parts.append(name)
    return " > ".join(parts)


def main():
    engine = create_engine(
        "postgresql+psycopg2:///rule4_test",
        connect_args={"host": "/tmp"},
    )
    DisasterEvent.__table__.create(engine, checkfirst=True)

    all_events = {}  # uri -> DisasterEvent

    for peril_type, config in DISASTER_QUERIES.items():
        print(f"\n  Fetching {peril_type}...", end=" ", flush=True)
        t0 = time.perf_counter()
        try:
            bindings = query_wikidata(config["sparql"])
            elapsed = time.perf_counter() - t0
            print(f"{len(bindings)} bindings ({elapsed:.1f}s)")

            for b in bindings:
                uri = extract_value(b, "item")
                label = extract_value(b, "label")
                if not uri or not label:
                    continue

                date_str = extract_value(b, "date")
                if date_str:
                    date_str = date_str[:10]  # truncate to YYYY-MM-DD

                country = extract_value(b, "countryLabel")
                region = extract_value(b, "locationLabel")
                deaths_str = extract_value(b, "deaths")
                damage_str = extract_value(b, "damage")
                mag_str = extract_value(b, "magnitude")

                path = build_hierarchical_path(peril_type, country, region, date_str, label)

                # Deduplicate by URI (keep richest record)
                if uri in all_events:
                    existing = all_events[uri]
                    # Keep existing if it has more fields filled
                    if date_str and not existing.event_date:
                        existing.event_date = date_str
                    if country and not existing.country:
                        existing.country = country
                    if region and not existing.region:
                        existing.region = region
                    continue

                all_events[uri] = DisasterEvent(
                    wikidata_uri=uri,
                    event_name=label,
                    event_date=date_str,
                    peril_type=peril_type,
                    peril_subtype=extract_value(b, "altLabel"),
                    country=country,
                    region=region,
                    deaths=int(float(deaths_str)) if deaths_str else None,
                    damage_usd=float(damage_str) if damage_str else None,
                    magnitude=mag_str,
                    hierarchical_path=path,
                )

        except Exception as e:
            print(f"ERROR: {e}")

        time.sleep(1)

    print(f"\n  Total unique events: {len(all_events)}")

    # ── Insert into PG ────────────────────────────────────────────
    print("\n  Inserting into PG...")
    with Session(engine) as session:
        for event in all_events.values():
            session.merge(event)
        session.commit()
    print(f"  Inserted/updated {len(all_events)} disaster events")

    # ── Build domain members ─────────────────────────────────────
    # Insert event names + hierarchical paths as domain members
    members = set()
    for event in all_events.values():
        members.add(event.event_name)
        members.add(event.hierarchical_path)
        if event.peril_subtype:
            members.add(event.peril_subtype)

    print(f"  {len(members)} domain members (names + paths + alt labels)")

    duck = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    duck.execute(f"LOAD '{BLOBFILTERS_EXT}'")
    duck.execute("INSTALL postgres; LOAD postgres;")
    duck.execute("ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES)")

    # Upsert the domain
    existing = duck.execute(
        "SELECT domain_name FROM pg.domain.enumeration WHERE domain_name = 'disasters_enriched'"
    ).fetchall()
    if not existing:
        duck.execute("""
            INSERT INTO pg.domain.enumeration (domain_name, domain_label, source, member_count)
            VALUES ('disasters_enriched', 'Enriched disaster events with hierarchical paths', 'wikidata:enriched', ?)
        """, [len(members)])
    else:
        duck.execute("DELETE FROM pg.domain.member WHERE domain_name = 'disasters_enriched'")

    duck.executemany(
        "INSERT INTO pg.domain.member (domain_name, label) VALUES ('disasters_enriched', ?)",
        [(m,) for m in members]
    )

    # ── Build normalized filter ─────────────────────────────────
    print("  Building normalized blobfilter...")
    result = duck.execute("""
        WITH MEMBERS AS (
            SELECT json_group_array(label) AS mj
            FROM pg.domain.member
            WHERE domain_name = 'disasters_enriched'
        )
        SELECT bf_to_base64(bf_build_json_normalized(mj)) AS fb64,
               bf_cardinality(bf_build_json_normalized(mj)) AS card
        FROM MEMBERS
    """).fetchone()

    fb64, card = result
    duck.execute("""
        UPDATE pg.domain.enumeration
        SET filter_b64 = ?, member_count = ?, updated_at = NOW()
        WHERE domain_name = 'disasters_enriched'
    """, [fb64, card])

    # ── Summary ─────────────────────────────────────────────────
    peril_counts = {}
    dated = 0
    for event in all_events.values():
        peril_counts[event.peril_type] = peril_counts.get(event.peril_type, 0) + 1
        if event.event_date:
            dated += 1

    print(f"\n{'='*60}")
    print(f"  Events by peril type:")
    for pt, n in sorted(peril_counts.items(), key=lambda x: -x[1]):
        print(f"    {pt:<25} {n:>5}")
    print(f"\n  Events with dates: {dated}/{len(all_events)} ({100*dated/len(all_events):.0f}%)")
    print(f"  Filter cardinality: {card}")

    print(f"\n  Sample hierarchical paths:")
    for event in list(all_events.values())[:10]:
        print(f"    {event.hierarchical_path}")

    duck.close()


if __name__ == "__main__":
    main()
