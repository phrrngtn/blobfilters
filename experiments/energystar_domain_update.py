"""Update MEP domains with Energy Star certified product data.

Fetches HPWH and ASHP CSVs from data.energystar.gov and adds
new manufacturers and model numbers to the existing domains.
"""

import duckdb
import time
import os

BLOBFILTERS_EXT = "/Users/paulharrington/checkouts/blobfilters/build/duckdb/blobfilters.duckdb_extension"
BHTTP_EXT = "/Users/paulharrington/checkouts/blobhttp/build/bhttp.duckdb_extension"

HPWH_URL = "https://data.energystar.gov/api/views/v7jr-74b4/rows.csv?accessType=DOWNLOAD"
ASHP_URL = "https://data.energystar.gov/api/views/w7cv-9xjt/rows.csv?accessType=DOWNLOAD"


def main():
    duck = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    duck.execute(f"LOAD '{BLOBFILTERS_EXT}'")
    duck.execute(f"LOAD '{BHTTP_EXT}'")
    duck.execute("INSTALL postgres; LOAD postgres;")
    duck.execute("ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES)")

    # ── Load HPWH CSV ────────────────────────────────────────────────
    print("Loading Energy Star HPWH CSV...")
    duck.execute(f"""
        CREATE TABLE energystar_hpwh AS
        SELECT * FROM read_csv_auto('{HPWH_URL}')
    """)
    hpwh_count = duck.execute("SELECT count(*) FROM energystar_hpwh").fetchone()[0]
    print(f"  {hpwh_count} rows")

    # ── Load ASHP CSV (download first to avoid timeout) ──────────────
    print("Downloading Energy Star ASHP CSV...")
    ashp_file = "/tmp/energystar_ashp.csv"
    duck.execute(f"""
        COPY (
            SELECT (bh_http_get('{ASHP_URL}',
                headers := MAP {{'User-Agent': 'blobboxes-research/0.1'}}
            )).response_body
        ) TO '{ashp_file}' (FORMAT CSV, HEADER false)
    """)

    # The response_body is one big string per row. Write it properly.
    import requests
    if not os.path.exists(ashp_file) or os.path.getsize(ashp_file) < 100000:
        print("  Downloading via requests...")
        r = requests.get(ASHP_URL, headers={"User-Agent": "blobboxes-research/0.1"}, timeout=120)
        with open(ashp_file, "wb") as f:
            f.write(r.content)

    print(f"  ASHP CSV: {os.path.getsize(ashp_file)} bytes")

    duck.execute(f"""
        CREATE TABLE energystar_ashp AS
        SELECT * FROM read_csv('{ashp_file}', all_varchar=true, ignore_errors=true)
    """)
    ashp_count = duck.execute("SELECT count(*) FROM energystar_ashp").fetchone()[0]
    print(f"  {ashp_count} rows")

    # ── Extract new manufacturers and models ─────────────────────────
    print("\nExtracting new manufacturers and models...")

    # HPWH brands and models
    hpwh_brands = duck.execute("""
        SELECT DISTINCT "Brand Name" FROM energystar_hpwh
        WHERE "Brand Name" IS NOT NULL AND "Brand Name" != 'TEST BRAND 001'
    """).fetchall()
    hpwh_models = duck.execute("""
        SELECT DISTINCT "Model Number" FROM energystar_hpwh
        WHERE "Model Number" IS NOT NULL
    """).fetchall()

    # ASHP brands and models (outdoor + indoor)
    ashp_brands = duck.execute("""
        SELECT DISTINCT "Outdoor Unit Brand Name" FROM energystar_ashp
        WHERE "Outdoor Unit Brand Name" IS NOT NULL
    """).fetchall()
    ashp_outdoor_models = duck.execute("""
        SELECT DISTINCT "Outdoor Unit Model Number" FROM energystar_ashp
        WHERE "Outdoor Unit Model Number" IS NOT NULL
    """).fetchall()
    ashp_indoor_models = duck.execute("""
        SELECT DISTINCT "Indoor Unit Model Number" FROM energystar_ashp
        WHERE "Indoor Unit Model Number" IS NOT NULL
    """).fetchall()

    all_brands = set(r[0] for r in hpwh_brands) | set(r[0] for r in ashp_brands)
    all_models = (set(r[0] for r in hpwh_models) |
                  set(r[0] for r in ashp_outdoor_models) |
                  set(r[0] for r in ashp_indoor_models))

    print(f"  Energy Star brands: {len(all_brands)}")
    print(f"  Energy Star models: {len(all_models)}")

    # ── Get existing domain members ──────────────────────────────────
    existing_mfrs = set(r[0] for r in duck.execute(
        "SELECT label FROM pg.domain.member WHERE domain_name = 'mep_manufacturers'"
    ).fetchall())
    existing_models = set(r[0] for r in duck.execute(
        "SELECT label FROM pg.domain.member WHERE domain_name = 'mep_model_numbers'"
    ).fetchall())

    print(f"  Existing mep_manufacturers: {len(existing_mfrs)}")
    print(f"  Existing mep_model_numbers: {len(existing_models)}")

    # ── Add new manufacturers ────────────────────────────────────────
    new_mfrs = [b for b in all_brands if b not in existing_mfrs and b.upper() not in {m.upper() for m in existing_mfrs}]
    if new_mfrs:
        print(f"\n  Adding {len(new_mfrs)} new manufacturers:")
        for m in sorted(new_mfrs):
            print(f"    {m}")
        duck.executemany(
            "INSERT INTO pg.domain.member (domain_name, label) VALUES ('mep_manufacturers', ?)",
            [(m,) for m in new_mfrs])

    # ── Add new models ───────────────────────────────────────────────
    new_models = [m for m in all_models if m not in existing_models]
    print(f"\n  Adding {len(new_models)} new model numbers (from {len(all_models)} Energy Star total)")

    if new_models:
        duck.executemany(
            "INSERT INTO pg.domain.member (domain_name, label) VALUES ('mep_model_numbers', ?)",
            [(m,) for m in new_models])

    # ── Update member counts ─────────────────────────────────────────
    for domain in ["mep_manufacturers", "mep_model_numbers"]:
        count = duck.execute(
            "SELECT count(*) FROM pg.domain.member WHERE domain_name = ?", [domain]
        ).fetchone()[0]
        duck.execute(
            "UPDATE pg.domain.enumeration SET member_count = ?, updated_at = NOW() WHERE domain_name = ?",
            [count, domain])
        # Also update source to reflect Energy Star addition
        duck.execute(
            "UPDATE pg.domain.enumeration SET source = source || '+energystar' WHERE domain_name = ? AND source NOT LIKE '%energystar%'",
            [domain])

    # ── Rebuild filters ──────────────────────────────────────────────
    print("\nRebuilding normalized filters for MEP domains...")
    t0 = time.perf_counter()
    filters = duck.execute("""
        WITH DOMAIN_MEMBERS AS (
            SELECT domain_name, json_group_array(label) AS members_json
            FROM pg.domain.member
            WHERE domain_name LIKE 'mep_%'
            GROUP BY domain_name
        )
        SELECT domain_name,
               bf_to_base64(bf_build_json_normalized(members_json)) AS filter_b64,
               bf_cardinality(bf_build_json_normalized(members_json)) AS cardinality
        FROM DOMAIN_MEMBERS
    """).fetchall()
    print(f"  {len(filters)} filters in {time.perf_counter()-t0:.2f}s")

    for dn, fb64, card in filters:
        duck.execute(
            "UPDATE pg.domain.enumeration SET filter_b64 = ?, member_count = ?, updated_at = NOW() WHERE domain_name = ?",
            [fb64, int(card), dn])

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    mep = duck.execute("""
        SELECT domain_name, member_count, source
        FROM pg.domain.enumeration WHERE domain_name LIKE 'mep_%'
        ORDER BY domain_name
    """).fetchall()
    for dn, mc, src in mep:
        print(f"  {dn:<25} {mc:>6} members  ({src})")
    print(f"{'='*60}")

    duck.close()


if __name__ == "__main__":
    main()
