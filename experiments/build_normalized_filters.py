"""Build normalized blobfilters for all domains and persist to PG."""
import time
import duckdb

BLOBFILTERS_EXT = "/Users/paulharrington/checkouts/blobfilters/build/duckdb/blobfilters.duckdb_extension"

duck = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
duck.execute(f"LOAD '{BLOBFILTERS_EXT}'")
duck.execute("INSTALL postgres; LOAD postgres;")
duck.execute("ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES)")

t0 = time.perf_counter()

# Build normalized filters and write base64 back to PG
filters = duck.execute("""
    WITH DOMAIN_MEMBERS AS (
        SELECT domain_name,
               json_group_array(label) AS members_json
        FROM pg.domain.member
        GROUP BY domain_name
    )
    SELECT domain_name,
           bf_to_base64(bf_build_json_normalized(members_json)) AS filter_b64,
           bf_cardinality(bf_build_json_normalized(members_json)) AS cardinality
    FROM DOMAIN_MEMBERS
""").fetchall()

t1 = time.perf_counter()
print(f"Built {len(filters)} normalized filters in {t1-t0:.2f}s\n")

# Write to PG
for dn, fb64, card in filters:
    duck.execute("""
        UPDATE pg.domain.enumeration
        SET filter_b64 = ?, member_count = ?, updated_at = NOW()
        WHERE domain_name = ?
    """, [fb64, int(card), dn])

t2 = time.perf_counter()
print(f"Persisted to PG in {t2-t1:.2f}s\n")

print(f"{'domain':<30} {'members':>7} {'b64_len':>8}")
print("-" * 50)
for dn, fb64, card in sorted(filters, key=lambda x: -x[2]):
    print(f"{dn:<30} {card:>7} {len(fb64):>8}")

duck.close()
