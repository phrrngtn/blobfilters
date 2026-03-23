"""Build domain filters from SEC EDGAR company data.

Uses the SEC's bulk company tickers file + submissions API to build:
1. SIC code → company name mappings for insurance/reinsurance sectors
2. All public company names as a domain (ticker → name)
3. Business segment data from XBRL company facts

SEC rate limit: 10 requests/second with User-Agent identifying the caller.
"""
import json
import time
import urllib.request
import duckdb

BLOBFILTERS_EXT = "/Users/paulharrington/checkouts/blobfilters/build/duckdb/blobfilters.duckdb_extension"
UA = "blobboxes-domain-builder/0.1 phrrngtn@panix.com"

# SIC codes for insurance industry
INSURANCE_SICS = {
    "6311": "Life Insurance",
    "6321": "Accident and Health Insurance",
    "6324": "Hospital & Medical Service Plans",
    "6331": "Fire, Marine & Casualty Insurance",
    "6351": "Surety Insurance",
    "6399": "Insurance Carriers, NEC",  # includes reinsurance
    "6411": "Insurance Agents, Brokers & Service",
}

# Broader financial SICs
FINANCIAL_SICS = {
    "6020": "State Commercial Banks",
    "6021": "National Commercial Banks",
    "6022": "State Chartered Banks, Fed Reserve",
    "6035": "Savings Institution, Federally Chartered",
    "6036": "Savings Institutions, Not Federally Chartered",
    "6141": "Personal Credit Institutions",
    "6153": "Short-Term Business Credit",
    "6159": "Federal-Sponsored Credit Agencies",
    "6199": "Finance Services",
    "6200": "Security & Commodity Services",
    "6211": "Security Brokers, Dealers & Flotation",
    "6282": "Investment Advice",
    "6311": "Life Insurance",
    "6321": "Accident and Health Insurance",
    "6324": "Hospital & Medical Service Plans",
    "6331": "Fire, Marine & Casualty Insurance",
    "6351": "Surety Insurance",
    "6399": "Insurance Carriers, NEC",
    "6411": "Insurance Agents, Brokers & Service",
    "6500": "Real Estate",
    "6510": "Real Estate Operators",
    "6512": "Operators of Apartment Buildings",
    "6552": "Land Subdividers & Developers",
    "6726": "Investment Offices",
    "6770": "Blank Checks",
    "6798": "Real Estate Investment Trusts",
}


def fetch_sec(url):
    """Fetch from SEC with rate-limit-friendly User-Agent."""
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def main():
    duck = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    duck.execute(f"LOAD '{BLOBFILTERS_EXT}'")
    duck.execute("INSTALL postgres; LOAD postgres;")
    duck.execute("ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES)")

    existing = set(r[0] for r in duck.execute(
        "SELECT domain_name FROM pg.domain.enumeration"
    ).fetchall())

    # ── Step 1: Load all company tickers ────────────────────────────
    print("Loading SEC company tickers...")
    with open("/tmp/company_tickers.json") as f:
        tickers_data = json.load(f)

    companies = []
    for v in tickers_data.values():
        companies.append({
            "cik": str(v["cik_str"]).zfill(10),
            "ticker": v["ticker"],
            "name": v["title"],
        })
    print(f"  {len(companies)} companies loaded")

    # ── Step 2: Get SIC codes for insurance companies ───────────────
    # Use the EDGAR full-text search to find companies by SIC
    print("\nFetching insurance/reinsurance companies by SIC...")

    insurance_cos = []
    for sic_code, sic_desc in INSURANCE_SICS.items():
        url = f"https://efts.sec.gov/LATEST/search-index?q=%22{sic_code}%22&dateRange=custom&startdt=2025-01-01&forms=10-K&from=0&size=0"
        # Actually, use the simpler company search
        url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&SIC={sic_code}&dateb=&owner=include&count=200&search_text=&action=getcompany&output=atom"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                content = resp.read().decode()
                # Parse the Atom XML for company names
                import re
                names = re.findall(r'<title[^>]*>([^<]+)</title>', content)
                # Skip the feed title
                names = [n for n in names[1:] if n.strip()]
                for name in names:
                    # Parse "CIK COMPANY NAME (SIC)" format
                    parts = name.strip()
                    insurance_cos.append({"name": parts, "sic": sic_code, "sic_desc": sic_desc})
            print(f"  SIC {sic_code} ({sic_desc}): {len(names)-1 if names else 0} companies")
        except Exception as e:
            print(f"  SIC {sic_code}: ERROR {e}")
        time.sleep(0.2)

    # ── Step 3: Get detailed info for top reinsurers ────────────────
    # Known major reinsurers by CIK
    REINSURER_CIKS = {
        "0000913144": "RenaissanceRe",
        "0001095073": "Everest Re Group",
        "0000844150": "Transatlantic Holdings",
        "0000082811": "Berkshire Hathaway",
        "0001267395": "Arch Capital Group",
        "0001273931": "Platinum Underwriters",
        "0001276707": "Aspen Insurance Holdings",
        "0001099219": "Markel",
        "0001070750": "Alleghany",
        "0001364742": "Fairfax Financial",
        "0001308606": "RenaissanceRe Finance",
        "0001061768": "Montpelier Re",
        "0001578318": "Third Point Reinsurance",
    }

    print(f"\nFetching details for {len(REINSURER_CIKS)} known reinsurers...")
    reinsurer_details = []
    for cik, name in REINSURER_CIKS.items():
        try:
            data = fetch_sec(f"https://data.sec.gov/submissions/CIK{cik}.json")
            detail = {
                "cik": cik,
                "name": data["name"],
                "sic": data.get("sic", ""),
                "sicDescription": data.get("sicDescription", ""),
                "tickers": data.get("tickers", []),
                "stateOfIncorporation": data.get("stateOfIncorporation", ""),
            }
            reinsurer_details.append(detail)
            print(f"  {detail['name']:<40} SIC={detail['sic']} tickers={detail['tickers']}")
            time.sleep(0.15)
        except Exception as e:
            print(f"  {name}: ERROR {e}")

    # ── Step 4: Build domains and insert ────────────────────────────
    print("\nBuilding domains...")

    # Domain: public_company_names — all SEC registrants
    if "public_company_names" not in existing:
        names = set(c["name"] for c in companies)
        tickers = set(c["ticker"] for c in companies)
        all_members = list(names | tickers)  # union, no dupes
        print(f"  public_company_names: {len(all_members)} members (names + tickers)")
        duck.execute("INSERT INTO pg.domain.enumeration (domain_name, domain_label, source, member_count) VALUES (?, ?, ?, ?)",
                     ["public_company_names", "SEC-registered public company names and ticker symbols", "sec:edgar", len(all_members)])
        duck.executemany("INSERT INTO pg.domain.member (domain_name, label) VALUES (?, ?)",
                         [("public_company_names", m) for m in all_members])

    # Domain: insurance_company_names — from SIC search
    if "insurance_company_names" not in existing and insurance_cos:
        names = list(set(c["name"] for c in insurance_cos))
        print(f"  insurance_company_names: {len(names)} members")
        duck.execute("INSERT INTO pg.domain.enumeration (domain_name, domain_label, source, member_count) VALUES (?, ?, ?, ?)",
                     ["insurance_company_names", "SEC-registered insurance company names (SIC 63xx)", "sec:edgar", len(names)])
        duck.executemany("INSERT INTO pg.domain.member (domain_name, label) VALUES (?, ?)",
                         [("insurance_company_names", m) for m in names])

    # Domain: sic_codes — all SIC code descriptions
    if "sic_codes" not in existing:
        # Fetch the full SIC list
        print("  Fetching SIC code list...")
        try:
            sic_page = fetch_sec("https://www.sec.gov/search-filings/standard-industrial-classification-sic-code-list")
            # This returns HTML, parse it
            import re
            # Actually let's just use a curated set of the major SIC divisions
            sic_members = []
            for code, desc in {**INSURANCE_SICS, **FINANCIAL_SICS}.items():
                sic_members.append(f"{code} {desc}")
                sic_members.append(desc)
                sic_members.append(code)
            # Add major SIC divisions
            sic_divisions = {
                "01-09": "Agriculture, Forestry, Fishing",
                "10-14": "Mining",
                "15-17": "Construction",
                "20-39": "Manufacturing",
                "40-49": "Transportation & Public Utilities",
                "50-51": "Wholesale Trade",
                "52-59": "Retail Trade",
                "60-67": "Finance, Insurance, Real Estate",
                "70-89": "Services",
                "91-99": "Public Administration",
            }
            for code, desc in sic_divisions.items():
                sic_members.append(f"SIC {code}: {desc}")
                sic_members.append(desc)

            print(f"  sic_codes: {len(sic_members)} members")
            duck.execute("INSERT INTO pg.domain.enumeration (domain_name, domain_label, source, member_count) VALUES (?, ?, ?, ?)",
                         ["sic_codes", "SIC industry classification codes", "sec:sic", len(sic_members)])
            duck.executemany("INSERT INTO pg.domain.member (domain_name, label) VALUES (?, ?)",
                             [("sic_codes", m) for m in sic_members])
        except Exception as e:
            print(f"  sic_codes: ERROR {e}")

    # ── Step 5: Rebuild filters ─────────────────────────────────────
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
        duck.execute("UPDATE pg.domain.enumeration SET filter_b64 = ?, member_count = ?, updated_at = NOW() WHERE domain_name = ?",
                     [fb64, int(card), dn])

    final = duck.execute("SELECT COUNT(DISTINCT domain_name), COUNT(*) FROM pg.domain.member").fetchone()
    print(f"\n{'='*50}")
    print(f"  {final[0]} domains, {final[1]} total members")
    print(f"{'='*50}")

    # Show all domains sorted by size
    all_domains = duck.execute("""
        SELECT domain_name, member_count, source
        FROM pg.domain.enumeration
        ORDER BY member_count DESC
    """).fetchall()
    print(f"\n  {'Domain':<35} {'Members':>7} {'Source':<25}")
    print(f"  {'-'*70}")
    for dn, mc, src in all_domains:
        print(f"  {dn:<35} {mc:>7} {src or '':<25}")

    duck.close()


if __name__ == "__main__":
    main()
