"""Third wave: medical, financial, reinsurance, catastrophe, NAICS domains."""
import json
import time
import urllib.request
import urllib.parse
import duckdb
import csv
import io

BLOBFILTERS_EXT = "/Users/paulharrington/checkouts/blobfilters/build/duckdb/blobfilters.duckdb_extension"
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"

WIKIDATA_DOMAINS = {
    # ── Medical (from LLM survey) ──
    "body_parts": {
        "description": "Human body parts, organs, and anatomical structures",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q712378 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 5000
        """,
    },
    "medical_procedures": {
        "description": "Medical and surgical procedures",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q796194 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 5000
        """,
    },
    "journals": {
        "description": "Academic and scientific journals",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31 wd:Q5633421 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 8000
        """,
    },
    # ── Financial ──
    "financial_instruments": {
        "description": "Financial instruments (bonds, derivatives, swaps, etc.)",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q11032 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 3000
        """,
    },
    "accounting_terms": {
        "description": "Accounting and financial reporting terms",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q4116214 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 2000
        """,
    },
    # ── Insurance / Reinsurance ──
    "insurance_companies": {
        "description": "Insurance and reinsurance companies",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              { ?item wdt:P31/wdt:P279* wd:Q6023791 . }
              UNION
              { ?item wdt:P31/wdt:P279* wd:Q2221906 . }
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 5000
        """,
    },
    "reinsurance_companies": {
        "description": "Reinsurance companies specifically",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q2221906 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            }
        """,
    },
    # ── Catastrophe / Natural disasters ──
    "natural_disasters": {
        "description": "Types of natural disasters",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q8065 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 3000
        """,
    },
    "historical_disasters": {
        "description": "Specific historical catastrophic events (named storms, earthquakes, etc.)",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              { ?item wdt:P31/wdt:P279* wd:Q8065 . ?item wdt:P585 ?date . }
              UNION
              { ?item wdt:P31/wdt:P279* wd:Q8065 . ?item wdt:P580 ?date . }
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 10000
        """,
    },
    "named_hurricanes": {
        "description": "Named tropical cyclones/hurricanes",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q63100541 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 5000
        """,
    },
    "earthquakes": {
        "description": "Notable historical earthquakes",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q7944 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 5000
        """,
    },
    # ── Regulatory / Classification ──
    "iso_standards": {
        "description": "ISO standards",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31 wd:Q317623 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            } LIMIT 3000
        """,
    },
    "tax_types": {
        "description": "Types of taxes",
        "sparql": """
            SELECT ?item ?label ?altLabel WHERE {
              ?item wdt:P31/wdt:P279* wd:Q8161 .
              ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
              OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
            }
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


def fetch_naics():
    """Fetch NAICS codes + descriptions from Census Bureau."""
    print("  Fetching NAICS codes from Census Bureau...")
    url = "https://www.census.gov/naics/2022NAICS/2-6%20digit_2022_Codes.xlsx"
    # NAICS is also available as a simpler list; let's use a curated set
    # of 2-digit sector names + 3-digit subsector names
    naics_sectors = {
        "11": "Agriculture, Forestry, Fishing and Hunting",
        "21": "Mining, Quarrying, and Oil and Gas Extraction",
        "22": "Utilities",
        "23": "Construction",
        "31-33": "Manufacturing",
        "42": "Wholesale Trade",
        "44-45": "Retail Trade",
        "48-49": "Transportation and Warehousing",
        "51": "Information",
        "52": "Finance and Insurance",
        "53": "Real Estate and Rental and Leasing",
        "54": "Professional, Scientific, and Technical Services",
        "55": "Management of Companies and Enterprises",
        "56": "Administrative and Support and Waste Management",
        "61": "Educational Services",
        "62": "Health Care and Social Assistance",
        "71": "Arts, Entertainment, and Recreation",
        "72": "Accommodation and Food Services",
        "81": "Other Services (except Public Administration)",
        "92": "Public Administration",
    }

    # Add 3-digit subsectors for key sectors
    naics_subsectors = {
        # Finance & Insurance
        "521": "Monetary Authorities-Central Bank",
        "522": "Credit Intermediation and Related Activities",
        "523": "Securities, Commodity Contracts, and Other Financial Investments",
        "524": "Insurance Carriers and Related Activities",
        "525": "Funds, Trusts, and Other Financial Vehicles",
        # Insurance detail
        "5241": "Insurance Carriers",
        "52411": "Direct Life, Health, and Medical Insurance Carriers",
        "52412": "Direct Insurance (except Life, Health, and Medical) Carriers",
        "52413": "Reinsurance Carriers",
        "5242": "Agencies, Brokerages, and Other Insurance Related Activities",
        # Real Estate
        "531": "Real Estate",
        "532": "Rental and Leasing Services",
        "533": "Lessors of Nonfinancial Intangible Assets",
        # Health Care
        "621": "Ambulatory Health Care Services",
        "622": "Hospitals",
        "623": "Nursing and Residential Care Facilities",
        "624": "Social Assistance",
        # Professional Services
        "5411": "Legal Services",
        "5412": "Accounting, Tax Preparation, Bookkeeping, and Payroll Services",
        "5413": "Architectural, Engineering, and Related Services",
        "5414": "Specialized Design Services",
        "5415": "Computer Systems Design and Related Services",
        "5416": "Management, Scientific, and Technical Consulting Services",
        "5417": "Scientific Research and Development Services",
        "5418": "Advertising, Public Relations, and Related Services",
        "5419": "Other Professional, Scientific, and Technical Services",
    }

    members = []
    for code, desc in {**naics_sectors, **naics_subsectors}.items():
        members.append(f"{code} {desc}")
        members.append(desc)  # Also add just the description
        members.append(code)  # And just the code

    return members


def fetch_insurance_terms():
    """Curated insurance/reinsurance terminology."""
    terms = [
        # Reinsurance types
        "Treaty reinsurance", "Facultative reinsurance",
        "Proportional reinsurance", "Non-proportional reinsurance",
        "Excess of loss", "Stop loss", "Quota share",
        "Surplus share", "Catastrophe excess of loss",
        "Per risk excess of loss", "Aggregate excess of loss",
        "Retrocession", "Retrocessionaire",
        # Insurance lines
        "Property insurance", "Casualty insurance",
        "Liability insurance", "Workers compensation",
        "Professional liability", "Directors and officers",
        "Errors and omissions", "General liability",
        "Commercial auto", "Commercial property",
        "Homeowners insurance", "Flood insurance",
        "Earthquake insurance", "Windstorm insurance",
        "Crop insurance", "Marine insurance",
        "Aviation insurance", "Surety bonds",
        "Fidelity bonds", "Title insurance",
        "Product liability", "Umbrella insurance",
        "Excess insurance",
        # Reinsurance concepts
        "Ceding company", "Cedant", "Cession",
        "Retention", "Attachment point",
        "Exhaustion point", "Reinstatement",
        "Occurrence limit", "Aggregate limit",
        "Annual aggregate deductible", "Loss corridor",
        "Burning cost", "Experience rating",
        "Exposure rating", "Catastrophe modeling",
        "Probable maximum loss", "PML",
        "Annual aggregate", "Occurrence",
        "Claims made", "Occurrence basis",
        "Incurred but not reported", "IBNR",
        "Loss adjustment expense", "LAE",
        "Combined ratio", "Loss ratio",
        "Expense ratio", "Underwriting profit",
        "Net premium", "Gross premium",
        "Ceded premium", "Assumed premium",
        "Unearned premium", "Earned premium",
        "Loss reserve", "Case reserve",
        "Bulk reserve", "ALAE", "ULAE",
        "Bordereaux", "Slip", "Binder",
        # Catastrophe terms
        "Catastrophe bond", "Cat bond", "ILS",
        "Insurance-linked securities",
        "Sidecar", "Collateralized reinsurance",
        "Industry loss warranty", "ILW",
        "Parametric trigger", "Indemnity trigger",
        "Modeled loss trigger",
        # Rating agencies
        "AM Best", "Standard & Poor's", "S&P",
        "Moody's", "Fitch Ratings", "DBRS",
        # Perils
        "Hurricane", "Typhoon", "Cyclone",
        "Earthquake", "Tsunami", "Flood",
        "Wildfire", "Tornado", "Hailstorm",
        "Winter storm", "Ice storm", "Drought",
        "Volcanic eruption", "Landslide", "Mudslide",
        "Storm surge", "Wind damage",
        "Terrorism", "Cyber risk", "Pandemic",
        # Reinsurance companies (major)
        "Munich Re", "Swiss Re", "Hannover Re",
        "SCOR", "Lloyd's of London", "Berkshire Hathaway",
        "RenaissanceRe", "Everest Re", "TransRe",
        "Arch Capital", "PartnerRe", "General Re",
        "Odyssey Re", "Markel", "Alleghany",
        "Fairfax Financial", "Aspen Re",
        "Axis Capital", "Platinum Underwriters",
        "Montpelier Re", "Endurance Specialty",
        "Validus", "Greenlight Capital Re",
        "Third Point Re", "Sirius International",
        "Korean Re", "China Re", "Toa Re",
        "Africa Re", "GIC Re",
        # Brokers
        "Aon", "Marsh", "Willis Towers Watson",
        "Guy Carpenter", "Gallagher Re",
        "Howden", "BMS Group",
        # Models
        "RMS", "AIR Worldwide", "CoreLogic",
        "Karen Clark", "KCC", "PERILS",
        "Verisk", "Moody's RMS",
    ]
    return terms


def fetch_financial_metrics():
    """Common financial reporting line items and metrics."""
    terms = [
        # Income statement
        "Revenue", "Net revenue", "Gross revenue",
        "Cost of goods sold", "COGS",
        "Gross profit", "Gross margin",
        "Operating income", "Operating expenses",
        "EBITDA", "EBIT", "Net income",
        "Earnings per share", "EPS",
        "Diluted EPS", "Basic EPS",
        "Interest income", "Interest expense",
        "Depreciation", "Amortization",
        "Impairment", "Write-off", "Write-down",
        "Tax expense", "Income tax",
        "Deferred tax", "Tax provision",
        # Balance sheet
        "Total assets", "Total liabilities",
        "Stockholders equity", "Shareholders equity",
        "Book value", "Tangible book value",
        "Current assets", "Current liabilities",
        "Working capital", "Cash and equivalents",
        "Accounts receivable", "Accounts payable",
        "Inventory", "Prepaid expenses",
        "Goodwill", "Intangible assets",
        "Property plant and equipment", "PP&E",
        "Long-term debt", "Short-term debt",
        "Retained earnings", "Treasury stock",
        "Accumulated other comprehensive income",
        # Cash flow
        "Operating cash flow", "Free cash flow",
        "Capital expenditure", "CapEx",
        "Dividends paid", "Share repurchase",
        "Debt issuance", "Debt repayment",
        # Ratios
        "Return on equity", "ROE",
        "Return on assets", "ROA",
        "Return on invested capital", "ROIC",
        "Price to earnings", "P/E ratio",
        "Price to book", "P/B ratio",
        "Debt to equity", "D/E ratio",
        "Current ratio", "Quick ratio",
        "Interest coverage ratio",
        "Dividend yield", "Payout ratio",
        # Insurance-specific financials
        "Gross written premium", "GWP",
        "Net written premium", "NWP",
        "Net earned premium", "NEP",
        "Net claims incurred",
        "Policy acquisition costs",
        "Underwriting result",
        "Investment income",
        "Realized gains", "Unrealized gains",
        "Fair value", "Mark to market",
        "Statutory surplus", "Policyholder surplus",
        "Risk-based capital", "RBC",
        "Solvency ratio", "Capital adequacy",
    ]
    return terms


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

    # Wikidata domains
    for domain_name, config in WIKIDATA_DOMAINS.items():
        if domain_name in existing:
            print(f"  SKIP {domain_name}")
            continue
        print(f"  {domain_name}: {config['description']}...", end=" ", flush=True)
        t0 = time.perf_counter()
        try:
            members = query_wikidata(config["sparql"])
            print(f"{len(members)} ({time.perf_counter()-t0:.1f}s)")
            if not members: continue
            duck.execute("INSERT INTO pg.domain.enumeration (domain_name, domain_label, source, member_count) VALUES (?, ?, 'wikidata', ?)",
                         [domain_name, config["description"], len(members)])
            duck.executemany("INSERT INTO pg.domain.member (domain_name, label) VALUES (?, ?)",
                             [(domain_name, m) for m in members])
            total_new += len(members)
        except Exception as e:
            print(f"ERROR: {e}")
            time.sleep(3)
            continue
        time.sleep(1)

    # Curated domains
    # FRED economic categories
    fred_terms = [
        # Major categories
        "GDP", "Gross Domestic Product", "Real GDP", "Nominal GDP",
        "GNP", "Gross National Product",
        "CPI", "Consumer Price Index", "Core CPI", "PCE",
        "PPI", "Producer Price Index",
        "Unemployment rate", "Nonfarm payrolls",
        "Labor force participation rate",
        "Industrial production", "Capacity utilization",
        "Housing starts", "Building permits",
        "Existing home sales", "New home sales",
        "Case-Shiller", "Home price index",
        "Federal funds rate", "Fed funds rate",
        "Prime rate", "Discount rate",
        "LIBOR", "SOFR", "Treasury yield",
        "10-year Treasury", "2-year Treasury", "30-year Treasury",
        "Yield curve", "Term spread",
        "M1", "M2", "Money supply", "Monetary base",
        "Trade balance", "Current account",
        "Balance of payments", "Capital account",
        "Consumer confidence", "Michigan sentiment",
        "ISM Manufacturing", "ISM Services",
        "PMI", "Purchasing Managers Index",
        "Retail sales", "Personal income",
        "Personal consumption expenditures",
        "Durable goods orders",
        "Initial jobless claims", "Continuing claims",
        "Unit labor costs", "Productivity",
        "Leading economic index", "LEI",
        "Coincident index", "Lagging index",
        "Beige Book", "FOMC",
        # Rates and indices
        "S&P 500", "Dow Jones", "NASDAQ", "Russell 2000",
        "VIX", "Volatility index",
        "CBOE", "NYSE",
        "WTI", "Brent", "Crude oil",
        "Natural gas", "Henry Hub",
        "Gold price", "Silver price", "Copper price",
        "Dollar index", "DXY", "EUR/USD", "USD/JPY",
        "TED spread", "MOVE index",
        "Breakeven inflation", "TIPS spread",
        # Macro concepts
        "Inflation", "Deflation", "Stagflation",
        "Recession", "Expansion", "Recovery",
        "Tightening", "Easing", "Quantitative easing", "QE",
        "Tapering", "Forward guidance",
        "Fiscal policy", "Monetary policy",
        "Budget deficit", "National debt",
        "Debt ceiling", "Sequestration",
    ]

    curated = {
        "naics_codes": ("NAICS industry classification codes and descriptions", "curated:census", fetch_naics()),
        "insurance_reinsurance": ("Insurance and reinsurance terminology, companies, and concepts", "curated:industry", fetch_insurance_terms()),
        "financial_metrics": ("Financial reporting line items and ratios", "curated:accounting", fetch_financial_metrics()),
        "economic_indicators": ("FRED economic indicators, rates, and macro concepts", "curated:fred", fred_terms),
    }

    for domain_name, (desc, source, members) in curated.items():
        if domain_name in existing:
            print(f"  SKIP {domain_name}")
            continue
        print(f"  {domain_name}: {desc}... {len(members)} members")
        duck.execute("INSERT INTO pg.domain.enumeration (domain_name, domain_label, source, member_count) VALUES (?, ?, ?, ?)",
                     [domain_name, desc, source, len(members)])
        duck.executemany("INSERT INTO pg.domain.member (domain_name, label) VALUES (?, ?)",
                         [(domain_name, m) for m in members])
        total_new += len(members)

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
    print(f"  {len(filters)} filters in {time.perf_counter()-t0:.2f}s")

    for dn, fb64, card in filters:
        duck.execute("UPDATE pg.domain.enumeration SET filter_b64 = ?, member_count = ?, updated_at = NOW() WHERE domain_name = ?",
                     [fb64, int(card), dn])

    final = duck.execute("SELECT COUNT(DISTINCT domain_name), COUNT(*) FROM pg.domain.member").fetchone()
    print(f"\n{'='*50}")
    print(f"  {final[0]} domains, {final[1]} total members (+{total_new} new)")
    print(f"{'='*50}")

    # Show top domains by size
    top = duck.execute("""
        SELECT domain_name, member_count FROM pg.domain.enumeration
        ORDER BY member_count DESC LIMIT 20
    """).fetchall()
    print(f"\n  Top 20 domains by member count:")
    for dn, mc in top:
        print(f"    {dn:<30} {mc:>6}")

    duck.close()


if __name__ == "__main__":
    main()
