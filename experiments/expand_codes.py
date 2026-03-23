"""Add numeric/code domains: phone country codes, FIPS, TLDs, ISO codes, etc.

These are short string codes that appear frequently in data but would
be missed by numeric TRY_CAST (they're codes, not numbers).
"""
import json
import time
import urllib.request
import duckdb

BLOBFILTERS_EXT = "/Users/paulharrington/checkouts/blobfilters/build/duckdb/blobfilters.duckdb_extension"
UA = "blobboxes-domain-builder/0.1 phrrngtn@panix.com"

def fetch_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def fetch_iana_tlds():
    """IANA top-level domains."""
    print("  Fetching IANA TLDs...")
    url = "https://data.iana.org/TLD/tlds-alpha-by-domain.txt"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as resp:
        lines = resp.read().decode().strip().split('\n')
    # Skip comment line, lowercase everything
    tlds = [line.strip().lower() for line in lines if not line.startswith('#') and line.strip()]
    # Also add with dot prefix
    dotted = ['.' + t for t in tlds]
    return tlds + dotted


def build_phone_country_codes():
    """ITU-T phone country calling codes."""
    # Comprehensive list — includes leading + variants
    codes = {
        "1": "North America", "7": "Russia/Kazakhstan",
        "20": "Egypt", "27": "South Africa", "30": "Greece", "31": "Netherlands",
        "32": "Belgium", "33": "France", "34": "Spain", "36": "Hungary",
        "39": "Italy", "40": "Romania", "41": "Switzerland", "43": "Austria",
        "44": "United Kingdom", "45": "Denmark", "46": "Sweden", "47": "Norway",
        "48": "Poland", "49": "Germany", "51": "Peru", "52": "Mexico",
        "53": "Cuba", "54": "Argentina", "55": "Brazil", "56": "Chile",
        "57": "Colombia", "58": "Venezuela", "60": "Malaysia", "61": "Australia",
        "62": "Indonesia", "63": "Philippines", "64": "New Zealand",
        "65": "Singapore", "66": "Thailand", "81": "Japan", "82": "South Korea",
        "84": "Vietnam", "86": "China", "90": "Turkey", "91": "India",
        "92": "Pakistan", "93": "Afghanistan", "94": "Sri Lanka",
        "95": "Myanmar", "98": "Iran",
        "212": "Morocco", "213": "Algeria", "216": "Tunisia",
        "218": "Libya", "220": "Gambia", "221": "Senegal",
        "222": "Mauritania", "223": "Mali", "224": "Guinea",
        "225": "Ivory Coast", "226": "Burkina Faso", "227": "Niger",
        "228": "Togo", "229": "Benin", "230": "Mauritius",
        "231": "Liberia", "232": "Sierra Leone", "233": "Ghana",
        "234": "Nigeria", "235": "Chad", "236": "Central African Republic",
        "237": "Cameroon", "238": "Cape Verde", "239": "São Tomé",
        "240": "Equatorial Guinea", "241": "Gabon", "242": "Congo",
        "243": "DR Congo", "244": "Angola", "245": "Guinea-Bissau",
        "246": "Diego Garcia", "247": "Ascension Island",
        "248": "Seychelles", "249": "Sudan", "250": "Rwanda",
        "251": "Ethiopia", "252": "Somalia", "253": "Djibouti",
        "254": "Kenya", "255": "Tanzania", "256": "Uganda",
        "257": "Burundi", "258": "Mozambique", "260": "Zambia",
        "261": "Madagascar", "262": "Réunion", "263": "Zimbabwe",
        "264": "Namibia", "265": "Malawi", "266": "Lesotho",
        "267": "Botswana", "268": "Eswatini", "269": "Comoros",
        "290": "Saint Helena", "291": "Eritrea",
        "297": "Aruba", "298": "Faroe Islands", "299": "Greenland",
        "350": "Gibraltar", "351": "Portugal", "352": "Luxembourg",
        "353": "Ireland", "354": "Iceland", "355": "Albania",
        "356": "Malta", "357": "Cyprus", "358": "Finland",
        "359": "Bulgaria", "370": "Lithuania", "371": "Latvia",
        "372": "Estonia", "373": "Moldova", "374": "Armenia",
        "375": "Belarus", "376": "Andorra", "377": "Monaco",
        "378": "San Marino", "380": "Ukraine", "381": "Serbia",
        "382": "Montenegro", "385": "Croatia", "386": "Slovenia",
        "387": "Bosnia", "389": "North Macedonia",
        "420": "Czech Republic", "421": "Slovakia",
        "423": "Liechtenstein",
        "500": "Falkland Islands", "501": "Belize",
        "502": "Guatemala", "503": "El Salvador", "504": "Honduras",
        "505": "Nicaragua", "506": "Costa Rica", "507": "Panama",
        "508": "Saint Pierre", "509": "Haiti",
        "590": "Guadeloupe", "591": "Bolivia", "592": "Guyana",
        "593": "Ecuador", "594": "French Guiana", "595": "Paraguay",
        "596": "Martinique", "597": "Suriname", "598": "Uruguay",
        "599": "Curaçao",
        "670": "East Timor", "672": "Norfolk Island",
        "673": "Brunei", "674": "Nauru", "675": "Papua New Guinea",
        "676": "Tonga", "677": "Solomon Islands", "678": "Vanuatu",
        "679": "Fiji", "680": "Palau", "681": "Wallis",
        "682": "Cook Islands", "683": "Niue", "685": "Samoa",
        "686": "Kiribati", "687": "New Caledonia", "688": "Tuvalu",
        "689": "French Polynesia", "690": "Tokelau",
        "691": "Micronesia", "692": "Marshall Islands",
        "850": "North Korea", "852": "Hong Kong", "853": "Macau",
        "855": "Cambodia", "856": "Laos",
        "880": "Bangladesh", "886": "Taiwan",
        "960": "Maldives", "961": "Lebanon", "962": "Jordan",
        "963": "Syria", "964": "Iraq", "965": "Kuwait",
        "966": "Saudi Arabia", "967": "Yemen", "968": "Oman",
        "970": "Palestine", "971": "UAE", "972": "Israel",
        "973": "Bahrain", "974": "Qatar", "975": "Bhutan",
        "976": "Mongolia", "977": "Nepal",
        "992": "Tajikistan", "993": "Turkmenistan",
        "994": "Azerbaijan", "995": "Georgia", "996": "Kyrgyzstan",
        "998": "Uzbekistan",
    }
    members = []
    for code, country in codes.items():
        members.append(code)
        members.append(f"+{code}")
        members.append(f"+{code} ({country})")
    return members


def build_fips_state_codes():
    """FIPS state codes (2-digit, with leading zeros)."""
    fips = {
        "01": "Alabama", "02": "Alaska", "04": "Arizona", "05": "Arkansas",
        "06": "California", "08": "Colorado", "09": "Connecticut",
        "10": "Delaware", "11": "District of Columbia", "12": "Florida",
        "13": "Georgia", "15": "Hawaii", "16": "Idaho", "17": "Illinois",
        "18": "Indiana", "19": "Iowa", "20": "Kansas", "21": "Kentucky",
        "22": "Louisiana", "23": "Maine", "24": "Maryland",
        "25": "Massachusetts", "26": "Michigan", "27": "Minnesota",
        "28": "Mississippi", "29": "Missouri", "30": "Montana",
        "31": "Nebraska", "32": "Nevada", "33": "New Hampshire",
        "34": "New Jersey", "35": "New Mexico", "36": "New York",
        "37": "North Carolina", "38": "North Dakota", "39": "Ohio",
        "40": "Oklahoma", "41": "Oregon", "42": "Pennsylvania",
        "44": "Rhode Island", "45": "South Carolina",
        "46": "South Dakota", "47": "Tennessee", "48": "Texas",
        "49": "Utah", "50": "Vermont", "51": "Virginia",
        "53": "Washington", "54": "West Virginia",
        "55": "Wisconsin", "56": "Wyoming",
        "60": "American Samoa", "66": "Guam",
        "69": "Northern Mariana Islands", "72": "Puerto Rico",
        "78": "US Virgin Islands",
    }
    members = []
    for code, name in fips.items():
        members.append(code)
        members.append(f"FIPS {code}")
        members.append(f"{code} {name}")
    return members


def build_http_status_codes():
    """HTTP status codes as strings."""
    codes = {
        "100": "Continue", "101": "Switching Protocols",
        "200": "OK", "201": "Created", "202": "Accepted",
        "204": "No Content", "206": "Partial Content",
        "301": "Moved Permanently", "302": "Found",
        "303": "See Other", "304": "Not Modified",
        "307": "Temporary Redirect", "308": "Permanent Redirect",
        "400": "Bad Request", "401": "Unauthorized", "403": "Forbidden",
        "404": "Not Found", "405": "Method Not Allowed",
        "408": "Request Timeout", "409": "Conflict", "410": "Gone",
        "413": "Payload Too Large", "415": "Unsupported Media Type",
        "422": "Unprocessable Entity", "429": "Too Many Requests",
        "500": "Internal Server Error", "501": "Not Implemented",
        "502": "Bad Gateway", "503": "Service Unavailable",
        "504": "Gateway Timeout",
    }
    members = []
    for code, desc in codes.items():
        members.append(code)
        members.append(f"{code} {desc}")
        members.append(desc)
    return members


def build_iso_4217_currency_codes():
    """ISO 4217 currency codes (3-letter)."""
    codes = [
        "AED", "AFN", "ALL", "AMD", "ANG", "AOA", "ARS", "AUD", "AWG", "AZN",
        "BAM", "BBD", "BDT", "BGN", "BHD", "BIF", "BMD", "BND", "BOB", "BRL",
        "BSD", "BTN", "BWP", "BYN", "BZD", "CAD", "CDF", "CHF", "CLP", "CNY",
        "COP", "CRC", "CUP", "CVE", "CZK", "DJF", "DKK", "DOP", "DZD", "EGP",
        "ERN", "ETB", "EUR", "FJD", "FKP", "GBP", "GEL", "GHS", "GIP", "GMD",
        "GNF", "GTQ", "GYD", "HKD", "HNL", "HRK", "HTG", "HUF", "IDR", "ILS",
        "INR", "IQD", "IRR", "ISK", "JMD", "JOD", "JPY", "KES", "KGS", "KHR",
        "KMF", "KPW", "KRW", "KWD", "KYD", "KZT", "LAK", "LBP", "LKR", "LRD",
        "LSL", "LYD", "MAD", "MDL", "MGA", "MKD", "MMK", "MNT", "MOP", "MRU",
        "MUR", "MVR", "MWK", "MXN", "MYR", "MZN", "NAD", "NGN", "NIO", "NOK",
        "NPR", "NZD", "OMR", "PAB", "PEN", "PGK", "PHP", "PKR", "PLN", "PYG",
        "QAR", "RON", "RSD", "RUB", "RWF", "SAR", "SBD", "SCR", "SDG", "SEK",
        "SGD", "SHP", "SLE", "SOS", "SRD", "SSP", "STN", "SVC", "SYP", "SZL",
        "THB", "TJS", "TMT", "TND", "TOP", "TRY", "TTD", "TWD", "TZS", "UAH",
        "UGX", "USD", "UYU", "UZS", "VES", "VND", "VUV", "WST", "XAF", "XCD",
        "XOF", "XPF", "YER", "ZAR", "ZMW", "ZWL",
        # Special codes
        "XAU", "XAG", "XPT", "XPD",  # precious metals
        "XDR", "XBA", "XBB", "XBC", "XBD",  # IMF/composite
    ]
    return codes


def build_iata_codes():
    """Common IATA airport codes (3-letter)."""
    # Top 100 busiest + notable airports
    codes = [
        "ATL", "DFW", "DEN", "ORD", "LAX", "CLT", "MCO", "LAS", "PHX", "MIA",
        "SEA", "IAH", "JFK", "EWR", "SFO", "FLL", "MSP", "BOS", "DTW", "LGA",
        "PHL", "SLC", "DCA", "SAN", "BWI", "TPA", "AUS", "IAD", "BNA", "MDW",
        "DAL", "HNL", "STL", "MSY", "RDU", "SMF", "SJC", "OAK", "CLE", "SAT",
        "RSW", "PIT", "IND", "CVG", "CMH", "MCI", "PDX", "ABQ", "ONT", "BUF",
        # International
        "LHR", "CDG", "FRA", "AMS", "MAD", "BCN", "FCO", "MUC", "ZRH", "VIE",
        "CPH", "OSL", "ARN", "HEL", "DUB", "BRU", "LIS", "ATH", "IST", "WAW",
        "PRG", "BUD", "GVA", "MAN", "EDI", "NCE",
        "HND", "NRT", "ICN", "PEK", "PVG", "HKG", "SIN", "BKK", "KUL", "CGK",
        "DEL", "BOM", "SYD", "MEL", "AKL",
        "DXB", "DOH", "AUH", "JED", "RUH", "TLV", "CAI",
        "GRU", "MEX", "BOG", "LIM", "SCL", "EZE", "GIG",
        "JNB", "CPT", "NBO", "LOS", "ADD",
        "YYZ", "YVR", "YUL", "YOW", "YEG", "YYC",
    ]
    return codes


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

    # Fetch IANA TLDs
    try:
        tlds = fetch_iana_tlds()
    except Exception as e:
        print(f"  TLD fetch error: {e}")
        tlds = []

    # CIK keys from the company tickers file (10-digit, zero-padded)
    print("  Loading EDGAR CIK keys...")
    with open("/tmp/company_tickers.json") as f:
        tickers_data = json.load(f)
    cik_members = list(set(
        str(v["cik_str"]).zfill(10) for v in tickers_data.values()
    ))
    # Also add without leading zeros (common in practice)
    cik_members += list(set(str(v["cik_str"]) for v in tickers_data.values()))
    cik_members = list(set(cik_members))
    print(f"    {len(cik_members)} CIK keys")

    domains = {
        "edgar_cik": ("SEC EDGAR Central Index Keys (10-digit, with and without leading zeros)", "sec:edgar", cik_members),
        "phone_country_codes": ("ITU-T telephone country calling codes", "curated:itu", build_phone_country_codes()),
        "fips_state_codes": ("US FIPS state/territory codes (2-digit with leading zeros)", "curated:census", build_fips_state_codes()),
        "http_status_codes": ("HTTP response status codes", "curated:rfc7231", build_http_status_codes()),
        "iso_4217_currencies": ("ISO 4217 currency codes (3-letter)", "curated:iso", build_iso_4217_currency_codes()),
        "iata_airport_codes": ("IATA 3-letter airport codes", "curated:iata", build_iata_codes()),
        "tld_domains": ("IANA top-level domains (with and without dot prefix)", "curated:iana", tlds),
    }

    for domain_name, (desc, source, members) in domains.items():
        if domain_name in existing:
            print(f"  SKIP {domain_name}")
            continue
        if not members:
            print(f"  SKIP {domain_name} (empty)")
            continue
        members = list(set(members))  # dedupe
        print(f"  {domain_name}: {len(members)} members")
        duck.execute("INSERT INTO pg.domain.enumeration (domain_name, domain_label, source, member_count) VALUES (?, ?, ?, ?)",
                     [domain_name, desc, source, len(members)])
        duck.executemany("INSERT INTO pg.domain.member (domain_name, label) VALUES (?, ?)",
                         [(domain_name, m) for m in members])
        total_new += len(members)

    # Rebuild filters
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

    duck.close()


if __name__ == "__main__":
    main()
