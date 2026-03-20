"""
Demo: domain detection with blobfilters Python bindings.

Build a few reference domain fingerprints, then probe a "wild" column
against them to see which domain best matches.

Usage:
    uv run python demo/python_demo.py
"""

import json
from blobfilters import RoaringFP, from_json, NORM_CASEFOLD

# -- 1. Build domain fingerprints from known reference data --------------------

us_states = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
    "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
    "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York",
    "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
    "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
    "West Virginia", "Wisconsin", "Wyoming",
]

currencies = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "INR", "BRL"]

colors = ["Red", "Blue", "Green", "Yellow", "Orange", "Purple", "Black", "White", "Pink", "Brown"]

domains = {
    "us_states": from_json(json.dumps(us_states)),
    "currencies": from_json(json.dumps(currencies)),
    "colors": from_json(json.dumps(colors)),
}

print("Domain fingerprints:")
for name, fp in domains.items():
    print(f"  {name}: {fp}")

# -- 2. Probe a wild column against all domains --------------------------------

wild_column = ["California", "Texas", "Florida", "New York", "Ohio", "unknown_value"]

print(f"\nWild column: {wild_column}")

probe = from_json(json.dumps(wild_column))
print(f"Probe bitmap: {probe}")

print("\nDomain scores:")
for name, domain_fp in domains.items():
    cont = probe.containment(domain_fp)
    jacc = probe.jaccard(domain_fp)
    hits = probe.intersection_card(domain_fp)
    print(f"  {name:12s}  containment={cont:.3f}  jaccard={jacc:.3f}  hits={hits}/{len(probe)}")

# -- 3. Build probe from JSON directly -----------------------------------------

print("\nJSON-based probe construction:")
probe2 = from_json(json.dumps(["USD", "EUR", "GBP", "mystery"]))
print(f"  Currency probe: {probe2}")
for name, domain_fp in domains.items():
    cont = probe2.containment(domain_fp)
    if cont > 0:
        print(f"  {name:12s}  containment={cont:.3f}")

# -- 4. Serialization round-trip -----------------------------------------------

print("\nSerialization:")
b64 = domains["us_states"].to_base64()
print(f"  us_states base64: {b64[:60]}...")

blob = domains["us_states"].serialize()
print(f"  us_states blob: {len(blob)} bytes")

# -- 5. Incremental domain building --------------------------------------------

print("\nIncremental build:")
custom = RoaringFP()
for val in ["alpha", "beta", "gamma"]:
    custom.add(val)
print(f"  After 3 adds: {custom}")

# Merge another domain into it
custom.or_inplace(domains["colors"])
print(f"  After OR with colors: {custom}")

# -- 6. Normalized vs raw hashing ---------------------------------------------

print("\n--- Normalized vs raw hashing ---")

# Build a domain with normalized hashing (NFKD + casefold)
states_norm = RoaringFP()
for s in us_states:
    states_norm.add_normalized(s, NORM_CASEFOLD)
print(f"\nus_states (normalized): {states_norm}")
print(f"us_states (raw):        {domains['us_states']}")

# Probe with messy case variants
messy_probe = ["CALIFORNIA", "texas", "FlOrIdA", "new york", "OHIO"]
print(f"\nMessy probe values: {messy_probe}")

# Raw probe against raw domain
raw_probe = RoaringFP()
for v in messy_probe:
    raw_probe.add(v)
print(f"  Raw probe vs raw domain:    containment={raw_probe.containment(domains['us_states']):.3f}")

# Normalized probe against normalized domain
norm_probe = RoaringFP()
for v in messy_probe:
    norm_probe.add_normalized(v, NORM_CASEFOLD)
print(f"  Norm probe vs norm domain:  containment={norm_probe.containment(states_norm):.3f}")

# Accented characters
print("\nAccent handling:")
accent_domain = RoaringFP()
for name in ["Jose", "Munoz", "Gomez", "Garcia", "Lopez"]:
    accent_domain.add_normalized(name, NORM_CASEFOLD)

accent_probe = RoaringFP()
for name in ["Jos\u00e9", "Mu\u00f1oz", "G\u00f3mez", "Garc\u00eda", "L\u00f3pez"]:
    accent_probe.add_normalized(name, NORM_CASEFOLD)

print(f"  Domain (ascii):  {accent_domain}")
print(f"  Probe (accented): {accent_probe}")
print(f"  Containment:      {accent_probe.containment(accent_domain):.3f}")

# Same test raw — should be 0.0
raw_domain = RoaringFP()
for name in ["Jose", "Munoz", "Gomez", "Garcia", "Lopez"]:
    raw_domain.add(name)
raw_accent = RoaringFP()
for name in ["Jos\u00e9", "Mu\u00f1oz", "G\u00f3mez", "Garc\u00eda", "L\u00f3pez"]:
    raw_accent.add(name)
print(f"  Raw containment:  {raw_accent.containment(raw_domain):.3f}")
