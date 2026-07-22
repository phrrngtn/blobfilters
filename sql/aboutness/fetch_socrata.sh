#!/usr/bin/env bash
# Fetch the public Socrata open-data samples used by the aboutness recipes.
# Clean-IP: all public open-data (City of Chicago) + a public authority file.
# No credentials, no day-job data. Run from this directory.
set -euo pipefail

B="https://data.cityofchicago.org/resource"

# Crimes: community_area, ward, district (zero-padded "008"), beat
curl -s --max-time 60 "$B/ijzp-q8t2.csv?\$select=community_area,ward,district,beat&\$limit=20000" -o crimes.csv

# Business Licenses: community_area, ward, police_district ("8"), zip_code, state, city
curl -s --max-time 60 "$B/r5kz-chrr.csv?\$select=community_area,ward,police_district,zip_code,state,city&\$limit=20000" -o licenses.csv

# Building Permits: community_area, ward
curl -s --max-time 60 "$B/ydr8-5enu.csv?\$select=community_area,ward&\$limit=20000" -o permits.csv

# Public US-states authority file (USPS 2-letter, 50 states + DC).
curl -s --max-time 25 "https://raw.githubusercontent.com/jasonong/List-of-US-States/master/states.csv" -o us_states.csv

echo "fetched: crimes.csv licenses.csv permits.csv us_states.csv"
