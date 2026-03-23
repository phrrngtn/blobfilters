-- expand_domains.sql — Reified version of expand_domains.py
--
-- Fetches domain members from Wikidata SPARQL using blobhttp macros.
-- Compare with expand_domains.py for the imperative Python version.
--
-- Requires: blobhttp extension, blobfilters extension, postgres scanner

-- ═══════════════════════════════════════════════════════════════════
-- Rate-limit config for Wikidata
-- ═══════════════════════════════════════════════════════════════════

SET VARIABLE bh_http_config = MAP {
    'default':                          '{"timeout": 30}',
    'https://query.wikidata.org/':      '{"rate_limit": "5/s", "timeout": 120}'
};

SET VARIABLE wikidata_headers = MAP {
    'User-Agent': 'blobboxes-domain-builder/0.1 (https://github.com/phrrngtn/blobboxes)',
    'Accept':     'application/sparql-results+json'
};

-- ═══════════════════════════════════════════════════════════════════
-- Core macro: execute a SPARQL query and return label + altLabel rows
--
-- Python equivalent (expand_domains.py lines 100-120):
--   params = urllib.parse.urlencode({"query": sparql, "format": "json"})
--   url = f"{WIKIDATA_ENDPOINT}?{params}"
--   req = urllib.request.Request(url, headers={...})
--   with urllib.request.urlopen(req, timeout=60) as resp:
--       data = json.loads(resp.read())
--   for binding in data["results"]["bindings"]:
--       label = binding.get("label", {}).get("value", "")
--       ...
-- ═══════════════════════════════════════════════════════════════════

CREATE OR REPLACE MACRO wikidata_sparql(sparql_query) AS TABLE (
    WITH RAW AS (
        SELECT (bh_http_get(
            'https://query.wikidata.org/sparql',
            headers := getvariable('wikidata_headers'),
            params  := json_object('query', sparql_query, 'format', 'json')
        )).response_body::JSON AS doc
    ),
    BINDINGS AS (
        SELECT unnest(from_json(doc->'results'->'bindings', '["json"]')) AS b
        FROM RAW
    )
    SELECT b->'label'->>'value'    AS label,
           b->'altLabel'->>'value' AS alt_label,
           b->'item'->>'value'     AS item_uri
    FROM BINDINGS
    WHERE b->'label'->>'value' IS NOT NULL
);

-- ═══════════════════════════════════════════════════════════════════
-- Domain-specific macros using wikidata_sparql
-- ═══════════════════════════════════════════════════════════════════

-- World cities with population > 100k
CREATE OR REPLACE MACRO wikidata_world_cities() AS TABLE (
    SELECT label, alt_label FROM wikidata_sparql('
        SELECT ?item ?label ?altLabel WHERE {
          ?item wdt:P31/wdt:P279* wd:Q515 .
          ?item wdt:P1082 ?pop . FILTER(?pop > 100000)
          ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
          OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
        }
    ')
);

-- Sovereign states with alt names
CREATE OR REPLACE MACRO wikidata_countries() AS TABLE (
    SELECT label, alt_label FROM wikidata_sparql('
        SELECT ?item ?label ?altLabel WHERE {
          ?item wdt:P31 wd:Q3624078 .
          ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
          OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
        }
    ')
);

-- Human diseases
CREATE OR REPLACE MACRO wikidata_diseases() AS TABLE (
    SELECT label, alt_label FROM wikidata_sparql('
        SELECT ?item ?label ?altLabel WHERE {
          ?item wdt:P31 wd:Q12136 .
          ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
          OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
        } LIMIT 5000
    ')
);

-- Insurance and reinsurance companies
CREATE OR REPLACE MACRO wikidata_insurance_companies() AS TABLE (
    SELECT label, alt_label FROM wikidata_sparql('
        SELECT ?item ?label ?altLabel WHERE {
          { ?item wdt:P31/wdt:P279* wd:Q6023791 . }
          UNION
          { ?item wdt:P31/wdt:P279* wd:Q2221906 . }
          ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
          OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
        } LIMIT 5000
    ')
);

-- Historical catastrophic events (with dates)
CREATE OR REPLACE MACRO wikidata_historical_disasters() AS TABLE (
    SELECT label, alt_label FROM wikidata_sparql('
        SELECT ?item ?label ?altLabel WHERE {
          { ?item wdt:P31/wdt:P279* wd:Q8065 . ?item wdt:P585 ?date . }
          UNION
          { ?item wdt:P31/wdt:P279* wd:Q8065 . ?item wdt:P580 ?date . }
          ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
          OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
        } LIMIT 10000
    ')
);

-- ═══════════════════════════════════════════════════════════════════
-- Usage: fetch domain members and build a normalized blobfilter
--
-- Python equivalent (expand_domains.py lines 130-150):
--   members = query_wikidata(config["sparql"])
--   duck.execute("INSERT INTO pg.domain.member ...")
--   ... bf_build_json_normalized(members_json) ...
-- ═══════════════════════════════════════════════════════════════════

-- Example: build a countries filter from Wikidata in one query
--
--   WITH MEMBERS AS (
--       SELECT DISTINCT label AS member FROM wikidata_countries()
--       UNION
--       SELECT DISTINCT alt_label FROM wikidata_countries()
--       WHERE alt_label IS NOT NULL
--   )
--   SELECT bf_to_base64(
--       bf_build_json_normalized(json_group_array(member))
--   ) AS filter_b64,
--   bf_cardinality(bf_build_json_normalized(json_group_array(member))) AS n_members
--   FROM MEMBERS;

-- Example: fetch and insert into PG in one pipeline
--
--   INSERT INTO pg.domain.member (domain_name, label)
--   SELECT 'world_cities', label FROM wikidata_world_cities()
--   UNION
--   SELECT 'world_cities', alt_label FROM wikidata_world_cities()
--   WHERE alt_label IS NOT NULL;
