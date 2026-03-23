-- edgar_domains.sql — Reified version of edgar_domains.py
--
-- Fetches SEC EDGAR company data using blobhttp macros instead of
-- Python urllib.  Compare with edgar_domains.py for the imperative version.
--
-- Requires: blobhttp extension, blobfilters extension, postgres scanner
--
-- SEC rate limit: 10 requests/second with User-Agent identifying the caller.
-- See: https://www.sec.gov/os/accessing-edgar-data
--
-- These are hand-coded macros.  The next step is to register them as
-- entries in the blobapi adapter registry (llm_adapter table pattern)
-- so they can be discovered and composed like LLM adapters.
-- See: ~/checkouts/blobapi/sql/create_llm_adapter.sql
-- See: ~/checkouts/blobapi/adapters/*.yaml

-- ═══════════════════════════════════════════════════════════════════
-- Rate-limit and auth config for SEC endpoints
-- ═══════════════════════════════════════════════════════════════════

SET VARIABLE bh_http_config = MAP {
    'default':                '{"timeout": 30}',
    'https://data.sec.gov/':  '{"rate_limit": "10/s", "timeout": 30}',
    'https://www.sec.gov/':   '{"rate_limit": "10/s", "timeout": 30}',
    'https://efts.sec.gov/':  '{"rate_limit": "10/s", "timeout": 30}'
};

-- Common headers — SEC requires a User-Agent with contact info
SET VARIABLE edgar_headers = MAP {
    'User-Agent': 'blobboxes-domain-builder/0.1 phrrngtn@panix.com',
    'Accept':     'application/json'
};

-- ═══════════════════════════════════════════════════════════════════
-- Macro 1: All SEC-registered companies (bulk download, ~10K rows)
--
-- Python equivalent (edgar_domains.py lines 120-130):
--   resp = urllib.request.urlopen("https://www.sec.gov/files/company_tickers.json")
--   tickers_data = json.load(resp)
--   for v in tickers_data.values():
--       companies.append({"cik": v["cik_str"], "ticker": v["ticker"], "name": v["title"]})
-- ═══════════════════════════════════════════════════════════════════

CREATE OR REPLACE MACRO edgar_company_tickers() AS TABLE (
    WITH RAW AS (
        SELECT (bh_http_get(
            'https://www.sec.gov/files/company_tickers.json',
            headers := getvariable('edgar_headers')
        )).response_body AS body
    ),
    -- The JSON is {"0": {"cik_str": ..., "ticker": ..., "title": ...}, "1": {...}, ...}
    -- Unnest the values (ignore the numeric keys)
    ENTRIES AS (
        SELECT unnest(from_json(body::JSON, '["json"]')) AS entry
        FROM RAW
    )
    SELECT entry->>'cik_str' AS cik,
           entry->>'ticker'  AS ticker,
           entry->>'title'   AS name
    FROM ENTRIES
);

-- Usage:
--   SELECT * FROM edgar_company_tickers() LIMIT 10;
--   SELECT count(*) FROM edgar_company_tickers();  -- ~10,442

-- ═══════════════════════════════════════════════════════════════════
-- Macro 2: Company submission details (per-CIK lookup)
--
-- Returns name, SIC code, tickers, state of incorporation, etc.
-- for a single company identified by its 10-digit zero-padded CIK.
--
-- Python equivalent (edgar_domains.py lines 150-160):
--   data = fetch_sec(f"https://data.sec.gov/submissions/CIK{cik}.json")
--   detail = {"name": data["name"], "sic": data.get("sic", ""), ...}
-- ═══════════════════════════════════════════════════════════════════

CREATE OR REPLACE MACRO edgar_submission(cik_padded) AS TABLE (
    WITH RAW AS (
        SELECT (bh_http_get(
            'https://data.sec.gov/submissions/CIK' || cik_padded || '.json',
            headers := getvariable('edgar_headers')
        )).response_body::JSON AS doc
    )
    SELECT doc->>'cik'                    AS cik,
           doc->>'name'                   AS name,
           doc->>'sic'                    AS sic,
           doc->>'sicDescription'         AS sic_description,
           doc->>'stateOfIncorporation'   AS state,
           doc->>'fiscalYearEnd'          AS fiscal_year_end,
           doc->>'category'              AS filer_category,
           doc->'tickers'                AS tickers_json,
           doc->'exchanges'              AS exchanges_json
    FROM RAW
);

-- Usage:
--   SELECT * FROM edgar_submission('0000913144');  -- RenaissanceRe
--   SELECT * FROM edgar_submission('0000320193');  -- Apple

-- ═══════════════════════════════════════════════════════════════════
-- Macro 3: All companies in a given SIC code
--
-- Composes macros 1 + 2: get the ticker list, then look up each
-- company's SIC via the submissions API.  Data-driven HTTP calls —
-- one per company, rate-limited by bh_http_config.
--
-- Python equivalent: the REINSURER_CIKS loop in edgar_domains.py
-- ═══════════════════════════════════════════════════════════════════

CREATE OR REPLACE MACRO edgar_companies_by_sic(target_sic) AS TABLE (
    -- Step 1: get all companies
    -- Step 2: for each, fetch submission to get SIC
    -- Step 3: filter to target SIC
    --
    -- NOTE: this makes ~10K HTTP calls (one per company).
    -- In practice, you'd use the bulk submissions ZIP instead.
    -- This macro demonstrates the pattern; for production use,
    -- see the bulk approach below.
    SELECT s.*
    FROM edgar_company_tickers() AS t,
         LATERAL edgar_submission(LPAD(t.cik, 10, '0')) AS s
    WHERE s.sic = target_sic
);

-- ═══════════════════════════════════════════════════════════════════
-- Practical approach: bulk SIC lookup from the tickers file
-- (no per-company HTTP calls needed for just names + tickers)
-- ═══════════════════════════════════════════════════════════════════

-- For building domain filters, we usually just need names + tickers:
--
--   CREATE TABLE public_company_names AS
--   SELECT DISTINCT name AS label FROM edgar_company_tickers()
--   UNION
--   SELECT DISTINCT ticker FROM edgar_company_tickers();
--
-- Then build the filter:
--
--   SELECT bf_to_base64(
--       bf_build_json_normalized(json_group_array(label))
--   ) AS filter_b64
--   FROM public_company_names;

-- ═══════════════════════════════════════════════════════════════════
-- Example: look up a few known reinsurers and show their details
-- ═══════════════════════════════════════════════════════════════════

-- SELECT * FROM edgar_submission('0000913144');  -- RenaissanceRe
-- SELECT * FROM edgar_submission('0001095073');  -- Everest Re
-- SELECT * FROM edgar_submission('0000082811');  -- Berkshire Hathaway
