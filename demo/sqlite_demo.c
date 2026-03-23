/*
 * SQLite Demo: Domain fingerprinting and cross-apply probing
 *
 * Creates domain tables with real symbol values, builds bitfields into a
 * unified fingerprint table, then probes a "wild" collection of symbols
 * against all domains via CROSS JOIN.
 */

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Our extension init — linked statically */
extern int sqlite3_roaring_init(sqlite3 *db, char **pzErrMsg,
                                const sqlite3_api_routines *pApi);

static void exec(sqlite3 *db, const char *sql, const char *label) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "ERROR [%s]: %s\n", label, err);
        sqlite3_free(err);
    }
}

static void query_print(sqlite3 *db, const char *sql, const char *label) {
    sqlite3_stmt *stmt;
    printf("\n-- %s\n", label);

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "ERROR: %s\n", sqlite3_errmsg(db));
        return;
    }

    int ncols = sqlite3_column_count(stmt);

    /* Print header */
    for (int c = 0; c < ncols; c++) {
        printf("%-22s", sqlite3_column_name(stmt, c));
    }
    printf("\n");
    for (int c = 0; c < ncols; c++) {
        printf("----------------------");
    }
    printf("\n");

    /* Print rows */
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        for (int c = 0; c < ncols; c++) {
            int type = sqlite3_column_type(stmt, c);
            switch (type) {
            case SQLITE_INTEGER:
                printf("%-22lld", (long long)sqlite3_column_int64(stmt, c));
                break;
            case SQLITE_FLOAT:
                printf("%-22.4f", sqlite3_column_double(stmt, c));
                break;
            case SQLITE_TEXT:
                printf("%-22s", (const char *)sqlite3_column_text(stmt, c));
                break;
            case SQLITE_BLOB:
                printf("%-22s", "<BLOB>");
                break;
            default:
                printf("%-22s", "NULL");
                break;
            }
        }
        printf("\n");
    }
    sqlite3_finalize(stmt);
}

int main(void) {
    sqlite3 *db;
    if (sqlite3_open(":memory:", &db) != SQLITE_OK) {
        fprintf(stderr, "Failed to open database\n");
        return 1;
    }

    /* Register our roaring functions */
    sqlite3_roaring_init(db, NULL, NULL);

    printf("=================================================================\n");
    printf("  SQLite Domain Fingerprint Demo\n");
    printf("=================================================================\n");

    /*
     * ===============================================================
     * STEP 1: Create domain tables with real symbols
     * ===============================================================
     */
    printf("\n>>> STEP 1: Create domain tables with symbol values\n");

    /* US States */
    exec(db,
        "CREATE TABLE domain_us_states (symbol TEXT);\n"
        "INSERT INTO domain_us_states VALUES\n"
        "  ('Alabama'),('Alaska'),('Arizona'),('Arkansas'),('California'),\n"
        "  ('Colorado'),('Connecticut'),('Delaware'),('Florida'),('Georgia'),\n"
        "  ('Hawaii'),('Idaho'),('Illinois'),('Indiana'),('Iowa'),\n"
        "  ('Kansas'),('Kentucky'),('Louisiana'),('Maine'),('Maryland'),\n"
        "  ('Massachusetts'),('Michigan'),('Minnesota'),('Mississippi'),('Missouri'),\n"
        "  ('Montana'),('Nebraska'),('Nevada'),('New Hampshire'),('New Jersey'),\n"
        "  ('New Mexico'),('New York'),('North Carolina'),('North Dakota'),('Ohio'),\n"
        "  ('Oklahoma'),('Oregon'),('Pennsylvania'),('Rhode Island'),('South Carolina'),\n"
        "  ('South Dakota'),('Tennessee'),('Texas'),('Utah'),('Vermont'),\n"
        "  ('Virginia'),('Washington'),('West Virginia'),('Wisconsin'),('Wyoming');",
        "create us_states");

    /* ISO Currency Codes */
    exec(db,
        "CREATE TABLE domain_currencies (symbol TEXT);\n"
        "INSERT INTO domain_currencies VALUES\n"
        "  ('USD'),('EUR'),('GBP'),('JPY'),('CHF'),('CAD'),('AUD'),('NZD'),\n"
        "  ('CNY'),('HKD'),('SGD'),('KRW'),('INR'),('BRL'),('MXN'),('ZAR'),\n"
        "  ('SEK'),('NOK'),('DKK'),('PLN'),('CZK'),('HUF'),('TRY'),('RUB'),\n"
        "  ('THB'),('MYR'),('IDR'),('PHP'),('TWD'),('ILS'),('AED'),('SAR'),\n"
        "  ('CLP'),('COP'),('PEN'),('ARS'),('EGP'),('NGN'),('KES'),('GHS');",
        "create currencies");

    /* HTTP Status Codes */
    exec(db,
        "CREATE TABLE domain_http_status (symbol TEXT);\n"
        "INSERT INTO domain_http_status VALUES\n"
        "  ('200 OK'),('201 Created'),('204 No Content'),\n"
        "  ('301 Moved Permanently'),('302 Found'),('304 Not Modified'),\n"
        "  ('400 Bad Request'),('401 Unauthorized'),('403 Forbidden'),\n"
        "  ('404 Not Found'),('405 Method Not Allowed'),('408 Request Timeout'),\n"
        "  ('409 Conflict'),('413 Payload Too Large'),('415 Unsupported Media Type'),\n"
        "  ('429 Too Many Requests'),\n"
        "  ('500 Internal Server Error'),('502 Bad Gateway'),\n"
        "  ('503 Service Unavailable'),('504 Gateway Timeout');",
        "create http_status");

    /* MIME Types */
    exec(db,
        "CREATE TABLE domain_mime_types (symbol TEXT);\n"
        "INSERT INTO domain_mime_types VALUES\n"
        "  ('text/html'),('text/css'),('text/plain'),('text/csv'),\n"
        "  ('application/json'),('application/xml'),('application/pdf'),\n"
        "  ('application/zip'),('application/gzip'),\n"
        "  ('application/octet-stream'),('application/javascript'),\n"
        "  ('image/png'),('image/jpeg'),('image/gif'),('image/svg+xml'),\n"
        "  ('image/webp'),('audio/mpeg'),('audio/ogg'),('video/mp4'),\n"
        "  ('video/webm'),('multipart/form-data'),\n"
        "  ('application/x-www-form-urlencoded');",
        "create mime_types");

    /* Programming Languages */
    exec(db,
        "CREATE TABLE domain_languages (symbol TEXT);\n"
        "INSERT INTO domain_languages VALUES\n"
        "  ('Python'),('JavaScript'),('TypeScript'),('Java'),('C'),('C++'),\n"
        "  ('C#'),('Go'),('Rust'),('Swift'),('Kotlin'),('Ruby'),('PHP'),\n"
        "  ('Scala'),('Haskell'),('Elixir'),('Clojure'),('Lua'),('R'),\n"
        "  ('Julia'),('Dart'),('Perl'),('MATLAB'),('SQL'),('Shell'),\n"
        "  ('Assembly'),('Fortran'),('COBOL'),('Erlang'),('OCaml');",
        "create languages");

    /* Chemical Elements (first 30) */
    exec(db,
        "CREATE TABLE domain_elements (symbol TEXT);\n"
        "INSERT INTO domain_elements VALUES\n"
        "  ('Hydrogen'),('Helium'),('Lithium'),('Beryllium'),('Boron'),\n"
        "  ('Carbon'),('Nitrogen'),('Oxygen'),('Fluorine'),('Neon'),\n"
        "  ('Sodium'),('Magnesium'),('Aluminium'),('Silicon'),('Phosphorus'),\n"
        "  ('Sulfur'),('Chlorine'),('Argon'),('Potassium'),('Calcium'),\n"
        "  ('Scandium'),('Titanium'),('Vanadium'),('Chromium'),('Manganese'),\n"
        "  ('Iron'),('Cobalt'),('Nickel'),('Copper'),('Zinc');",
        "create elements");

    query_print(db,
        "SELECT 'us_states' AS domain, COUNT(*) AS n FROM domain_us_states\n"
        "UNION ALL SELECT 'currencies', COUNT(*) FROM domain_currencies\n"
        "UNION ALL SELECT 'http_status', COUNT(*) FROM domain_http_status\n"
        "UNION ALL SELECT 'mime_types', COUNT(*) FROM domain_mime_types\n"
        "UNION ALL SELECT 'languages', COUNT(*) FROM domain_languages\n"
        "UNION ALL SELECT 'elements', COUNT(*) FROM domain_elements;",
        "Domain sizes");

    /*
     * ===============================================================
     * STEP 2: Build fingerprints into a unified table
     *
     * For each domain, we SELECT all symbols as a JSON array, then
     * bf_build_json() hashes them into a single bitmap BLOB.
     * ===============================================================
     */
    printf("\n>>> STEP 2: Build domain fingerprints into unified table\n");

    exec(db,
        "CREATE TABLE domain_fingerprints (\n"
        "    domain_name  TEXT PRIMARY KEY,\n"
        "    symbol_count INTEGER,\n"
        "    fingerprint  BLOB\n"
        ");",
        "create fingerprints table");

    /* Use bf_build() aggregate — no JSON intermediate needed */
    exec(db,
        "INSERT INTO domain_fingerprints\n"
        "SELECT 'us_states', COUNT(*), bf_build(symbol)\n"
        "FROM domain_us_states;",
        "build us_states fingerprint");

    exec(db,
        "INSERT INTO domain_fingerprints\n"
        "SELECT 'currencies', COUNT(*), bf_build(symbol)\n"
        "FROM domain_currencies;",
        "build currencies fingerprint");

    exec(db,
        "INSERT INTO domain_fingerprints\n"
        "SELECT 'http_status', COUNT(*), bf_build(symbol)\n"
        "FROM domain_http_status;",
        "build http_status fingerprint");

    exec(db,
        "INSERT INTO domain_fingerprints\n"
        "SELECT 'mime_types', COUNT(*), bf_build(symbol)\n"
        "FROM domain_mime_types;",
        "build mime_types fingerprint");

    exec(db,
        "INSERT INTO domain_fingerprints\n"
        "SELECT 'languages', COUNT(*), bf_build(symbol)\n"
        "FROM domain_languages;",
        "build languages fingerprint");

    exec(db,
        "INSERT INTO domain_fingerprints\n"
        "SELECT 'elements', COUNT(*), bf_build(symbol)\n"
        "FROM domain_elements;",
        "build elements fingerprint");

    query_print(db,
        "SELECT domain_name,\n"
        "       symbol_count,\n"
        "       bf_cardinality(fingerprint) AS fp_cardinality,\n"
        "       LENGTH(fingerprint) AS blob_bytes\n"
        "FROM domain_fingerprints\n"
        "ORDER BY symbol_count DESC;",
        "Unified fingerprint table (note: cardinality matches symbol_count)");

    /*
     * ===============================================================
     * STEP 3: Probe with "wild" data — a messy collection of symbols
     *
     * Imagine someone pastes a column from a spreadsheet. It contains
     * a mix from several domains plus garbage.
     * ===============================================================
     */
    printf("\n>>> STEP 3: Probe \"wild\" symbols against all domain fingerprints\n");

    /*
     * The probe: 8 US states + 5 currencies + 3 HTTP codes + 2 languages
     *            + 4 garbage values = 22 symbols
     */
    const char *probe_json =
        "'[\"California\",\"Texas\",\"New York\",\"Florida\",\"Ohio\","
        "\"Illinois\",\"Georgia\",\"Oregon\","
        "\"USD\",\"EUR\",\"GBP\",\"JPY\",\"CHF\","
        "\"404 Not Found\",\"500 Internal Server Error\",\"200 OK\","
        "\"Python\",\"Rust\","
        "\"FooBar\",\"DEADBEEF\",\"xyzzy\",\"42\"]'";

    /*
     * Build a probe bitmap from the JSON, then CROSS JOIN against
     * all domain fingerprints. This is the key pattern:
     *
     *   WITH probe AS (SELECT bf_build_json(...) AS fp)
     *   SELECT d.domain_name,
     *          bf_intersection_card(probe.fp, d.fingerprint) AS hits,
     *          bf_containment(probe.fp, d.fingerprint) AS containment
     *   FROM domain_fingerprints d, probe
     *   ORDER BY containment DESC;
     */

    char sql[2048];
    snprintf(sql, sizeof(sql),
        "WITH probe AS (\n"
        "    SELECT bf_build_json(%s) AS fp\n"
        ")\n"
        "SELECT\n"
        "    d.domain_name,\n"
        "    d.symbol_count                                        AS domain_size,\n"
        "    bf_cardinality(probe.fp)                         AS probe_size,\n"
        "    bf_intersection_card(probe.fp, d.fingerprint)    AS est_hits,\n"
        "    bf_containment(probe.fp, d.fingerprint)          AS containment,\n"
        "    bf_jaccard(probe.fp, d.fingerprint)              AS jaccard\n"
        "FROM domain_fingerprints d, probe\n"
        "ORDER BY containment DESC;",
        probe_json);

    query_print(db, sql,
        "CROSS JOIN probe against all domains\n"
        "--   8 states + 5 currencies + 3 HTTP + 2 languages + 4 garbage = 22 symbols");

    /*
     * ===============================================================
     * STEP 4: Show it works the other way — containment_json()
     *         takes raw JSON without materializing the bitmap first
     * ===============================================================
     */
    printf("\n>>> STEP 4: Same query using bf_containment_json (JSON in, no CTE needed)\n");

    snprintf(sql, sizeof(sql),
        "SELECT\n"
        "    domain_name,\n"
        "    symbol_count                                          AS domain_size,\n"
        "    bf_containment_json(%s, fingerprint)             AS containment\n"
        "FROM domain_fingerprints\n"
        "ORDER BY containment DESC;",
        probe_json);

    query_print(db, sql, "Direct JSON probe — no CTE, no intermediate bitmap");

    /*
     * ===============================================================
     * STEP 5: A second probe — pure currencies
     * ===============================================================
     */
    printf("\n>>> STEP 5: Second probe — pure currency codes\n");

    query_print(db,
        "WITH probe AS (\n"
        "    SELECT bf_build_json(\n"
        "        '[\"USD\",\"EUR\",\"GBP\",\"JPY\",\"CHF\",\"CAD\",\"AUD\",\"NZD\"]'\n"
        "    ) AS fp\n"
        ")\n"
        "SELECT\n"
        "    d.domain_name,\n"
        "    d.symbol_count                                        AS domain_size,\n"
        "    bf_intersection_card(probe.fp, d.fingerprint)    AS est_hits,\n"
        "    bf_containment(probe.fp, d.fingerprint)          AS containment\n"
        "FROM domain_fingerprints d, probe\n"
        "ORDER BY containment DESC;",
        "8 currency codes — should match currencies at 100%");

    /*
     * ===============================================================
     * STEP 6: A third probe — total garbage (no matches)
     * ===============================================================
     */
    printf("\n>>> STEP 6: Third probe — pure noise\n");

    query_print(db,
        "WITH probe AS (\n"
        "    SELECT bf_build_json(\n"
        "        '[\"xyzzy\",\"plugh\",\"42\",\"DEADBEEF\",\"asdf\"]'\n"
        "    ) AS fp\n"
        ")\n"
        "SELECT\n"
        "    d.domain_name,\n"
        "    bf_intersection_card(probe.fp, d.fingerprint)    AS est_hits,\n"
        "    bf_containment(probe.fp, d.fingerprint)          AS containment\n"
        "FROM domain_fingerprints d, probe\n"
        "ORDER BY containment DESC;",
        "5 garbage values — should match nothing");

    printf("\n=================================================================\n");
    printf("  Demo complete.\n");
    printf("=================================================================\n");

    sqlite3_close(db);
    return 0;
}
