/*
 * SQLite extension for roaring bitmap fingerprinting
 *
 * Functions:
 *   roaring_build_json(json_array TEXT) -> BLOB
 *   roaring_cardinality(blob BLOB) -> INTEGER
 *   roaring_intersection_card(a BLOB, b BLOB) -> INTEGER
 *   roaring_containment(probe BLOB, ref BLOB) -> REAL
 *   roaring_jaccard(a BLOB, b BLOB) -> REAL
 *   roaring_to_base64(blob BLOB) -> TEXT
 *   roaring_from_base64(text TEXT) -> BLOB
 *   roaring_probe_json(probe_blob BLOB, refs_json TEXT) -> TEXT
 */

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include "roaring_fp.h"

#include <cstdlib>
#include <cstring>

/* ========================================================================
 * roaring_build_json(json_array TEXT) -> BLOB
 * ======================================================================== */

static void sqlite_roaring_build_json(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 1 || sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    const char *json = reinterpret_cast<const char *>(sqlite3_value_text(argv[0]));
    int json_len = sqlite3_value_bytes(argv[0]);

    rfp_bitmap *bm = rfp_create();
    if (rfp_add_json_array(bm, json, static_cast<size_t>(json_len)) != 0) {
        rfp_free(bm);
        sqlite3_result_error(ctx, "Invalid JSON array", -1);
        return;
    }

    size_t size = rfp_serialized_size(bm);
    void *buf = sqlite3_malloc64(static_cast<sqlite3_int64>(size));
    if (!buf) {
        rfp_free(bm);
        sqlite3_result_error_nomem(ctx);
        return;
    }
    rfp_serialize(bm, static_cast<char *>(buf), size);
    rfp_free(bm);

    sqlite3_result_blob(ctx, buf, static_cast<int>(size), sqlite3_free);
}

/* ========================================================================
 * roaring_cardinality(blob BLOB) -> INTEGER
 * ======================================================================== */

static void sqlite_roaring_cardinality(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 1 || sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    const char *data = static_cast<const char *>(sqlite3_value_blob(argv[0]));
    int len = sqlite3_value_bytes(argv[0]);

    rfp_bitmap *bm = rfp_deserialize(data, static_cast<size_t>(len));
    if (!bm) {
        sqlite3_result_int64(ctx, 0);
        return;
    }

    sqlite3_result_int64(ctx, static_cast<sqlite3_int64>(rfp_cardinality(bm)));
    rfp_free(bm);
}

/* ========================================================================
 * roaring_intersection_card(a BLOB, b BLOB) -> INTEGER
 * ======================================================================== */

static void sqlite_roaring_intersection_card(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 2 || sqlite3_value_type(argv[0]) == SQLITE_NULL || sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    rfp_bitmap *a = rfp_deserialize(
        static_cast<const char *>(sqlite3_value_blob(argv[0])),
        static_cast<size_t>(sqlite3_value_bytes(argv[0])));
    rfp_bitmap *b = rfp_deserialize(
        static_cast<const char *>(sqlite3_value_blob(argv[1])),
        static_cast<size_t>(sqlite3_value_bytes(argv[1])));

    if (!a || !b) {
        rfp_free(a);
        rfp_free(b);
        sqlite3_result_int64(ctx, 0);
        return;
    }

    sqlite3_result_int64(ctx, static_cast<sqlite3_int64>(rfp_intersection_card(a, b)));
    rfp_free(a);
    rfp_free(b);
}

/* ========================================================================
 * roaring_containment(probe BLOB, ref BLOB) -> REAL
 * ======================================================================== */

static void sqlite_roaring_containment(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 2 || sqlite3_value_type(argv[0]) == SQLITE_NULL || sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    rfp_bitmap *probe = rfp_deserialize(
        static_cast<const char *>(sqlite3_value_blob(argv[0])),
        static_cast<size_t>(sqlite3_value_bytes(argv[0])));
    rfp_bitmap *ref = rfp_deserialize(
        static_cast<const char *>(sqlite3_value_blob(argv[1])),
        static_cast<size_t>(sqlite3_value_bytes(argv[1])));

    if (!probe || !ref) {
        rfp_free(probe);
        rfp_free(ref);
        sqlite3_result_double(ctx, 0.0);
        return;
    }

    sqlite3_result_double(ctx, rfp_containment(probe, ref));
    rfp_free(probe);
    rfp_free(ref);
}

/* ========================================================================
 * roaring_jaccard(a BLOB, b BLOB) -> REAL
 * ======================================================================== */

static void sqlite_roaring_jaccard(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 2 || sqlite3_value_type(argv[0]) == SQLITE_NULL || sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    rfp_bitmap *a = rfp_deserialize(
        static_cast<const char *>(sqlite3_value_blob(argv[0])),
        static_cast<size_t>(sqlite3_value_bytes(argv[0])));
    rfp_bitmap *b = rfp_deserialize(
        static_cast<const char *>(sqlite3_value_blob(argv[1])),
        static_cast<size_t>(sqlite3_value_bytes(argv[1])));

    if (!a || !b) {
        rfp_free(a);
        rfp_free(b);
        sqlite3_result_double(ctx, 0.0);
        return;
    }

    sqlite3_result_double(ctx, rfp_jaccard(a, b));
    rfp_free(a);
    rfp_free(b);
}

/* ========================================================================
 * roaring_to_base64(blob BLOB) -> TEXT
 * ======================================================================== */

static void sqlite_roaring_to_base64(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 1 || sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    rfp_bitmap *bm = rfp_deserialize(
        static_cast<const char *>(sqlite3_value_blob(argv[0])),
        static_cast<size_t>(sqlite3_value_bytes(argv[0])));
    if (!bm) {
        sqlite3_result_null(ctx);
        return;
    }

    size_t b64_size = rfp_base64_size(bm);
    char *buf = static_cast<char *>(sqlite3_malloc64(static_cast<sqlite3_int64>(b64_size + 1)));
    if (!buf) {
        rfp_free(bm);
        sqlite3_result_error_nomem(ctx);
        return;
    }
    rfp_to_base64(bm, buf, b64_size);
    buf[b64_size] = '\0';
    rfp_free(bm);

    sqlite3_result_text(ctx, buf, static_cast<int>(b64_size), sqlite3_free);
}

/* ========================================================================
 * roaring_from_base64(text TEXT) -> BLOB
 * ======================================================================== */

static void sqlite_roaring_from_base64(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 1 || sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    const char *b64 = reinterpret_cast<const char *>(sqlite3_value_text(argv[0]));
    int b64_len = sqlite3_value_bytes(argv[0]);

    rfp_bitmap *bm = rfp_from_base64(b64, static_cast<size_t>(b64_len));
    if (!bm) {
        sqlite3_result_error(ctx, "Invalid base64 bitmap", -1);
        return;
    }

    size_t size = rfp_serialized_size(bm);
    void *buf = sqlite3_malloc64(static_cast<sqlite3_int64>(size));
    if (!buf) {
        rfp_free(bm);
        sqlite3_result_error_nomem(ctx);
        return;
    }
    rfp_serialize(bm, static_cast<char *>(buf), size);
    rfp_free(bm);

    sqlite3_result_blob(ctx, buf, static_cast<int>(size), sqlite3_free);
}

/* ========================================================================
 * roaring_containment_json(symbols_json TEXT, ref_blob BLOB) -> REAL
 * Builds a probe from JSON array of strings, returns containment vs ref.
 * ======================================================================== */

static void sqlite_roaring_containment_json(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 2 || sqlite3_value_type(argv[0]) == SQLITE_NULL || sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    const char *json = reinterpret_cast<const char *>(sqlite3_value_text(argv[0]));
    int json_len = sqlite3_value_bytes(argv[0]);

    rfp_bitmap *probe = rfp_create();
    if (rfp_add_json_array(probe, json, static_cast<size_t>(json_len)) != 0) {
        rfp_free(probe);
        sqlite3_result_error(ctx, "Invalid JSON array", -1);
        return;
    }

    rfp_bitmap *ref = rfp_deserialize(
        static_cast<const char *>(sqlite3_value_blob(argv[1])),
        static_cast<size_t>(sqlite3_value_bytes(argv[1])));
    if (!ref) {
        rfp_free(probe);
        sqlite3_result_double(ctx, 0.0);
        return;
    }

    sqlite3_result_double(ctx, rfp_containment(probe, ref));
    rfp_free(probe);
    rfp_free(ref);
}

/* ========================================================================
 * Extension entry point
 * ======================================================================== */

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_roaring_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
    SQLITE_EXTENSION_INIT2(pApi);
    (void)pzErrMsg;

    sqlite3_create_function(db, "roaring_build_json", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_roaring_build_json, nullptr, nullptr);
    sqlite3_create_function(db, "roaring_cardinality", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_roaring_cardinality, nullptr, nullptr);
    sqlite3_create_function(db, "roaring_intersection_card", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_roaring_intersection_card, nullptr, nullptr);
    sqlite3_create_function(db, "roaring_containment", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_roaring_containment, nullptr, nullptr);
    sqlite3_create_function(db, "roaring_jaccard", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_roaring_jaccard, nullptr, nullptr);
    sqlite3_create_function(db, "roaring_to_base64", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_roaring_to_base64, nullptr, nullptr);
    sqlite3_create_function(db, "roaring_from_base64", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_roaring_from_base64, nullptr, nullptr);
    sqlite3_create_function(db, "roaring_containment_json", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_roaring_containment_json, nullptr, nullptr);

    return SQLITE_OK;
}

#ifdef __cplusplus
}
#endif
