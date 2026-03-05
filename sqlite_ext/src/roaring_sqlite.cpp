/*
 * SQLite extension for roaring bitmap fingerprinting
 *
 * Aggregate:
 *   roaring_build(value TEXT) -> BLOB    (hashes each value, builds bitmap)
 *
 * Scalar:
 *   roaring_build_json(json_array TEXT) -> BLOB
 *   roaring_cardinality(blob BLOB) -> INTEGER
 *   roaring_intersection_card(a BLOB, b BLOB) -> INTEGER
 *   roaring_containment(probe BLOB, ref BLOB) -> REAL
 *   roaring_jaccard(a BLOB, b BLOB) -> REAL
 *   roaring_to_base64(blob BLOB) -> TEXT
 *   roaring_from_base64(text TEXT) -> BLOB
 *   roaring_containment_json(symbols_json TEXT, ref BLOB) -> REAL
 */

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include "roaring_fp.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>

/* ========================================================================
 * roaring_build(value) -> BLOB   [aggregate]
 *
 * Hashes each non-NULL text value via FNV-1a and adds to a roaring bitmap.
 * Equivalent to DuckDB's roaring_build() aggregate.
 * ======================================================================== */

struct RoaringAggCtx {
    rfp_bitmap *bitmap;
};

static void sqlite_roaring_build_step(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 1 || sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        return;
    }

    auto *agg = static_cast<RoaringAggCtx *>(
        sqlite3_aggregate_context(ctx, sizeof(RoaringAggCtx)));
    if (!agg) return;

    if (!agg->bitmap) {
        agg->bitmap = rfp_create();
    }

    const void *data = sqlite3_value_text(argv[0]);
    int len = sqlite3_value_bytes(argv[0]);
    rfp_add_hash(agg->bitmap, data, static_cast<size_t>(len));
}

static void sqlite_roaring_build_final(sqlite3_context *ctx) {
    auto *agg = static_cast<RoaringAggCtx *>(
        sqlite3_aggregate_context(ctx, 0));

    if (!agg || !agg->bitmap) {
        /* No rows / all NULLs — return empty bitmap */
        rfp_bitmap *empty = rfp_create();
        size_t size = rfp_serialized_size(empty);
        void *buf = sqlite3_malloc64(static_cast<sqlite3_int64>(size));
        if (buf) {
            rfp_serialize(empty, static_cast<char *>(buf), size);
            sqlite3_result_blob(ctx, buf, static_cast<int>(size), sqlite3_free);
        } else {
            sqlite3_result_error_nomem(ctx);
        }
        rfp_free(empty);
        return;
    }

    size_t size = rfp_serialized_size(agg->bitmap);
    void *buf = sqlite3_malloc64(static_cast<sqlite3_int64>(size));
    if (!buf) {
        rfp_free(agg->bitmap);
        sqlite3_result_error_nomem(ctx);
        return;
    }
    rfp_serialize(agg->bitmap, static_cast<char *>(buf), size);
    rfp_free(agg->bitmap);
    agg->bitmap = nullptr;

    sqlite3_result_blob(ctx, buf, static_cast<int>(size), sqlite3_free);
}

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
 * roaring_build_histogram — two overloads:
 *   2-arg: (key TEXT, weight REAL) -> TEXT (JSON)       [source-agnostic]
 *   5-arg: (key, equal_rows, range_rows, distinct_range_rows, avg_range_rows)
 *                                                       [SQL Server convenience]
 * ======================================================================== */

struct HistogramAggCtx {
    rfp_histogram *hf;
};

static void sqlite_histogram_build_step_2(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 2 || sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        return;
    }

    auto *agg = static_cast<HistogramAggCtx *>(
        sqlite3_aggregate_context(ctx, sizeof(HistogramAggCtx)));
    if (!agg) return;

    if (!agg->hf) {
        agg->hf = rfp_histogram_create();
    }

    const char *key = reinterpret_cast<const char *>(sqlite3_value_text(argv[0]));
    int key_len = sqlite3_value_bytes(argv[0]);
    double weight = sqlite3_value_double(argv[1]);

    rfp_histogram_add_value(agg->hf, key, static_cast<size_t>(key_len), weight);
}

static void sqlite_histogram_build_step_5(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 5 || sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        return;
    }

    auto *agg = static_cast<HistogramAggCtx *>(
        sqlite3_aggregate_context(ctx, sizeof(HistogramAggCtx)));
    if (!agg) return;

    if (!agg->hf) {
        agg->hf = rfp_histogram_create();
    }

    const char *key = reinterpret_cast<const char *>(sqlite3_value_text(argv[0]));
    int key_len = sqlite3_value_bytes(argv[0]);
    double equal_rows = sqlite3_value_double(argv[1]);
    double range_rows = sqlite3_value_double(argv[2]);
    double distinct_range_rows = sqlite3_value_double(argv[3]);
    double avg_range_rows = sqlite3_value_double(argv[4]);

    rfp_histogram_add_step(agg->hf, key, static_cast<size_t>(key_len),
                           equal_rows, range_rows, distinct_range_rows, avg_range_rows);
}

static void sqlite_histogram_build_final(sqlite3_context *ctx) {
    auto *agg = static_cast<HistogramAggCtx *>(
        sqlite3_aggregate_context(ctx, 0));

    if (!agg || !agg->hf) {
        sqlite3_result_null(ctx);
        return;
    }

    rfp_histogram_finalize(agg->hf);
    char *json = rfp_histogram_to_json(agg->hf);
    rfp_histogram_free(agg->hf);
    agg->hf = nullptr;

    if (!json) {
        sqlite3_result_error(ctx, "Failed to serialize histogram", -1);
        return;
    }

    sqlite3_result_text(ctx, json, -1, free);
}

/* ========================================================================
 * roaring_histogram_set_shape(histogram_json TEXT, shape_json TEXT) -> TEXT
 * Merges shape metrics into an existing histogram fingerprint.
 * ======================================================================== */

static void sqlite_histogram_set_shape(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 2 || sqlite3_value_type(argv[0]) == SQLITE_NULL || sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    const char *hist_json = reinterpret_cast<const char *>(sqlite3_value_text(argv[0]));
    int hist_len = sqlite3_value_bytes(argv[0]);
    const char *shape_json = reinterpret_cast<const char *>(sqlite3_value_text(argv[1]));
    int shape_len = sqlite3_value_bytes(argv[1]);

    rfp_histogram *hf = rfp_histogram_from_json(hist_json, static_cast<size_t>(hist_len));
    if (!hf) {
        sqlite3_result_error(ctx, "Invalid histogram JSON", -1);
        return;
    }

    rfp_histogram_set_shape(hf, shape_json, static_cast<size_t>(shape_len));
    char *result = rfp_histogram_to_json(hf);
    rfp_histogram_free(hf);

    if (!result) {
        sqlite3_result_error(ctx, "Failed to serialize histogram", -1);
        return;
    }

    sqlite3_result_text(ctx, result, -1, free);
}

/* ========================================================================
 * roaring_histogram_containment(histogram_json TEXT, domain_blob BLOB) -> REAL
 * ======================================================================== */

static void sqlite_histogram_containment(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 2 || sqlite3_value_type(argv[0]) == SQLITE_NULL || sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    const char *json = reinterpret_cast<const char *>(sqlite3_value_text(argv[0]));
    int json_len = sqlite3_value_bytes(argv[0]);

    rfp_histogram *hf = rfp_histogram_from_json(json, static_cast<size_t>(json_len));
    if (!hf) {
        sqlite3_result_error(ctx, "Invalid histogram JSON", -1);
        return;
    }

    rfp_bitmap *domain = rfp_deserialize(
        static_cast<const char *>(sqlite3_value_blob(argv[1])),
        static_cast<size_t>(sqlite3_value_bytes(argv[1])));
    if (!domain) {
        rfp_histogram_free(hf);
        sqlite3_result_double(ctx, 0.0);
        return;
    }

    sqlite3_result_double(ctx, rfp_histogram_weighted_containment(hf, domain));
    rfp_histogram_free(hf);
    rfp_free(domain);
}

/* ========================================================================
 * roaring_histogram_bitmap(histogram_json TEXT) -> BLOB
 * ======================================================================== */

static void sqlite_histogram_bitmap(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 1 || sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    const char *json = reinterpret_cast<const char *>(sqlite3_value_text(argv[0]));
    int json_len = sqlite3_value_bytes(argv[0]);

    rfp_histogram *hf = rfp_histogram_from_json(json, static_cast<size_t>(json_len));
    if (!hf) {
        sqlite3_result_error(ctx, "Invalid histogram JSON", -1);
        return;
    }

    const rfp_bitmap *bm = rfp_histogram_bitmap(hf);
    if (!bm) {
        rfp_histogram_free(hf);
        sqlite3_result_null(ctx);
        return;
    }

    /* Copy the bitmap since hf owns it */
    rfp_bitmap *copy = rfp_copy(bm);
    rfp_histogram_free(hf);

    if (!copy) {
        sqlite3_result_null(ctx);
        return;
    }

    size_t size = rfp_serialized_size(copy);
    void *buf = sqlite3_malloc64(static_cast<sqlite3_int64>(size));
    if (!buf) {
        rfp_free(copy);
        sqlite3_result_error_nomem(ctx);
        return;
    }
    rfp_serialize(copy, static_cast<char *>(buf), size);
    rfp_free(copy);

    sqlite3_result_blob(ctx, buf, static_cast<int>(size), sqlite3_free);
}

/* ========================================================================
 * roaring_histogram_shape(histogram_json TEXT) -> TEXT (JSON)
 * ======================================================================== */

static void sqlite_histogram_shape(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 1 || sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    const char *json = reinterpret_cast<const char *>(sqlite3_value_text(argv[0]));
    int json_len = sqlite3_value_bytes(argv[0]);

    rfp_histogram *hf = rfp_histogram_from_json(json, static_cast<size_t>(json_len));
    if (!hf) {
        sqlite3_result_error(ctx, "Invalid histogram JSON", -1);
        return;
    }

    char *shape = rfp_histogram_shape_json(hf);
    rfp_histogram_free(hf);

    if (!shape) {
        sqlite3_result_error(ctx, "Failed to serialize shape", -1);
        return;
    }

    sqlite3_result_text(ctx, shape, -1, free);
}

/* ========================================================================
 * roaring_histogram_similarity(a_json TEXT, b_json TEXT) -> REAL
 * ======================================================================== */

static void sqlite_histogram_similarity(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 2 || sqlite3_value_type(argv[0]) == SQLITE_NULL || sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    const char *json_a = reinterpret_cast<const char *>(sqlite3_value_text(argv[0]));
    int len_a = sqlite3_value_bytes(argv[0]);
    const char *json_b = reinterpret_cast<const char *>(sqlite3_value_text(argv[1]));
    int len_b = sqlite3_value_bytes(argv[1]);

    rfp_histogram *a = rfp_histogram_from_json(json_a, static_cast<size_t>(len_a));
    rfp_histogram *b = rfp_histogram_from_json(json_b, static_cast<size_t>(len_b));

    if (!a || !b) {
        rfp_histogram_free(a);
        rfp_histogram_free(b);
        sqlite3_result_error(ctx, "Invalid histogram JSON", -1);
        return;
    }

    sqlite3_result_double(ctx, rfp_histogram_shape_similarity(a, b));
    rfp_histogram_free(a);
    rfp_histogram_free(b);
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

    /* Aggregate: roaring_build(value) -> BLOB */
    sqlite3_create_function(db, "roaring_build", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, nullptr, sqlite_roaring_build_step, sqlite_roaring_build_final);

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

    /* Histogram aggregate: 2-arg (key, weight) — source-agnostic */
    sqlite3_create_function(db, "roaring_build_histogram", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, nullptr, sqlite_histogram_build_step_2, sqlite_histogram_build_final);
    /* Histogram aggregate: 5-arg (key, equal_rows, range_rows, distinct_range_rows, avg_range_rows) — SQL Server */
    sqlite3_create_function(db, "roaring_build_histogram", 5, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, nullptr, sqlite_histogram_build_step_5, sqlite_histogram_build_final);

    /* Histogram scalar functions */
    sqlite3_create_function(db, "roaring_histogram_set_shape", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_histogram_set_shape, nullptr, nullptr);
    sqlite3_create_function(db, "roaring_histogram_containment", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_histogram_containment, nullptr, nullptr);
    sqlite3_create_function(db, "roaring_histogram_bitmap", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_histogram_bitmap, nullptr, nullptr);
    sqlite3_create_function(db, "roaring_histogram_shape", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_histogram_shape, nullptr, nullptr);
    sqlite3_create_function(db, "roaring_histogram_similarity", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_histogram_similarity, nullptr, nullptr);

    return SQLITE_OK;
}

#ifdef __cplusplus
}
#endif
