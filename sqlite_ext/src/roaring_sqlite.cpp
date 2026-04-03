/*
 * SQLite extension for roaring bitmap fingerprinting
 *
 * Aggregate:
 *   bf_build(value TEXT) -> BLOB    (hashes each value, builds bitmap)
 *
 * Scalar:
 *   bf_build_json(json_array TEXT) -> BLOB
 *   bf_cardinality(blob BLOB) -> INTEGER
 *   bf_intersection_card(a BLOB, b BLOB) -> INTEGER
 *   bf_containment(probe BLOB, ref BLOB) -> REAL
 *   bf_jaccard(a BLOB, b BLOB) -> REAL
 *   bf_to_base64(blob BLOB) -> TEXT
 *   bf_from_base64(text TEXT) -> BLOB
 *   bf_containment_json(symbols_json TEXT, ref BLOB) -> REAL
 */

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include "roaring_fp.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <nlohmann/json.hpp>

/* ========================================================================
 *   bf_build(value) -> BLOB   [aggregate]
 *
 * Hashes each non-NULL text value via FNV-1a and adds to a roaring bitmap.
 * Equivalent to DuckDB's bf_build() aggregate.
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
 *   bf_build_json(json_array TEXT) -> BLOB
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
 *   bf_cardinality(blob BLOB) -> INTEGER
 * ======================================================================== */

/* ── auxdata helpers for caching deserialized bitmaps/histograms ──────

   sqlite3_set_auxdata caches a pointer keyed by argument index.  SQLite
   automatically calls the destructor when the argument value changes or
   the statement is reset.  This gives us the same semantics as the
   DuckDB ref-cache but driven by the host rather than by pointer comparison.

   OWNERSHIP CONTRACT:
   -------------------
   These helpers return a pointer and set *owned to indicate who frees it:

     *owned == false  →  auxdata owns the pointer.  DO NOT free it.
                         It will be freed automatically when SQLite
                         evicts the auxdata (argument changes or
                         statement finalizes).

     *owned == true   →  caller owns the pointer.  MUST free it after
                         use.  This happens when sqlite3_set_auxdata
                         failed to retain the pointer (e.g., memory
                         pressure caused immediate eviction).

   The pattern in every calling function is:
     bool owned;
     rfp_bitmap *bm = get_cached_bitmap(ctx, arg_idx, argv, &owned);
     // ... use bm (read-only!) ...
     if (owned) rfp_free(bm);

   IMPORTANT: cached objects must be treated as READ-ONLY.  If a function
   needs to mutate the deserialized object (e.g., histogram_set_shape),
   it must NOT use the cache — deserialize a fresh copy instead.
   Mutating a cached object corrupts it for subsequent calls. */

static rfp_bitmap *get_cached_bitmap(sqlite3_context *ctx, int arg_idx,
                                      sqlite3_value **argv, bool *owned) {
    /* Try the cache first */
    rfp_bitmap *bm = static_cast<rfp_bitmap *>(sqlite3_get_auxdata(ctx, arg_idx));
    if (bm) {
        *owned = false;
        return bm;
    }

    /* Cache miss — deserialize */
    const char *data = static_cast<const char *>(sqlite3_value_blob(argv[arg_idx]));
    int len = sqlite3_value_bytes(argv[arg_idx]);
    bm = rfp_deserialize(data, static_cast<size_t>(len));
    if (!bm) {
        *owned = false;
        return nullptr;
    }

    /* Try to stash in auxdata */
    sqlite3_set_auxdata(ctx, arg_idx, bm, reinterpret_cast<void(*)(void*)>(rfp_free));

    /* Re-fetch: if SQLite kept it, we get it back and must not free.
       If SQLite evicted it (e.g., malloc failed), it's already freed
       and we need to deserialize again for this one call. */
    rfp_bitmap *cached = static_cast<rfp_bitmap *>(sqlite3_get_auxdata(ctx, arg_idx));
    if (cached) {
        *owned = false;
        return cached;
    }

    /* Evicted — re-deserialize, caller must free */
    bm = rfp_deserialize(data, static_cast<size_t>(len));
    *owned = true;
    return bm;
}

static rfp_histogram *get_cached_histogram(sqlite3_context *ctx, int arg_idx,
                                            sqlite3_value **argv, bool *owned) {
    rfp_histogram *hf = static_cast<rfp_histogram *>(sqlite3_get_auxdata(ctx, arg_idx));
    if (hf) {
        *owned = false;
        return hf;
    }

    const char *json = reinterpret_cast<const char *>(sqlite3_value_text(argv[arg_idx]));
    int len = sqlite3_value_bytes(argv[arg_idx]);
    hf = rfp_histogram_from_json(json, static_cast<size_t>(len));
    if (!hf) {
        *owned = false;
        return nullptr;
    }

    sqlite3_set_auxdata(ctx, arg_idx, hf, reinterpret_cast<void(*)(void*)>(rfp_histogram_free));
    rfp_histogram *cached = static_cast<rfp_histogram *>(sqlite3_get_auxdata(ctx, arg_idx));
    if (cached) {
        *owned = false;
        return cached;
    }

    hf = rfp_histogram_from_json(json, static_cast<size_t>(len));
    *owned = true;
    return hf;
}

/* ======================================================================== */

static void sqlite_roaring_cardinality(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 1 || sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    bool owned;
    rfp_bitmap *bm = get_cached_bitmap(ctx, 0, argv, &owned);
    if (!bm) { sqlite3_result_int64(ctx, 0); return; }

    sqlite3_result_int64(ctx, static_cast<sqlite3_int64>(rfp_cardinality(bm)));
    if (owned) rfp_free(bm);
}

/* ========================================================================
 *   bf_intersection_card(a BLOB, b BLOB) -> INTEGER
 * ======================================================================== */

static void sqlite_roaring_intersection_card(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 2 || sqlite3_value_type(argv[0]) == SQLITE_NULL || sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    bool owned_a, owned_b;
    rfp_bitmap *a = get_cached_bitmap(ctx, 0, argv, &owned_a);
    rfp_bitmap *b = get_cached_bitmap(ctx, 1, argv, &owned_b);

    if (!a || !b) {
        if (a && owned_a) rfp_free(a);
        if (b && owned_b) rfp_free(b);
        sqlite3_result_int64(ctx, 0);
        return;
    }

    sqlite3_result_int64(ctx, static_cast<sqlite3_int64>(rfp_intersection_card(a, b)));
    if (owned_a) rfp_free(a);
    if (owned_b) rfp_free(b);
}

/* ========================================================================
 *   bf_containment(probe BLOB, ref BLOB) -> REAL
 * ======================================================================== */

static void sqlite_roaring_containment(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 2 || sqlite3_value_type(argv[0]) == SQLITE_NULL || sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    bool owned_probe, owned_ref;
    rfp_bitmap *probe = get_cached_bitmap(ctx, 0, argv, &owned_probe);
    rfp_bitmap *ref   = get_cached_bitmap(ctx, 1, argv, &owned_ref);

    if (!probe || !ref) {
        if (probe && owned_probe) rfp_free(probe);
        if (ref && owned_ref) rfp_free(ref);
        sqlite3_result_double(ctx, 0.0);
        return;
    }

    sqlite3_result_double(ctx, rfp_containment(probe, ref));
    if (owned_probe) rfp_free(probe);
    if (owned_ref) rfp_free(ref);
}

/* ========================================================================
 *   bf_jaccard(a BLOB, b BLOB) -> REAL
 * ======================================================================== */

static void sqlite_roaring_jaccard(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 2 || sqlite3_value_type(argv[0]) == SQLITE_NULL || sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    bool owned_a, owned_b;
    rfp_bitmap *a = get_cached_bitmap(ctx, 0, argv, &owned_a);
    rfp_bitmap *b = get_cached_bitmap(ctx, 1, argv, &owned_b);

    if (!a || !b) {
        if (a && owned_a) rfp_free(a);
        if (b && owned_b) rfp_free(b);
        sqlite3_result_double(ctx, 0.0);
        return;
    }

    sqlite3_result_double(ctx, rfp_jaccard(a, b));
    if (owned_a) rfp_free(a);
    if (owned_b) rfp_free(b);
}

/* ========================================================================
 *   bf_to_base64(blob BLOB) -> TEXT
 * ======================================================================== */

static void sqlite_roaring_to_base64(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 1 || sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    bool owned;
    rfp_bitmap *bm = get_cached_bitmap(ctx, 0, argv, &owned);
    if (!bm) {
        sqlite3_result_null(ctx);
        return;
    }

    size_t b64_size = rfp_base64_size(bm);
    char *buf = static_cast<char *>(sqlite3_malloc64(static_cast<sqlite3_int64>(b64_size + 1)));
    if (!buf) {
        if (owned) rfp_free(bm);
        sqlite3_result_error_nomem(ctx);
        return;
    }
    rfp_to_base64(bm, buf, b64_size);
    buf[b64_size] = '\0';
    if (owned) rfp_free(bm);

    sqlite3_result_text(ctx, buf, static_cast<int>(b64_size), sqlite3_free);
}

/* ========================================================================
 *   bf_from_base64(text TEXT) -> BLOB
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
 *   bf_containment_json(symbols_json TEXT, ref_blob BLOB) -> REAL
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

    bool owned_ref;
    rfp_bitmap *ref = get_cached_bitmap(ctx, 1, argv, &owned_ref);
    if (!ref) {
        rfp_free(probe);
        sqlite3_result_double(ctx, 0.0);
        return;
    }

    sqlite3_result_double(ctx, rfp_containment(probe, ref));
    rfp_free(probe);
    if (owned_ref) rfp_free(ref);
}

/* ========================================================================
 *   bf_build_histogram — two overloads:
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
 *   bf_histogram_set_shape(histogram_json TEXT, shape_json TEXT) -> TEXT
 * Merges shape metrics into an existing histogram fingerprint.
 * ======================================================================== */

static void sqlite_histogram_set_shape(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 2 || sqlite3_value_type(argv[0]) == SQLITE_NULL || sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    const char *shape_json = reinterpret_cast<const char *>(sqlite3_value_text(argv[1]));
    int shape_len = sqlite3_value_bytes(argv[1]);

    /* Note: we DON'T cache arg[0] here because set_shape mutates the
       histogram.  If it were cached, the mutation would corrupt the
       auxdata for subsequent calls with the same input.  Mutation and
       caching are incompatible — only cache read-only operations. */
    const char *hist_json = reinterpret_cast<const char *>(sqlite3_value_text(argv[0]));
    int hist_len = sqlite3_value_bytes(argv[0]);

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
 *   bf_histogram_containment(histogram_json TEXT, domain_blob BLOB) -> REAL
 * ======================================================================== */

static void sqlite_histogram_containment(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 2 || sqlite3_value_type(argv[0]) == SQLITE_NULL || sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    bool owned_hf, owned_domain;
    rfp_histogram *hf = get_cached_histogram(ctx, 0, argv, &owned_hf);
    if (!hf) {
        sqlite3_result_error(ctx, "Invalid histogram JSON", -1);
        return;
    }

    rfp_bitmap *domain = get_cached_bitmap(ctx, 1, argv, &owned_domain);
    if (!domain) {
        if (owned_hf) rfp_histogram_free(hf);
        sqlite3_result_double(ctx, 0.0);
        return;
    }

    sqlite3_result_double(ctx, rfp_histogram_weighted_containment(hf, domain));
    if (owned_hf) rfp_histogram_free(hf);
    if (owned_domain) rfp_free(domain);
}

/* ========================================================================
 *   bf_histogram_bitmap(histogram_json TEXT) -> BLOB
 * ======================================================================== */

static void sqlite_histogram_bitmap(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 1 || sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    bool owned_hf;
    rfp_histogram *hf = get_cached_histogram(ctx, 0, argv, &owned_hf);
    if (!hf) {
        sqlite3_result_error(ctx, "Invalid histogram JSON", -1);
        return;
    }

    const rfp_bitmap *bm = rfp_histogram_bitmap(hf);
    if (!bm) {
        if (owned_hf) rfp_histogram_free(hf);
        sqlite3_result_null(ctx);
        return;
    }

    /* Copy the bitmap since hf owns it (whether cached or not) */
    rfp_bitmap *copy = rfp_copy(bm);
    if (owned_hf) rfp_histogram_free(hf);

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
 *   bf_histogram_shape(histogram_json TEXT) -> TEXT (JSON)
 * ======================================================================== */

static void sqlite_histogram_shape(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 1 || sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    bool owned_hf;
    rfp_histogram *hf = get_cached_histogram(ctx, 0, argv, &owned_hf);
    if (!hf) {
        sqlite3_result_error(ctx, "Invalid histogram JSON", -1);
        return;
    }

    char *shape = rfp_histogram_shape_json(hf);
    if (owned_hf) rfp_histogram_free(hf);

    if (!shape) {
        sqlite3_result_error(ctx, "Failed to serialize shape", -1);
        return;
    }

    sqlite3_result_text(ctx, shape, -1, free);
}

/* ========================================================================
 *   bf_histogram_similarity(a_json TEXT, b_json TEXT) -> REAL
 * ======================================================================== */

static void sqlite_histogram_similarity(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 2 || sqlite3_value_type(argv[0]) == SQLITE_NULL || sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    bool owned_a, owned_b;
    rfp_histogram *a = get_cached_histogram(ctx, 0, argv, &owned_a);
    rfp_histogram *b = get_cached_histogram(ctx, 1, argv, &owned_b);

    if (!a || !b) {
        if (a && owned_a) rfp_histogram_free(a);
        if (b && owned_b) rfp_histogram_free(b);
        sqlite3_result_error(ctx, "Invalid histogram JSON", -1);
        return;
    }

    sqlite3_result_double(ctx, rfp_histogram_shape_similarity(a, b));
    if (owned_a) rfp_histogram_free(a);
    if (owned_b) rfp_histogram_free(b);
}

/* ========================================================================
 * Extension entry point
 * ======================================================================== */

#ifdef __cplusplus
extern "C" {
#endif

/* ========================================================================
 *   bf_to_array(bitmap BLOB) -> TEXT (JSON array of uint32)
 * ======================================================================== */

static void sqlite_roaring_to_array(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 1 || sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    bool owned;
    rfp_bitmap *bm = get_cached_bitmap(ctx, 0, argv, &owned);
    if (!bm) { sqlite3_result_text(ctx, "[]", 2, SQLITE_STATIC); return; }

    uint64_t card = rfp_cardinality(bm);
    if (card == 0) {
        if (owned) rfp_free(bm);
        sqlite3_result_text(ctx, "[]", 2, SQLITE_STATIC);
        return;
    }

    uint32_t *arr = static_cast<uint32_t *>(sqlite3_malloc64(card * sizeof(uint32_t)));
    if (!arr) {
        if (owned) rfp_free(bm);
        sqlite3_result_error_nomem(ctx);
        return;
    }
    rfp_to_uint32_array(bm, arr, card);
    if (owned) rfp_free(bm);

    /* Build JSON array string */
    std::string json = "[";
    for (uint64_t i = 0; i < card; i++) {
        if (i > 0) json += ',';
        json += std::to_string(arr[i]);
    }
    json += ']';
    sqlite3_free(arr);

    char *result = static_cast<char *>(sqlite3_malloc64(json.size() + 1));
    if (!result) { sqlite3_result_error_nomem(ctx); return; }
    memcpy(result, json.data(), json.size());
    result[json.size()] = '\0';
    sqlite3_result_text(ctx, result, static_cast<int>(json.size()), sqlite3_free);
}

/* ========================================================================
 *   bf_from_array(json TEXT) -> BLOB
 *   Accepts a JSON array of unsigned integers: [1, 42, 1000]
 * ======================================================================== */

static void sqlite_roaring_from_array(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 1 || sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    const char *json = reinterpret_cast<const char *>(sqlite3_value_text(argv[0]));
    int json_len = sqlite3_value_bytes(argv[0]);
    if (!json || json_len <= 0) { sqlite3_result_null(ctx); return; }

    /* Parse JSON array of integers using nlohmann::json */
    nlohmann::json j;
    try {
        j = nlohmann::json::parse(json, json + json_len);
    } catch (...) {
        sqlite3_result_error(ctx, "bf_from_array: invalid JSON", -1);
        return;
    }
    if (!j.is_array()) {
        sqlite3_result_error(ctx, "bf_from_array: expected JSON array", -1);
        return;
    }

    rfp_bitmap *bm = rfp_create();
    if (!bm) { sqlite3_result_error_nomem(ctx); return; }

    for (const auto &elem : j) {
        if (elem.is_number_unsigned() || elem.is_number_integer()) {
            rfp_add_uint32(bm, static_cast<uint32_t>(elem.get<uint64_t>()));
        }
    }

    size_t sz = rfp_serialized_size(bm);
    char *buf = static_cast<char *>(sqlite3_malloc64(sz));
    if (!buf) { rfp_free(bm); sqlite3_result_error_nomem(ctx); return; }
    rfp_serialize(bm, buf, sz);
    rfp_free(bm);
    sqlite3_result_blob(ctx, buf, static_cast<int>(sz), sqlite3_free);
}

/* ========================================================================
 *   bf_contains(bitmap BLOB, value INTEGER) -> BOOLEAN
 * ======================================================================== */

static void sqlite_roaring_contains(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 2 || sqlite3_value_type(argv[0]) == SQLITE_NULL
                   || sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    bool owned;
    rfp_bitmap *bm = get_cached_bitmap(ctx, 0, argv, &owned);
    if (!bm) { sqlite3_result_int(ctx, 0); return; }

    uint32_t val = static_cast<uint32_t>(sqlite3_value_int64(argv[1]));
    sqlite3_result_int(ctx, rfp_contains(bm, val));
    if (owned) rfp_free(bm);
}


#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_roaring_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
    SQLITE_EXTENSION_INIT2(pApi);
    (void)pzErrMsg;

    /* Aggregate: bf_build(value) -> BLOB */
    sqlite3_create_function(db, "bf_build", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, nullptr, sqlite_roaring_build_step, sqlite_roaring_build_final);

    sqlite3_create_function(db, "bf_build_json", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_roaring_build_json, nullptr, nullptr);
    sqlite3_create_function(db, "bf_cardinality", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_roaring_cardinality, nullptr, nullptr);
    sqlite3_create_function(db, "bf_intersection_card", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_roaring_intersection_card, nullptr, nullptr);
    sqlite3_create_function(db, "bf_containment", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_roaring_containment, nullptr, nullptr);
    sqlite3_create_function(db, "bf_jaccard", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_roaring_jaccard, nullptr, nullptr);
    sqlite3_create_function(db, "bf_to_base64", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_roaring_to_base64, nullptr, nullptr);
    sqlite3_create_function(db, "bf_from_base64", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_roaring_from_base64, nullptr, nullptr);
    sqlite3_create_function(db, "bf_containment_json", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_roaring_containment_json, nullptr, nullptr);

    /* Histogram aggregate: 2-arg (key, weight) — source-agnostic */
    sqlite3_create_function(db, "bf_build_histogram", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, nullptr, sqlite_histogram_build_step_2, sqlite_histogram_build_final);
    /* Histogram aggregate: 5-arg (key, equal_rows, range_rows, distinct_range_rows, avg_range_rows) — SQL Server */
    sqlite3_create_function(db, "bf_build_histogram", 5, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, nullptr, sqlite_histogram_build_step_5, sqlite_histogram_build_final);

    /* Histogram scalar functions */
    sqlite3_create_function(db, "bf_histogram_set_shape", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_histogram_set_shape, nullptr, nullptr);
    sqlite3_create_function(db, "bf_histogram_containment", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_histogram_containment, nullptr, nullptr);
    sqlite3_create_function(db, "bf_histogram_bitmap", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_histogram_bitmap, nullptr, nullptr);
    sqlite3_create_function(db, "bf_histogram_shape", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_histogram_shape, nullptr, nullptr);
    sqlite3_create_function(db, "bf_histogram_similarity", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_histogram_similarity, nullptr, nullptr);

    /* Array conversion */
    sqlite3_create_function(db, "bf_to_array", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_roaring_to_array, nullptr, nullptr);
    sqlite3_create_function(db, "bf_from_array", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_roaring_from_array, nullptr, nullptr);
    sqlite3_create_function(db, "bf_contains", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            nullptr, sqlite_roaring_contains, nullptr, nullptr);

    return SQLITE_OK;
}

#ifdef __cplusplus
}
#endif
