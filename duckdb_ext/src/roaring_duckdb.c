/*
 * DuckDB C API extension for roaring bitmap fingerprinting.
 *
 * Registers one aggregate and a set of scalar functions with bf_ prefix:
 *   bf_build(value)                    [aggregate]   -> BLOB
 *   bf_build_json(json_array)                        -> BLOB
 *   bf_intersect(a, b)                               -> BLOB
 *   bf_union(a, b)                                   -> BLOB
 *   bf_difference(a, b)                              -> BLOB
 *   bf_cardinality(blob)                             -> UBIGINT
 *   bf_intersection_card(a, b)                       -> UBIGINT
 *   bf_containment(probe, ref)                       -> DOUBLE
 *   bf_jaccard(a, b)                                 -> DOUBLE
 *   bf_to_base64(blob)                               -> VARCHAR
 *   bf_from_base64(text)                             -> BLOB
 *   bf_containment_json(json, ref)                   -> DOUBLE
 *   bf_histogram_set_shape(hist_json, shape_json)    -> VARCHAR
 *   bf_histogram_containment(hist_json, domain_blob) -> DOUBLE
 *   bf_histogram_bitmap(hist_json)                   -> BLOB
 *   bf_histogram_shape(hist_json)                    -> VARCHAR
 *   bf_histogram_similarity(a_json, b_json)          -> DOUBLE
 */

#define DUCKDB_EXTENSION_NAME blobfilters
#include "duckdb_extension.h"

#include "roaring_fp.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

/* ── String helpers ───────────────────────────────────────────────── */

static const char *str_ptr(duckdb_string_t *s, uint32_t *out_len) {
    uint32_t len = s->value.inlined.length;
    *out_len = len;
    if (len <= 12) {
        return s->value.inlined.inlined;
    }
    return s->value.pointer.ptr;
}

static char *str_dup_z(duckdb_string_t *s) {
    uint32_t len;
    const char *p = str_ptr(s, &len);
    char *z = (char *)malloc(len + 1);
    memcpy(z, p, len);
    z[len] = '\0';
    return z;
}

/* ── Validity check macro ────────────────────────────────────────── */

#define CHECK_NULL_1(row) \
    if (val0 && !duckdb_validity_row_is_valid(val0, row)) { \
        duckdb_vector_ensure_validity_writable(output); \
        duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row); \
        continue; \
    }

#define CHECK_NULL_2(row) \
    if ((val0 && !duckdb_validity_row_is_valid(val0, row)) || \
        (val1 && !duckdb_validity_row_is_valid(val1, row))) { \
        duckdb_vector_ensure_validity_writable(output); \
        duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row); \
        continue; \
    }

/* ── Constant-operand caching for binary scalars ────────────────────
   In a probe query (many filter blobs × one constant LUT blob), the 2nd
   operand is the same BLOB on every row.  The DuckDB C API exposes no
   constant-vector flag, so we detect constancy by pointer+length identity:
   DuckDB's string storage keeps non-inlined (> 12 byte) blob pointers
   stable within a chunk, so if the next row's blob has the same data
   pointer and length we reuse the already-deserialized bitmap instead of
   paying rfp_deserialize per row.  If the operand does vary per row this
   degrades gracefully to one deserialize per row (same as no cache). */

typedef struct {
    const char *ref_ptr;   /* pointer to last ref blob data */
    uint32_t    ref_len;   /* length of last ref blob */
    rfp_bitmap *ref_bm;    /* deserialized bitmap (owned) */
} RefCache;

static rfp_bitmap *get_ref_cached(RefCache *cache,
                                   const char *blob, uint32_t len) {
    if (cache->ref_bm && cache->ref_len == len && cache->ref_ptr == blob)
        return cache->ref_bm;

    if (cache->ref_bm) rfp_free(cache->ref_bm);
    cache->ref_ptr = blob;
    cache->ref_len = len;
    /* zero-copy view: the DuckDB input vector data is stable for the whole chunk,
       and the cached view lives only for this chunk (freed after the row loop). */
    cache->ref_bm  = rfp_deserialize_frozen(blob, len);
    return cache->ref_bm;
}

/* ── bf_build(value VARCHAR) -> BLOB   [aggregate] ───────────────────
   Hashes each non-NULL value via FNV-1a into a roaring bitmap.  Lets a
   probe query build one filter per group with GROUP BY and reuse it,
   instead of regenerating filters.  Mirrors SQLite's bf_build aggregate. */

typedef struct {
    rfp_bitmap *bm;
} BfBuildState;

static idx_t bf_build_state_size(duckdb_function_info info) {
    (void)info;
    return sizeof(BfBuildState);
}

static void bf_build_init(duckdb_function_info info, duckdb_aggregate_state state) {
    (void)info;
    BfBuildState *s = (BfBuildState *)state;
    s->bm = rfp_create();
}

static void bf_build_update(duckdb_function_info info, duckdb_data_chunk input,
                            duckdb_aggregate_state *states) {
    (void)info;
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);

    for (idx_t row = 0; row < size; row++) {
        if (val0 && !duckdb_validity_row_is_valid(val0, row)) continue; /* ignore NULLs */
        BfBuildState *s = (BfBuildState *)states[row];
        if (!s->bm) s->bm = rfp_create();
        uint32_t len;
        const char *str = str_ptr(&data0[row], &len);
        rfp_add_hash(s->bm, str, len);
    }
}

static void bf_build_combine(duckdb_function_info info, duckdb_aggregate_state *source,
                             duckdb_aggregate_state *target, idx_t count) {
    (void)info;
    for (idx_t i = 0; i < count; i++) {
        BfBuildState *src = (BfBuildState *)source[i];
        BfBuildState *dst = (BfBuildState *)target[i];
        if (!src->bm) continue;
        if (!dst->bm) dst->bm = rfp_create();
        rfp_or_inplace(dst->bm, src->bm);
    }
}

static void bf_build_finalize(duckdb_function_info info, duckdb_aggregate_state *source,
                              duckdb_vector result, idx_t count, idx_t offset) {
    (void)info;
    for (idx_t i = 0; i < count; i++) {
        BfBuildState *s = (BfBuildState *)source[i];
        rfp_bitmap *bm = s->bm;
        int temp = 0;
        if (!bm) { bm = rfp_create(); temp = 1; } /* all-NULL / empty group */

        size_t sz = rfp_serialized_size(bm);
        char *buf = (char *)malloc(sz);
        rfp_serialize(bm, buf, sz);
        duckdb_vector_assign_string_element_len(result, offset + i, buf, sz);
        free(buf);

        if (temp) rfp_free(bm); /* s->bm is released in the destructor */
    }
}

/* ── bf_sha256(blob BLOB) -> VARCHAR ─────────────────────────────────
   SHA-256 hex of the raw bytes (matches hashlib/shasum/SubtleCrypto).
   Composes with bf_build for the canonical checksum: bf_sha256(bf_build(x)). */
static void bf_sha256_func(duckdb_function_info info,
                           duckdb_data_chunk input,
                           duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_1(row);
        uint32_t len;
        const char *blob = str_ptr(&data0[row], &len);
        char hex[65];
        rfp_sha256_hex(blob, len, hex, sizeof(hex));
        duckdb_vector_assign_string_element_len(output, row, hex, 64);
    }
}

/* ── bf_checksum(value VARCHAR) -> VARCHAR  [aggregate] ───────────────
   Canonical set-checksum in one call: build a roaring bitmap (order-
   independent) from the group's values, then SHA-256 its serialization.
   Equivalent to bf_sha256(bf_build(value)); reuses the bf_build state. */
static void bf_checksum_finalize(duckdb_function_info info, duckdb_aggregate_state *source,
                                 duckdb_vector result, idx_t count, idx_t offset) {
    (void)info;
    for (idx_t i = 0; i < count; i++) {
        BfBuildState *s = (BfBuildState *)source[i];
        rfp_bitmap *bm = s->bm;
        int temp = 0;
        if (!bm) { bm = rfp_create(); temp = 1; }
        char hex[65];
        rfp_bitmap_checksum_hex(bm, hex, sizeof(hex));
        duckdb_vector_assign_string_element_len(result, offset + i, hex, 64);
        if (temp) rfp_free(bm);
    }
}

static void bf_build_destroy(duckdb_aggregate_state *states, idx_t count) {
    for (idx_t i = 0; i < count; i++) {
        BfBuildState *s = (BfBuildState *)states[i];
        if (s->bm) { rfp_free(s->bm); s->bm = NULL; }
    }
}

/* ── bf_build_json(json_array VARCHAR) -> BLOB ───────────────────── */

static void bf_build_json_func(duckdb_function_info info,
                               duckdb_data_chunk input,
                               duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_1(row);
        uint32_t len;
        const char *json = str_ptr(&data0[row], &len);

        rfp_bitmap *bm = rfp_create();
        rfp_add_json_array(bm, json, len);
        size_t sz = rfp_serialized_size(bm);
        char *buf = (char *)malloc(sz);
        rfp_serialize(bm, buf, sz);
        rfp_free(bm);

        duckdb_vector_assign_string_element_len(output, row, buf, sz);
        free(buf);
    }
}

/* ── bf_build_json_normalized(json_array VARCHAR) -> BLOB ─────────
   NFKD decompose + strip combining marks + casefold before hashing.
   Build and probe must both use this function for matching to work. */

static void bf_build_json_normalized_func(duckdb_function_info info,
                                          duckdb_data_chunk input,
                                          duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_1(row);
        uint32_t len;
        const char *json = str_ptr(&data0[row], &len);

        rfp_bitmap *bm = rfp_create();
        rfp_add_json_array_normalized(bm, json, len, RFP_NORM_CASEFOLD);
        size_t sz = rfp_serialized_size(bm);
        char *buf = (char *)malloc(sz);
        rfp_serialize(bm, buf, sz);
        rfp_free(bm);

        duckdb_vector_assign_string_element_len(output, row, buf, sz);
        free(buf);
    }
}

/* ── bf_cardinality(blob BLOB) -> UBIGINT ────────────────────────── */

static void bf_cardinality_func(duckdb_function_info info,
                                duckdb_data_chunk input,
                                duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *result_data = (uint64_t *)duckdb_vector_get_data(output);

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_1(row);
        uint32_t len;
        const char *blob = str_ptr(&data0[row], &len);

        rfp_bitmap *bm = rfp_deserialize_frozen(blob, len);
        if (!bm) {
            result_data[row] = 0;
            continue;
        }
        result_data[row] = rfp_cardinality(bm);
        rfp_free(bm);
    }
}

/* ── bf_intersection_card(a BLOB, b BLOB) -> UBIGINT ─────────────── */

static void bf_intersection_card_func(duckdb_function_info info,
                                      duckdb_data_chunk input,
                                      duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);
    uint64_t *result_data = (uint64_t *)duckdb_vector_get_data(output);
    RefCache cache = {NULL, 0, NULL};  /* operand b is often a constant LUT */

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_2(row);
        uint32_t len_a, len_b;
        const char *blob_a = str_ptr(&data0[row], &len_a);
        const char *blob_b = str_ptr(&data1[row], &len_b);

        rfp_bitmap *a = rfp_deserialize_frozen(blob_a, len_a);
        rfp_bitmap *b = get_ref_cached(&cache, blob_b, len_b);
        if (!a || !b) {
            rfp_free(a);
            result_data[row] = 0;
            continue;
        }
        result_data[row] = rfp_intersection_card(a, b);
        rfp_free(a);
    }
    if (cache.ref_bm) rfp_free(cache.ref_bm);
}

/* ── bf_containment(probe BLOB, ref BLOB) -> DOUBLE ──────────────── */

static void bf_containment_func(duckdb_function_info info,
                                duckdb_data_chunk input,
                                duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);
    double *result_data = (double *)duckdb_vector_get_data(output);
    RefCache cache = {NULL, 0, NULL};  /* ref (arg 2) is often a constant */

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_2(row);
        uint32_t len_a, len_b;
        const char *blob_a = str_ptr(&data0[row], &len_a);
        const char *blob_b = str_ptr(&data1[row], &len_b);

        rfp_bitmap *probe = rfp_deserialize_frozen(blob_a, len_a);
        rfp_bitmap *ref = get_ref_cached(&cache, blob_b, len_b);
        if (!probe || !ref) {
            rfp_free(probe);
            result_data[row] = 0.0;
            continue;
        }
        result_data[row] = rfp_containment(probe, ref);
        rfp_free(probe);
    }
    if (cache.ref_bm) rfp_free(cache.ref_bm);
}

/* ── bf_jaccard(a BLOB, b BLOB) -> DOUBLE ────────────────────────── */

static void bf_jaccard_func(duckdb_function_info info,
                            duckdb_data_chunk input,
                            duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);
    double *result_data = (double *)duckdb_vector_get_data(output);
    RefCache cache = {NULL, 0, NULL};  /* operand b is often a constant */

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_2(row);
        uint32_t len_a, len_b;
        const char *blob_a = str_ptr(&data0[row], &len_a);
        const char *blob_b = str_ptr(&data1[row], &len_b);

        rfp_bitmap *a = rfp_deserialize_frozen(blob_a, len_a);
        rfp_bitmap *b = get_ref_cached(&cache, blob_b, len_b);
        if (!a || !b) {
            rfp_free(a);
            result_data[row] = 0.0;
            continue;
        }
        result_data[row] = rfp_jaccard(a, b);
        rfp_free(a);
    }
    if (cache.ref_bm) rfp_free(cache.ref_bm);
}

/* ── Binary blob set-ops: bf_intersect / bf_union / bf_difference ───
   Deserialize both operands, apply a core rfp binary op, serialize the
   result back to a BLOB.  Operand b is cached via RefCache since it is
   commonly a constant LUT in probe queries. */

typedef rfp_bitmap *(*rfp_binop)(const rfp_bitmap *, const rfp_bitmap *);

static void bf_binop_blob(duckdb_data_chunk input, duckdb_vector output,
                          rfp_binop op) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);
    RefCache cache = {NULL, 0, NULL};

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_2(row);
        uint32_t len_a, len_b;
        const char *blob_a = str_ptr(&data0[row], &len_a);
        const char *blob_b = str_ptr(&data1[row], &len_b);

        rfp_bitmap *a = rfp_deserialize_frozen(blob_a, len_a);
        rfp_bitmap *b = get_ref_cached(&cache, blob_b, len_b);
        if (!a || !b) {
            rfp_free(a);  /* b is cache-owned, freed once after the loop */
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }
        rfp_bitmap *r = op(a, b);
        rfp_free(a);
        if (!r) {
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }
        size_t sz = rfp_serialized_size(r);
        char *buf = (char *)malloc(sz);
        rfp_serialize(r, buf, sz);
        rfp_free(r);

        duckdb_vector_assign_string_element_len(output, row, buf, sz);
        free(buf);
    }
    if (cache.ref_bm) rfp_free(cache.ref_bm);
}

/* ── bf_intersect(a BLOB, b BLOB) -> BLOB ────────────────────────── */

static void bf_intersect_func(duckdb_function_info info,
                              duckdb_data_chunk input,
                              duckdb_vector output) {
    (void)info;
    bf_binop_blob(input, output, rfp_and);
}

/* ── bf_union(a BLOB, b BLOB) -> BLOB ────────────────────────────── */

static void bf_union_func(duckdb_function_info info,
                          duckdb_data_chunk input,
                          duckdb_vector output) {
    (void)info;
    bf_binop_blob(input, output, rfp_or);
}

/* ── bf_difference(a BLOB, b BLOB) -> BLOB ───────────────────────── */

static void bf_difference_func(duckdb_function_info info,
                               duckdb_data_chunk input,
                               duckdb_vector output) {
    (void)info;
    bf_binop_blob(input, output, rfp_andnot);
}

/* ── bf_to_base64(blob BLOB) -> VARCHAR ──────────────────────────── */

static void bf_to_base64_func(duckdb_function_info info,
                              duckdb_data_chunk input,
                              duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_1(row);
        uint32_t len;
        const char *blob = str_ptr(&data0[row], &len);

        rfp_bitmap *bm = rfp_deserialize(blob, len);
        if (!bm) {
            duckdb_vector_assign_string_element_len(output, row, "", 0);
            continue;
        }
        size_t b64_size = rfp_base64_size(bm);
        char *buf = (char *)malloc(b64_size + 1);
        rfp_to_base64(bm, buf, b64_size);
        buf[b64_size] = '\0';
        rfp_free(bm);

        duckdb_vector_assign_string_element_len(output, row, buf, b64_size);
        free(buf);
    }
}

/* ── bf_from_base64(text VARCHAR) -> BLOB ────────────────────────── */

static void bf_from_base64_func(duckdb_function_info info,
                                duckdb_data_chunk input,
                                duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_1(row);
        uint32_t len;
        const char *b64 = str_ptr(&data0[row], &len);

        rfp_bitmap *bm = rfp_from_base64(b64, len);
        if (!bm) {
            duckdb_vector_assign_string_element_len(output, row, "", 0);
            continue;
        }
        size_t sz = rfp_serialized_size(bm);
        char *buf = (char *)malloc(sz);
        rfp_serialize(bm, buf, sz);
        rfp_free(bm);

        duckdb_vector_assign_string_element_len(output, row, buf, sz);
        free(buf);
    }
}

/* ── bf_containment_json(json VARCHAR, ref BLOB) -> DOUBLE ─────────
   The single-string / JSON-array probe paths below (rfp_add_hash vs
   rfp_add_json_array) let one value be probed without JSON parsing when
   the input has no '[' prefix; the ref BLOB (arg 2) is cached via
   RefCache exactly as in the binary blob scalars above. */

static void bf_containment_json_func(duckdb_function_info info,
                                     duckdb_data_chunk input,
                                     duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);
    double *result_data = (double *)duckdb_vector_get_data(output);
    RefCache cache = {NULL, 0, NULL};

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_2(row);
        uint32_t json_len, ref_len;
        const char *json = str_ptr(&data0[row], &json_len);
        const char *ref_blob = str_ptr(&data1[row], &ref_len);

        rfp_bitmap *ref = get_ref_cached(&cache, ref_blob, ref_len);
        if (!ref) { result_data[row] = 0.0; continue; }

        rfp_bitmap *probe = rfp_create();
        /* Fast path: single value (no [ prefix) → hash directly */
        if (json_len > 0 && json[0] != '[') {
            rfp_add_hash(probe, json, json_len);
        } else if (rfp_add_json_array(probe, json, json_len) != 0) {
            rfp_free(probe);
            result_data[row] = 0.0;
            continue;
        }
        result_data[row] = rfp_containment(probe, ref);
        rfp_free(probe);
    }
    if (cache.ref_bm) rfp_free(cache.ref_bm);
}

/* ── bf_containment_json_normalized(json VARCHAR, ref BLOB) -> DOUBLE
   Same as above but with NFKD + casefold normalization.  Both build
   and probe must use normalized functions for hashes to match. ────── */

static void bf_containment_json_normalized_func(duckdb_function_info info,
                                                duckdb_data_chunk input,
                                                duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);
    double *result_data = (double *)duckdb_vector_get_data(output);
    RefCache cache = {NULL, 0, NULL};

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_2(row);
        uint32_t json_len, ref_len;
        const char *json = str_ptr(&data0[row], &json_len);
        const char *ref_blob = str_ptr(&data1[row], &ref_len);

        rfp_bitmap *ref = get_ref_cached(&cache, ref_blob, ref_len);
        if (!ref) { result_data[row] = 0.0; continue; }

        rfp_bitmap *probe = rfp_create();
        /* Fast path: single value → normalized hash directly */
        if (json_len > 0 && json[0] != '[') {
            rfp_add_hash_normalized(probe, json, json_len, RFP_NORM_CASEFOLD);
        } else if (rfp_add_json_array_normalized(probe, json, json_len,
                                                  RFP_NORM_CASEFOLD) != 0) {
            rfp_free(probe);
            result_data[row] = 0.0;
            continue;
        }
        result_data[row] = rfp_containment(probe, ref);
        rfp_free(probe);
    }
    if (cache.ref_bm) rfp_free(cache.ref_bm);
}

/* ── bf_histogram_set_shape(hist VARCHAR, shape VARCHAR) -> VARCHAR ── */

static void bf_histogram_set_shape_func(duckdb_function_info info,
                                        duckdb_data_chunk input,
                                        duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_2(row);
        uint32_t hist_len, shape_len;
        const char *hist_json = str_ptr(&data0[row], &hist_len);
        const char *shape_json = str_ptr(&data1[row], &shape_len);

        rfp_histogram *hf = rfp_histogram_from_json(hist_json, hist_len);
        if (!hf) {
            duckdb_vector_assign_string_element_len(output, row, "", 0);
            continue;
        }
        rfp_histogram_set_shape(hf, shape_json, shape_len);
        char *out = rfp_histogram_to_json(hf);
        rfp_histogram_free(hf);

        if (!out) {
            duckdb_vector_assign_string_element_len(output, row, "", 0);
            continue;
        }
        duckdb_vector_assign_string_element(output, row, out);
        rfp_free_string(out);
    }
}

/* ── bf_histogram_containment(hist VARCHAR, domain BLOB) -> DOUBLE ── */

static void bf_histogram_containment_func(duckdb_function_info info,
                                          duckdb_data_chunk input,
                                          duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);
    double *result_data = (double *)duckdb_vector_get_data(output);

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_2(row);
        uint32_t hist_len, dom_len;
        const char *hist_json = str_ptr(&data0[row], &hist_len);
        const char *dom_blob = str_ptr(&data1[row], &dom_len);

        rfp_histogram *hf = rfp_histogram_from_json(hist_json, hist_len);
        if (!hf) {
            result_data[row] = 0.0;
            continue;
        }
        rfp_bitmap *domain = rfp_deserialize(dom_blob, dom_len);
        if (!domain) {
            rfp_histogram_free(hf);
            result_data[row] = 0.0;
            continue;
        }
        result_data[row] = rfp_histogram_weighted_containment(hf, domain);
        rfp_histogram_free(hf);
        rfp_free(domain);
    }
}

/* ── bf_histogram_bitmap(hist VARCHAR) -> BLOB ───────────────────── */

static void bf_histogram_bitmap_func(duckdb_function_info info,
                                     duckdb_data_chunk input,
                                     duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_1(row);
        uint32_t len;
        const char *hist_json = str_ptr(&data0[row], &len);

        rfp_histogram *hf = rfp_histogram_from_json(hist_json, len);
        if (!hf) {
            duckdb_vector_assign_string_element_len(output, row, "", 0);
            continue;
        }
        const rfp_bitmap *bm = rfp_histogram_bitmap(hf);
        rfp_bitmap *copy = rfp_copy(bm);
        rfp_histogram_free(hf);

        if (!copy) {
            duckdb_vector_assign_string_element_len(output, row, "", 0);
            continue;
        }
        size_t sz = rfp_serialized_size(copy);
        char *buf = (char *)malloc(sz);
        rfp_serialize(copy, buf, sz);
        rfp_free(copy);

        duckdb_vector_assign_string_element_len(output, row, buf, sz);
        free(buf);
    }
}

/* ── bf_histogram_shape(hist VARCHAR) -> VARCHAR ─────────────────── */

static void bf_histogram_shape_func(duckdb_function_info info,
                                    duckdb_data_chunk input,
                                    duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_1(row);
        uint32_t len;
        const char *hist_json = str_ptr(&data0[row], &len);

        rfp_histogram *hf = rfp_histogram_from_json(hist_json, len);
        if (!hf) {
            duckdb_vector_assign_string_element_len(output, row, "", 0);
            continue;
        }
        char *shape = rfp_histogram_shape_json(hf);
        rfp_histogram_free(hf);

        if (!shape) {
            duckdb_vector_assign_string_element_len(output, row, "", 0);
            continue;
        }
        duckdb_vector_assign_string_element(output, row, shape);
        rfp_free_string(shape);
    }
}

/* ── bf_histogram_similarity(a VARCHAR, b VARCHAR) -> DOUBLE ─────── */

static void bf_histogram_similarity_func(duckdb_function_info info,
                                         duckdb_data_chunk input,
                                         duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);
    double *result_data = (double *)duckdb_vector_get_data(output);

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_2(row);
        uint32_t len_a, len_b;
        const char *json_a = str_ptr(&data0[row], &len_a);
        const char *json_b = str_ptr(&data1[row], &len_b);

        rfp_histogram *a = rfp_histogram_from_json(json_a, len_a);
        rfp_histogram *b = rfp_histogram_from_json(json_b, len_b);
        if (!a || !b) {
            rfp_histogram_free(a);
            rfp_histogram_free(b);
            result_data[row] = 1.0;
            continue;
        }
        result_data[row] = rfp_histogram_shape_similarity(a, b);
        rfp_histogram_free(a);
        rfp_histogram_free(b);
    }
}

/* ── bf_hash(text VARCHAR) -> UINTEGER ────────────────────────────── */

static void bf_hash_func(duckdb_function_info info,
                          duckdb_data_chunk input,
                          duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint32_t *result_data = (uint32_t *)duckdb_vector_get_data(output);

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_1(row);
        uint32_t len;
        const char *str = str_ptr(&data0[row], &len);
        result_data[row] = rfp_fnv1a(str, len);
    }
}

/* ── bf_hash_normalized(text VARCHAR) -> UINTEGER ────────────────── */

static void bf_hash_normalized_func(duckdb_function_info info,
                                     duckdb_data_chunk input,
                                     duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint32_t *result_data = (uint32_t *)duckdb_vector_get_data(output);

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_1(row);
        uint32_t len;
        const char *str = str_ptr(&data0[row], &len);
        result_data[row] = rfp_fnv1a_normalized(str, len, RFP_NORM_CASEFOLD);
    }
}

/* ── bf_cc_signature(text VARCHAR) -> UBIGINT ────────────────────────
   One-pass char-class structural feature bitmask (the structural type layer).
   Compose domain envelopes with bit_and (necessary) / bit_or (envelope). */
static void bf_cc_signature_func(duckdb_function_info info,
                                 duckdb_data_chunk input,
                                 duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *result_data = (uint64_t *)duckdb_vector_get_data(output);
    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_1(row);
        uint32_t len;
        const char *str = str_ptr(&data0[row], &len);
        result_data[row] = rfp_cc_signature(str, len);
    }
}

/* ── bf_cc_feature_name(bit INTEGER) -> VARCHAR ──────────────────────
   Decode a feature-bit index to its name (the tiny feature registry). */
static void bf_cc_feature_name_func(duckdb_function_info info,
                                    duckdb_data_chunk input,
                                    duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    int32_t *data0 = (int32_t *)duckdb_vector_get_data(vec0);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_1(row);
        const char *nm = rfp_cc_feature_name((int)data0[row]);
        if (!nm) {
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }
        duckdb_vector_assign_string_element_len(output, row, nm, strlen(nm));
    }
}

/* ── bf_cc_feature_bit(name VARCHAR) -> INTEGER ──────────────────────
   Inverse of bf_cc_feature_name; NULL when the name is unknown (a divergence
   flag when harmonizing feature definitions across sources). */
static void bf_cc_feature_bit_func(duckdb_function_info info,
                                   duckdb_data_chunk input,
                                   duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    int32_t *result_data = (int32_t *)duckdb_vector_get_data(output);
    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_1(row);
        char *name = str_dup_z(&data0[row]);
        int bit = rfp_cc_feature_bit(name);
        free(name);
        if (bit < 0) {
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }
        result_data[row] = bit;
    }
}

/* ── bf_cc_eval(sig UBIGINT, expr VARCHAR) -> BOOLEAN ────────────────
   Evaluate a boolean feature-expression against a precomputed signature; NULL
   when the expression references an unknown name or fails to parse (broken FK /
   syntax error). Pairs with bf_cc_signature so composites-as-data run in SQL:
   bf_cc_eval(bf_cc_signature(x), 'has_dollar & has_digit'). */
static void bf_cc_eval_func(duckdb_function_info info,
                            duckdb_data_chunk input,
                            duckdb_vector output) {
    (void)info;
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    uint64_t *data0 = (uint64_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);
    bool *result_data = (bool *)duckdb_vector_get_data(output);
    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_2(row);
        char *expr = str_dup_z(&data1[row]);
        int v = rfp_cc_eval(data0[row], expr);
        free(expr);
        if (v < 0) {
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }
        result_data[row] = (v != 0);
    }
}

/* ── bf_cc_features_json() -> VARCHAR ────────────────────────────────
   Dump the whole feature registry as JSON, for interning into the host DB. */
static void bf_cc_features_json_func(duckdb_function_info info,
                                     duckdb_data_chunk input,
                                     duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    char *js = rfp_cc_features_json();
    size_t jlen = js ? strlen(js) : 0;
    for (idx_t row = 0; row < size; row++)
        duckdb_vector_assign_string_element_len(output, row, js ? js : "", jlen);
    if (js) rfp_free_string(js);
}

/* ── bf_to_array(blob BLOB) -> UINTEGER[] ────────────────────────── */

static void bf_to_array_func(duckdb_function_info info,
                              duckdb_data_chunk input,
                              duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);

    duckdb_list_entry *entries = (duckdb_list_entry *)duckdb_vector_get_data(output);
    duckdb_vector child = duckdb_list_vector_get_child(output);

    idx_t total_elements = 0;

    /* First pass: count total elements to reserve space */
    for (idx_t row = 0; row < size; row++) {
        if (!duckdb_validity_row_is_valid(val0, row)) {
            entries[row].offset = total_elements;
            entries[row].length = 0;
            continue;
        }
        uint32_t len;
        const char *blob = str_ptr(&data0[row], &len);
        rfp_bitmap *bm = rfp_deserialize(blob, len);
        uint64_t card = bm ? rfp_cardinality(bm) : 0;
        entries[row].offset = total_elements;
        entries[row].length = card;
        total_elements += card;
        if (bm) rfp_free(bm);
    }

    duckdb_list_vector_reserve(output, total_elements);
    duckdb_list_vector_set_size(output, total_elements);
    uint32_t *child_data = (uint32_t *)duckdb_vector_get_data(child);

    /* Second pass: fill child data */
    for (idx_t row = 0; row < size; row++) {
        if (!duckdb_validity_row_is_valid(val0, row) || entries[row].length == 0)
            continue;
        uint32_t len;
        const char *blob = str_ptr(&data0[row], &len);
        rfp_bitmap *bm = rfp_deserialize(blob, len);
        if (bm) {
            rfp_to_uint32_array(bm, child_data + entries[row].offset,
                                entries[row].length);
            rfp_free(bm);
        }
    }
}

/* ── bf_from_array(arr UINTEGER[]) -> BLOB ───────────────────────── */

static void bf_from_array_func(duckdb_function_info info,
                                duckdb_data_chunk input,
                                duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_list_entry *list_entries = (duckdb_list_entry *)duckdb_vector_get_data(vec0);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    duckdb_vector child = duckdb_list_vector_get_child(vec0);
    uint32_t *child_data = (uint32_t *)duckdb_vector_get_data(child);

    for (idx_t row = 0; row < size; row++) {
        if (!duckdb_validity_row_is_valid(val0, row)) {
            duckdb_validity_set_row_invalid(
                duckdb_vector_get_validity(output), row);
            continue;
        }
        duckdb_list_entry entry = list_entries[row];
        rfp_bitmap *bm = rfp_from_uint32_array(
            child_data + entry.offset, entry.length);
        if (!bm) {
            duckdb_validity_set_row_invalid(
                duckdb_vector_get_validity(output), row);
            continue;
        }
        size_t sz = rfp_serialized_size(bm);
        char *buf = (char *)malloc(sz);
        if (!buf) {
            rfp_free(bm);
            duckdb_validity_set_row_invalid(
                duckdb_vector_get_validity(output), row);
            continue;
        }
        rfp_serialize(bm, buf, sz);
        rfp_free(bm);

        duckdb_vector_assign_string_element_len(output, row, buf, sz);
        free(buf);
    }
}

/* ── bf_contains(bitmap BLOB, value UINTEGER) -> BOOLEAN ─────────── */

static void bf_contains_func(duckdb_function_info info,
                              duckdb_data_chunk input,
                              duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    uint32_t *data1 = (uint32_t *)duckdb_vector_get_data(vec1);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);
    bool *result_data = (bool *)duckdb_vector_get_data(output);

    /* Cache: when the bitmap argument is constant across the chunk
     * (common in joins), deserialize once and reuse. */
    rfp_bitmap *cached_bm = NULL;
    const char *cached_ptr = NULL;
    uint32_t cached_len = 0;

    for (idx_t row = 0; row < size; row++) {
        if (!duckdb_validity_row_is_valid(val0, row) ||
            !duckdb_validity_row_is_valid(val1, row)) {
            result_data[row] = false;
            continue;
        }
        uint32_t len;
        const char *blob = str_ptr(&data0[row], &len);

        /* Reuse cached bitmap if same blob pointer and length */
        if (blob != cached_ptr || len != cached_len) {
            if (cached_bm) rfp_free(cached_bm);
            cached_bm = rfp_deserialize(blob, len);
            cached_ptr = blob;
            cached_len = len;
        }
        if (!cached_bm) { result_data[row] = false; continue; }
        result_data[row] = rfp_contains(cached_bm, data1[row]);
    }
    if (cached_bm) rfp_free(cached_bm);
}

/* ── Register all functions ──────────────────────────────────────── */

static void register_functions(duckdb_connection connection) {
    duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_logical_type blob_type    = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    duckdb_logical_type ubigint_type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_logical_type double_type  = duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);

    duckdb_logical_type uint_type = duckdb_create_logical_type(DUCKDB_TYPE_UINTEGER);

    /* bf_hash(text VARCHAR) -> UINTEGER */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_hash");
        duckdb_scalar_function_add_parameter(f, varchar_type);
        duckdb_scalar_function_set_return_type(f, uint_type);
        duckdb_scalar_function_set_function(f, bf_hash_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_cc_signature(text VARCHAR) -> UBIGINT */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_cc_signature");
        duckdb_scalar_function_add_parameter(f, varchar_type);
        duckdb_scalar_function_set_return_type(f, ubigint_type);
        duckdb_scalar_function_set_function(f, bf_cc_signature_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_cc_feature_name(bit INTEGER) -> VARCHAR */
    {
        duckdb_logical_type int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_cc_feature_name");
        duckdb_scalar_function_add_parameter(f, int_type);
        duckdb_scalar_function_set_return_type(f, varchar_type);
        duckdb_scalar_function_set_function(f, bf_cc_feature_name_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
        duckdb_destroy_logical_type(&int_type);
    }

    /* bf_cc_feature_bit(name VARCHAR) -> INTEGER */
    {
        duckdb_logical_type int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_cc_feature_bit");
        duckdb_scalar_function_add_parameter(f, varchar_type);
        duckdb_scalar_function_set_return_type(f, int_type);
        duckdb_scalar_function_set_function(f, bf_cc_feature_bit_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
        duckdb_destroy_logical_type(&int_type);
    }

    /* bf_cc_eval(sig UBIGINT, expr VARCHAR) -> BOOLEAN */
    {
        duckdb_logical_type bool_type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_cc_eval");
        duckdb_scalar_function_add_parameter(f, ubigint_type);
        duckdb_scalar_function_add_parameter(f, varchar_type);
        duckdb_scalar_function_set_return_type(f, bool_type);
        duckdb_scalar_function_set_function(f, bf_cc_eval_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
        duckdb_destroy_logical_type(&bool_type);
    }

    /* bf_cc_features_json() -> VARCHAR  (no args) */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_cc_features_json");
        duckdb_scalar_function_set_return_type(f, varchar_type);
        duckdb_scalar_function_set_function(f, bf_cc_features_json_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_hash_normalized(text VARCHAR) -> UINTEGER */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_hash_normalized");
        duckdb_scalar_function_add_parameter(f, varchar_type);
        duckdb_scalar_function_set_return_type(f, uint_type);
        duckdb_scalar_function_set_function(f, bf_hash_normalized_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_build_json(json VARCHAR) -> BLOB */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_build_json");
        duckdb_scalar_function_add_parameter(f, varchar_type);
        duckdb_scalar_function_set_return_type(f, blob_type);
        duckdb_scalar_function_set_function(f, bf_build_json_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_build(value VARCHAR) -> BLOB   [aggregate] */
    {
        duckdb_aggregate_function f = duckdb_create_aggregate_function();
        duckdb_aggregate_function_set_name(f, "bf_build");
        duckdb_aggregate_function_add_parameter(f, varchar_type);
        duckdb_aggregate_function_set_return_type(f, blob_type);
        duckdb_aggregate_function_set_functions(f, bf_build_state_size,
                                                bf_build_init, bf_build_update,
                                                bf_build_combine, bf_build_finalize);
        duckdb_aggregate_function_set_destructor(f, bf_build_destroy);
        duckdb_register_aggregate_function(connection, f);
        duckdb_destroy_aggregate_function(&f);
    }

    /* bf_sha256(blob BLOB) -> VARCHAR */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_sha256");
        duckdb_scalar_function_add_parameter(f, blob_type);
        duckdb_scalar_function_set_return_type(f, varchar_type);
        duckdb_scalar_function_set_function(f, bf_sha256_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_checksum(value VARCHAR) -> VARCHAR   [aggregate] */
    {
        duckdb_aggregate_function f = duckdb_create_aggregate_function();
        duckdb_aggregate_function_set_name(f, "bf_checksum");
        duckdb_aggregate_function_add_parameter(f, varchar_type);
        duckdb_aggregate_function_set_return_type(f, varchar_type);
        duckdb_aggregate_function_set_functions(f, bf_build_state_size,
                                                bf_build_init, bf_build_update,
                                                bf_build_combine, bf_checksum_finalize);
        duckdb_aggregate_function_set_destructor(f, bf_build_destroy);
        duckdb_register_aggregate_function(connection, f);
        duckdb_destroy_aggregate_function(&f);
    }

    /* bf_intersect(a BLOB, b BLOB) -> BLOB */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_intersect");
        duckdb_scalar_function_add_parameter(f, blob_type);
        duckdb_scalar_function_add_parameter(f, blob_type);
        duckdb_scalar_function_set_return_type(f, blob_type);
        duckdb_scalar_function_set_function(f, bf_intersect_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_union(a BLOB, b BLOB) -> BLOB */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_union");
        duckdb_scalar_function_add_parameter(f, blob_type);
        duckdb_scalar_function_add_parameter(f, blob_type);
        duckdb_scalar_function_set_return_type(f, blob_type);
        duckdb_scalar_function_set_function(f, bf_union_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_difference(a BLOB, b BLOB) -> BLOB */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_difference");
        duckdb_scalar_function_add_parameter(f, blob_type);
        duckdb_scalar_function_add_parameter(f, blob_type);
        duckdb_scalar_function_set_return_type(f, blob_type);
        duckdb_scalar_function_set_function(f, bf_difference_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_cardinality(blob BLOB) -> UBIGINT */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_cardinality");
        duckdb_scalar_function_add_parameter(f, blob_type);
        duckdb_scalar_function_set_return_type(f, ubigint_type);
        duckdb_scalar_function_set_function(f, bf_cardinality_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_intersection_card(a BLOB, b BLOB) -> UBIGINT */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_intersection_card");
        duckdb_scalar_function_add_parameter(f, blob_type);
        duckdb_scalar_function_add_parameter(f, blob_type);
        duckdb_scalar_function_set_return_type(f, ubigint_type);
        duckdb_scalar_function_set_function(f, bf_intersection_card_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_containment(probe BLOB, ref BLOB) -> DOUBLE */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_containment");
        duckdb_scalar_function_add_parameter(f, blob_type);
        duckdb_scalar_function_add_parameter(f, blob_type);
        duckdb_scalar_function_set_return_type(f, double_type);
        duckdb_scalar_function_set_function(f, bf_containment_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_jaccard(a BLOB, b BLOB) -> DOUBLE */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_jaccard");
        duckdb_scalar_function_add_parameter(f, blob_type);
        duckdb_scalar_function_add_parameter(f, blob_type);
        duckdb_scalar_function_set_return_type(f, double_type);
        duckdb_scalar_function_set_function(f, bf_jaccard_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_to_base64(blob BLOB) -> VARCHAR */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_to_base64");
        duckdb_scalar_function_add_parameter(f, blob_type);
        duckdb_scalar_function_set_return_type(f, varchar_type);
        duckdb_scalar_function_set_function(f, bf_to_base64_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_from_base64(text VARCHAR) -> BLOB */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_from_base64");
        duckdb_scalar_function_add_parameter(f, varchar_type);
        duckdb_scalar_function_set_return_type(f, blob_type);
        duckdb_scalar_function_set_function(f, bf_from_base64_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_containment_json(json VARCHAR, ref BLOB) -> DOUBLE */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_containment_json");
        duckdb_scalar_function_add_parameter(f, varchar_type);
        duckdb_scalar_function_add_parameter(f, blob_type);
        duckdb_scalar_function_set_return_type(f, double_type);
        duckdb_scalar_function_set_function(f, bf_containment_json_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_build_json_normalized(json VARCHAR) -> BLOB */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_build_json_normalized");
        duckdb_scalar_function_add_parameter(f, varchar_type);
        duckdb_scalar_function_set_return_type(f, blob_type);
        duckdb_scalar_function_set_function(f, bf_build_json_normalized_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_containment_json_normalized(json VARCHAR, ref BLOB) -> DOUBLE */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_containment_json_normalized");
        duckdb_scalar_function_add_parameter(f, varchar_type);
        duckdb_scalar_function_add_parameter(f, blob_type);
        duckdb_scalar_function_set_return_type(f, double_type);
        duckdb_scalar_function_set_function(f, bf_containment_json_normalized_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_histogram_set_shape(hist VARCHAR, shape VARCHAR) -> VARCHAR */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_histogram_set_shape");
        duckdb_scalar_function_add_parameter(f, varchar_type);
        duckdb_scalar_function_add_parameter(f, varchar_type);
        duckdb_scalar_function_set_return_type(f, varchar_type);
        duckdb_scalar_function_set_function(f, bf_histogram_set_shape_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_histogram_containment(hist VARCHAR, domain BLOB) -> DOUBLE */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_histogram_containment");
        duckdb_scalar_function_add_parameter(f, varchar_type);
        duckdb_scalar_function_add_parameter(f, blob_type);
        duckdb_scalar_function_set_return_type(f, double_type);
        duckdb_scalar_function_set_function(f, bf_histogram_containment_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_histogram_bitmap(hist VARCHAR) -> BLOB */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_histogram_bitmap");
        duckdb_scalar_function_add_parameter(f, varchar_type);
        duckdb_scalar_function_set_return_type(f, blob_type);
        duckdb_scalar_function_set_function(f, bf_histogram_bitmap_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_histogram_shape(hist VARCHAR) -> VARCHAR */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_histogram_shape");
        duckdb_scalar_function_add_parameter(f, varchar_type);
        duckdb_scalar_function_set_return_type(f, varchar_type);
        duckdb_scalar_function_set_function(f, bf_histogram_shape_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_histogram_similarity(a VARCHAR, b VARCHAR) -> DOUBLE */
    {
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_histogram_similarity");
        duckdb_scalar_function_add_parameter(f, varchar_type);
        duckdb_scalar_function_add_parameter(f, varchar_type);
        duckdb_scalar_function_set_return_type(f, double_type);
        duckdb_scalar_function_set_function(f, bf_histogram_similarity_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
    }

    /* bf_to_array(blob BLOB) -> UINTEGER[] */
    {
        duckdb_logical_type list_type = duckdb_create_list_type(uint_type);
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_to_array");
        duckdb_scalar_function_add_parameter(f, blob_type);
        duckdb_scalar_function_set_return_type(f, list_type);
        duckdb_scalar_function_set_function(f, bf_to_array_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
        duckdb_destroy_logical_type(&list_type);
    }

    /* bf_from_array(arr UINTEGER[]) -> BLOB */
    {
        duckdb_logical_type list_type = duckdb_create_list_type(uint_type);
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_from_array");
        duckdb_scalar_function_add_parameter(f, list_type);
        duckdb_scalar_function_set_return_type(f, blob_type);
        duckdb_scalar_function_set_function(f, bf_from_array_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
        duckdb_destroy_logical_type(&list_type);
    }

    /* bf_contains(bitmap BLOB, value UINTEGER) -> BOOLEAN */
    {
        duckdb_logical_type bool_type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
        duckdb_scalar_function f = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(f, "bf_contains");
        duckdb_scalar_function_add_parameter(f, blob_type);
        duckdb_scalar_function_add_parameter(f, uint_type);
        duckdb_scalar_function_set_return_type(f, bool_type);
        duckdb_scalar_function_set_function(f, bf_contains_func);
        duckdb_register_scalar_function(connection, f);
        duckdb_destroy_scalar_function(&f);
        duckdb_destroy_logical_type(&bool_type);
    }

    duckdb_destroy_logical_type(&uint_type);
    duckdb_destroy_logical_type(&varchar_type);
    duckdb_destroy_logical_type(&blob_type);
    duckdb_destroy_logical_type(&ubigint_type);
    duckdb_destroy_logical_type(&double_type);
}

/* ── Extension entrypoint ────────────────────────────────────────── */

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection,
                             duckdb_extension_info info,
                             struct duckdb_extension_access *access) {
    register_functions(connection);
    return true;
}
