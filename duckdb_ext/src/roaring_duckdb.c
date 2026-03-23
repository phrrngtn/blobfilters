/*
 * DuckDB C API extension for roaring bitmap fingerprinting.
 *
 * Registers 13 scalar functions with bf_ prefix:
 *   bf_build_json(json_array)                        -> BLOB
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

        rfp_bitmap *bm = rfp_deserialize(blob, len);
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

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_2(row);
        uint32_t len_a, len_b;
        const char *blob_a = str_ptr(&data0[row], &len_a);
        const char *blob_b = str_ptr(&data1[row], &len_b);

        rfp_bitmap *a = rfp_deserialize(blob_a, len_a);
        rfp_bitmap *b = rfp_deserialize(blob_b, len_b);
        if (!a || !b) {
            rfp_free(a);
            rfp_free(b);
            result_data[row] = 0;
            continue;
        }
        result_data[row] = rfp_intersection_card(a, b);
        rfp_free(a);
        rfp_free(b);
    }
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

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_2(row);
        uint32_t len_a, len_b;
        const char *blob_a = str_ptr(&data0[row], &len_a);
        const char *blob_b = str_ptr(&data1[row], &len_b);

        rfp_bitmap *probe = rfp_deserialize(blob_a, len_a);
        rfp_bitmap *ref = rfp_deserialize(blob_b, len_b);
        if (!probe || !ref) {
            rfp_free(probe);
            rfp_free(ref);
            result_data[row] = 0.0;
            continue;
        }
        result_data[row] = rfp_containment(probe, ref);
        rfp_free(probe);
        rfp_free(ref);
    }
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

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_2(row);
        uint32_t len_a, len_b;
        const char *blob_a = str_ptr(&data0[row], &len_a);
        const char *blob_b = str_ptr(&data1[row], &len_b);

        rfp_bitmap *a = rfp_deserialize(blob_a, len_a);
        rfp_bitmap *b = rfp_deserialize(blob_b, len_b);
        if (!a || !b) {
            rfp_free(a);
            rfp_free(b);
            result_data[row] = 0.0;
            continue;
        }
        result_data[row] = rfp_jaccard(a, b);
        rfp_free(a);
        rfp_free(b);
    }
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

/* ── bf_containment_json(json VARCHAR, ref BLOB) -> DOUBLE ───────── */

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

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_2(row);
        uint32_t json_len, ref_len;
        const char *json = str_ptr(&data0[row], &json_len);
        const char *ref_blob = str_ptr(&data1[row], &ref_len);

        rfp_bitmap *probe = rfp_create();
        if (rfp_add_json_array(probe, json, json_len) != 0) {
            rfp_free(probe);
            result_data[row] = 0.0;
            continue;
        }
        rfp_bitmap *ref = rfp_deserialize(ref_blob, ref_len);
        if (!ref) {
            rfp_free(probe);
            result_data[row] = 0.0;
            continue;
        }
        result_data[row] = rfp_containment(probe, ref);
        rfp_free(probe);
        rfp_free(ref);
    }
}

/* ── bf_containment_json_normalized(json VARCHAR, ref BLOB) -> DOUBLE
   Probe with NFKD + casefold normalization.  The ref bitmap must have
   been built with bf_build_json_normalized for hashes to match. ────── */

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

    for (idx_t row = 0; row < size; row++) {
        CHECK_NULL_2(row);
        uint32_t json_len, ref_len;
        const char *json = str_ptr(&data0[row], &json_len);
        const char *ref_blob = str_ptr(&data1[row], &ref_len);

        rfp_bitmap *probe = rfp_create();
        if (rfp_add_json_array_normalized(probe, json, json_len,
                                          RFP_NORM_CASEFOLD) != 0) {
            rfp_free(probe);
            result_data[row] = 0.0;
            continue;
        }
        rfp_bitmap *ref = rfp_deserialize(ref_blob, ref_len);
        if (!ref) {
            rfp_free(probe);
            result_data[row] = 0.0;
            continue;
        }
        result_data[row] = rfp_containment(probe, ref);
        rfp_free(probe);
        rfp_free(ref);
    }
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

/* ── Register all functions ──────────────────────────────────────── */

static void register_functions(duckdb_connection connection) {
    duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_logical_type blob_type    = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    duckdb_logical_type ubigint_type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_logical_type double_type  = duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);

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
