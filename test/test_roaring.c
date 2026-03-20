/*
 * Test program for roaring_fp core library
 */

#include "roaring_fp.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

static int tests_run = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else { printf("  FAIL: %s\n", msg); } \
} while(0)

#define CHECK_CLOSE(a, b, tol, msg) do { \
    tests_run++; \
    if (fabs((a) - (b)) < (tol)) { tests_passed++; printf("  PASS: %s (%.4f)\n", msg, (double)(a)); } \
    else { printf("  FAIL: %s (got %.4f, expected ~%.4f)\n", msg, (double)(a), (double)(b)); } \
} while(0)

int main(void) {
    printf("=== Test 1: Create and add values ===\n");
    {
        rfp_bitmap *bm = rfp_create();
        CHECK(bm != NULL, "rfp_create returns non-null");

        for (uint32_t i = 0; i < 1000; i++) {
            rfp_add_uint32(bm, i);
        }
        CHECK(rfp_cardinality(bm) == 1000, "cardinality after adding 1000 values");
        rfp_free(bm);
    }

    printf("\n=== Test 2: Hash-based add ===\n");
    {
        rfp_bitmap *bm = rfp_create();
        const char *strings[] = {"hello", "world", "foo", "bar", "baz"};
        for (int i = 0; i < 5; i++) {
            rfp_add_hash(bm, strings[i], strlen(strings[i]));
        }
        CHECK(rfp_cardinality(bm) == 5, "cardinality of 5 distinct strings");

        /* Adding duplicate should not increase cardinality */
        rfp_add_hash(bm, "hello", 5);
        CHECK(rfp_cardinality(bm) == 5, "cardinality unchanged after duplicate");
        rfp_free(bm);
    }

    printf("\n=== Test 3: JSON array add ===\n");
    {
        rfp_bitmap *bm = rfp_create();
        const char *json = "[\"alpha\",\"beta\",\"gamma\"]";
        int rc = rfp_add_json_array(bm, json, strlen(json));
        CHECK(rc == 0, "rfp_add_json_array returns 0 on valid JSON");
        CHECK(rfp_cardinality(bm) == 3, "cardinality of 3 JSON strings");

        /* Invalid JSON */
        rc = rfp_add_json_array(bm, "not json", 8);
        CHECK(rc == -1, "rfp_add_json_array returns -1 on invalid JSON");
        rfp_free(bm);
    }

    printf("\n=== Test 4: Serialization round-trip ===\n");
    {
        rfp_bitmap *bm = rfp_create();
        for (uint32_t i = 0; i < 500; i++) {
            rfp_add_uint32(bm, i);
        }

        size_t size = rfp_serialized_size(bm);
        CHECK(size > 0, "serialized size > 0");

        char *buf = (char *)malloc(size);
        size_t written = rfp_serialize(bm, buf, size);
        CHECK(written == size, "serialize writes expected bytes");

        rfp_bitmap *bm2 = rfp_deserialize(buf, written);
        CHECK(bm2 != NULL, "deserialize returns non-null");
        CHECK(rfp_cardinality(bm2) == 500, "deserialized bitmap has correct cardinality");

        free(buf);
        rfp_free(bm);
        rfp_free(bm2);
    }

    printf("\n=== Test 5: Base64 round-trip ===\n");
    {
        rfp_bitmap *bm = rfp_create();
        rfp_add_json_array(bm, "[\"x\",\"y\",\"z\"]", 13);

        size_t b64_size = rfp_base64_size(bm);
        CHECK(b64_size > 0, "base64 size > 0");

        char *b64 = (char *)malloc(b64_size + 1);
        size_t written = rfp_to_base64(bm, b64, b64_size);
        CHECK(written > 0, "to_base64 writes bytes");
        b64[written] = '\0';
        printf("    base64: %s\n", b64);

        rfp_bitmap *bm2 = rfp_from_base64(b64, written);
        CHECK(bm2 != NULL, "from_base64 returns non-null");
        CHECK(rfp_cardinality(bm2) == rfp_cardinality(bm), "base64 round-trip preserves cardinality");

        free(b64);
        rfp_free(bm);
        rfp_free(bm2);
    }

    printf("\n=== Test 6: Intersection and metrics ===\n");
    {
        rfp_bitmap *a = rfp_create();
        rfp_bitmap *b = rfp_create();

        /* a = {0..99}, b = {50..149} */
        for (uint32_t i = 0; i < 100; i++) rfp_add_uint32(a, i);
        for (uint32_t i = 50; i < 150; i++) rfp_add_uint32(b, i);

        CHECK(rfp_cardinality(a) == 100, "set A has 100 elements");
        CHECK(rfp_cardinality(b) == 100, "set B has 100 elements");
        CHECK(rfp_intersection_card(a, b) == 50, "intersection has 50 elements");

        /* containment(a, b) = 50/100 = 0.5 */
        CHECK_CLOSE(rfp_containment(a, b), 0.5, 0.001, "containment(a,b)");

        /* jaccard = 50/150 = 0.333 */
        CHECK_CLOSE(rfp_jaccard(a, b), 0.3333, 0.01, "jaccard(a,b)");

        rfp_free(a);
        rfp_free(b);
    }

    printf("\n=== Test 7: OR in-place ===\n");
    {
        rfp_bitmap *a = rfp_create();
        rfp_bitmap *b = rfp_create();
        for (uint32_t i = 0; i < 50; i++) rfp_add_uint32(a, i);
        for (uint32_t i = 25; i < 75; i++) rfp_add_uint32(b, i);

        rfp_or_inplace(a, b);
        CHECK(rfp_cardinality(a) == 75, "OR gives union of 75 elements");

        rfp_free(a);
        rfp_free(b);
    }

    printf("\n=== Test 8: Copy ===\n");
    {
        rfp_bitmap *bm = rfp_create();
        for (uint32_t i = 0; i < 100; i++) rfp_add_uint32(bm, i);

        rfp_bitmap *copy = rfp_copy(bm);
        CHECK(copy != NULL, "copy returns non-null");
        CHECK(rfp_cardinality(copy) == 100, "copy has same cardinality");

        /* Mutating original doesn't affect copy */
        rfp_add_uint32(bm, 999);
        CHECK(rfp_cardinality(bm) == 101, "original grows");
        CHECK(rfp_cardinality(copy) == 100, "copy unchanged");

        rfp_free(bm);
        rfp_free(copy);
    }

    printf("\n=== Test 9: Probe JSON ===\n");
    {
        /* Build two reference bitmaps */
        rfp_bitmap *ref_a = rfp_create();
        rfp_bitmap *ref_b = rfp_create();
        rfp_add_hash(ref_a, "foo", 3);
        rfp_add_hash(ref_a, "bar", 3);
        rfp_add_hash(ref_a, "baz", 3);
        rfp_add_hash(ref_b, "qux", 3);
        rfp_add_hash(ref_b, "quux", 4);

        /* Probe with a JSON array of symbols against those references */
        const char *symbols = "[\"foo\",\"bar\",\"unknown\"]";
        const rfp_bitmap *refs[] = { ref_a, ref_b };
        char *result = rfp_probe_json(symbols, strlen(symbols), refs, 2);
        CHECK(result != NULL, "rfp_probe_json returns non-null");
        if (result) {
            printf("    result: %s\n", result);
            rfp_free_string(result);
        }

        rfp_free(ref_a);
        rfp_free(ref_b);
    }

    printf("\n=== Test 10: Histogram fingerprint — create and add steps ===\n");
    {
        rfp_histogram *hf = rfp_histogram_create();
        CHECK(hf != NULL, "rfp_histogram_create returns non-null");

        /* Simulate a low-cardinality dimension histogram (e.g., state_code)
         * 5 steps, all discrete (range_rows=0), high equal_rows */
        rfp_histogram_add_step(hf, "AL", 2, 10000, 0, 0, 0);
        rfp_histogram_add_step(hf, "CA", 2, 50000, 0, 0, 0);
        rfp_histogram_add_step(hf, "NY", 2, 40000, 0, 0, 0);
        rfp_histogram_add_step(hf, "TX", 2, 35000, 0, 0, 0);
        rfp_histogram_add_step(hf, "FL", 2, 25000, 0, 0, 0);

        rfp_histogram_finalize(hf);

        const rfp_bitmap *bm = rfp_histogram_bitmap(hf);
        CHECK(bm != NULL, "histogram bitmap is non-null");
        CHECK(rfp_cardinality(bm) == 5, "histogram bitmap has 5 entries");

        CHECK_CLOSE(rfp_histogram_discreteness(hf), 1.0, 0.001, "fully discrete histogram");
        CHECK(rfp_histogram_repeatability(hf) > 0, "repeatability > 0");
        CHECK(rfp_histogram_cardinality_ratio(hf) < 1.0, "cardinality_ratio < 1");

        rfp_histogram_free(hf);
    }

    printf("\n=== Test 11: Histogram JSON round-trip ===\n");
    {
        rfp_histogram *hf = rfp_histogram_create();
        rfp_histogram_add_step(hf, "AL", 2, 10000, 0, 0, 0);
        rfp_histogram_add_step(hf, "CA", 2, 50000, 0, 0, 0);
        rfp_histogram_add_step(hf, "NY", 2, 40000, 0, 0, 0);
        rfp_histogram_finalize(hf);

        char *json = rfp_histogram_to_json(hf);
        CHECK(json != NULL, "rfp_histogram_to_json returns non-null");
        if (json) {
            printf("    JSON: %.200s...\n", json);

            rfp_histogram *hf2 = rfp_histogram_from_json(json, strlen(json));
            CHECK(hf2 != NULL, "rfp_histogram_from_json returns non-null");

            if (hf2) {
                const rfp_bitmap *bm2 = rfp_histogram_bitmap(hf2);
                CHECK(rfp_cardinality(bm2) == 3, "round-tripped bitmap has 3 entries");
                CHECK_CLOSE(rfp_histogram_discreteness(hf2), rfp_histogram_discreteness(hf),
                            0.001, "discreteness preserved");
                CHECK_CLOSE(rfp_histogram_repeatability(hf2), rfp_histogram_repeatability(hf),
                            0.01, "repeatability preserved");
                rfp_histogram_free(hf2);
            }
            rfp_free_string(json);
        }
        rfp_histogram_free(hf);
    }

    printf("\n=== Test 12: Histogram weighted containment ===\n");
    {
        /* Build a histogram with known keys */
        rfp_histogram *hf = rfp_histogram_create();
        rfp_histogram_add_step(hf, "AL", 2, 10000, 0, 0, 0);
        rfp_histogram_add_step(hf, "CA", 2, 50000, 0, 0, 0);
        rfp_histogram_add_step(hf, "NY", 2, 40000, 0, 0, 0);
        rfp_histogram_add_step(hf, "TX", 2, 35000, 0, 0, 0);
        rfp_histogram_add_step(hf, "FL", 2, 25000, 0, 0, 0);
        rfp_histogram_finalize(hf);

        /* Build a domain bitmap containing AL, CA, NY (same hash) */
        rfp_bitmap *domain = rfp_create();
        rfp_add_hash(domain, "AL", 2);
        rfp_add_hash(domain, "CA", 2);
        rfp_add_hash(domain, "NY", 2);

        /* Weighted containment: (10000+50000+40000) / (10000+50000+40000+35000+25000)
         * = 100000 / 160000 = 0.625 */
        double wc = rfp_histogram_weighted_containment(hf, domain);
        CHECK_CLOSE(wc, 0.625, 0.001, "weighted containment AL+CA+NY");

        /* Unweighted bitmap containment: 3/5 = 0.6 */
        const rfp_bitmap *hf_bm = rfp_histogram_bitmap(hf);
        double uc = rfp_containment(hf_bm, domain);
        CHECK_CLOSE(uc, 0.6, 0.001, "unweighted containment 3/5");

        rfp_free(domain);
        rfp_histogram_free(hf);
    }

    printf("\n=== Test 13: Histogram shape similarity ===\n");
    {
        /* Two identical-shape histograms */
        rfp_histogram *a = rfp_histogram_create();
        rfp_histogram_add_step(a, "X", 1, 100, 0, 0, 0);
        rfp_histogram_add_step(a, "Y", 1, 100, 0, 0, 0);
        rfp_histogram_finalize(a);

        rfp_histogram *b = rfp_histogram_create();
        rfp_histogram_add_step(b, "P", 1, 100, 0, 0, 0);
        rfp_histogram_add_step(b, "Q", 1, 100, 0, 0, 0);
        rfp_histogram_finalize(b);

        double sim = rfp_histogram_shape_similarity(a, b);
        CHECK_CLOSE(sim, 0.0, 0.001, "identical shapes have similarity 0");

        /* A continuous histogram (high range_rows) */
        rfp_histogram *c = rfp_histogram_create();
        rfp_histogram_add_step(c, "1", 1, 1, 500, 50, 10);
        rfp_histogram_add_step(c, "100", 3, 1, 500, 50, 10);
        rfp_histogram_finalize(c);

        double diff = rfp_histogram_shape_similarity(a, c);
        CHECK(diff > 0.1, "discrete vs continuous shapes differ significantly");

        rfp_histogram_free(a);
        rfp_histogram_free(b);
        rfp_histogram_free(c);
    }

    printf("\n=== Test 14: Source-agnostic add_value (2-arg) ===\n");
    {
        rfp_histogram *hf = rfp_histogram_create();
        CHECK(hf != NULL, "rfp_histogram_create returns non-null");

        /* Simulate TABLESAMPLE: key=value, weight=count(*) */
        rfp_histogram_add_value(hf, "CA", 2, 50);
        rfp_histogram_add_value(hf, "NY", 2, 40);
        rfp_histogram_add_value(hf, "TX", 2, 35);

        const rfp_bitmap *bm = rfp_histogram_bitmap(hf);
        CHECK(rfp_cardinality(bm) == 3, "add_value bitmap has 3 entries");

        /* Weighted containment against a domain with CA and NY */
        rfp_bitmap *domain = rfp_create();
        rfp_add_hash(domain, "CA", 2);
        rfp_add_hash(domain, "NY", 2);

        /* (50+40) / (50+40+35) = 90/125 = 0.72 */
        double wc = rfp_histogram_weighted_containment(hf, domain);
        CHECK_CLOSE(wc, 0.72, 0.001, "add_value weighted containment");

        rfp_free(domain);
        rfp_histogram_free(hf);
    }

    printf("\n=== Test 15: set_shape — external shape injection ===\n");
    {
        rfp_histogram *hf = rfp_histogram_create();
        rfp_histogram_add_value(hf, "CA", 2, 50);
        rfp_histogram_add_value(hf, "NY", 2, 40);

        /* Set shape externally (as computed by SQL layer) */
        const char *shape = "{\"cardinality_ratio\":0.05,\"repeatability\":250.0,"
                            "\"discreteness\":0.9,\"range_density\":0.0,"
                            "\"data_type\":\"varchar\",\"source_table\":\"orders\"}";
        rfp_histogram_set_shape(hf, shape, strlen(shape));

        CHECK_CLOSE(rfp_histogram_cardinality_ratio(hf), 0.05, 0.001, "injected cardinality_ratio");
        CHECK_CLOSE(rfp_histogram_repeatability(hf), 250.0, 0.01, "injected repeatability");
        CHECK_CLOSE(rfp_histogram_discreteness(hf), 0.9, 0.001, "injected discreteness");

        /* finalize should be a no-op since shape is already set */
        rfp_histogram_finalize(hf);
        CHECK_CLOSE(rfp_histogram_cardinality_ratio(hf), 0.05, 0.001, "shape unchanged after finalize");

        /* JSON round-trip preserves shape + extras */
        char *json = rfp_histogram_to_json(hf);
        CHECK(json != NULL, "to_json non-null");
        if (json) {
            rfp_histogram *hf2 = rfp_histogram_from_json(json, strlen(json));
            CHECK(hf2 != NULL, "from_json non-null");
            if (hf2) {
                CHECK_CLOSE(rfp_histogram_cardinality_ratio(hf2), 0.05, 0.001,
                            "round-tripped cardinality_ratio");

                /* Verify shape_json includes extras */
                char *shape_out = rfp_histogram_shape_json(hf2);
                CHECK(shape_out != NULL, "shape_json non-null");
                if (shape_out) {
                    CHECK(strstr(shape_out, "data_type") != NULL, "extra key 'data_type' preserved");
                    CHECK(strstr(shape_out, "source_table") != NULL, "extra key 'source_table' preserved");
                    rfp_free_string(shape_out);
                }
                rfp_histogram_free(hf2);
            }
            rfp_free_string(json);
        }
        rfp_histogram_free(hf);
    }

    printf("\n=== Test 16: set_source — provenance tag ===\n");
    {
        rfp_histogram *hf = rfp_histogram_create();
        rfp_histogram_add_value(hf, "foo", 3, 1.0);
        rfp_histogram_set_source(hf, "tablesample", 11);
        rfp_histogram_finalize(hf);

        char *json = rfp_histogram_to_json(hf);
        CHECK(json != NULL, "to_json with source non-null");
        if (json) {
            CHECK(strstr(json, "\"source\":\"tablesample\"") != NULL, "source tag in JSON");
            rfp_free_string(json);
        }
        rfp_histogram_free(hf);
    }

    printf("\n=== Test 17: shape_similarity across sources ===\n");
    {
        /* Histogram A: dimension-like shape (from TABLESAMPLE) */
        rfp_histogram *a = rfp_histogram_create();
        rfp_histogram_add_value(a, "X", 1, 100);
        const char *shape_a = "{\"cardinality_ratio\":0.01,\"repeatability\":500,"
                               "\"discreteness\":1.0,\"range_density\":0}";
        rfp_histogram_set_shape(a, shape_a, strlen(shape_a));
        rfp_histogram_set_source(a, "tablesample", 11);

        /* Histogram B: measure-like shape (from pg_stats) */
        rfp_histogram *b = rfp_histogram_create();
        rfp_histogram_add_value(b, "Y", 1, 1);
        const char *shape_b = "{\"cardinality_ratio\":0.95,\"repeatability\":1.1,"
                               "\"discreteness\":0.0,\"range_density\":200}";
        rfp_histogram_set_shape(b, shape_b, strlen(shape_b));
        rfp_histogram_set_source(b, "pg_stats", 8);

        double dist = rfp_histogram_shape_similarity(a, b);
        CHECK(dist > 0.5, "dimension vs measure shapes differ significantly across sources");

        rfp_histogram_free(a);
        rfp_histogram_free(b);
    }

    printf("\n=== Test 18: Normalized hash — casefold ===\n");
    {
        rfp_bitmap *bm = rfp_create();

        /* All three should hash to the same value after NFKD + casefold */
        rfp_add_hash_normalized(bm, "California", 10, RFP_NORM_CASEFOLD);
        rfp_add_hash_normalized(bm, "california", 10, RFP_NORM_CASEFOLD);
        rfp_add_hash_normalized(bm, "CALIFORNIA", 10, RFP_NORM_CASEFOLD);
        CHECK(rfp_cardinality(bm) == 1, "case variants produce single bitmap entry");

        /* Raw hashing should produce distinct entries */
        rfp_bitmap *raw = rfp_create();
        rfp_add_hash(raw, "California", 10);
        rfp_add_hash(raw, "california", 10);
        rfp_add_hash(raw, "CALIFORNIA", 10);
        CHECK(rfp_cardinality(raw) == 3, "raw hashing keeps case variants distinct");

        rfp_free(bm);
        rfp_free(raw);
    }

    printf("\n=== Test 19: Normalized hash — accented characters ===\n");
    {
        rfp_bitmap *bm = rfp_create();

        /* José vs Jose — accent stripping via NFKD + STRIPMARK */
        rfp_add_hash_normalized(bm, "Jos\xc3\xa9", 5, RFP_NORM_CASEFOLD);  /* José (UTF-8) */
        rfp_add_hash_normalized(bm, "Jose", 4, RFP_NORM_CASEFOLD);
        CHECK(rfp_cardinality(bm) == 1, "accented and unaccented merge");

        /* Fullwidth characters — NFKD compatibility decomposition */
        rfp_bitmap *fw = rfp_create();
        rfp_add_hash_normalized(fw, "ABC", 3, RFP_NORM_CASEFOLD);
        /* Ａ Ｂ Ｃ in UTF-8: each fullwidth char is 3 bytes */
        rfp_add_hash_normalized(fw, "\xef\xbc\xa1\xef\xbc\xa2\xef\xbc\xa3", 9, RFP_NORM_CASEFOLD);
        CHECK(rfp_cardinality(fw) == 1, "fullwidth and ASCII merge");

        rfp_free(bm);
        rfp_free(fw);
    }

    printf("\n=== Test 20: Normalized JSON array ===\n");
    {
        rfp_bitmap *norm_bm = rfp_create();
        const char *json = "[\"California\",\"CALIFORNIA\",\"california\"]";
        int rc = rfp_add_json_array_normalized(norm_bm, json, strlen(json), RFP_NORM_CASEFOLD);
        CHECK(rc == 0, "rfp_add_json_array_normalized returns 0");
        CHECK(rfp_cardinality(norm_bm) == 1, "JSON case variants merge to 1");

        rfp_bitmap *raw_bm = rfp_create();
        rc = rfp_add_json_array(raw_bm, json, strlen(json));
        CHECK(rc == 0, "rfp_add_json_array returns 0");
        CHECK(rfp_cardinality(raw_bm) == 3, "JSON raw keeps 3 distinct");

        rfp_free(norm_bm);
        rfp_free(raw_bm);
    }

    printf("\n=== Test 21: Normalized histogram add_value ===\n");
    {
        rfp_histogram *hf = rfp_histogram_create();

        /* Add same state in different cases — should merge into one hash */
        rfp_histogram_add_value_normalized(hf, "CA", 2, 100, RFP_NORM_CASEFOLD);
        rfp_histogram_add_value_normalized(hf, "ca", 2, 50, RFP_NORM_CASEFOLD);
        rfp_histogram_add_value_normalized(hf, "Ca", 2, 25, RFP_NORM_CASEFOLD);

        const rfp_bitmap *bm = rfp_histogram_bitmap(hf);
        CHECK(rfp_cardinality(bm) == 1, "normalized histogram: case variants merge");

        /* Build a domain with the normalized form */
        rfp_bitmap *domain = rfp_create();
        rfp_add_hash_normalized(domain, "CA", 2, RFP_NORM_CASEFOLD);

        /* Weighted containment: all weight should match since all forms → same hash */
        double wc = rfp_histogram_weighted_containment(hf, domain);
        CHECK_CLOSE(wc, 1.0, 0.001, "normalized histogram: full weighted containment");

        rfp_free(domain);
        rfp_histogram_free(hf);
    }

    printf("\n=== Test 22: NORM_NONE passes through unchanged ===\n");
    {
        rfp_bitmap *bm_none = rfp_create();
        rfp_bitmap *bm_raw = rfp_create();

        rfp_add_hash_normalized(bm_none, "Hello", 5, RFP_NORM_NONE);
        rfp_add_hash(bm_raw, "Hello", 5);

        CHECK(rfp_intersection_card(bm_none, bm_raw) == 1, "NORM_NONE matches raw hash");

        rfp_free(bm_none);
        rfp_free(bm_raw);
    }

    printf("\n=== Results: %d/%d tests passed ===\n", tests_passed, tests_run);
    return tests_passed == tests_run ? 0 : 1;
}
