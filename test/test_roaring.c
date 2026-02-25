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

    printf("\n=== Results: %d/%d tests passed ===\n", tests_passed, tests_run);
    return tests_passed == tests_run ? 0 : 1;
}
