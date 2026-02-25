/*
 * Fingerprint Demo: Column matching using roaring_fp core library
 *
 * Demonstrates:
 * 1. Building reference fingerprints from known column values
 * 2. Probing a JSON array of symbols against those references
 */

#include "roaring_fp.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(void) {
    printf("=======================================================\n");
    printf("  Roaring Bitmap Fingerprint Demo (Core Library)\n");
    printf("=======================================================\n\n");

    /* Step 1: Build reference fingerprints (simulating stored column data) */
    printf("--- Step 1: Build reference column fingerprints ---\n");

    rfp_bitmap *customers = rfp_create();
    rfp_bitmap *products = rfp_create();
    rfp_bitmap *orders = rfp_create();

    char buf[64];
    for (int i = 1; i <= 5000; i++) {
        snprintf(buf, sizeof(buf), "CUS-%04d", i);
        rfp_add_hash(customers, buf, strlen(buf));
    }
    for (int i = 1; i <= 1000; i++) {
        snprintf(buf, sizeof(buf), "SKU-%04d", i);
        rfp_add_hash(products, buf, strlen(buf));
    }
    for (int i = 1; i <= 10000; i++) {
        snprintf(buf, sizeof(buf), "ORD-%04d", i);
        rfp_add_hash(orders, buf, strlen(buf));
    }

    printf("  customers: cardinality=%llu\n", (unsigned long long)rfp_cardinality(customers));
    printf("  products:  cardinality=%llu\n", (unsigned long long)rfp_cardinality(products));
    printf("  orders:    cardinality=%llu\n", (unsigned long long)rfp_cardinality(orders));

    /* Step 2: Probe with a JSON array of symbols (e.g. pasted from Excel) */
    printf("\n--- Step 2: Probe JSON symbols against references ---\n");

    const char *symbols =
        "[\"CUS-0100\",\"CUS-0101\",\"CUS-0102\",\"CUS-0103\",\"CUS-0104\","
        "\"CUS-0105\",\"CUS-0106\",\"CUS-0107\",\"CUS-0108\",\"CUS-0109\","
        "\"SKU-0050\",\"SKU-0051\",\"SKU-0052\","
        "\"GARBAGE-1\",\"GARBAGE-2\"]";

    printf("  input: 10 customer IDs + 3 SKUs + 2 garbage values\n\n");

    const rfp_bitmap *refs[] = { customers, products, orders };
    const char *names[] = { "customers", "products", "orders" };

    char *result = rfp_probe_json(symbols, strlen(symbols), refs, 3);
    if (result) {
        printf("  rfp_probe_json result:\n  %s\n", result);
        rfp_free_string(result);
    }

    /* Step 3: Also show individual comparisons for clarity */
    printf("\n--- Step 3: Individual comparison (for reference) ---\n");

    rfp_bitmap *probe = rfp_create();
    rfp_add_json_array(probe, symbols, strlen(symbols));
    printf("  probe cardinality: %llu\n", (unsigned long long)rfp_cardinality(probe));

    for (int i = 0; i < 3; i++) {
        printf("  vs %-10s: containment=%.4f, jaccard=%.6f\n",
               names[i],
               rfp_containment(probe, refs[i]),
               rfp_jaccard(probe, refs[i]));
    }

    /* Step 4: Show database storage sizes */
    printf("\n--- Step 4: Serialized sizes for database storage ---\n");
    printf("  customers BLOB: %zu bytes\n", rfp_serialized_size(customers));
    printf("  products  BLOB: %zu bytes\n", rfp_serialized_size(products));
    printf("  orders    BLOB: %zu bytes\n", rfp_serialized_size(orders));

    /* Cleanup */
    rfp_free(probe);
    rfp_free(customers);
    rfp_free(products);
    rfp_free(orders);

    printf("\n=======================================================\n");
    printf("  Demo complete!\n");
    printf("=======================================================\n");
    return 0;
}
