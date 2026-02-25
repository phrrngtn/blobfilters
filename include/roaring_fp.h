/*
 * roaring_fp.h — Core C API for roaring bitmap fingerprinting
 *
 * Provides a simplified interface to CRoaring with JSON array support
 * for cross-environment symbol lookups.
 */

#ifndef ROARING_FP_H
#define ROARING_FP_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque handle */
typedef struct rfp_bitmap rfp_bitmap;

/* Lifecycle */
rfp_bitmap *rfp_create(void);
rfp_bitmap *rfp_copy(const rfp_bitmap *bm);
void        rfp_free(rfp_bitmap *bm);

/* Add values */
void rfp_add_uint32(rfp_bitmap *bm, uint32_t val);
void rfp_add_hash(rfp_bitmap *bm, const void *data, size_t len);  /* FNV-1a -> add */

/* Build from JSON array of strings: ["foo","bar","baz"]
 * Hashes each string and adds to bitmap. Returns 0 on success, -1 on parse error. */
int rfp_add_json_array(rfp_bitmap *bm, const char *json, size_t json_len);

/* Serialization (portable binary format for database BLOB storage) */
size_t rfp_serialized_size(const rfp_bitmap *bm);
size_t rfp_serialize(const rfp_bitmap *bm, char *buf, size_t buf_len);
rfp_bitmap *rfp_deserialize(const char *buf, size_t len);

/* Base64 serialization (for embedding in JSON documents) */
size_t rfp_base64_size(const rfp_bitmap *bm);
size_t rfp_to_base64(const rfp_bitmap *bm, char *buf, size_t buf_len);
rfp_bitmap *rfp_from_base64(const char *b64, size_t len);

/* Set operations */
void rfp_or_inplace(rfp_bitmap *dst, const rfp_bitmap *src);

/* Metrics */
uint64_t rfp_cardinality(const rfp_bitmap *bm);
uint64_t rfp_intersection_card(const rfp_bitmap *a, const rfp_bitmap *b);
double   rfp_containment(const rfp_bitmap *probe, const rfp_bitmap *ref);
double   rfp_jaccard(const rfp_bitmap *a, const rfp_bitmap *b);

/* Batch probe: given a JSON array of symbol strings and an array of reference
 * bitmaps, builds a probe bitmap from the symbols and compares against each ref.
 * Returns JSON: [{"idx":0,"containment":0.85,"jaccard":0.42}, ...]
 * Caller must free returned string with rfp_free_string(). */
char *rfp_probe_json(const char *symbols_json, size_t json_len,
                     const rfp_bitmap *const *refs, size_t num_refs);
void  rfp_free_string(char *s);

#ifdef __cplusplus
}
#endif

#endif /* ROARING_FP_H */
