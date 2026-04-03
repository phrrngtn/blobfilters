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

/* Normalization modes (composable via bitwise OR) */
typedef enum {
    RFP_NORM_NONE     = 0,
    RFP_NORM_CASEFOLD = 1  /* NFKD + strip combining marks + casefold */
} rfp_norm_mode;

/* Lifecycle */
rfp_bitmap *rfp_create(void);
rfp_bitmap *rfp_copy(const rfp_bitmap *bm);
void        rfp_free(rfp_bitmap *bm);

/* Hash functions (FNV-1a) — exposed for direct use */
uint32_t rfp_fnv1a(const void *data, size_t len);
uint32_t rfp_fnv1a_normalized(const void *data, size_t len, rfp_norm_mode mode);

/* Add values */
void rfp_add_uint32(rfp_bitmap *bm, uint32_t val);
void rfp_add_hash(rfp_bitmap *bm, const void *data, size_t len);  /* FNV-1a -> add */

/* Normalized hash: applies normalization before FNV-1a hashing.
 * RFP_NORM_CASEFOLD: NFKD decomposition, strip combining marks, casefold. */
void rfp_add_hash_normalized(rfp_bitmap *bm, const void *data, size_t len,
                              rfp_norm_mode mode);

/* Build from JSON array of strings: ["foo","bar","baz"]
 * Hashes each string and adds to bitmap. Returns 0 on success, -1 on parse error. */
int rfp_add_json_array(rfp_bitmap *bm, const char *json, size_t json_len);

/* Like rfp_add_json_array but applies normalization before hashing. */
int rfp_add_json_array_normalized(rfp_bitmap *bm, const char *json, size_t json_len,
                                   rfp_norm_mode mode);

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

/* Array conversion: expand bitmap to sorted uint32 array / build from array */
/* Returns number of elements written. Caller provides buf of at least
 * rfp_cardinality(bm) uint32_t slots. */
uint64_t rfp_to_uint32_array(const rfp_bitmap *bm, uint32_t *buf, uint64_t buf_len);
/* Build bitmap from array of uint32 values. */
rfp_bitmap *rfp_from_uint32_array(const uint32_t *vals, uint64_t count);

/* Membership test */
int rfp_contains(const rfp_bitmap *bm, uint32_t val);

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

/* ========================================================================
 * Histogram fingerprint — combines bitmap with frequency weights and shape
 * ======================================================================== */

typedef struct rfp_histogram rfp_histogram;

rfp_histogram *rfp_histogram_create(void);
void           rfp_histogram_free(rfp_histogram *hf);

/* Add a value with a frequency weight (source-agnostic).
 * This is the universal entry point — works for any source:
 *   TABLESAMPLE:  key=sampled_value, weight=COUNT(*)
 *   pg_stats MCV: key=most_common_val, weight=freq*reltuples
 *   blobboxes:    key=extracted_cell, weight=occurrence_count
 *   full take:    key=column_value, weight=1.0 */
void rfp_histogram_add_value(rfp_histogram *hf,
                             const char *key, size_t key_len,
                             double weight);

/* Like rfp_histogram_add_value but applies normalization before hashing. */
void rfp_histogram_add_value_normalized(rfp_histogram *hf,
                                         const char *key, size_t key_len,
                                         double weight, rfp_norm_mode mode);

/* Add a SQL Server histogram step (5-column convenience wrapper).
 * Accumulates range_rows/distinct_range_rows for shape computation. */
void rfp_histogram_add_step(rfp_histogram *hf,
                            const char *key, size_t key_len,
                            double equal_rows, double range_rows,
                            double distinct_range_rows, double avg_range_rows);

/* Set shape metrics from an externally-computed JSON object.
 * Recognizes well-known keys: cardinality_ratio, repeatability, discreteness,
 * range_density. Any additional keys are preserved as-is.
 * Call this INSTEAD of finalize when shape is computed in the SQL layer. */
void rfp_histogram_set_shape(rfp_histogram *hf,
                             const char *shape_json, size_t len);

/* Set provenance tag (e.g., "full_take", "tablesample", "sqlserver_histogram",
 * "pg_stats", "blobboxes"). Stored in the JSON envelope. */
void rfp_histogram_set_source(rfp_histogram *hf,
                              const char *source, size_t len);

/* Finalize shape metrics from accumulated step data.
 * Only needed for the 5-arg add_step path (SQL Server histograms).
 * No-op if shape was already set via set_shape. */
void rfp_histogram_finalize(rfp_histogram *hf);

/* Serialize to/from JSON string. Caller must free returned string with rfp_free_string(). */
char          *rfp_histogram_to_json(const rfp_histogram *hf);
rfp_histogram *rfp_histogram_from_json(const char *json, size_t len);

/* Extract the raw bitmap (for use with existing rfp_containment etc.)
 * Returns a pointer owned by the histogram — do NOT free it. */
const rfp_bitmap *rfp_histogram_bitmap(const rfp_histogram *hf);

/* Weighted containment: sum(equal_rows for matched keys) / sum(all equal_rows)
 * "What fraction of the column's rows have values in this domain?" */
double rfp_histogram_weighted_containment(const rfp_histogram *hf,
                                          const rfp_bitmap *domain);

/* Shape metric accessors */
double rfp_histogram_cardinality_ratio(const rfp_histogram *hf);
double rfp_histogram_repeatability(const rfp_histogram *hf);
double rfp_histogram_discreteness(const rfp_histogram *hf);
double rfp_histogram_range_density(const rfp_histogram *hf);

/* Return full shape as JSON string (well-known + extra fields).
 * Caller must free returned string with rfp_free_string(). */
char *rfp_histogram_shape_json(const rfp_histogram *hf);

/* Shape similarity between two histograms (Euclidean distance in
 * normalized feature space: cardinality_ratio, repeatability, discreteness).
 * Returns 0.0 for identical shapes, higher for more different. */
double rfp_histogram_shape_similarity(const rfp_histogram *a,
                                      const rfp_histogram *b);

/* Last error message (thread-local) */
const char *rfp_errmsg(void);

#ifdef __cplusplus
}
#endif

#endif /* ROARING_FP_H */
