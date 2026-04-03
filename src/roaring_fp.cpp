/*
 * roaring_fp.cpp — Core library implementation
 *
 * Wraps CRoaring with a C API, adds FNV-1a hashing, JSON array support
 * (via nlohmann/json), and base64 serialization.
 */

#include "roaring_fp.h"

#include "roaring.h"
#include "utf8proc.h"

#include <nlohmann/json.hpp>

#include <cmath>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <vector>

using json = nlohmann::json;

static thread_local std::string g_errmsg;

/* ========================================================================
 * Opaque handle
 * ======================================================================== */

struct rfp_bitmap {
    roaring_bitmap_t *roaring;
};

/* ========================================================================
 * FNV-1a hash
 * ======================================================================== */

static uint32_t fnv1a(const void *data, size_t len) {
    const uint8_t *bytes = static_cast<const uint8_t *>(data);
    uint32_t hash = 2166136261u;
    for (size_t i = 0; i < len; i++) {
        hash ^= bytes[i];
        hash *= 16777619u;
    }
    return hash;
}

/* ========================================================================
 * Base64 encode/decode
 * ======================================================================== */

static const char b64_table[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static size_t base64_encoded_size(size_t input_len) {
    return 4 * ((input_len + 2) / 3);
}

static size_t base64_encode(const uint8_t *input, size_t input_len, char *output, size_t output_len) {
    size_t needed = base64_encoded_size(input_len);
    if (output_len < needed) return 0;

    size_t j = 0;
    for (size_t i = 0; i < input_len; i += 3) {
        uint32_t a = input[i];
        uint32_t b = (i + 1 < input_len) ? input[i + 1] : 0;
        uint32_t c = (i + 2 < input_len) ? input[i + 2] : 0;
        uint32_t triple = (a << 16) | (b << 8) | c;

        output[j++] = b64_table[(triple >> 18) & 0x3F];
        output[j++] = b64_table[(triple >> 12) & 0x3F];
        output[j++] = (i + 1 < input_len) ? b64_table[(triple >> 6) & 0x3F] : '=';
        output[j++] = (i + 2 < input_len) ? b64_table[triple & 0x3F] : '=';
    }
    return j;
}

static int b64_decode_char(char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1;
}

static std::vector<uint8_t> base64_decode(const char *input, size_t input_len) {
    std::vector<uint8_t> result;
    result.reserve(input_len * 3 / 4);

    for (size_t i = 0; i < input_len; i += 4) {
        if (i + 3 >= input_len) break;
        int a = b64_decode_char(input[i]);
        int b = b64_decode_char(input[i + 1]);
        int c = b64_decode_char(input[i + 2]);
        int d = b64_decode_char(input[i + 3]);

        if (a < 0 || b < 0) break;

        uint32_t triple = (static_cast<uint32_t>(a) << 18) |
                          (static_cast<uint32_t>(b) << 12);
        result.push_back(static_cast<uint8_t>((triple >> 16) & 0xFF));

        if (c >= 0) {
            triple |= static_cast<uint32_t>(c) << 6;
            result.push_back(static_cast<uint8_t>((triple >> 8) & 0xFF));
        }
        if (d >= 0) {
            triple |= static_cast<uint32_t>(d);
            result.push_back(static_cast<uint8_t>(triple & 0xFF));
        }
    }
    return result;
}

/* ========================================================================
 * Lifecycle
 * ======================================================================== */

extern "C" {

rfp_bitmap *rfp_create(void) {
    auto *bm = new (std::nothrow) rfp_bitmap;
    if (!bm) return nullptr;
    bm->roaring = roaring_bitmap_create();
    if (!bm->roaring) {
        delete bm;
        return nullptr;
    }
    return bm;
}

rfp_bitmap *rfp_copy(const rfp_bitmap *bm) {
    if (!bm || !bm->roaring) return nullptr;
    auto *copy = new (std::nothrow) rfp_bitmap;
    if (!copy) return nullptr;
    copy->roaring = roaring_bitmap_copy(bm->roaring);
    if (!copy->roaring) {
        delete copy;
        return nullptr;
    }
    return copy;
}

void rfp_free(rfp_bitmap *bm) {
    if (!bm) return;
    if (bm->roaring) roaring_bitmap_free(bm->roaring);
    delete bm;
}

/* ========================================================================
 * Add values
 * ======================================================================== */

void rfp_add_uint32(rfp_bitmap *bm, uint32_t val) {
    if (bm && bm->roaring) {
        roaring_bitmap_add(bm->roaring, val);
    }
}

void rfp_add_hash(rfp_bitmap *bm, const void *data, size_t len) {
    if (bm && bm->roaring) {
        roaring_bitmap_add(bm->roaring, fnv1a(data, len));
    }
}

/* ========================================================================
 * Unicode normalization: NFKD + strip combining marks + casefold
 * ======================================================================== */

static std::string normalize_casefold(const void *data, size_t len) {
    /* utf8proc_map with NFKD + casefold + strip ignorable marks */
    utf8proc_uint8_t *result = nullptr;
    utf8proc_ssize_t result_len = utf8proc_map(
        static_cast<const utf8proc_uint8_t *>(data),
        static_cast<utf8proc_ssize_t>(len),
        &result,
        static_cast<utf8proc_option_t>(
            UTF8PROC_DECOMPOSE | UTF8PROC_COMPAT |
            UTF8PROC_CASEFOLD | UTF8PROC_STRIPMARK
        )
    );
    if (result_len < 0 || !result) {
        /* Fallback: return input as-is */
        free(result);
        return std::string(static_cast<const char *>(data), len);
    }
    std::string normalized(reinterpret_cast<char *>(result),
                           static_cast<size_t>(result_len));
    free(result);
    return normalized;
}

void rfp_add_hash_normalized(rfp_bitmap *bm, const void *data, size_t len,
                              rfp_norm_mode mode) {
    if (!bm || !bm->roaring || !data) return;

    if (mode & RFP_NORM_CASEFOLD) {
        std::string norm = normalize_casefold(data, len);
        roaring_bitmap_add(bm->roaring, fnv1a(norm.data(), norm.size()));
    } else {
        roaring_bitmap_add(bm->roaring, fnv1a(data, len));
    }
}

/* ========================================================================
 * Public hash functions (after normalize_casefold is defined)
 * ======================================================================== */

uint32_t rfp_fnv1a(const void *data, size_t len) {
    return fnv1a(data, len);
}

uint32_t rfp_fnv1a_normalized(const void *data, size_t len, rfp_norm_mode mode) {
    if (mode & RFP_NORM_CASEFOLD) {
        std::string norm = normalize_casefold(data, len);
        return fnv1a(norm.data(), norm.size());
    }
    return fnv1a(data, len);
}

int rfp_add_json_array(rfp_bitmap *bm, const char *json_str, size_t json_len) {
    if (!bm || !bm->roaring || !json_str) return -1;

    try {
        auto arr = json::parse(json_str, json_str + json_len);
        if (!arr.is_array()) return -1;

        for (const auto &item : arr) {
            if (item.is_string()) {
                std::string s = item.get<std::string>();
                roaring_bitmap_add(bm->roaring, fnv1a(s.data(), s.size()));
            }
        }
        return 0;
    } catch (...) {
        return -1;
    }
}

int rfp_add_json_array_normalized(rfp_bitmap *bm, const char *json_str, size_t json_len,
                                   rfp_norm_mode mode) {
    if (!bm || !bm->roaring || !json_str) return -1;
    if (mode == RFP_NORM_NONE) return rfp_add_json_array(bm, json_str, json_len);

    try {
        auto arr = json::parse(json_str, json_str + json_len);
        if (!arr.is_array()) return -1;

        for (const auto &item : arr) {
            if (item.is_string()) {
                std::string s = item.get<std::string>();
                rfp_add_hash_normalized(bm, s.data(), s.size(), mode);
            }
        }
        return 0;
    } catch (...) {
        return -1;
    }
}

/* ========================================================================
 * Serialization (portable binary)
 * ======================================================================== */

size_t rfp_serialized_size(const rfp_bitmap *bm) {
    if (!bm || !bm->roaring) return 0;
    roaring_bitmap_run_optimize(bm->roaring);
    return roaring_bitmap_portable_size_in_bytes(bm->roaring);
}

size_t rfp_serialize(const rfp_bitmap *bm, char *buf, size_t buf_len) {
    if (!bm || !bm->roaring || !buf) return 0;
    roaring_bitmap_run_optimize(bm->roaring);
    size_t needed = roaring_bitmap_portable_size_in_bytes(bm->roaring);
    if (buf_len < needed) return 0;
    return roaring_bitmap_portable_serialize(bm->roaring, buf);
}

rfp_bitmap *rfp_deserialize(const char *buf, size_t len) {
    if (!buf || len == 0) return nullptr;
    roaring_bitmap_t *r = roaring_bitmap_portable_deserialize_safe(buf, len);
    if (!r) return nullptr;
    auto *bm = new (std::nothrow) rfp_bitmap;
    if (!bm) {
        roaring_bitmap_free(r);
        return nullptr;
    }
    bm->roaring = r;
    return bm;
}

/* ========================================================================
 * Base64 serialization
 * ======================================================================== */

size_t rfp_base64_size(const rfp_bitmap *bm) {
    size_t raw = rfp_serialized_size(bm);
    return base64_encoded_size(raw);
}

size_t rfp_to_base64(const rfp_bitmap *bm, char *buf, size_t buf_len) {
    if (!bm || !bm->roaring || !buf) return 0;

    size_t raw_size = rfp_serialized_size(bm);
    std::vector<char> raw(raw_size);
    size_t written = rfp_serialize(bm, raw.data(), raw_size);
    if (written == 0) return 0;

    return base64_encode(reinterpret_cast<const uint8_t *>(raw.data()), written, buf, buf_len);
}

rfp_bitmap *rfp_from_base64(const char *b64, size_t len) {
    if (!b64 || len == 0) return nullptr;
    auto decoded = base64_decode(b64, len);
    if (decoded.empty()) return nullptr;
    return rfp_deserialize(reinterpret_cast<const char *>(decoded.data()), decoded.size());
}

/* ========================================================================
 * Set operations
 * ======================================================================== */

void rfp_or_inplace(rfp_bitmap *dst, const rfp_bitmap *src) {
    if (!dst || !dst->roaring || !src || !src->roaring) return;
    roaring_bitmap_or_inplace(dst->roaring, src->roaring);
}

/* ========================================================================
 * Array conversion
 * ======================================================================== */

uint64_t rfp_to_uint32_array(const rfp_bitmap *bm, uint32_t *buf, uint64_t buf_len) {
    if (!bm || !bm->roaring || !buf) return 0;
    uint64_t card = roaring_bitmap_get_cardinality(bm->roaring);
    if (card > buf_len) card = buf_len;
    roaring_bitmap_to_uint32_array(bm->roaring, buf);
    return card;
}

rfp_bitmap *rfp_from_uint32_array(const uint32_t *vals, uint64_t count) {
    if (!vals || count == 0) return rfp_create();
    rfp_bitmap *bm = rfp_create();
    if (!bm) return nullptr;
    for (uint64_t i = 0; i < count; i++) {
        roaring_bitmap_add(bm->roaring, vals[i]);
    }
    roaring_bitmap_run_optimize(bm->roaring);
    return bm;
}

int rfp_contains(const rfp_bitmap *bm, uint32_t val) {
    if (!bm || !bm->roaring) return 0;
    return roaring_bitmap_contains(bm->roaring, val) ? 1 : 0;
}

/* ========================================================================
 * Metrics
 * ======================================================================== */

uint64_t rfp_cardinality(const rfp_bitmap *bm) {
    if (!bm || !bm->roaring) return 0;
    return roaring_bitmap_get_cardinality(bm->roaring);
}

uint64_t rfp_intersection_card(const rfp_bitmap *a, const rfp_bitmap *b) {
    if (!a || !a->roaring || !b || !b->roaring) return 0;
    return roaring_bitmap_and_cardinality(a->roaring, b->roaring);
}

double rfp_containment(const rfp_bitmap *probe, const rfp_bitmap *ref) {
    if (!probe || !probe->roaring || !ref || !ref->roaring) return 0.0;
    uint64_t probe_card = roaring_bitmap_get_cardinality(probe->roaring);
    if (probe_card == 0) return 0.0;
    uint64_t inter = roaring_bitmap_and_cardinality(probe->roaring, ref->roaring);
    return static_cast<double>(inter) / static_cast<double>(probe_card);
}

double rfp_jaccard(const rfp_bitmap *a, const rfp_bitmap *b) {
    if (!a || !a->roaring || !b || !b->roaring) return 0.0;
    uint64_t card_a = roaring_bitmap_get_cardinality(a->roaring);
    uint64_t card_b = roaring_bitmap_get_cardinality(b->roaring);
    uint64_t inter = roaring_bitmap_and_cardinality(a->roaring, b->roaring);
    uint64_t uni = card_a + card_b - inter;
    return uni == 0 ? 1.0 : static_cast<double>(inter) / static_cast<double>(uni);
}

/* ========================================================================
 * Batch probe (JSON in, JSON out)
 * ======================================================================== */

char *rfp_probe_json(const char *symbols_json, size_t json_len,
                     const rfp_bitmap *const *refs, size_t num_refs) {
    if (!symbols_json || !refs || num_refs == 0) return nullptr;

    try {
        /* Build probe bitmap from the JSON array of symbol strings */
        rfp_bitmap *probe = rfp_create();
        if (!probe) return nullptr;

        if (rfp_add_json_array(probe, symbols_json, json_len) != 0) {
            rfp_free(probe);
            return nullptr;
        }

        uint64_t probe_card = rfp_cardinality(probe);

        json results = json::array();
        for (size_t i = 0; i < num_refs; i++) {
            if (!refs[i]) continue;

            uint64_t inter = rfp_intersection_card(probe, refs[i]);
            uint64_t ref_card = rfp_cardinality(refs[i]);
            uint64_t uni = probe_card + ref_card - inter;

            double cont = probe_card == 0 ? 0.0
                        : static_cast<double>(inter) / static_cast<double>(probe_card);
            double jacc = uni == 0 ? 1.0
                        : static_cast<double>(inter) / static_cast<double>(uni);

            json entry;
            entry["idx"] = i;
            entry["containment"] = cont;
            entry["jaccard"] = jacc;
            results.push_back(entry);
        }

        rfp_free(probe);

        std::string out = results.dump();
        char *result = static_cast<char *>(std::malloc(out.size() + 1));
        if (!result) return nullptr;
        std::memcpy(result, out.data(), out.size());
        result[out.size()] = '\0';
        return result;
    } catch (...) {
        return nullptr;
    }
}

void rfp_free_string(char *s) {
    std::free(s);
}

/* ========================================================================
 * Histogram fingerprint
 * ======================================================================== */

struct rfp_histogram {
    rfp_bitmap *bitmap;
    /* hash -> weight for each key */
    std::map<uint32_t, double> weights;
    /* accumulated stats for SQL Server 5-arg finalization */
    double total_rows;
    double total_equal_rows;
    double total_range_rows;
    double total_distinct_range_rows;
    uint32_t steps;
    uint32_t zero_range_steps; /* steps where range_rows == 0 (discrete) */
    /* shape metrics (well-known fields) */
    double cardinality_ratio;
    double repeatability;
    double discreteness;
    double range_density;
    /* extra shape keys from external JSON (beyond well-known fields) */
    json shape_extra;
    /* provenance */
    std::string source;
    bool finalized;
};

rfp_histogram *rfp_histogram_create(void) {
    auto *hf = new (std::nothrow) rfp_histogram;
    if (!hf) return nullptr;
    hf->bitmap = rfp_create();
    if (!hf->bitmap) {
        delete hf;
        return nullptr;
    }
    hf->total_rows = 0;
    hf->total_equal_rows = 0;
    hf->total_range_rows = 0;
    hf->total_distinct_range_rows = 0;
    hf->steps = 0;
    hf->zero_range_steps = 0;
    hf->cardinality_ratio = 0;
    hf->repeatability = 0;
    hf->discreteness = 0;
    hf->range_density = 0;
    hf->shape_extra = json::object();
    hf->finalized = false;
    return hf;
}

void rfp_histogram_free(rfp_histogram *hf) {
    if (!hf) return;
    rfp_free(hf->bitmap);
    delete hf;
}

void rfp_histogram_add_value(rfp_histogram *hf,
                             const char *key, size_t key_len,
                             double weight) {
    if (!hf || !key) return;

    uint32_t hash = fnv1a(key, key_len);
    rfp_add_uint32(hf->bitmap, hash);
    hf->weights[hash] += weight;
    hf->total_equal_rows += weight;
    hf->steps++;
}

void rfp_histogram_add_value_normalized(rfp_histogram *hf,
                                         const char *key, size_t key_len,
                                         double weight, rfp_norm_mode mode) {
    if (!hf || !key) return;

    if (mode & RFP_NORM_CASEFOLD) {
        std::string norm = normalize_casefold(key, key_len);
        uint32_t hash = fnv1a(norm.data(), norm.size());
        rfp_add_uint32(hf->bitmap, hash);
        hf->weights[hash] += weight;
    } else {
        uint32_t hash = fnv1a(key, key_len);
        rfp_add_uint32(hf->bitmap, hash);
        hf->weights[hash] += weight;
    }
    hf->total_equal_rows += weight;
    hf->steps++;
}

void rfp_histogram_add_step(rfp_histogram *hf,
                            const char *key, size_t key_len,
                            double equal_rows, double range_rows,
                            double distinct_range_rows, double avg_range_rows) {
    if (!hf || !key) return;

    uint32_t hash = fnv1a(key, key_len);
    rfp_add_uint32(hf->bitmap, hash);
    hf->weights[hash] = equal_rows;

    hf->total_equal_rows += equal_rows;
    hf->total_range_rows += range_rows;
    hf->total_distinct_range_rows += distinct_range_rows;
    hf->steps++;
    if (range_rows == 0) hf->zero_range_steps++;

    (void)avg_range_rows; /* tracked via range_rows / distinct_range_rows */
}

void rfp_histogram_set_shape(rfp_histogram *hf,
                             const char *shape_json, size_t len) {
    if (!hf || !shape_json || len == 0) return;

    try {
        auto j = json::parse(shape_json, shape_json + len);
        if (!j.is_object()) return;

        /* Extract well-known fields into struct for similarity/getter use */
        if (j.contains("cardinality_ratio")) hf->cardinality_ratio = j["cardinality_ratio"].get<double>();
        if (j.contains("repeatability")) hf->repeatability = j["repeatability"].get<double>();
        if (j.contains("discreteness")) hf->discreteness = j["discreteness"].get<double>();
        if (j.contains("range_density")) hf->range_density = j["range_density"].get<double>();

        /* Store any extra keys beyond the well-known ones */
        hf->shape_extra = json::object();
        for (auto &[key, val] : j.items()) {
            if (key != "cardinality_ratio" && key != "repeatability" &&
                key != "discreteness" && key != "range_density") {
                hf->shape_extra[key] = val;
            }
        }

        hf->finalized = true;
    } catch (...) {
        /* silently ignore parse errors */
    }
}

void rfp_histogram_set_source(rfp_histogram *hf,
                              const char *source, size_t len) {
    if (!hf || !source) return;
    hf->source.assign(source, len);
}

void rfp_histogram_finalize(rfp_histogram *hf) {
    if (!hf || hf->steps == 0) return;
    if (hf->finalized) return; /* shape already set via set_shape */

    hf->total_rows = hf->total_equal_rows + hf->total_range_rows;

    /* cardinality_ratio: distinct values / total rows
     * distinct values ≈ steps (boundary values) + total_distinct_range_rows */
    double distinct_values = hf->steps + hf->total_distinct_range_rows;
    hf->cardinality_ratio = (hf->total_rows > 0)
        ? distinct_values / hf->total_rows
        : 0;

    /* repeatability: average equal_rows per step */
    hf->repeatability = hf->total_equal_rows / hf->steps;

    /* discreteness: fraction of steps with zero range_rows */
    hf->discreteness = static_cast<double>(hf->zero_range_steps) / hf->steps;

    /* range_density: average distinct_range_rows per step (for non-zero range steps) */
    uint32_t range_steps = hf->steps - hf->zero_range_steps;
    hf->range_density = (range_steps > 0)
        ? hf->total_distinct_range_rows / range_steps
        : 0;

    hf->finalized = true;
}

char *rfp_histogram_to_json(const rfp_histogram *hf) {
    if (!hf) return nullptr;

    try {
        json j;
        j["v"] = 2;

        /* bitmap as base64 */
        size_t b64_size = rfp_base64_size(hf->bitmap);
        std::vector<char> b64_buf(b64_size + 1);
        size_t written = rfp_to_base64(hf->bitmap, b64_buf.data(), b64_size);
        b64_buf[written] = '\0';
        j["bitmap"] = std::string(b64_buf.data(), written);

        j["steps"] = hf->steps;

        /* provenance */
        if (!hf->source.empty()) {
            j["source"] = hf->source;
        }

        /* shape as nested object — well-known fields + extras */
        json shape;
        shape["cardinality_ratio"] = hf->cardinality_ratio;
        shape["repeatability"] = hf->repeatability;
        shape["discreteness"] = hf->discreteness;
        shape["range_density"] = hf->range_density;
        /* merge in any extra shape keys from external set_shape */
        if (!hf->shape_extra.empty()) {
            for (auto &[key, val] : hf->shape_extra.items()) {
                shape[key] = val;
            }
        }
        j["shape"] = shape;

        /* accumulation context (for Combine round-trip) */
        j["total_rows"] = hf->total_rows;
        j["total_equal_rows"] = hf->total_equal_rows;
        j["total_range_rows"] = hf->total_range_rows;
        j["total_distinct_range_rows"] = hf->total_distinct_range_rows;
        j["zero_range_steps"] = hf->zero_range_steps;

        /* weights: hash (as string) -> weight */
        json w = json::object();
        for (const auto &kv : hf->weights) {
            w[std::to_string(kv.first)] = kv.second;
        }
        j["weights"] = w;

        std::string out = j.dump();
        char *result = static_cast<char *>(std::malloc(out.size() + 1));
        if (!result) return nullptr;
        std::memcpy(result, out.data(), out.size());
        result[out.size()] = '\0';
        return result;
    } catch (...) {
        return nullptr;
    }
}

rfp_histogram *rfp_histogram_from_json(const char *json_str, size_t len) {
    if (!json_str || len == 0) return nullptr;

    try {
        auto j = json::parse(json_str, json_str + len);

        auto *hf = new (std::nothrow) rfp_histogram;
        if (!hf) return nullptr;

        /* Decode bitmap from base64 */
        std::string b64 = j.value("bitmap", "");
        hf->bitmap = rfp_from_base64(b64.c_str(), b64.size());
        if (!hf->bitmap) {
            hf->bitmap = rfp_create();
        }

        hf->steps = j.value("steps", (uint32_t)0);
        hf->source = j.value("source", "");

        /* Shape: v2 has nested "shape" object, v1 has flat fields */
        hf->shape_extra = json::object();
        if (j.contains("shape") && j["shape"].is_object()) {
            auto &s = j["shape"];
            hf->cardinality_ratio = s.value("cardinality_ratio", 0.0);
            hf->repeatability = s.value("repeatability", 0.0);
            hf->discreteness = s.value("discreteness", 0.0);
            hf->range_density = s.value("range_density", 0.0);
            /* Preserve extra shape keys */
            for (auto &[key, val] : s.items()) {
                if (key != "cardinality_ratio" && key != "repeatability" &&
                    key != "discreteness" && key != "range_density") {
                    hf->shape_extra[key] = val;
                }
            }
        } else {
            /* v1 flat format */
            hf->cardinality_ratio = j.value("cardinality_ratio", 0.0);
            hf->repeatability = j.value("repeatability", 0.0);
            hf->discreteness = j.value("discreteness", 0.0);
            hf->range_density = j.value("range_density", 0.0);
        }

        /* Restore accumulation fields */
        hf->total_rows = j.value("total_rows", 0.0);
        hf->total_equal_rows = j.value("total_equal_rows", 0.0);
        hf->total_range_rows = j.value("total_range_rows", 0.0);
        hf->total_distinct_range_rows = j.value("total_distinct_range_rows", 0.0);
        hf->zero_range_steps = j.value("zero_range_steps", (uint32_t)0);

        /* Reconstruct weights map */
        if (j.contains("weights") && j["weights"].is_object()) {
            for (auto &[key, val] : j["weights"].items()) {
                uint32_t hash = static_cast<uint32_t>(std::stoul(key));
                double weight = val.get<double>();
                hf->weights[hash] = weight;
            }
            /* If total_equal_rows wasn't in JSON (old format), reconstruct from weights */
            if (hf->total_equal_rows == 0 && !hf->weights.empty()) {
                for (const auto &kv : hf->weights) {
                    hf->total_equal_rows += kv.second;
                }
            }
        }

        hf->finalized = true;
        return hf;
    } catch (...) {
        return nullptr;
    }
}

const rfp_bitmap *rfp_histogram_bitmap(const rfp_histogram *hf) {
    if (!hf) return nullptr;
    return hf->bitmap;
}

double rfp_histogram_weighted_containment(const rfp_histogram *hf,
                                          const rfp_bitmap *domain) {
    if (!hf || !domain || hf->weights.empty()) return 0.0;

    double matched_weight = 0;
    double total_weight = 0;

    for (const auto &kv : hf->weights) {
        total_weight += kv.second;
        /* Check if this hash is in the domain bitmap */
        if (domain->roaring && roaring_bitmap_contains(domain->roaring, kv.first)) {
            matched_weight += kv.second;
        }
    }

    return (total_weight > 0) ? matched_weight / total_weight : 0.0;
}

double rfp_histogram_cardinality_ratio(const rfp_histogram *hf) {
    return hf ? hf->cardinality_ratio : 0.0;
}

double rfp_histogram_repeatability(const rfp_histogram *hf) {
    return hf ? hf->repeatability : 0.0;
}

double rfp_histogram_discreteness(const rfp_histogram *hf) {
    return hf ? hf->discreteness : 0.0;
}

double rfp_histogram_range_density(const rfp_histogram *hf) {
    return hf ? hf->range_density : 0.0;
}

char *rfp_histogram_shape_json(const rfp_histogram *hf) {
    if (!hf) return nullptr;

    try {
        json shape;
        shape["cardinality_ratio"] = hf->cardinality_ratio;
        shape["repeatability"] = hf->repeatability;
        shape["discreteness"] = hf->discreteness;
        shape["range_density"] = hf->range_density;
        if (!hf->shape_extra.empty()) {
            for (auto &[key, val] : hf->shape_extra.items()) {
                shape[key] = val;
            }
        }

        std::string out = shape.dump();
        char *result = static_cast<char *>(std::malloc(out.size() + 1));
        if (!result) return nullptr;
        std::memcpy(result, out.data(), out.size());
        result[out.size()] = '\0';
        return result;
    } catch (...) {
        return nullptr;
    }
}

double rfp_histogram_shape_similarity(const rfp_histogram *a,
                                      const rfp_histogram *b) {
    if (!a || !b) return 1.0;

    /* Normalize features to [0,1] range using reasonable bounds:
     * - cardinality_ratio: already in [0,1]
     * - repeatability: log-scale, normalize by log(max_reasonable)
     * - discreteness: already in [0,1]
     */
    double cr_diff = a->cardinality_ratio - b->cardinality_ratio;

    /* Log-scale repeatability: log(1 + r) / log(1 + 1e6) as normalizer */
    double log_max = std::log(1.0 + 1e6);
    double rep_a = std::log(1.0 + a->repeatability) / log_max;
    double rep_b = std::log(1.0 + b->repeatability) / log_max;
    double rep_diff = rep_a - rep_b;

    double disc_diff = a->discreteness - b->discreteness;

    return std::sqrt(cr_diff * cr_diff + rep_diff * rep_diff + disc_diff * disc_diff);
}

const char *rfp_errmsg(void) {
    return g_errmsg.c_str();
}

} /* extern "C" */
