/*
 * roaring_fp.cpp — Core library implementation
 *
 * Wraps CRoaring with a C API, adds FNV-1a hashing, JSON array support
 * (via nlohmann/json), and base64 serialization.
 */

#include "roaring_fp.h"

#include "roaring.h"

#include <nlohmann/json.hpp>

#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

using json = nlohmann::json;

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

} /* extern "C" */
