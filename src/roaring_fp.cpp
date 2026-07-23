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

/* Zero-copy read-only view over the portable buffer. The frozen reader is
 * unchecked, so first validate that a complete bitmap fits within `len`. */
rfp_bitmap *rfp_deserialize_frozen(const char *buf, size_t len) {
    if (!buf || len == 0) return nullptr;
    if (roaring_bitmap_portable_deserialize_size(buf, len) == 0) return nullptr;
    roaring_bitmap_t *r = roaring_bitmap_portable_deserialize_frozen(buf);
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
 * SHA-256 (FIPS 180-4) — self-contained pure C, no dependencies, endian-safe.
 * Deterministic across native and WASM so checksums match everywhere.
 * ======================================================================== */

namespace {

struct Sha256Ctx { uint8_t data[64]; uint32_t datalen; uint64_t bitlen; uint32_t state[8]; };

static const uint32_t kSha256K[64] = {
    0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
    0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
    0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
    0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
    0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
    0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
    0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
    0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2};

#define RR(a,b) (((a) >> (b)) | ((a) << (32 - (b))))
#define S0(x) (RR(x,2) ^ RR(x,13) ^ RR(x,22))
#define S1(x) (RR(x,6) ^ RR(x,11) ^ RR(x,25))
#define s0(x) (RR(x,7) ^ RR(x,18) ^ ((x) >> 3))
#define s1(x) (RR(x,17) ^ RR(x,19) ^ ((x) >> 10))

static void sha256_transform(Sha256Ctx *c, const uint8_t d[64]) {
    uint32_t m[64], a,b,cc,dd,e,f,g,h,t1,t2;
    for (int i=0,j=0;i<16;i++,j+=4)
        m[i] = (uint32_t(d[j])<<24)|(uint32_t(d[j+1])<<16)|(uint32_t(d[j+2])<<8)|uint32_t(d[j+3]);
    for (int i=16;i<64;i++) m[i] = s1(m[i-2]) + m[i-7] + s0(m[i-15]) + m[i-16];
    a=c->state[0];b=c->state[1];cc=c->state[2];dd=c->state[3];
    e=c->state[4];f=c->state[5];g=c->state[6];h=c->state[7];
    for (int i=0;i<64;i++) {
        t1 = h + S1(e) + ((e&f)^(~e&g)) + kSha256K[i] + m[i];
        t2 = S0(a) + ((a&b)^(a&cc)^(b&cc));
        h=g;g=f;f=e;e=dd+t1;dd=cc;cc=b;b=a;a=t1+t2;
    }
    c->state[0]+=a;c->state[1]+=b;c->state[2]+=cc;c->state[3]+=dd;
    c->state[4]+=e;c->state[5]+=f;c->state[6]+=g;c->state[7]+=h;
}
static void sha256_init(Sha256Ctx *c){ c->datalen=0;c->bitlen=0;
    c->state[0]=0x6a09e667;c->state[1]=0xbb67ae85;c->state[2]=0x3c6ef372;c->state[3]=0xa54ff53a;
    c->state[4]=0x510e527f;c->state[5]=0x9b05688c;c->state[6]=0x1f83d9ab;c->state[7]=0x5be0cd19; }
static void sha256_update(Sha256Ctx *c,const uint8_t*d,size_t len){
    for (size_t i=0;i<len;i++){ c->data[c->datalen++]=d[i];
        if (c->datalen==64){ sha256_transform(c,c->data); c->bitlen+=512; c->datalen=0; } } }
static void sha256_final(Sha256Ctx *c,uint8_t out[32]){
    uint32_t i=c->datalen;
    c->data[i++]=0x80;
    if (c->datalen<56){ while(i<56)c->data[i++]=0; }
    else { while(i<64)c->data[i++]=0; sha256_transform(c,c->data); i=0; while(i<56)c->data[i++]=0; }
    c->bitlen += (uint64_t)c->datalen*8;
    for (int k=7;k>=0;k--) c->data[56+(7-k)] = (uint8_t)(c->bitlen >> (k*8));
    sha256_transform(c,c->data);
    for (int k=0;k<4;k++) for (int j=0;j<8;j++)
        out[j*4+k] = (uint8_t)(c->state[j] >> (24 - k*8));
}
#undef RR
#undef S0
#undef S1
#undef s0
#undef s1

} // namespace

void rfp_sha256(const void *data, size_t len, unsigned char out[32]) {
    Sha256Ctx c; sha256_init(&c);
    if (data && len) sha256_update(&c, static_cast<const uint8_t*>(data), len);
    sha256_final(&c, out);
}

void rfp_sha256_hex(const void *data, size_t len, char *out, size_t out_cap) {
    if (!out || out_cap < 65) { if (out && out_cap) out[0]='\0'; return; }
    unsigned char h[32]; rfp_sha256(data, len, h);
    static const char *hx = "0123456789abcdef";
    for (int i=0;i<32;i++){ out[i*2]=hx[h[i]>>4]; out[i*2+1]=hx[h[i]&0xf]; }
    out[64]='\0';
}

void rfp_bitmap_checksum_hex(const rfp_bitmap *bm, char *out, size_t out_cap) {
    if (!out || out_cap < 65) { if (out && out_cap) out[0]='\0'; return; }
    if (!bm || !bm->roaring) { rfp_sha256_hex("", 0, out, out_cap); return; }
    size_t sz = rfp_serialized_size(bm);
    std::vector<char> raw(sz);
    size_t n = rfp_serialize(bm, raw.data(), sz);
    rfp_sha256_hex(raw.data(), n, out, out_cap);
}

/* ========================================================================
 * Char-class structural signature — one pass, no alloc, no regex.
 * ======================================================================== */

/* Feature-bit layout (the tiny "registry", as code). Keep <= 63. */
enum {
    /* primitive presence (0-16) */
    CCF_HAS_DIGIT=0, CCF_HAS_UPPER, CCF_HAS_LOWER, CCF_HAS_SPACE, CCF_HAS_DOT,
    CCF_HAS_COMMA, CCF_HAS_DASH, CCF_HAS_SLASH, CCF_HAS_COLON, CCF_HAS_DOLLAR,
    CCF_HAS_PERCENT, CCF_HAS_AT, CCF_HAS_PAREN, CCF_HAS_PLUS, CCF_HAS_UNDER,
    CCF_HAS_APOS, CCF_HAS_OTHER,
    /* shape composites (20-31) */
    CCF_ALL_DIGITS=20, CCF_ALL_ALPHA, CCF_ALL_UPPER, CCF_ALL_LOWER, CCF_TITLE_CASE,
    CCF_ALNUM_ONLY, CCF_DIGIT_AND_ALPHA, CCF_LEADS_DIGIT, CCF_LEADS_ALPHA,
    CCF_SINGLE_TOKEN, CCF_MULTI_TOKEN,
    /* length buckets (32-40) */
    CCF_LEN_0=32, CCF_LEN_1, CCF_LEN_2, CCF_LEN_3, CCF_LEN_4, CCF_LEN_5,
    CCF_LEN_6_10, CCF_LEN_11_20, CCF_LEN_GT20,
    /* higher shapes (44-49) */
    CCF_MONEY_SHAPED=44, CCF_DATE_SHAPED, CCF_DECIMAL_SHAPED, CCF_CODE_SHAPED,
    CCF_EMAIL_SHAPED, CCF_YEAR_SHAPED
};

static const char *kCcNames[64] = {
    "has_digit","has_upper","has_lower","has_space","has_dot","has_comma","has_dash",
    "has_slash","has_colon","has_dollar","has_percent","has_at","has_paren","has_plus",
    "has_under","has_apos","has_other", 0,0,0,
    "all_digits","all_alpha","all_upper","all_lower","title_case","alnum_only",
    "digit_and_alpha","leads_digit","leads_alpha","single_token","multi_token", 0,
    "len_0","len_1","len_2","len_3","len_4","len_5","len_6_10","len_11_20","len_gt20",
    0,0,0,
    "money_shaped","date_shaped","decimal_shaped","code_shaped","email_shaped","year_shaped",
    0,0,0,0,0,0,0,0,0,0,0,0,0,0
};

const char *rfp_cc_feature_name(int bit) {
    if (bit < 0 || bit >= 64) return nullptr;
    return kCcNames[bit];
}

/* Inverse: feature name -> bit index, or -1 if unknown (harmonize diverged sources). */
int rfp_cc_feature_bit(const char *name) {
    if (!name) return -1;
    for (int b = 0; b < 64; b++)
        if (kCcNames[b] && std::strcmp(kCcNames[b], name) == 0) return b;
    return -1;
}

/* -------- cc_eval: a boolean interpreter over the feature bits --------
   Evaluates a user composite expressed over the FIXED vocabulary names against a
   precomputed signature. Grammar (whitespace-insensitive):
       expr := or ; or := xor ('|' xor)* ; xor := and ('^' and)* ;
       and  := not ('&' not)* ; not := ('!'|'~') not | primary ;
       primary := '(' expr ')' | name ; name := [A-Za-z0-9_]+
   A name resolves via rfp_cc_feature_bit; an UNKNOWN name (broken FK / diverged
   source) or any syntax error aborts to -1 — the same one-sided integrity signal
   as bf_cc_feature_bit. No allocation; pure function of (sig, expr) so every
   surface agrees and the result is soundly cacheable. Atoms 1/0, error -1.
   User-land expands composite->composite DAGs to primitive-name exprs first; this
   only ever sees vocabulary names. */
namespace {
struct CcEval {
    const char *p;
    uint64_t sig;
    bool err = false;

    void skip_ws() { while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++; }

    /* '(' expr ')' | name */
    int primary() {
        skip_ws();
        if (*p == '(') {
            p++;
            int v = parse_or();
            skip_ws();
            if (*p == ')') p++; else err = true;
            return v;
        }
        char name[64];
        size_t n = 0;
        while ((*p >= 'A' && *p <= 'Z') || (*p >= 'a' && *p <= 'z') ||
               (*p >= '0' && *p <= '9') || *p == '_') {
            if (n < sizeof(name) - 1) name[n++] = *p;
            p++;
        }
        if (n == 0) { err = true; return 0; }
        name[n] = '\0';
        int bit = rfp_cc_feature_bit(name);
        if (bit < 0) { err = true; return 0; }   /* unknown name = broken reference */
        return (int)((sig >> bit) & 1u);
    }

    /* ('!'|'~') not | primary */
    int parse_not() {
        skip_ws();
        if (*p == '!' || *p == '~') { p++; return parse_not() ? 0 : 1; }
        return primary();
    }

    int parse_and() {
        int v = parse_not();
        for (;;) {
            skip_ws();
            if (*p == '&') { p++; int r = parse_not(); v = (v && r) ? 1 : 0; }
            else break;
        }
        return v;
    }

    int parse_xor() {
        int v = parse_and();
        for (;;) {
            skip_ws();
            if (*p == '^') { p++; int r = parse_and(); v = (v ^ r) ? 1 : 0; }
            else break;
        }
        return v;
    }

    int parse_or() {
        int v = parse_xor();
        for (;;) {
            skip_ws();
            if (*p == '|') { p++; int r = parse_xor(); v = (v || r) ? 1 : 0; }
            else break;
        }
        return v;
    }
};
} // namespace

/* Evaluate a boolean feature-expression against a signature. 1/0, or -1 on
   unknown name or syntax error. */
int rfp_cc_eval(uint64_t sig, const char *expr) {
    if (!expr) return -1;
    CcEval e;
    e.p = expr;
    e.sig = sig;
    int v = e.parse_or();
    e.skip_ws();
    if (e.err || *e.p != '\0') return -1;   /* trailing garbage = error */
    return v;
}

/* -------- cc_profile: per-feature-bit popcount accumulator over a column -------- */
void rfp_cc_profile_update(uint64_t *counts, uint64_t *n, const void *data, size_t len) {
    uint64_t sig = rfp_cc_signature(data, len);
    (*n)++;
    while (sig) { int b = __builtin_ctzll(sig); counts[b]++; sig &= sig - 1; }
}

void rfp_cc_profile_merge(uint64_t *dst, uint64_t *dn, const uint64_t *src, uint64_t sn) {
    for (int b = 0; b < 64; b++) dst[b] += src[b];
    *dn += sn;
}

char *rfp_cc_profile_json(const uint64_t *counts, uint64_t n) {
    std::string s = "{\"n\":" + std::to_string(n) + ",\"features\":[";
    bool first = true;
    for (int b = 0; b < 64; b++) {
        if (!counts[b]) continue;
        const char *name = rfp_cc_feature_name(b);
        if (!name) continue;
        char pbuf[32];
        std::snprintf(pbuf, sizeof(pbuf), "%.2f", n ? 100.0 * (double)counts[b] / (double)n : 0.0);
        if (!first) s += ",";
        first = false;
        s += "{\"bit\":" + std::to_string(b) + ",\"name\":\"" + name +
             "\",\"count\":" + std::to_string(counts[b]) + ",\"pct\":" + pbuf + "}";
    }
    s += "]}";
    char *out = (char *)std::malloc(s.size() + 1);
    if (out) std::memcpy(out, s.c_str(), s.size() + 1);
    return out;
}

/* The feature vocabulary is the FK target for user-land domain->feature tables;
   bump this (append-only!) whenever bits are added, never renumber/rename. */
#define CC_VOCAB_VERSION 1

/* Canonical RE2 regexp per feature bit — verified to reproduce rfp_cc_signature.
   The universal exact definition (documents each bit; drives SQL-only vectors). */
static const char *cc_regex(int bit) {
    switch (bit) {
    case CCF_HAS_DIGIT:  return "[0-9]";
    case CCF_HAS_UPPER:  return "[A-Z]";
    case CCF_HAS_LOWER:  return "[a-z]";
    case CCF_HAS_SPACE:  return "\\s";
    case CCF_HAS_DOT:    return "\\.";
    case CCF_HAS_COMMA:  return ",";
    case CCF_HAS_DASH:   return "-";
    case CCF_HAS_SLASH:  return "/";
    case CCF_HAS_COLON:  return ":";
    case CCF_HAS_DOLLAR: return "\\$";
    case CCF_HAS_PERCENT:return "%";
    case CCF_HAS_AT:     return "@";
    case CCF_HAS_PAREN:  return "[()]";
    case CCF_HAS_PLUS:   return "\\+";
    case CCF_HAS_UNDER:  return "_";
    case CCF_HAS_APOS:   return "[\\x27\\x60]";
    case CCF_HAS_OTHER:  return "[^\\x20-\\x7e]";
    case CCF_ALL_DIGITS: return "^[0-9]+$";
    case CCF_ALL_ALPHA:  return "^[A-Za-z]+$";
    case CCF_ALL_UPPER:  return "^[A-Z]+$";
    case CCF_ALL_LOWER:  return "^[a-z]+$";
    case CCF_TITLE_CASE: return "^[A-Z].*[a-z]";
    case CCF_ALNUM_ONLY: return "^[A-Za-z0-9]+$";
    case CCF_DIGIT_AND_ALPHA:
        return "^([A-Za-z0-9]*[0-9][A-Za-z0-9]*[A-Za-z]|[A-Za-z0-9]*[A-Za-z][A-Za-z0-9]*[0-9])[A-Za-z0-9]*$";
    case CCF_LEADS_DIGIT:  return "^[0-9]";
    case CCF_LEADS_ALPHA:  return "^[A-Za-z]";
    case CCF_SINGLE_TOKEN: return "^\\s*\\S+\\s*$";
    case CCF_MULTI_TOKEN:  return "\\S\\s+\\S";
    case CCF_LEN_0:     return "^$";
    case CCF_LEN_1:     return "^.$";
    case CCF_LEN_2:     return "^.{2}$";
    case CCF_LEN_3:     return "^.{3}$";
    case CCF_LEN_4:     return "^.{4}$";
    case CCF_LEN_5:     return "^.{5}$";
    case CCF_LEN_6_10:  return "^.{6,10}$";
    case CCF_LEN_11_20: return "^.{11,20}$";
    case CCF_LEN_GT20:  return "^.{21,}$";
    case CCF_MONEY_SHAPED:   return "(\\$.*[0-9])|([0-9].*\\$)";
    case CCF_DATE_SHAPED:    return "((\\D*[0-9]){4}.*[-/])|([-/].*(\\D*[0-9]){4})|([0-9]+[-/][0-9]+[-/][0-9]+)";
    case CCF_DECIMAL_SHAPED: return "^[^A-Za-z]*[0-9][^A-Za-z]*\\.[^A-Za-z]*$|^[^A-Za-z]*\\.[^A-Za-z]*[0-9][^A-Za-z]*$";
    case CCF_CODE_SHAPED:    return "^[A-Z0-9]*[A-Z][A-Z0-9]*$";
    case CCF_EMAIL_SHAPED:   return "(@.*\\.)|(\\..*@)";
    case CCF_YEAR_SHAPED:    return "^[12][0-9]{3}$";
    default: return nullptr;
    }
}

/* Logical-operator definition for the composites that ARE a boolean function of
   lower-level features (null for primitives / regexp-only / count-dependent ones). */
static const char *cc_expr(int bit) {
    switch (bit) {
    case CCF_DIGIT_AND_ALPHA: return "alnum_only & has_digit & (has_upper | has_lower)";
    case CCF_MONEY_SHAPED:    return "has_dollar & has_digit";
    case CCF_DECIMAL_SHAPED:  return "has_digit & has_dot & !has_upper & !has_lower & !has_other";
    case CCF_CODE_SHAPED:     return "alnum_only & has_upper & !has_lower & single_token";
    case CCF_EMAIL_SHAPED:    return "has_at & has_dot";
    default: return nullptr;
    }
}

/* Dump the whole feature vocabulary (the FK target) as versioned JSON:
   {"version":N,"features":[{"bit","name","regexp","expr"},...]}.
   Caller frees with rfp_free_string. */
char *rfp_cc_features_json(void) {
    nlohmann::json feats = nlohmann::json::array();
    for (int b = 0; b < 64; b++) {
        if (!kCcNames[b]) continue;
        const char *rx = cc_regex(b), *ex = cc_expr(b);
        feats.push_back({{"bit", b}, {"name", kCcNames[b]},
                         {"regexp", rx ? nlohmann::json(rx) : nlohmann::json(nullptr)},
                         {"expr",   ex ? nlohmann::json(ex) : nlohmann::json(nullptr)}});
    }
    nlohmann::json j = {{"version", CC_VOCAB_VERSION}, {"features", feats}};
    std::string s = j.dump();
    char *out = (char *)std::malloc(s.size() + 1);
    if (out) std::memcpy(out, s.c_str(), s.size() + 1);
    return out;
}

#define CCBIT(b) (UINT64_C(1) << (b))

uint64_t rfp_cc_signature(const void *data, size_t len) {
    const unsigned char *s = static_cast<const unsigned char *>(data);
    uint64_t sig = 0;
    size_t nd=0, nu=0, nl=0, nsp=0, nalpha=0, ntok=0, nother=0;
    unsigned char first=0; int in_tok=0;

    for (size_t i=0;i<len;i++) {
        unsigned char c = s[i];
        int is_sp = (c==' '||c=='\t'||c=='\n'||c=='\r'||c=='\f'||c=='\v');
        if (c>='0'&&c<='9') { nd++; sig|=CCBIT(CCF_HAS_DIGIT); }
        else if (c>='A'&&c<='Z') { nu++; nalpha++; sig|=CCBIT(CCF_HAS_UPPER); }
        else if (c>='a'&&c<='z') { nl++; nalpha++; sig|=CCBIT(CCF_HAS_LOWER); }
        else if (is_sp) { nsp++; sig|=CCBIT(CCF_HAS_SPACE); }
        else if (c<32 || c>126) { nother++; sig|=CCBIT(CCF_HAS_OTHER); }
        switch (c) {
            case '.': sig|=CCBIT(CCF_HAS_DOT);     break;
            case ',': sig|=CCBIT(CCF_HAS_COMMA);   break;
            case '-': sig|=CCBIT(CCF_HAS_DASH);    break;
            case '/': sig|=CCBIT(CCF_HAS_SLASH);   break;
            case ':': sig|=CCBIT(CCF_HAS_COLON);   break;
            case '$': sig|=CCBIT(CCF_HAS_DOLLAR);  break;
            case '%': sig|=CCBIT(CCF_HAS_PERCENT); break;
            case '@': sig|=CCBIT(CCF_HAS_AT);      break;
            case '(': case ')': sig|=CCBIT(CCF_HAS_PAREN); break;
            case '+': sig|=CCBIT(CCF_HAS_PLUS);    break;
            case '_': sig|=CCBIT(CCF_HAS_UNDER);   break;
            case '\'': case '`': sig|=CCBIT(CCF_HAS_APOS); break;
        }
        if (is_sp) in_tok=0; else if (!in_tok){ in_tok=1; ntok++; }
        if (i==0) first=c;
    }

    /* length buckets */
    if (len==0) sig|=CCBIT(CCF_LEN_0);
    else if (len==1) sig|=CCBIT(CCF_LEN_1);
    else if (len==2) sig|=CCBIT(CCF_LEN_2);
    else if (len==3) sig|=CCBIT(CCF_LEN_3);
    else if (len==4) sig|=CCBIT(CCF_LEN_4);
    else if (len==5) sig|=CCBIT(CCF_LEN_5);
    else if (len<=10) sig|=CCBIT(CCF_LEN_6_10);
    else if (len<=20) sig|=CCBIT(CCF_LEN_11_20);
    else sig|=CCBIT(CCF_LEN_GT20);

    /* shape composites */
    if (len>0 && nd==len)                 sig|=CCBIT(CCF_ALL_DIGITS);
    if (len>0 && nalpha==len)             sig|=CCBIT(CCF_ALL_ALPHA);
    if (len>0 && nu==len)                 sig|=CCBIT(CCF_ALL_UPPER);
    if (len>0 && nl==len)                 sig|=CCBIT(CCF_ALL_LOWER);
    if (nalpha+nd==len && nd>0 && nalpha>0) sig|=CCBIT(CCF_DIGIT_AND_ALPHA);
    if (len>0 && nalpha+nd==len)          sig|=CCBIT(CCF_ALNUM_ONLY);
    if (first>='A'&&first<='Z' && nl>0)   sig|=CCBIT(CCF_TITLE_CASE);
    if (first>='0'&&first<='9')           sig|=CCBIT(CCF_LEADS_DIGIT);
    if ((first>='A'&&first<='Z')||(first>='a'&&first<='z')) sig|=CCBIT(CCF_LEADS_ALPHA);
    if (ntok==1)                          sig|=CCBIT(CCF_SINGLE_TOKEN);
    if (ntok>1)                           sig|=CCBIT(CCF_MULTI_TOKEN);

    /* higher shapes */
    if ((sig&CCBIT(CCF_HAS_DOLLAR)) && nd>0)                       sig|=CCBIT(CCF_MONEY_SHAPED);
    if (nd>=4 && ((sig&CCBIT(CCF_HAS_DASH))||(sig&CCBIT(CCF_HAS_SLASH)))) sig|=CCBIT(CCF_DATE_SHAPED);
    if (nd>0 && (sig&CCBIT(CCF_HAS_DOT)) && nalpha==0)             sig|=CCBIT(CCF_DECIMAL_SHAPED);
    if ((sig&CCBIT(CCF_ALNUM_ONLY)) && ntok==1 && nu>0 && nl==0)  sig|=CCBIT(CCF_CODE_SHAPED);
    if ((sig&CCBIT(CCF_HAS_AT)) && (sig&CCBIT(CCF_HAS_DOT)))       sig|=CCBIT(CCF_EMAIL_SHAPED);
    if (len==4 && nd==4 && (first=='1'||first=='2'))              sig|=CCBIT(CCF_YEAR_SHAPED);

    return sig;
}
#undef CCBIT

/* ========================================================================
 * Set operations
 * ======================================================================== */

void rfp_or_inplace(rfp_bitmap *dst, const rfp_bitmap *src) {
    if (!dst || !dst->roaring || !src || !src->roaring) return;
    roaring_bitmap_or_inplace(dst->roaring, src->roaring);
}

/* Binary set operations returning a NEW bitmap (allocated like rfp_copy). */

rfp_bitmap *rfp_and(const rfp_bitmap *a, const rfp_bitmap *b) {
    if (!a || !a->roaring || !b || !b->roaring) return nullptr;
    auto *out = new (std::nothrow) rfp_bitmap;
    if (!out) return nullptr;
    out->roaring = roaring_bitmap_and(a->roaring, b->roaring);
    if (!out->roaring) {
        delete out;
        return nullptr;
    }
    return out;
}

rfp_bitmap *rfp_or(const rfp_bitmap *a, const rfp_bitmap *b) {
    if (!a || !a->roaring || !b || !b->roaring) return nullptr;
    auto *out = new (std::nothrow) rfp_bitmap;
    if (!out) return nullptr;
    out->roaring = roaring_bitmap_or(a->roaring, b->roaring);
    if (!out->roaring) {
        delete out;
        return nullptr;
    }
    return out;
}

rfp_bitmap *rfp_andnot(const rfp_bitmap *a, const rfp_bitmap *b) {
    if (!a || !a->roaring || !b || !b->roaring) return nullptr;
    auto *out = new (std::nothrow) rfp_bitmap;
    if (!out) return nullptr;
    out->roaring = roaring_bitmap_andnot(a->roaring, b->roaring);
    if (!out->roaring) {
        delete out;
        return nullptr;
    }
    return out;
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
