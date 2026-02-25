/*
 * Python bindings for roaring_fp core library via nanobind
 */

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

#include "roaring_fp.h"

#include <string>
#include <vector>
#include <cstring>

namespace nb = nanobind;

class RoaringFP {
public:
    RoaringFP() : bm_(rfp_create()) {}

    ~RoaringFP() {
        if (bm_) rfp_free(bm_);
    }

    RoaringFP(const RoaringFP &other) : bm_(rfp_copy(other.bm_)) {}

    RoaringFP &operator=(const RoaringFP &other) {
        if (this != &other) {
            if (bm_) rfp_free(bm_);
            bm_ = rfp_copy(other.bm_);
        }
        return *this;
    }

    RoaringFP(RoaringFP &&other) noexcept : bm_(other.bm_) {
        other.bm_ = nullptr;
    }

    /* Construct from raw bitmap pointer (takes ownership) */
    explicit RoaringFP(rfp_bitmap *bm) : bm_(bm) {}

    void add(const std::string &s) {
        rfp_add_hash(bm_, s.data(), s.size());
    }

    void add_uint32(uint32_t val) {
        rfp_add_uint32(bm_, val);
    }

    int add_json(const std::string &json_array) {
        return rfp_add_json_array(bm_, json_array.data(), json_array.size());
    }

    uint64_t cardinality() const {
        return rfp_cardinality(bm_);
    }

    nb::bytes serialize() const {
        size_t size = rfp_serialized_size(bm_);
        std::vector<char> buf(size);
        rfp_serialize(bm_, buf.data(), size);
        return nb::bytes(buf.data(), size);
    }

    std::string to_base64() const {
        size_t size = rfp_base64_size(bm_);
        std::string buf(size, '\0');
        rfp_to_base64(bm_, buf.data(), size);
        return buf;
    }

    uint64_t intersection_card(const RoaringFP &other) const {
        return rfp_intersection_card(bm_, other.bm_);
    }

    double containment(const RoaringFP &other) const {
        return rfp_containment(bm_, other.bm_);
    }

    double jaccard(const RoaringFP &other) const {
        return rfp_jaccard(bm_, other.bm_);
    }

    void or_inplace(const RoaringFP &other) {
        rfp_or_inplace(bm_, other.bm_);
    }

    rfp_bitmap *raw() const { return bm_; }

private:
    rfp_bitmap *bm_;
};

/* Module-level factory functions */
static RoaringFP from_blob(nb::bytes data) {
    rfp_bitmap *bm = rfp_deserialize(data.c_str(), data.size());
    if (!bm) throw std::runtime_error("Failed to deserialize bitmap");
    return RoaringFP(bm);
}

static RoaringFP from_base64(const std::string &b64) {
    rfp_bitmap *bm = rfp_from_base64(b64.data(), b64.size());
    if (!bm) throw std::runtime_error("Failed to decode base64 bitmap");
    return RoaringFP(bm);
}

static RoaringFP from_json(const std::string &json_array) {
    RoaringFP fp;
    if (fp.add_json(json_array) != 0) {
        throw std::runtime_error("Failed to parse JSON array");
    }
    return fp;
}

static std::string probe_json(const std::string &symbols_json, const std::vector<RoaringFP *> &refs) {
    std::vector<const rfp_bitmap *> raw_refs;
    raw_refs.reserve(refs.size());
    for (auto *r : refs) {
        raw_refs.push_back(r ? r->raw() : nullptr);
    }
    char *result = rfp_probe_json(symbols_json.data(), symbols_json.size(),
                                  raw_refs.data(), raw_refs.size());
    if (!result) throw std::runtime_error("Failed to probe JSON");
    std::string out(result);
    rfp_free_string(result);
    return out;
}

NB_MODULE(_core, m) {
    m.doc() = "Roaring bitmap fingerprint library";

    nb::class_<RoaringFP>(m, "RoaringFP")
        .def(nb::init<>())
        .def("add", &RoaringFP::add, "Add a string (hashed via FNV-1a)")
        .def("add_uint32", &RoaringFP::add_uint32, "Add a raw uint32 value")
        .def("add_json", &RoaringFP::add_json, "Add strings from a JSON array")
        .def("cardinality", &RoaringFP::cardinality, "Number of elements")
        .def("serialize", &RoaringFP::serialize, "Serialize to bytes (portable format)")
        .def("to_base64", &RoaringFP::to_base64, "Serialize to base64 string")
        .def("intersection_card", &RoaringFP::intersection_card, "Intersection cardinality with another bitmap")
        .def("containment", &RoaringFP::containment, "Containment score: |self & other| / |self|")
        .def("jaccard", &RoaringFP::jaccard, "Jaccard similarity with another bitmap")
        .def("or_inplace", &RoaringFP::or_inplace, "Union with another bitmap in place")
        .def("__len__", &RoaringFP::cardinality)
        .def("__repr__", [](const RoaringFP &fp) {
            return "RoaringFP(cardinality=" + std::to_string(fp.cardinality()) + ")";
        });

    m.def("from_blob", &from_blob, "Deserialize a bitmap from bytes");
    m.def("from_base64", &from_base64, "Deserialize a bitmap from base64 string");
    m.def("from_json", &from_json, "Build a bitmap from a JSON array of strings");
    m.def("probe_json", &probe_json, "Probe JSON array of symbols against reference bitmaps");
}
