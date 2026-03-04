#define DUCKDB_EXTENSION_MAIN

#include "roaring_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/aggregate_function.hpp"

#include "roaring_fp.h"

namespace duckdb {

//===--------------------------------------------------------------------===//
// FNV-1a Hash for string types (must match core lib's fnv1a)
//===--------------------------------------------------------------------===//

static uint32_t FNVHash(const char *data, idx_t len) {
    uint32_t hash = 2166136261u;
    for (idx_t i = 0; i < len; i++) {
        hash ^= static_cast<uint8_t>(data[i]);
        hash *= 16777619u;
    }
    return hash;
}

//===--------------------------------------------------------------------===//
// Aggregate State for roaring_build
//===--------------------------------------------------------------------===//

struct RoaringState {
    rfp_bitmap *bitmap = nullptr;

    void Initialize() {
        bitmap = nullptr;
    }

    void Destroy() {
        if (bitmap) {
            rfp_free(bitmap);
            bitmap = nullptr;
        }
    }

    void Add(uint32_t val) {
        if (!bitmap) {
            bitmap = rfp_create();
        }
        rfp_add_uint32(bitmap, val);
    }

    void AddHash(const char *data, idx_t len) {
        if (!bitmap) {
            bitmap = rfp_create();
        }
        rfp_add_hash(bitmap, data, len);
    }

    void Combine(RoaringState &other) {
        if (!other.bitmap) return;
        if (!bitmap) {
            bitmap = rfp_copy(other.bitmap);
        } else {
            rfp_or_inplace(bitmap, other.bitmap);
        }
    }

    string_t Serialize(Vector &result) {
        if (!bitmap) {
            bitmap = rfp_create();
        }
        size_t size = rfp_serialized_size(bitmap);
        auto target = StringVector::EmptyString(result, size);
        rfp_serialize(bitmap, target.GetDataWriteable(), size);
        target.Finalize();
        return target;
    }
};

//===--------------------------------------------------------------------===//
// Aggregate Function Operations
//===--------------------------------------------------------------------===//

struct RoaringBuildOp {
    template <class STATE>
    static void Initialize(STATE &state) {
        state.Initialize();
    }

    template <class STATE, class OP>
    static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
        target.Combine(const_cast<STATE &>(source));
    }

    template <class RESULT_TYPE, class STATE>
    static void Finalize(STATE &state, RESULT_TYPE &target, AggregateFinalizeData &finalize_data) {
        target = state.Serialize(finalize_data.result);
    }

    template <class STATE>
    static void Destroy(STATE &state, AggregateInputData &) {
        state.Destroy();
    }

    static bool IgnoreNull() {
        return true;
    }
};

// Update functions for different types
template <class T>
static void RoaringUpdateNumeric(Vector inputs[], AggregateInputData &, idx_t input_count,
                                  Vector &state_vector, idx_t count) {
    auto &input = inputs[0];
    UnifiedVectorFormat idata;
    input.ToUnifiedFormat(count, idata);
    auto vals = UnifiedVectorFormat::GetData<T>(idata);

    UnifiedVectorFormat sdata;
    state_vector.ToUnifiedFormat(count, sdata);
    auto states = (RoaringState **)sdata.data;

    for (idx_t i = 0; i < count; i++) {
        auto idx = idata.sel->get_index(i);
        if (!idata.validity.RowIsValid(idx)) continue;

        auto sidx = sdata.sel->get_index(i);
        T val = vals[idx];

        uint32_t hash;
        if constexpr (sizeof(T) <= 4) {
            hash = static_cast<uint32_t>(val);
        } else {
            hash = FNVHash(reinterpret_cast<const char *>(&val), sizeof(T));
        }
        states[sidx]->Add(hash);
    }
}

static void RoaringUpdateString(Vector inputs[], AggregateInputData &, idx_t input_count,
                                 Vector &state_vector, idx_t count) {
    auto &input = inputs[0];
    UnifiedVectorFormat idata;
    input.ToUnifiedFormat(count, idata);
    auto vals = UnifiedVectorFormat::GetData<string_t>(idata);

    UnifiedVectorFormat sdata;
    state_vector.ToUnifiedFormat(count, sdata);
    auto states = (RoaringState **)sdata.data;

    for (idx_t i = 0; i < count; i++) {
        auto idx = idata.sel->get_index(i);
        if (!idata.validity.RowIsValid(idx)) continue;

        auto sidx = sdata.sel->get_index(i);
        string_t s = vals[idx];
        states[sidx]->AddHash(s.GetData(), s.GetSize());
    }
}

static unique_ptr<FunctionData> RoaringBuildBind(ClientContext &context, AggregateFunction &func,
                                                   vector<unique_ptr<Expression>> &args) {
    auto &type = args[0]->return_type;
    func.arguments[0] = type;

    switch (type.id()) {
    case LogicalTypeId::TINYINT:   func.update = RoaringUpdateNumeric<int8_t>; break;
    case LogicalTypeId::SMALLINT:  func.update = RoaringUpdateNumeric<int16_t>; break;
    case LogicalTypeId::INTEGER:   func.update = RoaringUpdateNumeric<int32_t>; break;
    case LogicalTypeId::BIGINT:    func.update = RoaringUpdateNumeric<int64_t>; break;
    case LogicalTypeId::UTINYINT:  func.update = RoaringUpdateNumeric<uint8_t>; break;
    case LogicalTypeId::USMALLINT: func.update = RoaringUpdateNumeric<uint16_t>; break;
    case LogicalTypeId::UINTEGER:  func.update = RoaringUpdateNumeric<uint32_t>; break;
    case LogicalTypeId::UBIGINT:   func.update = RoaringUpdateNumeric<uint64_t>; break;
    case LogicalTypeId::VARCHAR:
    case LogicalTypeId::BLOB:      func.update = RoaringUpdateString; break;
    default:
        throw BinderException("roaring_build does not support type '%s'", type.ToString());
    }
    return nullptr;
}

static AggregateFunction GetRoaringBuildFunction() {
    auto func = AggregateFunction(
        "roaring_build",
        {LogicalType::ANY},
        LogicalType::BLOB,
        AggregateFunction::StateSize<RoaringState>,
        AggregateFunction::StateInitialize<RoaringState, RoaringBuildOp>,
        RoaringUpdateNumeric<int64_t>,
        AggregateFunction::StateCombine<RoaringState, RoaringBuildOp>,
        AggregateFunction::StateFinalize<RoaringState, string_t, RoaringBuildOp>,
        nullptr,
        RoaringBuildBind,
        AggregateFunction::StateDestroy<RoaringState, RoaringBuildOp>
    );
    return func;
}

//===--------------------------------------------------------------------===//
// Scalar Functions — use core library for bitmap operations
//===--------------------------------------------------------------------===//

static void CardinalityFunc(DataChunk &args, ExpressionState &, Vector &result) {
    UnaryExecutor::Execute<string_t, uint64_t>(args.data[0], result, args.size(), [](string_t blob) {
        rfp_bitmap *bm = rfp_deserialize(blob.GetData(), blob.GetSize());
        if (!bm) return (uint64_t)0;
        uint64_t card = rfp_cardinality(bm);
        rfp_free(bm);
        return card;
    });
}

static void IntersectionCardFunc(DataChunk &args, ExpressionState &, Vector &result) {
    BinaryExecutor::Execute<string_t, string_t, uint64_t>(
        args.data[0], args.data[1], result, args.size(),
        [](string_t a, string_t b) {
            rfp_bitmap *bm_a = rfp_deserialize(a.GetData(), a.GetSize());
            rfp_bitmap *bm_b = rfp_deserialize(b.GetData(), b.GetSize());
            if (!bm_a || !bm_b) {
                rfp_free(bm_a);
                rfp_free(bm_b);
                return (uint64_t)0;
            }
            uint64_t r = rfp_intersection_card(bm_a, bm_b);
            rfp_free(bm_a);
            rfp_free(bm_b);
            return r;
        });
}

static void ContainmentFunc(DataChunk &args, ExpressionState &, Vector &result) {
    BinaryExecutor::Execute<string_t, string_t, double>(
        args.data[0], args.data[1], result, args.size(),
        [](string_t probe, string_t ref) {
            rfp_bitmap *bm_probe = rfp_deserialize(probe.GetData(), probe.GetSize());
            rfp_bitmap *bm_ref = rfp_deserialize(ref.GetData(), ref.GetSize());
            if (!bm_probe || !bm_ref) {
                rfp_free(bm_probe);
                rfp_free(bm_ref);
                return 0.0;
            }
            double r = rfp_containment(bm_probe, bm_ref);
            rfp_free(bm_probe);
            rfp_free(bm_ref);
            return r;
        });
}

static void JaccardFunc(DataChunk &args, ExpressionState &, Vector &result) {
    BinaryExecutor::Execute<string_t, string_t, double>(
        args.data[0], args.data[1], result, args.size(),
        [](string_t a, string_t b) {
            rfp_bitmap *bm_a = rfp_deserialize(a.GetData(), a.GetSize());
            rfp_bitmap *bm_b = rfp_deserialize(b.GetData(), b.GetSize());
            if (!bm_a || !bm_b) {
                rfp_free(bm_a);
                rfp_free(bm_b);
                return 0.0;
            }
            double r = rfp_jaccard(bm_a, bm_b);
            rfp_free(bm_a);
            rfp_free(bm_b);
            return r;
        });
}

//===--------------------------------------------------------------------===//
// New: roaring_build_json — build bitmap from JSON array of strings
//===--------------------------------------------------------------------===//

static void BuildJsonFunc(DataChunk &args, ExpressionState &, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(),
        [&result](string_t json_input) {
            rfp_bitmap *bm = rfp_create();
            rfp_add_json_array(bm, json_input.GetData(), json_input.GetSize());
            size_t size = rfp_serialized_size(bm);
            auto target = StringVector::EmptyString(result, size);
            rfp_serialize(bm, target.GetDataWriteable(), size);
            target.Finalize();
            rfp_free(bm);
            return target;
        });
}

//===--------------------------------------------------------------------===//
// New: roaring_to_base64 / roaring_from_base64
//===--------------------------------------------------------------------===//

static void ToBase64Func(DataChunk &args, ExpressionState &, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(),
        [&result](string_t blob) {
            rfp_bitmap *bm = rfp_deserialize(blob.GetData(), blob.GetSize());
            if (!bm) {
                return StringVector::EmptyString(result, 0);
            }
            size_t b64_size = rfp_base64_size(bm);
            auto target = StringVector::EmptyString(result, b64_size);
            rfp_to_base64(bm, target.GetDataWriteable(), b64_size);
            target.Finalize();
            rfp_free(bm);
            return target;
        });
}

static void FromBase64Func(DataChunk &args, ExpressionState &, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(),
        [&result](string_t b64) {
            rfp_bitmap *bm = rfp_from_base64(b64.GetData(), b64.GetSize());
            if (!bm) {
                return StringVector::EmptyString(result, 0);
            }
            size_t size = rfp_serialized_size(bm);
            auto target = StringVector::EmptyString(result, size);
            rfp_serialize(bm, target.GetDataWriteable(), size);
            target.Finalize();
            rfp_free(bm);
            return target;
        });
}

//===--------------------------------------------------------------------===//
// New: roaring_containment_json — builds probe from JSON, compares to ref BLOB
//===--------------------------------------------------------------------===//

static void ContainmentJsonFunc(DataChunk &args, ExpressionState &, Vector &result) {
    BinaryExecutor::Execute<string_t, string_t, double>(
        args.data[0], args.data[1], result, args.size(),
        [](string_t json_input, string_t ref_blob) {
            rfp_bitmap *probe = rfp_create();
            if (rfp_add_json_array(probe, json_input.GetData(), json_input.GetSize()) != 0) {
                rfp_free(probe);
                return 0.0;
            }
            rfp_bitmap *ref = rfp_deserialize(ref_blob.GetData(), ref_blob.GetSize());
            if (!ref) {
                rfp_free(probe);
                return 0.0;
            }
            double r = rfp_containment(probe, ref);
            rfp_free(probe);
            rfp_free(ref);
            return r;
        });
}

//===--------------------------------------------------------------------===//
// Extension Loading
//===--------------------------------------------------------------------===//

static void LoadInternal(ExtensionLoader &loader) {
    loader.RegisterFunction(GetRoaringBuildFunction());

    loader.RegisterFunction(ScalarFunction("roaring_build_json", {LogicalType::VARCHAR},
                                           LogicalType::BLOB, BuildJsonFunc));
    loader.RegisterFunction(ScalarFunction("roaring_cardinality", {LogicalType::BLOB},
                                           LogicalType::UBIGINT, CardinalityFunc));
    loader.RegisterFunction(ScalarFunction("roaring_intersection_card",
                                           {LogicalType::BLOB, LogicalType::BLOB},
                                           LogicalType::UBIGINT, IntersectionCardFunc));
    loader.RegisterFunction(ScalarFunction("roaring_containment",
                                           {LogicalType::BLOB, LogicalType::BLOB},
                                           LogicalType::DOUBLE, ContainmentFunc));
    loader.RegisterFunction(ScalarFunction("roaring_jaccard",
                                           {LogicalType::BLOB, LogicalType::BLOB},
                                           LogicalType::DOUBLE, JaccardFunc));
    loader.RegisterFunction(ScalarFunction("roaring_to_base64", {LogicalType::BLOB},
                                           LogicalType::VARCHAR, ToBase64Func));
    loader.RegisterFunction(ScalarFunction("roaring_from_base64", {LogicalType::VARCHAR},
                                           LogicalType::BLOB, FromBase64Func));
    loader.RegisterFunction(ScalarFunction("roaring_containment_json",
                                           {LogicalType::VARCHAR, LogicalType::BLOB},
                                           LogicalType::DOUBLE, ContainmentJsonFunc));
}

void RoaringExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}

std::string RoaringExtension::Name() {
    return "roaring";
}

std::string RoaringExtension::Version() const {
#ifdef EXT_VERSION_ROARING
    return EXT_VERSION_ROARING;
#else
    return "0.1.0";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(roaring, loader) {
    duckdb::LoadInternal(loader);
}
}
