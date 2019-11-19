
#pragma once

#include <ray/api/ray_object.h>
#include <ray/api/uniqueId.h>
#include <ray/api/impl/function_argument.h>
#include <ray/api/blob.h>

namespace ray {

    template<typename T>
    inline void marshall(::ray::binary_writer& writer, const T& val)
    {
        assert (false);
    }

    template<typename T>
    inline void unmarshall(::ray::binary_reader& reader, /*out*/ T& val)
    {
        assert (false);
    }

    //------------------ implementation ------------------------
    # define DEFINE_NATIVE_TYPE_SERIALIZATION_FUNCTIONS(T) \
    inline void marshall(::ray::binary_writer& writer, const T& val) \
    { \
        writer.write(val); \
    } \
    inline void unmarshall(::ray::binary_reader& reader, /*out*/ T& val) \
    { \
        reader.read(val); \
    }

    DEFINE_NATIVE_TYPE_SERIALIZATION_FUNCTIONS(bool)
    DEFINE_NATIVE_TYPE_SERIALIZATION_FUNCTIONS(int8_t)
    DEFINE_NATIVE_TYPE_SERIALIZATION_FUNCTIONS(uint8_t)
    DEFINE_NATIVE_TYPE_SERIALIZATION_FUNCTIONS(int16_t)
    DEFINE_NATIVE_TYPE_SERIALIZATION_FUNCTIONS(uint16_t)
    DEFINE_NATIVE_TYPE_SERIALIZATION_FUNCTIONS(int32_t)
    DEFINE_NATIVE_TYPE_SERIALIZATION_FUNCTIONS(uint32_t)
    DEFINE_NATIVE_TYPE_SERIALIZATION_FUNCTIONS(int64_t)
    DEFINE_NATIVE_TYPE_SERIALIZATION_FUNCTIONS(uint64_t)
    DEFINE_NATIVE_TYPE_SERIALIZATION_FUNCTIONS(float)
    DEFINE_NATIVE_TYPE_SERIALIZATION_FUNCTIONS(double)
    DEFINE_NATIVE_TYPE_SERIALIZATION_FUNCTIONS(std::string)

inline void marshall(::ray::binary_writer &writer, const UniqueId &uniqueId) {
  writer.write((const char *)uniqueId.data(), (int)plasma::kUniqueIDSize);
}

inline void unmarshall(::ray::binary_reader &reader, UniqueId &uniqueId) {
  reader.read((char *)uniqueId.mutable_data(), (int)plasma::kUniqueIDSize);
}

template <typename T>
inline void marshall(::ray::binary_writer &writer, const RayObject<T> &rayObject) {
  marshall(writer, rayObject.id());
}

template <typename T>
inline void unmarshall(::ray::binary_reader &reader, RayObject<T> &rayObject) {
  UniqueId uniqueId;
  unmarshall(reader, uniqueId);
  rayObject.assign(std::move(uniqueId));
}

template <typename T>
inline void marshall(::ray::binary_writer &writer, const FunctionArgument<T> &funcArg) {
  marshall(writer, funcArg.rayObjectFlag);
  marshall(writer, funcArg.argument);
}

template <typename T>
inline void unmarshall(::ray::binary_reader &reader, FunctionArgument<T> &funcArg) {
  unmarshall(reader, funcArg.rayObjectFlag);
  unmarshall(reader, funcArg.argument);
}
}
