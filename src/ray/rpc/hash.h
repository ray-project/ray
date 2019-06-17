#ifndef RAY_PROTOBUF_HASH_H
#define RAY_PROTOBUF_HASH_H

#include "ray/protobuf/common.pb.h"

namespace std {

template <>
struct hash<ray::rpc::Language> {
  size_t operator()(const ray::rpc::Language &language) const {
    return std::hash<int32_t>()(static_cast<int32_t>(language));
  }
};

template <>
struct hash<const ray::rpc::Language> {
  size_t operator()(const ray::rpc::Language &language) const {
    return std::hash<int32_t>()(static_cast<int32_t>(language));
  }
};

}  // namespace std

#endif  // RAY_PROTOBUF_HASH_H
