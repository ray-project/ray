
#ifndef RAY_RPC_COMMON_H
#define RAY_RPC_COMMON_H

#include "ray/protobuf/common.pb.h"

namespace ray {
namespace rpc {

#define ProtobufEnumName(Type, Value) (Type##_Name(Value))
#define ProtobufEnumMin(Type) (Type##_MIN)
#define ProtobufEnumMax(Type) (Type##_MAX)

template <typename T>
const std::vector<std::string> GenerateProtobufEnumNames() {
  std::vector<std::string> enum_names;

  for (int i = static_cast<int>(ProtobufEnumMin(T));
      i <= static_cast<int>(ProtobufEnumMax(T)); i++) {
    enum_names.push_back(EnumName(T, static_cast<T>(i)));
  }
  return enum_names;
}

}  // namespace rpc
}  // namespace ray

#endif


