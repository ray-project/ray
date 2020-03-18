
#pragma once

#include <ray/api/serializer.h>
#include <msgpack.hpp>

namespace ray {
namespace api {

class Arguments {
 public:
  static void WrapArgs(msgpack::packer<msgpack::sbuffer> &packer);

  template <typename Arg1Type>
  static void WrapArgs(msgpack::packer<msgpack::sbuffer> &packer, Arg1Type &arg1);

  template <typename Arg1Type, typename... OtherArgTypes>
  static void WrapArgs(msgpack::packer<msgpack::sbuffer> &packer, Arg1Type &arg1,
                       OtherArgTypes &... args);
};
}  // namespace api
}  // namespace ray

#include <typeinfo>
namespace ray {
namespace api {

inline void Arguments::WrapArgs(msgpack::packer<msgpack::sbuffer> &packer) {}

template <typename Arg1Type>
inline void Arguments::WrapArgs(msgpack::packer<msgpack::sbuffer> &packer,
                                Arg1Type &arg1) {
  /// Notice RayObjectClassPrefix should be modified by RayObject class name or namespace.
  static const std::string RayObjectClassPrefix = "N3ray3api9RayObject";
  std::string typeName = typeid(arg1).name();
  if (typeName.rfind(RayObjectClassPrefix, 0) == 0) {
    /// Pass by reference.
    Serializer::Serialize(packer, true);
  } else {
    /// Pass by value.
    Serializer::Serialize(packer, false);
  }
  Serializer::Serialize(packer, arg1);
}

template <typename Arg1Type, typename... OtherArgTypes>
inline void Arguments::WrapArgs(msgpack::packer<msgpack::sbuffer> &packer, Arg1Type &arg1,
                                OtherArgTypes &... args) {
  WrapArgs(packer, arg1);
  WrapArgs(packer, args...);
}

}  // namespace api
}  // namespace ray