
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

  static void UnwrapArgs(msgpack::unpacker &unpacker);

  template <typename Arg1Type>
  static void UnwrapArgs(msgpack::unpacker &unpacker, std::shared_ptr<Arg1Type> *arg1);

  template <typename Arg1Type, typename... OtherArgTypes>
  static void UnwrapArgs(msgpack::unpacker &unpacker, std::shared_ptr<Arg1Type> *arg1,
                         std::shared_ptr<OtherArgTypes> *... args);
};

// --------- inline implementation ------------
#include <typeinfo>

inline void Arguments::WrapArgs(msgpack::packer<msgpack::sbuffer> &packer) {}

template <typename Arg1Type>
inline void Arguments::WrapArgs(msgpack::packer<msgpack::sbuffer> &packer,
                                Arg1Type &arg1) {
  /// Notice RayObjectClassPrefix should be modified by RayObject class name or namespace.
  static const std::string RayObjectClassPrefix = "N3ray3api9RayObject";
  std::string type_name = typeid(arg1).name();
  if (type_name.rfind(RayObjectClassPrefix, 0) == 0) {
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

inline void Arguments::UnwrapArgs(msgpack::unpacker &unpacker) {}

template <typename Arg1Type>
inline void Arguments::UnwrapArgs(msgpack::unpacker &unpacker,
                                  std::shared_ptr<Arg1Type> *arg1) {
  bool is_ray_object;
  Serializer::Deserialize(unpacker, &is_ray_object);
  if (is_ray_object) {
    RayObject<Arg1Type> ray_object;
    Serializer::Deserialize(unpacker, &ray_object);
    *arg1 = ray_object.Get();
  } else {
    Serializer::Deserialize(unpacker, arg1);
  }
}

template <typename Arg1Type, typename... OtherArgTypes>
inline void Arguments::UnwrapArgs(msgpack::unpacker &unpacker,
                                  std::shared_ptr<Arg1Type> *arg1,
                                  std::shared_ptr<OtherArgTypes> *... args) {
  UnwrapArgs(unpacker, arg1);
  UnwrapArgs(unpacker, args...);
}

}  // namespace api
}  // namespace ray