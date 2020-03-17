
#pragma once

#include <msgpack.hpp>

namespace ray {
namespace api {

class Serializer {
 public:
  template <typename T>
  static void Serialize(msgpack::packer<msgpack::sbuffer> &packer, const T &val);

  static void Serialize(msgpack::packer<msgpack::sbuffer> &packer);

  template <typename Arg1Type, typename... OtherArgTypes>
  static void Serialize(msgpack::packer<msgpack::sbuffer> &packer, const Arg1Type &arg1,
                        const OtherArgTypes &... args);

  template <typename T>
  static void Deserialize(msgpack::unpacker &unpacker, T &val);

  static void Deserialize(msgpack::unpacker &unpacker);

  template <typename Arg1Type, typename... OtherArgTypes>
  static void Deserialize(msgpack::unpacker &unpacker, Arg1Type &arg1,
                          OtherArgTypes &... args);
};
}  // namespace api
}  // namespace ray

namespace ray {
namespace api {

using namespace ::ray;

template <typename T>
inline void Serializer::Serialize(msgpack::packer<msgpack::sbuffer> &packer,
                                  const T &val) {
  packer.pack(val);
  return;
}

inline void Serializer::Serialize(msgpack::packer<msgpack::sbuffer> &packer) { return; }

template <typename Arg1Type, typename... OtherArgTypes>
inline void Serializer::Serialize(msgpack::packer<msgpack::sbuffer> &packer,
                                  const Arg1Type &arg1, const OtherArgTypes &... args) {
  Serialize(packer, arg1);
  Serialize(packer, args...);
  return;
}

template <typename T>
inline void Serializer::Deserialize(msgpack::unpacker &unpacker, T &val) {
  msgpack::object_handle oh;
  bool result = unpacker.next(oh);
  if (result == false) {
    throw "unpack error";
  }
  msgpack::object obj = oh.get();
  obj.convert(val);
  return;
}

inline void Serializer::Deserialize(msgpack::unpacker &unpacker) { return; }

template <typename Arg1Type, typename... OtherArgTypes>
inline void Serializer::Deserialize(msgpack::unpacker &unpacker, Arg1Type &arg1,
                                    OtherArgTypes &... args) {
  Deserialize(unpacker, arg1);
  Deserialize(unpacker, args...);
  return;
}

}  // namespace api
}  // namespace ray