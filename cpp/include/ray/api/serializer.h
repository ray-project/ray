
#pragma once

#include <ray/api/ray_exception.h>
#include <msgpack.hpp>

namespace ray {
namespace api {

class Serializer {
 public:
  template <typename T>
  static void Serialize(msgpack::packer<msgpack::sbuffer> &packer, const T &val);

  template <typename T>
  static void Deserialize(msgpack::unpacker &unpacker, T *val);
};

// ---------- implementation ----------

template <typename T>
inline void Serializer::Serialize(msgpack::packer<msgpack::sbuffer> &packer,
                                  const T &val) {
  packer.pack(val);
  return;
}

template <typename T>
inline void Serializer::Deserialize(msgpack::unpacker &unpacker, T *val) {
  msgpack::object_handle oh;
  bool result = unpacker.next(oh);
  if (result == false) {
    throw RayException("unpack error");
  }
  msgpack::object obj = oh.get();
  obj.convert(*val);
  return;
}

}  // namespace api
}  // namespace ray