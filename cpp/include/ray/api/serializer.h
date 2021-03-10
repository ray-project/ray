
#pragma once

#include <ray/api/ray_exception.h>

#include <msgpack.hpp>

namespace ray {
namespace api {

class Serializer {
 public:
  template <typename T>
  static msgpack::sbuffer Serialize(const T &t) {
    msgpack::sbuffer buffer;
    msgpack::pack(buffer, t);
    return buffer;
  }

  template <typename T>
  static T Deserialize(const char *data, size_t size) {
    msgpack::unpacked unpacked;
    msgpack::unpack(unpacked, data, size);
    return unpacked.get().as<T>();
  }

  template <typename T>
  static T Deserialize(const char *data, size_t size, size_t offset) {
    return Deserialize<T>(data + offset, size - offset);
  }

  template <typename T>
  static T Deserialize(const char *data, size_t size, size_t &off) {
    msgpack::unpacked unpacked = msgpack::unpack(data, size, off);
    return unpacked.get().as<T>();
  }

  static bool HasError(char *data, size_t size) {
    size_t off = 0;
    msgpack::unpacked unpacked = msgpack::unpack(data, 1, off);
    return unpacked.get().is_nil() && size > off;
  }
};

}  // namespace api
}  // namespace ray