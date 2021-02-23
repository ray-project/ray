
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
    try {
      msgpack::unpacked unpacked;
      msgpack::unpack(unpacked, data, size);
      return unpacked.get().as<T>();
    } catch (std::exception &e) {
      throw RayException(std::string("unpack failed, reason: ") + e.what());
    } catch (...) {
      throw RayException("unpack failed, reason: unknown error");
    }
  }
};

}  // namespace api
}  // namespace ray