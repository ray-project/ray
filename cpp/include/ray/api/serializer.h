// Copyright 2020-2021 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <ray/api/ray_exception.h>
#include <ray/api/type_traits.h>

#include <msgpack.hpp>

namespace ray {
namespace internal {

class Serializer {
 public:
  template <typename T>
  static msgpack::sbuffer Serialize(T &&t) {
    WriteExternal(std::forward<T>(t));
    msgpack::sbuffer buffer;
    msgpack::pack(buffer, std::forward<T>(t));
    return buffer;
  }

  template <typename T>
  static T Deserialize(const char *data, size_t size) {
    msgpack::unpacked unpacked;
    msgpack::unpack(unpacked, data, size);
    auto val = unpacked.get().as<T>();
    ReadExternal(val);
    return val;
  }

  template <typename T>
  static T Deserialize(const char *data, size_t size, size_t offset) {
    return Deserialize<T>(data + offset, size - offset);
  }

  template <typename T>
  static T Deserialize(const char *data, size_t size, size_t *off) {
    msgpack::unpacked unpacked = msgpack::unpack(data, size, *off);
    return unpacked.get().as<T>();
  }

  template <typename T>
  static std::pair<bool, T> DeserializeWhenNil(const char *data, size_t size) {
    T val;
    size_t off = 0;
    msgpack::unpacked unpacked = msgpack::unpack(data, size, off);
    if (!unpacked.get().convert_if_not_nil(val)) {
      return {false, {}};
    }
    ReadExternal(val);

    return {true, val};
  }

  static bool HasError(char *data, size_t size) {
    msgpack::unpacked unpacked = msgpack::unpack(data, size);
    return unpacked.get().is_nil() && size > 1;
  }

 private:
  template <typename T>
  static void WriteExternal(T &&t) {
    using U = std::remove_const_t<std::remove_reference_t<T>>;
    if constexpr (has_value_type_v<U>) {
      using value_type = typename U::value_type;
      if constexpr (is_object_ref_v<value_type>) {
        for (auto &obj : t) {
          obj.WriteExternal();
        }
      }
    }
  }

  template <typename T>
  static void ReadExternal(const T &val) {
    if constexpr (has_value_type_v<T>) {
      using U = typename T::value_type;
      if constexpr (is_object_ref_v<U>) {
        for (auto &obj : val) {
          obj.ReadExternal();
        }
      }
    }
  }
};

}  // namespace internal
}  // namespace ray