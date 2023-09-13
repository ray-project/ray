// Copyright 2023 The Ray Authors.
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

#include <any>
#include <msgpack.hpp>

// TODO(Larry Lian) Adapt on windows
#ifndef _WIN32

namespace msgpack {
namespace adaptor {

template <>
struct pack<std::any> {
  template <typename Stream>
  msgpack::packer<Stream> &operator()(msgpack::packer<Stream> &o,
                                      const std::any &v) const {
    const auto &any_type = v.type();
    if (any_type == typeid(msgpack::type::nil_t)) {
      o.pack(std::any_cast<msgpack::type::nil_t>(v));
    } else if (any_type == typeid(bool)) {
      o.pack(std::any_cast<bool>(v));
    } else if (any_type == typeid(uint64_t)) {
      o.pack(std::any_cast<uint64_t>(v));
    } else if (any_type == typeid(int64_t)) {
      o.pack(std::any_cast<int64_t>(v));
    } else if (any_type == typeid(double)) {
      o.pack(std::any_cast<double>(v));
    } else if (any_type == typeid(std::string)) {
      o.pack(std::any_cast<std::string>(v));
    } else if (any_type == typeid(std::vector<char>)) {
      o.pack(std::any_cast<std::vector<char>>(v));
    } else {
      throw msgpack::type_error();
    }
    return o;
  }
};

template <>
struct convert<std::any> {
  msgpack::object const &operator()(msgpack::object const &o, std::any &v) const {
    switch (o.type) {
    case type::NIL:
      v = o.as<msgpack::type::nil_t>();
      break;
    case type::BOOLEAN:
      v = o.as<bool>();
      break;
    case type::POSITIVE_INTEGER:
      v = o.as<uint64_t>();
      break;
    case type::NEGATIVE_INTEGER:
      v = o.as<int64_t>();
      break;
    case type::FLOAT32:
    case type::FLOAT64:
      v = o.as<double>();
      break;
    case type::STR:
      v = o.as<std::string>();
      break;
    case type::BIN:
      v = o.as<std::vector<char>>();
      break;
    default:
      throw msgpack::type_error();
    }
    return o;
  }
};

}  // namespace adaptor
}  // namespace msgpack
#endif
