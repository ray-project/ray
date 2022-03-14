// Copyright 2022 The Ray Authors.
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

#include <map>
#include <set>
#include <sstream>
#include <unordered_set>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/util/logging.h"

namespace ray {

template <typename T>
std::string debug_string(const T &obj) {
  std::stringstream ss;
  ss << obj;
  return ss.str();
}

template <typename... Ts>
std::string debug_string(const std::pair<Ts...> &pair) {
  std::stringstream ss;
  ss << "(" << debug_string(pair.first) << ", " << debug_string(pair.second) << ")";
  return ss.str();
}

template <typename C>
std::string _container_debug_string(const C &c) {
  std::stringstream ss;
  ss << "[";
  for (auto it = c.begin(); it != c.end(); ++it) {
    if (it != c.begin()) {
      ss << ", ";
    }
    ss << debug_string(*it);
  }
  ss << "]";
  return ss.str();
}

template <typename... Ts>
std::string debug_string(const std::vector<Ts...> &c) {
  return _container_debug_string(c);
}
template <typename... Ts>
std::string debug_string(const std::set<Ts...> &c) {
  return _container_debug_string(c);
}
template <typename... Ts>
std::string debug_string(const std::unordered_set<Ts...> &c) {
  return _container_debug_string(c);
}
template <typename... Ts>
std::string debug_string(const absl::flat_hash_set<Ts...> &c) {
  return _container_debug_string(c);
}
template <typename... Ts>
std::string debug_string(const std::map<Ts...> &c) {
  return _container_debug_string(c);
}
template <typename... Ts>
std::string debug_string(const absl::flat_hash_map<Ts...> &c) {
  return _container_debug_string(c);
}

template <typename C>
const typename C::mapped_type &map_find_or_die(const C &c,
                                               const typename C::key_type &k) {
  auto iter = c.find(k);
  if (iter == c.end()) {
    RAY_LOG(FATAL) << "Key " << k << " doesn't exist";
  }

  return iter->second;
}

template <typename C>
typename C::mapped_type &map_find_or_die(C &c, const typename C::key_type &k) {
  return const_cast<typename C::mapped_type &>(
      map_find_or_die(const_cast<const C &>(c), k));
}

}  // namespace ray
