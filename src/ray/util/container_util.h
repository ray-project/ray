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

#include <iostream>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ray/util/logging.h"

namespace ray {

template <typename T1, typename T2>
std::ostream &operator<<(std::ostream &os, const std::pair<T1, T2> &pair) {
  os << "(" << pair.first << ", " << pair.second << ")";
  return os;
}

#define DEFINE_CONTAINER_OSTREAM_INSERTION_OPERATOR(container)            \
  template <typename... Ts>                                               \
  std::ostream &operator<<(std::ostream &os, const container<Ts...> &c) { \
    os << "[";                                                            \
    for (auto it = c.begin(); it != c.end(); ++it) {                      \
      if (it != c.begin()) {                                              \
        os << ", ";                                                       \
      }                                                                   \
      os << *it;                                                          \
    }                                                                     \
    os << "]";                                                            \
    return os;                                                            \
  }

DEFINE_CONTAINER_OSTREAM_INSERTION_OPERATOR(std::vector);
DEFINE_CONTAINER_OSTREAM_INSERTION_OPERATOR(std::set);
DEFINE_CONTAINER_OSTREAM_INSERTION_OPERATOR(std::unordered_set);
DEFINE_CONTAINER_OSTREAM_INSERTION_OPERATOR(std::map);
DEFINE_CONTAINER_OSTREAM_INSERTION_OPERATOR(std::unordered_map);
#undef DEFINE_CONTAINER_OSTREAM_INSERTION_OPERATOR

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
