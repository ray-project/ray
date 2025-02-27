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

#include <algorithm>
#include <array>
#include <deque>
#include <map>
#include <ostream>
#include <set>
#include <sstream>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "ray/util/logging.h"

namespace ray {

template <typename T>
class DebugStringWrapper;

// The actual interface.
template <typename T>
DebugStringWrapper<T> debug_string(const T &t) {
  return DebugStringWrapper<T>(t);
}

/// Wrapper for `debug_string(const T&)`.
template <typename T>
class DebugStringWrapper {
 public:
  explicit DebugStringWrapper(const T &obj) : obj_(obj) {}

  // Only initialized for the blessed container types listed below with operator<<
  // specializations.
  std::ostream &StringifyContainer(std::ostream &os) const {
    os << "[";
    for (auto it = obj_.begin(); it != obj_.end(); ++it) {
      if (it != obj_.begin()) {
        os << ", ";
      }
      os << debug_string(*it);
    }
    os << "]";
    return os;
  }

  // Public but OK, since it's const &.
  const T &obj_;
};

template <typename T>
std::ostream &operator<<(std::ostream &os, DebugStringWrapper<T> wrapper) {
  return os << wrapper.obj_;
}

// TODO(hjiang): Implement debug string for `std::variant`.
template <>
inline std::ostream &operator<<(std::ostream &os,
                                DebugStringWrapper<std::nullopt_t> wrapper) {
  return os << "(nullopt)";
}

template <typename... Ts>
std::ostream &operator<<(std::ostream &os, DebugStringWrapper<std::pair<Ts...>> pair) {
  return os << "(" << debug_string(pair.obj_.first) << ", "
            << debug_string(pair.obj_.second) << ")";
}

template <typename... Ts>
std::ostream &operator<<(std::ostream &os, DebugStringWrapper<std::tuple<Ts...>> tuple) {
  os << "(";
  // This specialization is needed, or the compiler complains the lambda in std::apply
  // does not use capture &os.
  if constexpr (sizeof...(Ts) != 0) {
    std::apply(
        [&os](const Ts &...args) {
          size_t n = 0;
          ((os << debug_string(args) << (++n != sizeof...(Ts) ? ", " : "")), ...);
        },
        tuple.obj_);
  } else {
    // Avoid unused variable warning.
    (void)tuple;
  }
  os << ")";
  return os;
}

template <typename T, std::size_t N>
std::ostream &operator<<(std::ostream &os, DebugStringWrapper<std::array<T, N>> c) {
  return c.StringifyContainer(os);
}
template <typename... Ts>
std::ostream &operator<<(std::ostream &os, DebugStringWrapper<std::vector<Ts...>> c) {
  return c.StringifyContainer(os);
}
template <typename T, std::size_t N>
std::ostream &operator<<(std::ostream &os,
                         DebugStringWrapper<absl::InlinedVector<T, N>> c) {
  return c.StringifyContainer(os);
}
template <typename... Ts>
std::ostream &operator<<(std::ostream &os, DebugStringWrapper<std::set<Ts...>> c) {
  return c.StringifyContainer(os);
}
template <typename... Ts>
std::ostream &operator<<(std::ostream &os,
                         DebugStringWrapper<std::unordered_set<Ts...>> c) {
  return c.StringifyContainer(os);
}
template <typename... Ts>
std::ostream &operator<<(std::ostream &os,
                         DebugStringWrapper<absl::flat_hash_set<Ts...>> c) {
  return c.StringifyContainer(os);
}
template <typename... Ts>
std::ostream &operator<<(std::ostream &os, DebugStringWrapper<std::map<Ts...>> c) {
  return c.StringifyContainer(os);
}
template <typename... Ts>
std::ostream &operator<<(std::ostream &os,
                         DebugStringWrapper<absl::flat_hash_map<Ts...>> c) {
  return c.StringifyContainer(os);
}
template <typename... Ts>
std::ostream &operator<<(std::ostream &os,
                         DebugStringWrapper<std::unordered_map<Ts...>> c) {
  return c.StringifyContainer(os);
}

template <typename T>
std::ostream &operator<<(std::ostream &os, DebugStringWrapper<std::optional<T>> c) {
  if (!c.obj_.has_value()) {
    return os << debug_string(std::nullopt);
  }
  return os << debug_string(c.obj_.value());
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

// This is guaranteed that predicate is applied to each element exactly once,
// so it can have side effect.
template <typename K, typename V>
void erase_if(absl::flat_hash_map<K, std::deque<V>> &map,
              std::function<bool(const V &)> predicate) {
  for (auto map_it = map.begin(); map_it != map.end();) {
    auto &queue = map_it->second;
    for (auto queue_it = queue.begin(); queue_it != queue.end();) {
      if (predicate(*queue_it)) {
        queue_it = queue.erase(queue_it);
      } else {
        ++queue_it;
      }
    }
    if (queue.empty()) {
      map.erase(map_it++);
    } else {
      ++map_it;
    }
  }
}

template <typename T>
void erase_if(std::list<T> &list, std::function<bool(const T &)> predicate) {
  for (auto list_it = list.begin(); list_it != list.end();) {
    if (predicate(*list_it)) {
      list_it = list.erase(list_it);
    } else {
      ++list_it;
    }
  }
}

// [T] -> (T -> U) -> [U]
// Only supports && input.
template <typename T, typename F>
auto move_mapped(std::vector<T> &&vec, F transform) {
  std::vector<decltype(transform(std::declval<T>()))> result;
  result.reserve(vec.size());
  for (T &elem : vec) {
    result.emplace_back(transform(std::move(elem)));
  }
  return result;
}

}  // namespace ray
