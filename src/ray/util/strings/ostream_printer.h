// Copyright 2025 The Ray Authors.
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

// Printer for types which supports stream operator.

#pragma once

#include <ostream>
#include <type_traits>

namespace ray {

template <typename T, typename = void>
struct is_streamable : std::false_type {};
template <typename T>
struct is_streamable<
    T,
    std::void_t<decltype(std::declval<std::ostream &>() << std::declval<T>())>>
    : std::true_type {};
template <typename T>
inline constexpr bool is_streamable_v = is_streamable<T>::value;

struct OstreamPrinter {
  template <typename T, std::enable_if_t<is_streamable_v<T>, int> = 0>
  void operator()(std::ostream &os, const T &value) const {
    os << value;
  }

  // Specialized boolean implementation.
  template <>
  void operator()<bool>(std::ostream &os, const bool &value) const {
    os << (value ? "true" : "false");
  }
};

}  // namespace ray
