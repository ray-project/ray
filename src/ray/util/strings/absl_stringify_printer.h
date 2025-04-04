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

// Printer for types which defines `AbslStringify`.

#pragma once

#include <iostream>
#include <type_traits>

#include "absl/strings/str_cat.h"

namespace ray {

// TODO(hjiang): Later version of abseil contains abseil string defined trait.
// TODO(hjiang): Use concepts after we upgrade to C++20.
template <typename T, typename = void, typename = void>
struct is_stringifyable : std::false_type {};

template <typename T, typename SinkType>
struct is_stringifyable<T,
                        SinkType,
                        std::void_t<decltype(AbslStringify(std::declval<SinkType &>(),
                                                           std::declval<const T &>()))>>
    : std::true_type {};

template <typename T>
inline constexpr bool is_stringifyable_v = is_stringifyable<T, std::ostream>::value;

struct AbslStringifyPrinter {
  template <typename T, std::enable_if_t<is_stringifyable_v<T>, int> = 0>
  void operator()(std::ostream &os, const T &obj) const {
    os << absl::StrCat(obj);
  }
};

}  // namespace ray
