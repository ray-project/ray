// Copyright 2024 The Ray Authors.
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

#include <sstream>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/strings/str_cat.h"

namespace ray {

template <typename T>
constexpr bool AlwaysFalse = false;

template <typename T>
constexpr bool AlwaysTrue = true;

template <int N>
constexpr bool AlwaysFalseValue = false;

template <int N>
constexpr bool AlwaysTrueValue = true;

template <typename, typename = void>
struct has_equal_operator : std::false_type {};

template <typename T>
struct has_equal_operator<T,
                          std::void_t<decltype(std::declval<T>() == std::declval<T>())>>
    : std::true_type {};

template <typename T, typename = void>
struct can_absl_str_append : std::false_type {};

template <typename T>
struct can_absl_str_append<
    T,
    std::void_t<decltype(absl::StrAppend(std::declval<std::string *>(),
                                         std::forward<T>(std::declval<T>())))>>
    : std::true_type {};

template <typename T>
inline constexpr bool can_absl_str_append_v = can_absl_str_append<std::decay_t<T>>::value;

template <typename T, typename = void>
struct is_streamable : std::false_type {};

template <typename T>
struct is_streamable<
    T,
    std::void_t<decltype(std::declval<std::ostringstream &>() << std::declval<T>())>>
    : std::true_type {};

template <typename T>
inline constexpr bool is_streamable_v = is_streamable<T>::value;
}  // namespace ray
