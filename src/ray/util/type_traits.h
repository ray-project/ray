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

#include <cstddef>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/meta/type_traits.h"
#include "absl/strings/str_format.h"

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

// Container type traits.
template <typename, typename = void>
struct is_container {
  static constexpr bool value = false;
};

template <typename T>
struct is_container<T,
                    std::void_t<typename absl::remove_cvref_t<T>::value_type,
                                typename absl::remove_cvref_t<T>::iterator,
                                decltype(std::declval<T>().begin()),
                                decltype(std::declval<T>().end()),
                                decltype(std::declval<T>().size())>> {
  static constexpr bool value = true;
};

template <typename T>
inline constexpr bool is_container_v{is_container<T>::value};

// Map type traits.
template <typename T, typename = void>
struct is_map : std::false_type {};
template <typename T>
struct is_map<T, std::void_t<typename absl::remove_cvref_t<T>::mapped_type>>
    : is_container<T> {};
template <typename T>
inline constexpr bool is_map_v = is_map<T>::value;

// Tuple related type traits.
template <typename T>
struct is_tuple : std::false_type {};
template <typename... Ts>
struct is_tuple<std::tuple<Ts...>> : std::true_type {};

template <typename T>
inline constexpr bool is_tuple_v{is_tuple<T>::value};

// Pair related type traits.
template <typename T>
struct is_pair : std::false_type {};
template <typename T1, typename T2>
struct is_pair<std::pair<T1, T2>> : std::true_type {};

template <typename T>
inline constexpr bool is_pair_v{is_pair<T>::value};

// Optional related type traits.
template <typename T>
struct is_optional : std::false_type {};
template <typename T>
struct is_optional<std::optional<T>> : std::true_type {};

template <typename T>
inline constexpr bool is_optional_v{is_optional<T>::value};

// A comprehensive type traits for all types with internal types.
template <typename T>
inline constexpr bool has_internal_type_v =
    is_container_v<T> || is_map_v<T> || is_tuple_v<T> || is_pair_v<T> || is_optional_v<T>;

}  // namespace ray
