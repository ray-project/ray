// Copyright 2017 The Ray Authors.
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

#include <type_traits>

namespace ray {
namespace internal {

template <typename>
struct RemoveFirst;

template <class First, class... Second>
struct RemoveFirst<std::tuple<First, Second...>> {
  using type = std::tuple<Second...>;
};

template <class Tuple>
using RemoveFirst_t = typename RemoveFirst<Tuple>::type;

template <typename>
struct RemoveReference;

template <class... T>
struct RemoveReference<std::tuple<T...>> {
  using type = std::tuple<std::remove_const_t<std::remove_reference_t<T>>...>;
};

template <class Tuple>
using RemoveReference_t = typename RemoveReference<Tuple>::type;

template <class, class = void>
struct is_object_ref_t : std::false_type {};

template <class T>
struct is_object_ref_t<T, std::void_t<decltype(std::declval<T>().IsObjectRef())>>
    : std::true_type {};

template <typename T>
auto constexpr is_object_ref_v = is_object_ref_t<T>::value;

template <class, class = void>
struct is_python_t : std::false_type {};

template <class T>
struct is_python_t<T, std::void_t<decltype(std::declval<T>().IsPython())>>
    : std::true_type {};

template <typename T>
auto constexpr is_python_v = is_python_t<T>::value;

template <class, class = void>
struct is_java_t : std::false_type {};

template <class T>
struct is_java_t<T, std::void_t<decltype(std::declval<T>().IsJava())>> : std::true_type {
};

template <typename T>
auto constexpr is_java_v = is_java_t<T>::value;

template <typename T>
auto constexpr is_x_lang_v = is_java_v<T> || is_python_v<T>;

}  // namespace internal
}  // namespace ray