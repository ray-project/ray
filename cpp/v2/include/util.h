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

#include "absl/utility/utility.h"

namespace ray {

template <typename OriginTuple, typename BareInput, typename InputTuple>
inline static std::enable_if_t<std::is_same<OriginTuple, BareInput>::value,
                               OriginTuple>
get_arguments(const InputTuple &input_tp) {
  return input_tp;
}

template <typename OriginTuple, typename BareInput, typename InputTuple>
std::enable_if_t<!std::is_same<OriginTuple, BareInput>::value,
                 OriginTuple> inline static get_arguments(const InputTuple
                                                              &input_tp) {
  OriginTuple tp;
  apply(tp, input_tp);
  return tp;
}

template <class OriginTuple, class Tuple>
inline static constexpr decltype(auto) apply(OriginTuple &origin, Tuple &&tp) {
  apply_impl(origin, std::forward<Tuple>(tp),
             std::make_index_sequence<
                 std::tuple_size<std::remove_reference_t<Tuple>>::value>{});
}

template <typename R, typename T> inline static R transform(T t) {
  return R{t};
}

template <typename R, typename T>
inline static T transform(const ObjectRef<T> &t) {
  return t.Get();
}

template <class OriginTuple, class Tuple, std::size_t... I>
inline static constexpr decltype(auto)
apply_impl(OriginTuple &origin, Tuple &&tp, std::index_sequence<I...>) {
  (void)std::initializer_list<int>{
      (std::get<I>(origin) =
           transform<std::tuple_element_t<I, OriginTuple>>(std::get<I>(tp)),
       0)...};
}
}
