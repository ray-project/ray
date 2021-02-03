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
#include "object_ref.h"
#include "ray_register.h"
#include "function_traits.h"

namespace ray {

namespace detail {
template <typename T> T transform(const T &t) { return t; }

template <typename T> T transform(const ObjectRef<T> &t) { return t.Get(); }

template <class OriginTuple, class Tuple, std::size_t... I>
constexpr decltype(auto) apply_impl(OriginTuple &origin, Tuple &&tp,
                                    std::index_sequence<I...>) {
  (void)std::initializer_list<int>{
      (std::get<I>(origin) = transform(std::get<I>(tp)), 0)...};
}
} // namespace detail

template <class OriginTuple, class Tuple>
constexpr decltype(auto) apply(OriginTuple &origin, Tuple &&tp) {
  detail::apply_impl(
      origin, std::forward<Tuple>(tp),
      std::make_index_sequence<
          std::tuple_size<std::remove_reference_t<Tuple>>::value>{});
}

template <typename F> struct NormalTask {

  template <typename OriginTuple, typename BareInput, typename InputTuple>
  std::enable_if_t<std::is_same<OriginTuple, BareInput>::value, OriginTuple>
  get_arguments(const InputTuple &input_tp) {
    return input_tp;
  }

  template <typename OriginTuple, typename BareInput, typename InputTuple>
  std::enable_if_t<!std::is_same<OriginTuple, BareInput>::value, OriginTuple>
  get_arguments(const InputTuple &input_tp) {
    OriginTuple tp;
    apply(tp, input_tp);
    return tp;
  }

  template <typename Arg, typename... Args> auto Remote(Arg arg, Args... args) {
    // TODO
    // send function name and arguments to the remote node
    using args_tuple = typename function_traits<F>::args_tuple;
    using input_args_tuple =
        std::tuple<std::remove_const_t<std::remove_reference_t<Arg>>,
                   std::remove_const_t<std::remove_reference_t<Args>>...>;

    auto tp = get_arguments<args_tuple, input_args_tuple>(
        std::make_tuple(arg, args...));
    (void)tp;// Will send to the remote node

    using R = typename function_traits<F>::return_type;
    return ObjectRef<R>{};
  }

  auto Remote() {
    // TODO
    // send function name and arguments to the remote node

    using R = std::result_of_t<decltype(f_)()>;
    return ObjectRef<R>{};
  }

  absl::string_view func_name_;
  const F &f_;
};

template <typename F> inline static auto Task(const F &f) {
  auto func_name = get_function_name(f);
  if (func_name.empty()) {
    throw std::invalid_argument("no such function!");
  }

  return NormalTask<F>{func_name, f};
}
}
