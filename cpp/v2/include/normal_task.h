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
#include "util.h"
#include "absl/utility/utility.h"

namespace ray {

template <typename F> struct NormalTask {

  template <typename Arg, typename... Args> auto Remote(Arg arg, Args... args) {
    // TODO
    // send function name and arguments to the remote node
    using args_tuple = typename function_traits<F>::args_tuple;
    using input_args_tuple =
        std::tuple<std::remove_const_t<std::remove_reference_t<Arg>>,
                   std::remove_const_t<std::remove_reference_t<Args>>...>;

    static_assert(std::tuple_size<args_tuple>::value ==
                      std::tuple_size<input_args_tuple>::value,
                  "arguments not match");

    auto tp = get_arguments<args_tuple, input_args_tuple>(
        std::make_tuple(arg, args...));
    //TODO will send to the remote node.
    (void)tp;

    using R = typename function_traits<F>::return_type;
    return get_result<R>(tp);
    // R result = absl::apply(f_, tp);// Just for test.
    // return ObjectRef<R>{result};
  }

  auto Remote() {
    // TODO
    // send function name and arguments to the remote node

    using R = std::result_of_t<decltype(f_)()>;
    return get_result<R>();
  }

  absl::string_view func_name_;
  const F &f_;

private:
  template<typename R>
  std::enable_if_t<std::is_void<R>::value, ObjectRef<R>> get_result() {
    f_();
    return ObjectRef<R>{};
  }

  template<typename R>
  std::enable_if_t<!std::is_void<R>::value, ObjectRef<R>> get_result() {
    return ObjectRef<R>{f_()};
  }

  template<typename R, typename Tuple>
  std::enable_if_t<std::is_void<R>::value, ObjectRef<R>> get_result(const Tuple& tp) {
    absl::apply(f_, tp);// Just for test.
    return ObjectRef<R>{};
  }

  template<typename R, typename Tuple>
  std::enable_if_t<!std::is_void<R>::value, ObjectRef<R>> get_result(const Tuple& tp) {
    return ObjectRef<R>{absl::apply(f_, tp)};
  }
};

template <typename F> inline static auto Task(const F &f) {
  auto func_name = get_function_name(f);
  if (func_name.empty()) {
    throw std::invalid_argument("no such function!");
  }

  return NormalTask<F>{func_name, f};
}
}