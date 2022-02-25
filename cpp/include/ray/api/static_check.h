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

#include <ray/api/object_ref.h>

#include <boost/callable_traits.hpp>
#include <type_traits>

namespace ray {
namespace internal {

template <typename T>
struct FilterArgType {
  using type = T;
};

template <typename T>
struct FilterArgType<ObjectRef<T>> {
  using type = T;
};

template <typename T>
struct FilterArgType<ObjectRef<T> &> {
  using type = T;
};

template <typename T>
struct FilterArgType<ObjectRef<T> &&> {
  using type = T;
};

template <typename T>
struct FilterArgType<const ObjectRef<T> &> {
  using type = T;
};

template <typename F, typename... Args>
struct is_invocable
    : std::is_constructible<
          std::function<void(Args...)>,
          std::reference_wrapper<typename std::remove_reference<F>::type>> {};

template <typename Function, typename... Args>
inline std::enable_if_t<!std::is_member_function_pointer<Function>::value> StaticCheck() {
  static_assert(is_invocable<Function, typename FilterArgType<Args>::type...>::value ||
                    is_invocable<Function, Args...>::value,
                "arguments not match");
}

template <typename Function, typename... Args>
inline std::enable_if_t<std::is_member_function_pointer<Function>::value> StaticCheck() {
  using ActorType = boost::callable_traits::class_of_t<Function>;
  static_assert(
      is_invocable<Function, ActorType &, typename FilterArgType<Args>::type...>::value ||
          is_invocable<Function, ActorType &, Args...>::value,
      "arguments not match");
}

}  // namespace internal
}  // namespace ray
