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

#include <boost/type_traits/function_traits.hpp>
#include <functional>
#include <type_traits>

namespace ray {

namespace internal {

template <typename T>
struct function_traits_helper : public boost::function_traits<T> {
  using type = T;
};

template <typename NewR, typename T>
struct rebind_result;

template <typename NewR, typename R, typename... Args>
struct rebind_result<NewR, R(Args...)> {
  using type = NewR(Args...);
};

}  // namespace internal

// Generalized boost::function_traits to support more callable types.
// - function pointers
// - std::function
// - member function pointers
// - lambdas and callable objects, or anything with an operator()
//
// For usage, see function_trait_test.cc
template <typename T, typename = void>
struct function_traits;

// Specialization for function pointers
template <typename R, typename... Args>
struct function_traits<R (*)(Args...)>
    : ::ray::internal::function_traits_helper<R(Args...)> {};

template <typename R, typename... Args>
struct function_traits<R(Args...)> : ::ray::internal::function_traits_helper<R(Args...)> {
};

// Specialization for member function pointers
template <typename C, typename R, typename... Args>
struct function_traits<R (C::*)(Args...)>
    : ::ray::internal::function_traits_helper<R(Args...)> {};

// Specialization for const member function pointers
template <typename C, typename R, typename... Args>
struct function_traits<R (C::*)(Args...) const>
    : ::ray::internal::function_traits_helper<R(Args...)> {};

template <typename C, typename R, typename... Args>
struct function_traits<R (C::*)(Args...) &&>
    : ::ray::internal::function_traits_helper<R(Args...)> {};

// Specialization for callable objects (e.g., std::function, lambdas and functors)
template <typename T>
struct function_traits<T, decltype(void(&T::operator()))>
    : function_traits<decltype(&T::operator())> {};

// std::function<FuncType>&, && -> FuncType
template <typename T>
struct function_traits<T &> : function_traits<T> {};

template <typename T>
struct function_traits<T &&> : function_traits<T> {};

// Because you can't manipulate argument packs easily, we need to do rebind to instead
// manipulate the result type.
template <typename NewR, typename T>
using rebind_result_t = typename ::ray::internal::rebind_result<NewR, T>::type;
}  // namespace ray
