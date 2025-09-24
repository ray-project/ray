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

#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/util/function_traits.h"

namespace ray {

template <typename FuncType>
class Postable;

namespace internal {

template <typename FuncType>
struct ToPostableHelper;

template <typename FuncType>
struct ToPostableHelper<std::function<FuncType>> {
  using type = Postable<FuncType>;
};

}  // namespace internal

template <typename FuncType>
using ToPostable = typename internal::ToPostableHelper<FuncType>::type;

/// Postable wraps a std::function and an instrumented_io_context together, ensuring the
/// function can only be Post()ed or Dispatch()ed to that specific io_context. This
/// provides thread safety and prevents accidentally running the function on the wrong
/// io_context.
template <typename FuncType>
class Postable {
  static_assert(std::is_void_v<typename function_traits<FuncType>::result_type>,
                "Postable return type must be void");

 public:
  Postable(std::function<FuncType> func, instrumented_io_context &io_context)
      : func_(std::move(func)), io_context_(io_context) {
    RAY_CHECK(func_ != nullptr)
        << "Postable must be constructed with a non-null function.";
  }

  template <typename... Args>
  void Post(const std::string &name, Args &&...args) && {
    RAY_CHECK(func_ != nullptr) << "Postable has already been invoked.";
    io_context_.post(
        [func = std::move(func_),
         args_tuple = std::make_tuple(std::forward<Args>(args)...)]() mutable {
          std::apply(func, std::move(args_tuple));
        },
        name);
  }

  template <typename... Args>
  void Post(const std::string &name, Args &&...args) const & {
    RAY_CHECK(func_ != nullptr) << "Postable has already been invoked.";
    io_context_.post(
        [func = func_,
         args_tuple = std::make_tuple(std::forward<Args>(args)...)]() mutable {
          std::apply(func, std::move(args_tuple));
        },
        name);
  }

  template <typename... Args>
  void Dispatch(const std::string &name, Args &&...args) && {
    RAY_CHECK(func_ != nullptr) << "Postable has already been invoked.";
    io_context_.dispatch(
        [func = std::move(func_),
         args_tuple = std::make_tuple(std::forward<Args>(args)...)]() mutable {
          std::apply(func, std::move(args_tuple));
        },
        name);
  }

  // OnInvocation
  // Adds an observer that will be called on the io_context before the original function.
  Postable OnInvocation(std::function<void()> observer) && {
    auto original_func = std::move(func_);
    func_ = [observer = std::move(observer),
             func = std::move(original_func)](auto &&...args) {
      observer();
      return func(std::forward<decltype(args)>(args)...);
    };
    return std::move(*this);
  }

  // Transforms the argument by applying `arg_mapper` to the input argument.
  // Basically, adds a arg_mapper and becomes io_context.Post(func(arg_mapper(input...))).
  //
  // Constraints in template arguments:
  // - `this->func_` must take exactly 0 or 1 argument.
  // - `arg_mapper` may take multiple arguments.
  // - `arg_mapper` must return the same type as `this->func_`'s argument.
  //
  // Result:
  // `this` is Postable<void(OldInputType)>
  // `arg_mapper` is a std::function<OldInputType(NewInputTypes...)>
  // The result is Postable<void(NewInputTypes...)>
  //
  // Example:
  // Postable<void(int)> p = ...;
  // Postable<void(char, float)> p2 = p.TransformArg([](char a, float b) -> int {
  //     return a + b;
  // });
  template <typename ArgMapper>
  auto TransformArg(ArgMapper arg_mapper) && {
    using ArgMapperFuncType = typename function_traits<ArgMapper>::type;

    if constexpr (function_traits<FuncType>::arity == 0) {
      static_assert(
          std::is_same_v<typename function_traits<ArgMapperFuncType>::result_type, void>,
          "ArgMapper's return value must == void because func_ takes no argument");

      return Postable<rebind_result_t<void, ArgMapperFuncType>>(
          // Use mutable to allow func and arg_mapper to have mutable captures.
          [func = std::move(func_),
           arg_mapper = std::move(arg_mapper)](auto &&...args) mutable -> void {
            arg_mapper(std::forward<decltype(args)>(args)...);  // returns void
            return func();
          },
          io_context_);

    } else if constexpr (function_traits<FuncType>::arity == 1) {
      static_assert(
          std::is_same_v<typename function_traits<ArgMapperFuncType>::result_type,
                         typename function_traits<FuncType>::arg1_type>,
          "ArgMapper's return value must == func_'s argument");

      return Postable<rebind_result_t<void, ArgMapperFuncType>>(
          // Use mutable to allow func and arg_mapper to have mutable captures.
          [func = std::move(func_),
           arg_mapper = std::move(arg_mapper)](auto &&...args) mutable -> void {
            return func(arg_mapper(std::forward<decltype(args)>(args)...));
          },
          io_context_);
    } else {
      static_assert(
          function_traits<FuncType>::arity <= 1,
          "TransformArg requires `this` taking exactly one argument or no argument");
    }
  }

  // Rebind the function.
  // `func_converter`: func_ -> NewFuncType
  // The result is ToPostable<NewFuncType>
  //
  // Changed func_converter to be a template parameter to accept lambdas.
  template <typename FuncConverter>
  auto Rebind(FuncConverter func_converter) && {  //  -> Postable<NewFuncType>
    using NewFuncType =
        typename function_traits<decltype(func_converter(std::move(func_)))>::type;
    return Postable<NewFuncType>(func_converter(std::move(func_)), io_context_);
  }

  instrumented_io_context &io_context() const { return io_context_; }

  std::function<FuncType> func_;
  instrumented_io_context &io_context_;
};

}  // namespace ray
