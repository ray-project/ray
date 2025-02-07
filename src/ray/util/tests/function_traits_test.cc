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

#include "ray/util/function_traits.h"

#include <gtest/gtest.h>

#include <functional>
#include <string>
#include <type_traits>

// Tests for function_traits.h
// All tests are compile time tests with static_assert, because they are intended to be
// used in compile time code.
namespace ray {

inline double SimpleFunction(int i) { return 2.0 * i; }

struct Foo {
  inline double member(int i) { return i * 1.0; }
  inline double const_member(int i) const { return i * 1.0; }
  inline double move_member(int i) && { return i * 1.0; }
};

// 1) function_traits on decltype(SimpleFunction)
static_assert(
    std::is_same_v<function_traits<decltype(SimpleFunction)>::result_type, double>,
    "SimpleFunction should return double");
static_assert(function_traits<decltype(SimpleFunction)>::arity == 1,
              "SimpleFunction should have 1 parameter");
static_assert(std::is_same_v<function_traits<decltype(SimpleFunction)>::arg1_type, int>,
              "SimpleFunction's param should be int");

// 2) function_traits on decltype(&SimpleFunction)
static_assert(
    std::is_same_v<function_traits<decltype(&SimpleFunction)>::result_type, double>,
    "&SimpleFunction should return double");
static_assert(function_traits<decltype(&SimpleFunction)>::arity == 1,
              "&SimpleFunction should have 1 parameter");
static_assert(std::is_same_v<function_traits<decltype(&SimpleFunction)>::arg1_type, int>,
              "&SimpleFunction's param should be int");

// 3) function_traits on decltype(&Foo::member)
static_assert(
    std::is_same_v<function_traits<decltype(&Foo::member)>::result_type, double>,
    "Foo::member should return double");
static_assert(function_traits<decltype(&Foo::member)>::arity == 1,
              "Foo::member should have 1 parameter");
static_assert(std::is_same_v<function_traits<decltype(&Foo::member)>::arg1_type, int>,
              "Foo::member's param should be int");

// 4) function_traits on decltype(&Foo::const_member)
static_assert(
    std::is_same_v<function_traits<decltype(&Foo::const_member)>::result_type, double>,
    "Foo::const_member should return double");
static_assert(function_traits<decltype(&Foo::const_member)>::arity == 1,
              "Foo::const_member should have 1 parameter");
static_assert(
    std::is_same_v<function_traits<decltype(&Foo::const_member)>::arg1_type, int>,
    "Foo::const_member's param should be int");

// 5) function_traits on decltype(&Foo::move_member)
static_assert(
    std::is_same_v<function_traits<decltype(&Foo::move_member)>::result_type, double>,
    "Foo::move_member should return double");
static_assert(function_traits<decltype(&Foo::move_member)>::arity == 1,
              "Foo::move_member should have 1 parameter");
static_assert(
    std::is_same_v<function_traits<decltype(&Foo::move_member)>::arg1_type, int>,
    "Foo::move_member's param should be int");

// 6) function_traits on std::function<double(int)>
static_assert(
    std::is_same_v<function_traits<std::function<double(int)>>::result_type, double>,
    "std::function<double(int)> should return double");
static_assert(function_traits<std::function<double(int)>>::arity == 1,
              "std::function<double(int)> should have 1 parameter");
static_assert(std::is_same_v<function_traits<std::function<double(int)>>::arg1_type, int>,
              "std::function<double(int)>'s param should be int");

// 7) function_traits on std::function<double(int)>&
static_assert(
    std::is_same_v<function_traits<std::function<double(int)> &>::result_type, double>,
    "std::function<double(int)>& should return double");
static_assert(function_traits<std::function<double(int)> &>::arity == 1,
              "std::function<double(int)>& should have 1 parameter");
static_assert(
    std::is_same_v<function_traits<std::function<double(int)> &>::arg1_type, int>,
    "std::function<double(int)>&'s param should be int");

// 8) Plain lambda
static_assert(
    [] {
      auto plain_lambda = [](int i) { return 2.0 * i; };
      using traits = function_traits<decltype(plain_lambda)>;
      return std::is_same_v<traits::result_type, double> && (traits::arity == 1) &&
             std::is_same_v<traits::arg1_type, int>;
    }(),
    "Plain lambda should have signature double(int)");

// 9) Capturing lambda (by copy)
static_assert(
    [] {
      const double c = 2.0;
      auto capturing_lambda = [c](int i) { return c * i; };
      using traits = function_traits<decltype(capturing_lambda)>;
      return std::is_same_v<traits::result_type, double> && (traits::arity == 1) &&
             std::is_same_v<traits::arg1_type, int>;
    }(),
    "Capturing lambda should have signature double(int)");

// 10) Move-only capturing lambda

struct MoveOnly {
  int value;
  // A constexpr constructor makes this usable in a constant expression.
  constexpr explicit MoveOnly(int v) : value(v) {}
  ~MoveOnly() = default;
  MoveOnly(const MoveOnly &) = delete;
  MoveOnly(MoveOnly &&) = default;
  MoveOnly &operator=(const MoveOnly &) = delete;
  MoveOnly &operator=(MoveOnly &&) = default;
};

// Demonstrate a lambda capturing this move-only object at compile time.
static_assert(
    [] {
      // The lambda is marked constexpr, so it can be used in a constant expression.
      constexpr auto capturing_lambda = [m = MoveOnly(2)](int i) { return m.value * i; };
      using Traits = function_traits<decltype(capturing_lambda)>;
      // Return a bool for immediate invocation in the static_assert.
      return std::is_same_v<typename Traits::result_type, int> && (Traits::arity == 1) &&
             std::is_same_v<typename Traits::arg1_type, int>;
    }(),
    "Capturing a constexpr move-only object should yield a lambda with signature "
    "int(int)");

}  // namespace ray
