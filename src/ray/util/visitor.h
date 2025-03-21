// Copyright 2025 The Ray Authors.
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
#include <utility>

#include "absl/meta/type_traits.h"

namespace ray {

// Ranked overload tag to make visitor and fallback.
// Reference: https://abseil.io/tips/229
struct general {};
struct specialize : general {};

struct VisitImpl {
  template <typename V, typename F, typename... Args>
  std::invoke_result_t<V, Args...> operator()(V &&visitor,
                                              F &&,
                                              specialize,
                                              Args &&...args) const {
    return visitor(std::forward<Args>(args)...);
  }

  template <typename V, typename F, typename... Args>
  std::invoke_result_t<F, Args...> operator()(V &&,
                                              F &&fallback,
                                              general,
                                              Args &&...args) const {
    return fallback(std::forward<Args>(args)...);
  }
};

template <typename V, typename... Args>
struct visit_result {
  using type = decltype(std::declval<V>()(std::declval<Args>()...));
};

template <typename V, typename F, typename... Args>
using access_visitor_result_t =
    typename visit_result<VisitImpl, V, F, specialize, Args...>::type;

template <typename V, typename F, typename... Args>
access_visitor_result_t<V, F, Args...> access_visitor(V &&visitor,
                                                      F &&fallback,
                                                      Args &&...args) {
  return VisitImpl{}(std::forward<V>(visitor),
                     std::forward<F>(fallback),
                     specialize{},
                     std::forward<Args>(args)...);
}

template <typename V, typename F, typename... Args>
using visitor_invoke_result_t =
    std::invoke_result_t<VisitImpl, V, F, specialize, Args...>;

// A visitor that forwards calls to the first applicable visitor in the given visitors.
// @param VT: Ordered types of visitors to use.
template <typename... VT>
class OrderedVisitor;

// Single visitor case.
template <typename V>
class OrderedVisitor<V> {
 public:
  OrderedVisitor() = default;

  // Disable copy and move.
  OrderedVisitor(const OrderedVisitor &) = delete;
  OrderedVisitor &operator=(const OrderedVisitor &) = delete;

  explicit OrderedVisitor(const V &visitor) : visitor_(visitor) {}
  explicit OrderedVisitor(V &&visitor) : visitor_(std::move(visitor)) {}

  // Non-const version.
  template <typename... Args>
  std::invoke_result_t<V &, Args &&...> operator()(Args &&...args) {
    return visitor_(std::forward<Args>(args)...);
  }

  // Const version.
  template <typename... Args>
  std::invoke_result_t<const V &, Args &&...> operator()(Args &&...args) const {
    return visitor_(std::forward<Args>(args)...);
  }

 private:
  V visitor_;
};

// Multiple visitors, try first and fallback on left if needed.
template <typename First, typename... Left>
class OrderedVisitor<First, Left...> {
  using V = First;
  using F = OrderedVisitor<Left...>;

 public:
  OrderedVisitor() = default;

  // Disable copy and move.
  OrderedVisitor(const OrderedVisitor &) = delete;
  OrderedVisitor &operator=(const OrderedVisitor &) = delete;

  template <typename T, typename... Args>
  explicit OrderedVisitor(T &&visitor, Args &&...args)
      : visitor_(std::forward<T>(visitor)), fallback_(std::forward<Args>(args)...) {}

  // Non-const overload.
  template <typename... Args>
  visitor_invoke_result_t<V &, F &, Args...> operator()(Args &&...args) {
    return access_visitor(visitor_, fallback_, std::forward<Args>(args)...);
  }

  // Const overload.
  template <typename... Args>
  visitor_invoke_result_t<const V &, const F &, Args...> operator()(
      Args &&...args) const {
    return access_visitor(visitor_, fallback_, std::forward<Args>(args)...);
  }

 private:
  V visitor_;
  F fallback_;
};

template <class... Ts>
OrderedVisitor(Ts...) -> OrderedVisitor<absl::remove_cvref_t<Ts>...>;

}  // namespace ray
