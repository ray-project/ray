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

// Printer for containers.

#pragma once

#include <optional>
#include <ostream>
#include <tuple>
#include <type_traits>
#include <variant>

#include "ray/util/strings/ostream_printer.h"
#include "ray/util/type_traits.h"
#include "ray/util/visitor.h"

namespace ray {

// Forward declare.
template <typename ValuePrinter>
struct ContainerPrinter;

namespace internal {

template <typename ValuePrinter = OstreamPrinter>
struct VariantPrinter {
 public:
  std::ostream &os;

  void operator()(const std::monostate &) const { os << "(monostate)"; }

  template <typename T>
  void operator()(const T &value) const;

 private:
  using OrderedInternalVisitor =
      OrderedVisitor<ValuePrinter, ContainerPrinter<ValuePrinter>>;
};

}  // namespace internal

// A printer for containers.
template <typename ValuePrinter = OstreamPrinter>
struct ContainerPrinter {
  ValuePrinter printer{};

  // Container specialization.
  template <typename C, std::enable_if_t<is_container_v<C>, int> = 0>
  void operator()(std::ostream &os, const C &container) const {
    if constexpr (is_map_v<C>) {
      PrintMap(os, container);
    } else {
      PrintArray(os, container);
    }
  }

  // Variant specialization.
  template <typename... Args>
  void operator()(std::ostream &os, const std::variant<Args...> &value) const {
    std::visit(internal::VariantPrinter<ValuePrinter>{.os = os}, value);
  }

  // Tuple specialization.
  template <typename C, std::enable_if_t<is_tuple_v<C>, int> = 0>
  void operator()(std::ostream &os, const C &tpl) const {
    os << "[";
    PrintTupleImpl(os, tpl);
    os << "]";
  }

  // Pair specialization.
  template <typename P, std::enable_if_t<is_pair_v<P>, int> = 0>
  void operator()(std::ostream &os, const P &p) const {
    os << "{";
    PrintArg(os, p.first);
    os << ", ";
    PrintArg(os, p.second);
    os << "}";
  }

  // Optional specialization.
  template <typename T>
  void operator()(std::ostream &os, const std::optional<T> &opt) const {
    if (!opt.has_value()) {
      os << "(nullopt)";
      return;
    }
    PrintArg(os, *opt);
  }

 private:
  using OrderedArgVisitor = OrderedVisitor<ValuePrinter, ContainerPrinter<ValuePrinter>>;

  template <typename C>
  void PrintMap(std::ostream &os, const C &container) const {
    static_assert(is_map_v<C>);
    os << "[";
    int key_value_idx = 0;
    const int key_value_cnt = container.size();
    for (const auto &[key, val] : container) {
      os << "{";
      PrintArg(os, key);
      os << ", ";
      PrintArg(os, val);
      os << "}";

      // If not the last key-value pair.
      if (++key_value_idx < key_value_cnt) {
        os << ", ";
      }
    }
    os << "]";
  }

  // Print containers which are with an array-like indexable accessor.
  template <typename C>
  void PrintArray(std::ostream &os, const C &container) const {
    static_assert(is_container_v<C>);
    os << "[";
    for (size_t idx = 0; idx < container.size(); ++idx) {
      PrintArg(os, container[idx]);
      if (idx != container.size() - 1) {
        os << ", ";
      }
    }
    os << "]";
  }

  template <std::size_t Index = 0, typename... Ts>
  void PrintTupleImpl(std::ostream &os, const std::tuple<Ts...> &tpl) const {
    if constexpr (Index < sizeof...(Ts)) {
      PrintArg(os, std::get<Index>(tpl));
      if constexpr (Index + 1 < sizeof...(Ts)) {
        os << ", ";
      }
      PrintTupleImpl<Index + 1>(os, tpl);
    }
  }

  template <typename Arg>
  void PrintArg(std::ostream &os, const Arg &arg) const {
    OrderedArgVisitor{}(os, arg);
  }
};

namespace internal {

template <typename ValuePrinter>
template <typename T>
void VariantPrinter<ValuePrinter>::operator()(const T &value) const {
  OrderedInternalVisitor{}(os, value);
}

}  // namespace internal

}  // namespace ray
