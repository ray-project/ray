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

// Printer for variable type.

#pragma once

#include <boost/core/demangle.hpp>
#include <ostream>
#include <typeinfo>

#include "absl/strings/str_format.h"
#include "ray/util/strings/get_type_name.h"

namespace ray {

struct TypePrinter {
  template <typename T>
  void operator()(std::ostream &os, const T &value) const {
    os << absl::StreamFormat("<Type: %s, address: %p>", GetTypeName<T>(), &value);
  }

  void operator()(std::ostream &os, const std::type_info &value) const {
    // `std::type_info::name` returns mangled symbol name.
    os << boost::core::demangle(value.name());
  }
};

}  // namespace ray
