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

// Printer for types which supports stream operator.

#pragma once

#include <ostream>
#include <type_traits>

#include "absl/strings/str_format.h"
#include "ray/util/strings/ostream_printer.h"

namespace ray {

template <typename ValuePrinter = OstreamPrinter>
struct StdPrinter {
 public:
  ValuePrinter printer{};

  // Printer for `std::byte`.
  void operator()(std::ostream &os, const std::byte &value) const {
    os << absl::StreamFormat("b[%#04x]", static_cast<unsigned int>(value));
  }
};

}  // namespace ray
