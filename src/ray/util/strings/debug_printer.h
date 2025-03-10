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

#include <ostream>
#include <sstream>
#include <string>

#include "ray/util/strings/absl_stringify_printer.h"
#include "ray/util/strings/container_printer.h"
#include "ray/util/strings/debug_string_printer.h"
#include "ray/util/strings/enum_printer.h"
#include "ray/util/strings/ostream_printer.h"
#include "ray/util/strings/std_printer.h"
#include "ray/util/strings/type_printer.h"
#include "ray/util/visitor.h"

namespace ray {

// The ordered visitor of printers to use for printing debug strings.
namespace internal {
using BaseDebugStringPrinters = OrderedVisitor<StdPrinter<>,
                                               OstreamPrinter,
                                               EnumPrinter,
                                               ContainerPrinter<>,
                                               AbslStringifyPrinter,
                                               DebugStringPrinter,
                                               TypePrinter>;
}  // namespace internal
using DebugStringPrinters =
    OrderedVisitor<StdPrinter<internal::BaseDebugStringPrinters>,
                   OstreamPrinter,
                   EnumPrinter,
                   ContainerPrinter<internal::BaseDebugStringPrinters>,
                   AbslStringifyPrinter,
                   DebugStringPrinter,
                   TypePrinter>;

// TODO(hjiang): Performance-wise, we should return a proxy type which is ostream-able.
template <typename T>
std::string DebugString(const T &value) {
  std::stringstream ss;
  DebugStringPrinters{}(ss, value);
  return ss.str();
}

}  // namespace ray
