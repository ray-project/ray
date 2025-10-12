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

#include <charconv>
#include <string>
#include <vector>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "ray/common/status_or.h"

namespace ray {

// Transfer the string to the Hex format. It can be more readable than the ANSI mode
std::string StringToHex(const std::string &str);

/// Uses sscanf() to read a token matching from the string, advancing the iterator.
/// \param c_str A string iterator that is dereferenceable. (i.e.: c_str < string::end())
/// \param format The pattern. It must not produce any output. (e.g., use %*d, not %d.)
/// \return The scanned prefix of the string, if any.
std::string ScanToken(std::string::const_iterator &c_str, std::string format);

template <typename T>
std::string GetDebugString(const T &element,
                           std::string (*debug_string_func)(const T &)) {
  return debug_string_func(element);
}

template <typename T>
std::string GetDebugString(const T &element,
                           const std::string (T::*debug_string_func)() const) {
  return (element.*debug_string_func)();
}

template <typename T, typename F>
inline std::string VectorToString(const std::vector<T> &vec, const F &debug_string_func) {
  std::string result = "[";
  bool first = true;
  for (const auto &element : vec) {
    if (!first) {
      absl::StrAppend(&result, ", ");
    }
    absl::StrAppend(&result, GetDebugString(element, debug_string_func));
    first = false;
  }
  absl::StrAppend(&result, "]");
  return result;
}

/**
  Usage:
    StatusOr<int64_t> parsed_int = StringToInt<int64_t>("12345");
    if (!parsed_int.ok()) {
      // handle the error
    }
    // Otherwise safe to use.
    DoHardMath(*parsed_int)

  @tparam IntType any signed or unsigned integer type.
  @param str the string to convert to an integer type.

  @return OK if the conversion was successful,
  @return InvalidArgument if the string contains non-integer characters or if the
  integer overflows based on the type.
*/
template <typename IntType>
StatusOr<IntType> StringToInt(const std::string &input) noexcept {
  IntType value;
  std::from_chars_result ret =
      std::from_chars(input.data(), input.data() + input.size(), value);
  if (ret.ec == std::errc::invalid_argument || ret.ptr != input.data() + input.size()) {
    return Status::InvalidArgument(
        absl::StrFormat("Failed to convert %s to an integer type because the input "
                        "contains invalid characters.",
                        input));
  }
  if (ret.ec == std::errc::result_out_of_range) {
    // There isn't a straightforward and portable way to print out the unmangled type
    // information.
    return Status::InvalidArgument(
        absl::StrFormat("Failed to convert %s into the integer "
                        "type. The result is too large to fit into the type provided.",
                        input));
  }
  return StatusOr<IntType>(value);
}

// Prepend the prefix to each line of str.
std::string PrependToEachLine(const std::string &str, const std::string &prefix);

}  // namespace ray
