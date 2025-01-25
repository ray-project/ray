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

#include <filesystem>
#include <string>
#include <vector>

#include "absl/strings/str_cat.h"

namespace ray {

// Transfer the string to the Hex format. It can be more readable than the ANSI mode
std::string StringToHex(const std::string &str);

/// Uses sscanf() to read a token matching from the string, advancing the iterator.
/// \param c_str A string iterator that is dereferenceable. (i.e.: c_str < string::end())
/// \param format The pattern. It must not produce any output. (e.g., use %*d, not %d.)
/// \return The scanned prefix of the string, if any.
std::string ScanToken(std::string::const_iterator &c_str, std::string format);

/// \return The result of joining multiple path components.
template <class... Paths>
std::string JoinPaths(std::string base, const Paths &...components) {
  auto join = [](auto &joined_path, const auto &component) {
    // if the components begin with "/" or "////", just get the path name.
    if (!component.empty() &&
        component.front() == std::filesystem::path::preferred_separator) {
      joined_path = std::filesystem::path(joined_path)
                        .append(std::filesystem::path(component).filename().string())
                        .string();
    } else {
      joined_path = std::filesystem::path(joined_path).append(component).string();
    }
  };
  (join(base, std::string_view(components)), ...);
  return base;
}

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

}  // namespace ray
