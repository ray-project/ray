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

// Human-readable type names.
// https://stackoverflow.com/questions/281818/unmangling-the-result-of-stdtype-infoname

#pragma once

#include <string_view>

namespace ray {

namespace internal {

template <typename T>
constexpr auto GetFullFunctionName() {
#if defined(__clang__) || defined(__GNUC__)
  return std::string_view{__PRETTY_FUNCTION__};
#elif defined(_MSC_VER)
  return std::string_view{__FUNCSIG__};
#else
#error Unsupported compiler
#endif
}

// Outside of the template so its computed once
struct TypeNameInfo {
  static constexpr auto sentinel_function = GetFullFunctionName<double>();
  static constexpr auto prefix_offset = sentinel_function.find("double");
  static constexpr auto suffix_offset =
      sentinel_function.size() - prefix_offset - /* strlen("double") */ 6;
};

}  // namespace internal

// TODO(hjiang): Use `consteval` after we upgrade to C++20.
// Reference: https://en.cppreference.com/w/cpp/language/consteval
template <typename T>
constexpr std::string_view GetTypeName() {
  constexpr auto function = internal::GetFullFunctionName<T>();
  constexpr auto start = internal::TypeNameInfo::prefix_offset;
  constexpr auto end = function.size() - internal::TypeNameInfo::suffix_offset;
  constexpr auto size = end - start;
  return function.substr(start, size);
}

}  // namespace ray
