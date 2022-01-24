// Copyright 2020-2021 The Ray Authors.
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

#include <string>
#include <string_view>

namespace ray {

template <typename R>
struct PyFunction {
  bool is_python() { return true; }
  R operator()() { return {}; }

  std::string module_name;
  std::string function_name;
};

struct PyActorClass {
  bool is_python() { return true; }
  void operator()() {}

  std::string module_name;
  std::string class_name;
  std::string function_name = "__init__";
};

template <typename R>
struct PyActorMethod {
  bool is_python() { return true; }
  R operator()() { return {}; }

  std::string function_name;
};

namespace internal {

enum class LangType {
  CPP,
  PYTHON,
  JAVA,
};

inline constexpr size_t XLANG_HEADER_LEN = 9;
inline constexpr std::string_view METADATA_STR_DUMMY = "__RAY_DUMMY__";
inline constexpr std::string_view METADATA_STR_RAW = "RAW";
inline constexpr std::string_view METADATA_STR_XLANG = "XLANG";

}  // namespace internal

}  // namespace ray