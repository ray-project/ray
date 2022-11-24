// Copyright 2017 The Ray Authors.
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

#include <cstdint>
#include <sstream>
#include <string>
#include <thread>

#include "absl/strings/escaping.h"
#include "ray/util/logging.h"

template <typename T>
T ConvertValue(const std::string &type_string, const std::string &value) {
  std::istringstream stream(value);
  T parsed_value;
  stream >> parsed_value;
  RAY_CHECK(!value.empty() && stream.eof())
      << "Cannot parse \"" << value << "\" to " << type_string;
  return parsed_value;
}

template <>
inline std::string ConvertValue<std::string>(const std::string &type_string,
                                             const std::string &value) {
  return value;
}

template <>
inline bool ConvertValue<bool>(const std::string &type_string, const std::string &value) {
  auto new_value = absl::AsciiStrToLower(value);
  return new_value == "true" || new_value == "1";
}

class RayConfig {
/// -----------Include ray_config_def.h to define config items.----------------
/// A helper macro that defines a config item.
/// In particular, this generates a private field called `name_` and a public getter
/// method called `name()` for a given config item.
///
/// Configs defined in this way can be overridden by setting the env variable
/// RAY_{name}=value where {name} is the variable name.
///
/// \param type Type of the config item.
/// \param name Name of the config item.
/// \param default_value Default value of the config item.
#define RAY_CONFIG(type, name, default_value)                       \
 private:                                                           \
  type name##_ = ReadEnv<type>("RAY_" #name, #type, default_value); \
                                                                    \
 public:                                                            \
  inline type &name() { return name##_; }

#include "ray/common/ray_config_def.h"

/// -----------Include ray_internal_flag_def.h to define internal flags-------
/// RAY_INTERNAL_FLAG defines RayConfig fields similar to the RAY_CONFIG macro.
/// The difference is that RAY_INTERNAL_FLAG is intended for Ray internal
/// settings that users should not modify.
#define RAY_INTERNAL_FLAG RAY_CONFIG

#include "ray/common/ray_internal_flag_def.h"

#undef RAY_INTERNAL_FLAG
#undef RAY_CONFIG

 public:
  static RayConfig &instance();

  void initialize(const std::string &config_list);

 private:
  template <typename T>
  T ReadEnv(const std::string &name, const std::string &type_string, T default_value) {
    auto value = std::getenv(name.c_str());
    if (value == nullptr) {
      return default_value;
    } else {
      return ConvertValue<T>(type_string, value);
    }
  }
};
