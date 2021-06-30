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

#include <algorithm>
#include <sstream>
#include <typeinfo>
#include <unordered_map>

#include "absl/strings/escaping.h"
#include "ray/util/logging.h"

class RayConfig {
/// -----------Include ray_config_def.h to define config items.----------------
/// A helper macro that defines a config item.
/// In particular, this generates a private field called `name_` and a public getter
/// method called `name()` for a given config item.
///
/// Configs defined in this way can be overriden by setting the env variable
/// RAY_{name}=value where {name} is the capitalized variable name.
///
/// \param type Type of the config item.
/// \param name Name of the config item.
/// \param default_value Default value of the config item.
#define RAY_CONFIG(type, name, default_value)                                         \
 private:                                                                             \
  type name##_ = ReadEnv<type>("RAY_" + absl::AsciiStrToUpper(#name), default_value); \
                                                                                      \
 public:                                                                              \
  inline type name() { return name##_; }

#include "ray/common/ray_config_def.h"
/// -------------------------------------------------------------------------
#undef RAY_CONFIG

 public:
  static RayConfig &instance() {
    static RayConfig config;
    return config;
  }

/// -----------Include ray_config_def.h to set config items.-------------------
/// A helper macro that helps to set a value to a config item.
#define RAY_CONFIG(type, name, default_value)                                           \
  if (pair.first == #name) {                                                            \
    if (typeid(type) == typeid(std::string)) {                                          \
      RAY_CHECK(                                                                        \
          absl::Base64Unescape(pair.second, reinterpret_cast<std::string *>(&name##_))) \
          << "key: " << #name << ", value: " << pair.second;                            \
    } else {                                                                            \
      name##_ = ConvertValue<type>(pair.second);                                        \
    }                                                                                   \
    continue;                                                                           \
  }

  void initialize(const std::string &config_list) {
    // Parse the configuration list.
    std::unordered_map<std::string, std::string> config_map;
    std::istringstream config_string(config_list);
    std::string config_name;
    std::string config_value;

    while (std::getline(config_string, config_name, ',')) {
      RAY_CHECK(std::getline(config_string, config_value, ';'));
      // TODO(rkn): The line below could throw an exception. What should we do about this?
      config_map[config_name] = config_value;
    }

    for (auto const &pair : config_map) {
      // We use a big chain of if else statements because C++ doesn't allow
      // switch statements on strings.
#include "ray/common/ray_config_def.h"
      RAY_LOG(FATAL) << "Received unexpected config parameter " << pair.first;
    }
  }
  /// ---------------------------------------------------------------------
#undef RAY_CONFIG

 private:
  template <typename T>
  T ConvertValue(const std::string &value) {
    std::istringstream stream(value);
    T parsed_value;
    stream >> parsed_value;
    RAY_CHECK(stream.eof()) << "Cannot parse \"" << value << "\" to a "
                            << typeid(T).name() << " value.";
    return parsed_value;
  }

  template <>
  std::string ConvertValue<std::string>(const std::string &value) {
    return value;
  }

  template <>
  bool ConvertValue<bool>(const std::string &value) {
    auto new_value = absl::AsciiStrToLower(value);
    return new_value == "true" || new_value == "1";
  }

  template <typename T>
  T ReadEnv(const std::string &name, T default_value) {
    auto value = getenv(name.c_str());
    if (value == nullptr) {
      return default_value;
    } else {
      return ConvertValue<T>(value);
    }
  }
};
