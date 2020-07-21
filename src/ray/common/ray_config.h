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

#include "ray/util/logging.h"

class RayConfig {
/// -----------Include ray_config_def.h to define config items.----------------
/// A helper macro that defines a config item.
/// In particular, this generates a private field called `name_` and a public getter
/// method called `name()` for a given config item.
///
/// \param type Type of the config item.
/// \param name Name of the config item.
/// \param default_value Default value of the config item.
#define RAY_CONFIG(type, name, default_value) \
 private:                                     \
  type name##_ = default_value;               \
                                              \
 public:                                      \
  inline type name() { return name##_; }

#include "ray/common/ray_config_def.h"
/// -------------------------------------------------------------------------
#undef RAY_CONFIG

 public:
  static RayConfig &instance() {
    static RayConfig config;
    return config;
  }

// clang-format off
/// -----------Include ray_config_def.h to set config items.-------------------
/// A helper macro that helps to set a value to a config item.
#define RAY_CONFIG(type, name, default_value) \
  if (pair.first == #name) {                  \
    if (typeid(type) == typeid(bool)) {       \
       std::string value = pair.second;       \
       std::transform(value.begin(),          \
                      value.end(),            \
                      value.begin(),          \
                      ::tolower);             \
       name##_ = value == "true" ||           \
                 value == "1";                \
    } else {                                  \
      std::istringstream stream(pair.second); \
      stream >> name##_;                      \
    }                                         \
    continue;                                 \
  }

  void initialize(const std::unordered_map<std::string, std::string> &config_map) {
    for (auto const &pair : config_map) {
      // We use a big chain of if else statements because C++ doesn't allow
      // switch statements on strings.
#include "ray/common/ray_config_def.h"
      RAY_LOG(FATAL) << "Received unexpected config parameter " << pair.first;
    }
  }
/// ---------------------------------------------------------------------
#undef RAY_CONFIG
};
// clang-format on
