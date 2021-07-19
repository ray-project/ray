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
#include <string>

class RayConfig {
  // Needed for the RAY_CONFIG macro below to work properly for strings.
  typedef std::string string_type;
#define TOSTRING0(x) #x
#define TOSTRING(x) TOSTRING0(x)

/// -----------Include ray_config_def.h to define config items.----------------
/// A helper macro that defines a config item.
/// In particular, this generates a private field called `name_` and a public getter
/// method called `name()` for a given config item.
///
/// Configs defined in this way can be overriden by setting the env variable
/// RAY_{name}=value where {name} is the variable name.
///
/// \param type Type of the config item.
/// \param name Name of the config item.
/// \param default_value Default value of the config item.
#define RAY_CONFIG(type, name, default_value)                      \
 private:                                                          \
  type name##_ = env_##type("RAY_" TOSTRING(name), default_value); \
                                                                   \
 public:                                                           \
  inline type name() { return name##_; }

#include "ray/common/ray_config_def.h"
/// -------------------------------------------------------------------------
#undef RAY_CONFIG

 public:
  static RayConfig &instance();

  void initialize(const std::string &config_list);

 private:
  bool env_bool(const std::string &name, bool default_value) {
    auto value = getenv(name.c_str());
    if (value == nullptr) {
      return default_value;
    } else {
      return value != std::string("0");
    }
  }

  float env_float(const std::string &name, float default_value) {
    auto value = getenv(name.c_str());
    if (value == nullptr) {
      return default_value;
    } else {
      return std::stof(value);
    }
  }

  double env_double(const std::string &name, double default_value) {
    auto value = getenv(name.c_str());
    if (value == nullptr) {
      return default_value;
    } else {
      return std::stod(value);
    }
  }

  int env_int(const std::string &name, int default_value) {
    auto value = getenv(name.c_str());
    if (value == nullptr) {
      return default_value;
    } else {
      return std::stoi(value);
    }
  }

  std::string env_string_type(const std::string &name, std::string default_value) {
    auto value = getenv(name.c_str());
    if (value == nullptr) {
      return default_value;
    } else {
      return value;
    }
  }

  size_t env_size_t(const std::string &name, size_t default_value) {
    auto value = getenv(name.c_str());
    if (value == nullptr) {
      return default_value;
    } else {
      return std::stoull(value);
    }
  }

  int64_t env_int64_t(const std::string &name, int64_t default_value) {
    auto value = getenv(name.c_str());
    if (value == nullptr) {
      return default_value;
    } else {
      return std::stoll(value);
    }
  }

  uint64_t env_uint64_t(const std::string &name, uint64_t default_value) {
    auto value = getenv(name.c_str());
    if (value == nullptr) {
      return default_value;
    } else {
      return std::stoull(value);
    }
  }

  int32_t env_int32_t(const std::string &name, int32_t default_value) {
    auto value = getenv(name.c_str());
    if (value == nullptr) {
      return default_value;
    } else {
      return std::stol(value);
    }
  }

  uint32_t env_uint32_t(const std::string &name, uint32_t default_value) {
    auto value = getenv(name.c_str());
    if (value == nullptr) {
      return default_value;
    } else {
      return std::stoul(value);
    }
  }
};
