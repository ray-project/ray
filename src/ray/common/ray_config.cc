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

#include "ray/common/ray_config.h"

#include <sstream>
#include <typeinfo>

#include "nlohmann/json.hpp"

using json = nlohmann::json;

RayConfig &RayConfig::instance() {
  static RayConfig config;
  return config;
}

void RayConfig::initialize(const std::string &config_list) {
#define RAY_CONFIG(type, name, default_value) \
  name##_ = ReadEnv<type>("RAY_" #name, #type, default_value);

#include "ray/common/ray_config_def.h"
#undef RAY_CONFIG

  if (config_list.empty()) {
    return;
  }

  try {
    // Parse the configuration list.
    json config_map = json::parse(config_list);

/// -----------Include ray_config_def.h to set config items.-------------------
/// A helper macro that helps to set a value to a config item.
#define RAY_CONFIG(type, name, default_value) \
  if (pair.key() == #name) {                  \
    name##_ = pair.value().get<type>();       \
    continue;                                 \
  }

    for (const auto &pair : config_map.items()) {
      // We use a big chain of if else statements because C++ doesn't allow
      // switch statements on strings.
#include "ray/common/ray_config_def.h"
      // "ray/common/ray_internal_flag_def.h" is intentionally not included,
      // because it contains Ray internal settings.
      RAY_LOG(FATAL) << "Received unexpected config parameter " << pair.key();
    }

/// ---------------------------------------------------------------------
#undef RAY_CONFIG

    if (RAY_LOG_ENABLED(DEBUG)) {
      std::ostringstream oss;
      oss << "RayConfig is initialized with: ";
      for (auto const &pair : config_map.items()) {
        oss << pair.key() << "=" << pair.value() << ",";
      }
      RAY_LOG(DEBUG) << oss.str();
    }
  } catch (json::exception &ex) {
    RAY_LOG(FATAL) << "Failed to initialize RayConfig: " << ex.what()
                   << " The config string is: " << config_list;
  }
}
