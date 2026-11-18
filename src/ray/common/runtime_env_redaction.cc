// Copyright 2026 The Ray Authors.
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

#include "ray/common/runtime_env_redaction.h"

#include <array>
#include <string>

#include "nlohmann/json.hpp"

namespace ray {

namespace {

// `runtime_env` fields whose values are treated as secrets.
constexpr std::array<const char *, 1> kSecretRuntimeEnvFields = {"env_vars"};

}  // namespace

std::string RedactSerializedRuntimeEnv(const std::string &serialized_runtime_env) {
  if (serialized_runtime_env.empty()) {
    return serialized_runtime_env;
  }

  auto runtime_env = nlohmann::json::parse(serialized_runtime_env,
                                           /*cb=*/nullptr,
                                           /*allow_exceptions=*/false);
  if (runtime_env.is_discarded() || !runtime_env.is_object()) {
    return std::string(kRedactedPlaceholder);
  }

  bool masked = false;
  for (const char *field : kSecretRuntimeEnvFields) {
    auto it = runtime_env.find(field);
    if (it != runtime_env.end() && it->is_object() && !it->empty()) {
      for (auto &value : *it) {
        value = std::string(kRedactedPlaceholder);
      }
      masked = true;
    }
  }
  if (!masked) {
    // Nothing to mask, so keep the original string instead of re-serializing it.
    return serialized_runtime_env;
  }
  return runtime_env.dump(
      /*indent=*/-1,
      /*indent_char=*/' ',
      /*ensure_ascii=*/false,
      nlohmann::json::error_handler_t::replace);
}

}  // namespace ray
