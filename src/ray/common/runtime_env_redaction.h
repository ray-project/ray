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

#pragma once

#include <string>
#include <string_view>

namespace ray {

/**
 * @brief Placeholder substituted for every redacted value.
 *
 * Keys are left intact so that operators can still see which variables are set.
 * Keep in sync with `REDACTED_PLACEHOLDER` in
 * python/ray/_private/runtime_env/redaction.py.
 */
inline constexpr std::string_view kRedactedPlaceholder = "<redacted>";

/**
 * @brief Masks the secret values in a serialized runtime env.
 *
 * Mirrors `redact_serialized_runtime_env` in
 * python/ray/_private/runtime_env/redaction.py: every value under `env_vars` is
 * replaced with `kRedactedPlaceholder`, and the keys are kept. Also works for a
 * serialized `RuntimeEnvContext`, which keeps `env_vars` at the top level too.
 *
 * Fails closed: anything that can't be parsed into a JSON object is replaced
 * wholesale, since we can't rule out that it holds secrets. Malformed input,
 * including invalid UTF-8, never throws.
 *
 * @param serialized_runtime_env A JSON-serialized runtime env or runtime env
 *        context. Can be empty.
 * @return The input unchanged if it's empty or has no secret values to mask,
 *         `kRedactedPlaceholder` if it isn't a JSON object, and otherwise the
 *         re-serialized JSON with every `env_vars` value masked.
 */
std::string RedactSerializedRuntimeEnv(const std::string &serialized_runtime_env);

}  // namespace ray
