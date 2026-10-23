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

/// Placeholder substituted for every redacted value. Keys are left intact so that
/// operators can still see which variables are set. Keep in sync with
/// `REDACTED_PLACEHOLDER` in python/ray/_common/runtime_env_redaction.py.
inline constexpr std::string_view kRedactedPlaceholder = "<redacted>";

/// Return `serialized_runtime_env` (a JSON string) with secret values masked.
///
/// Mirrors `redact_serialized_runtime_env` in
/// python/ray/_common/runtime_env_redaction.py: every value under `env_vars` is
/// replaced with `kRedactedPlaceholder`. Also works for a serialized
/// `RuntimeEnvContext`, which keeps `env_vars` at the top level too.
///
/// Fails closed: anything that can't be parsed into a JSON object is replaced
/// wholesale, since we can't rule out that it holds secrets. An empty string, or
/// one with no secret values to mask, is returned as-is.
std::string RedactSerializedRuntimeEnv(const std::string &serialized_runtime_env);

}  // namespace ray
