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

#pragma once

#include <string>

namespace ray {
namespace rpc {

enum class AuthenticationMode {
  DISABLED,
  TOKEN,
  K8S,
};

/// Get the authentication mode from the RayConfig.
/// \return The authentication mode enum value. returns AuthenticationMode::DISABLED if
/// the authentication mode is not set or is invalid.
AuthenticationMode GetAuthenticationMode();

// Checks authentication mode and returns whether token authentication is required.
// \return bool
bool RequiresTokenAuthentication();

}  // namespace rpc
}  // namespace ray
