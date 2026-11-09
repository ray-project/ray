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

#include "ray/rpc/authentication/authentication_mode.h"

#include <stdexcept>
#include <string>

#include "absl/strings/ascii.h"
#include "ray/common/ray_config.h"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {

AuthenticationMode GetAuthenticationMode() {
  const std::string auth_mode_lower =
      absl::AsciiStrToLower(RayConfig::instance().AUTH_MODE());
  if (auth_mode_lower == "token") {
    return AuthenticationMode::TOKEN;
  }
  if (auth_mode_lower != "disabled") {
    // A config error, not a Ray bug, so not RAY_CHECK.
    RAY_LOG(FATAL) << InvalidAuthenticationModeError();
  }
  return AuthenticationMode::DISABLED;
}

std::string InvalidAuthenticationModeError() {
  const std::string &auth_mode = RayConfig::instance().AUTH_MODE();
  const std::string auth_mode_lower = absl::AsciiStrToLower(auth_mode);
  if (auth_mode_lower == "token" || auth_mode_lower == "disabled") {
    return "";
  }
  return "Invalid RAY_AUTH_MODE '" + auth_mode + "': set it to 'token' or 'disabled'.";
}

bool IsK8sTokenAuthEnabled() { return RayConfig::instance().ENABLE_K8S_TOKEN_AUTH(); }

}  // namespace rpc
}  // namespace ray
