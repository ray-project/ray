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

namespace ray {
namespace rpc {

AuthenticationMode GetAuthenticationMode() {
  std::string auth_mode_lower = absl::AsciiStrToLower(RayConfig::instance().auth_mode());

  if (auth_mode_lower == "token") {
    return AuthenticationMode::TOKEN;
  } else {
    return AuthenticationMode::DISABLED;
  }
}

}  // namespace rpc
}  // namespace ray
