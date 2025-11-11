// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions
// and limitations under the License.

#pragma once

#include <optional>
#include <unordered_map>

#include "ray/rpc/authentication/authentication_token.h"

namespace ray {
namespace rpc {

class AuthenticationTokenValidator {
 public:
  static AuthenticationTokenValidator &instance();
  /// Validate the provided authentication token against the expected token.
  /// When auth_mode=token, this is a simple equality check.
  /// When auth_mode=k8s, provided_token is validated against Kubernetes API.
  /// \param expected_token The expected token (optional).
  /// \param provided_token The token to validate.
  /// \return true if the tokens are equal, false otherwise.
  bool ValidateToken(const std::optional<AuthenticationToken> &expected_token,
                     const AuthenticationToken &provided_token);

 private:
  // Cache for K8s tokens.
  struct K8sCacheEntry {
    bool allowed;
    std::chrono::steady_clock::time_point expiration;
  };
  std::mutex k8s_token_cache_mutex_;
  std::unordered_map<AuthenticationToken, K8sCacheEntry, AuthenticationTokenHash>
      k8s_token_cache_;
};

}  // namespace rpc
}  // namespace ray
