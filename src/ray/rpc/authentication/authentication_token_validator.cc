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

#include "ray/rpc/authentication/authentication_token_validator.h"

#include "ray/rpc/authentication/authentication_mode.h"
#include "ray/rpc/authentication/k8s_util.h"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {

const std::chrono::minutes kCacheTTL(5);

AuthenticationTokenValidator &AuthenticationTokenValidator::instance() {
  static AuthenticationTokenValidator instance;
  return instance;
}

bool AuthenticationTokenValidator::ValidateToken(
    const std::optional<AuthenticationToken> &expected_token,
    const AuthenticationToken &provided_token) {
  if (GetAuthenticationMode() == AuthenticationMode::TOKEN) {
    RAY_CHECK(expected_token.has_value() && !expected_token->empty())
        << "Ray token authentication is enabled but expected token is empty";

    return expected_token->Equals(provided_token);
  }

  if (GetAuthenticationMode() == AuthenticationMode::K8S) {
    std::call_once(k8s::k8s_client_config_flag, k8s::InitK8sClientConfig);
    if (!k8s::k8s_client_initialized) {
      RAY_LOG(WARNING) << "Kubernetes client not initialized, K8s authentication failed.";
      return false;
    }

    // Check cache first.
    {
      std::lock_guard<std::mutex> lock(k8s_token_cache_mutex_);
      auto it = k8s_token_cache_.find(provided_token);
      if (it != k8s_token_cache_.end()) {
        if (std::chrono::steady_clock::now() < it->second.expiration) {
          RAY_LOG(DEBUG) << "K8s token found in cache and is valid.";
          return it->second.allowed;
        } else {
          RAY_LOG(DEBUG) << "K8s token in cache expired, removing from cache.";
          k8s_token_cache_.erase(it);
        }
      }
    }

    bool is_allowed = false;
    is_allowed = k8s::ValidateToken(provided_token);

    // Only cache validated tokens for now. We don't want to invalidate a token
    // due to unrelated errors from Kubernetes API server. This has the downside of
    // causing more load if an unauthenticated client continues to make calls.
    // TODO(andrewsykim): cache invalid tokens once k8s::ValidateToken can distinguish
    // between invalid token errors and server errors.
    if (is_allowed) {
      std::lock_guard<std::mutex> lock(k8s_token_cache_mutex_);
      k8s_token_cache_[provided_token] = {is_allowed,
                                          std::chrono::steady_clock::now() + kCacheTTL};
      RAY_LOG(DEBUG) << "K8s token validated and saved to cache.";
    }

    return is_allowed;
  }

  RAY_LOG(DEBUG) << "Authentication mode is disabled, token considered valid.";
  return true;
}

}  // namespace rpc
}  // namespace ray
