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

#include <chrono>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "ray/rpc/authentication/authentication_mode.h"
#include "ray/rpc/authentication/authentication_token.h"
#include "ray/rpc/authentication/k8s_util.h"

namespace ray {
namespace rpc {

// Hash function for AuthenticationToken
struct AuthenticationTokenHash {
  std::size_t operator()(const AuthenticationToken &token) const {
    return std::hash<std::string>()(token.ToValue());
  }
};

/// Singleton class for loading and caching authentication tokens.
/// Supports loading tokens from multiple sources with precedence:
/// 1. RAY_AUTH_TOKEN environment variable
/// 2. RAY_AUTH_TOKEN_PATH environment variable (path to token file)
/// 3. Default token path: ~/.ray/auth_token (Unix) or %USERPROFILE%\.ray\auth_token
///
/// Thread-safe with internal caching to avoid repeated file I/O.
class AuthenticationTokenLoader {
 public:
  static AuthenticationTokenLoader &instance();

  /// Get the authentication token.
  /// If token authentication is enabled but no token is found, fails with RAY_CHECK.
  /// \return The authentication token, or std::nullopt if auth is disabled.
  std::optional<AuthenticationToken> GetToken();

  /// Check if a token exists without crashing.
  /// Caches the token if it loads it afresh.
  /// \return true if a token exists, false otherwise.
  bool HasToken();

  /// Validate the provided authentication token.
  /// For TOKEN mode, it compares with the loaded token.
  /// For K8S mode, it uses Kubernetes TokenReview and SubjectAccessReview APIs.
  /// The results for K8S mode are cached.
  /// \param provided_token The token to validate.
  /// \return true if the token is valid, false otherwise.
  bool ValidateToken(const AuthenticationToken &provided_token);

  void ResetCache() {
    std::lock_guard<std::mutex> lock(token_mutex_);
    cached_token_.reset();
  }

  AuthenticationTokenLoader(const AuthenticationTokenLoader &) = delete;
  AuthenticationTokenLoader &operator=(const AuthenticationTokenLoader &) = delete;

 private:
  AuthenticationTokenLoader() = default;
  ~AuthenticationTokenLoader() = default;

  /// Read and trim token from file.
  std::string ReadTokenFromFile(const std::string &file_path);

  /// Load token from environment or file.
  AuthenticationToken LoadTokenFromSources();

  /// Default token file path (~/.ray/auth_token or %USERPROFILE%\.ray\auth_token).
  std::string GetDefaultTokenPath();

  /// Trim whitespace from the beginning and end of the string.
  std::string TrimWhitespace(const std::string &str);

  std::mutex token_mutex_;
  std::optional<AuthenticationToken> cached_token_;

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
