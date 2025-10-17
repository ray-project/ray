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

#pragma once

#include <mutex>
#include <string>

namespace ray {
namespace rpc {

/// Singleton class for loading and caching authentication tokens.
/// Supports loading tokens from multiple sources with precedence:
/// 1. RAY_AUTH_TOKEN environment variable
/// 2. RAY_AUTH_TOKEN_PATH environment variable (path to token file)
/// 3. Default token path: ~/.ray/auth_token
///
/// Thread-safe with internal caching to avoid repeated file I/O.
class RayAuthTokenLoader {
 public:
  /// Get the singleton instance.
  static RayAuthTokenLoader &instance();

  /// Get the authentication token.
  /// If token authentication is enabled but no token is found, throws an error.
  /// \return The authentication token, or empty string if auth is disabled.
  const std::string &GetToken();

  /// Check if an authentication token exists.
  /// \return True if a token is available (cached or can be loaded).
  bool HasToken();

  // Prevent copying and moving
  RayAuthTokenLoader(const RayAuthTokenLoader &) = delete;
  RayAuthTokenLoader &operator=(const RayAuthTokenLoader &) = delete;

 private:
  RayAuthTokenLoader() = default;
  ~RayAuthTokenLoader() = default;

  /// Load token from available sources (env vars and file).
  std::string LoadTokenFromSources();

  /// Get the default token file path (~/.ray/auth_token).
  std::string GetDefaultTokenPath();

  std::mutex token_mutex_;
  std::string cached_token_;
  bool token_loaded_ = false;
};

}  // namespace rpc
}  // namespace ray
