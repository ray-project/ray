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

#include "ray/rpc/authentication/authentication_token_loader.h"

#include <fstream>
#include <string>
#include <utility>

#include "ray/rpc/authentication/k8s_constants.h"
#include "ray/util/logging.h"

#ifdef _WIN32
#ifndef _WINDOWS_
#ifndef WIN32_LEAN_AND_MEAN  // Sorry for the inconvenience. Please include any related
                             // headers you need manually.
                             // (https://stackoverflow.com/a/8294669)
#define WIN32_LEAN_AND_MEAN  // Prevent inclusion of WinSock2.h
#endif
#include <Windows.h>  // Force inclusion of WinGDI here to resolve name conflict
#endif
#endif

namespace ray {
namespace rpc {

constexpr const char *kNoTokenErrorMessage =
    "Token authentication is enabled but Ray couldn't find an "
    "authentication token. "
    "Create a token file at ~/.ray/auth_token, "
    "or store the token in any file and set RAY_AUTH_TOKEN_PATH to point to it, "
    "or set the RAY_AUTH_TOKEN environment variable.";

AuthenticationTokenLoader &AuthenticationTokenLoader::instance() {
  static AuthenticationTokenLoader instance;
  return instance;
}

std::optional<AuthenticationToken> AuthenticationTokenLoader::GetToken(
    bool ignore_auth_mode) {
  absl::MutexLock lock(&token_mutex_);

  // If already loaded, return cached value
  if (cached_token_.has_value()) {
    return cached_token_;
  }

  // If token or k8s auth is not enabled, return std::nullopt (unless ignoring auth mode)
  if (!ignore_auth_mode && !RequiresTokenAuthentication()) {
    cached_token_ = std::nullopt;
    return std::nullopt;
  }

  // Token auth is enabled (or we're ignoring auth mode), try to load from sources
  TokenLoadResult result = TryLoadTokenFromSources();

  // If there was an error loading (e.g., RAY_AUTH_TOKEN_PATH set but file missing), crash
  RAY_CHECK(!result.hasError()) << result.error_message;

  // If no token found and auth is enabled, fail with RAY_CHECK
  bool has_token = result.token.has_value() && !result.token->empty();
  RAY_CHECK(has_token || ignore_auth_mode) << kNoTokenErrorMessage;

  // Cache and return the loaded token
  if (has_token) {
    cached_token_ = std::move(result.token);
    return *cached_token_;
  }
  return std::nullopt;
}

TokenLoadResult AuthenticationTokenLoader::TryLoadToken(bool ignore_auth_mode) {
  absl::MutexLock lock(&token_mutex_);
  TokenLoadResult result;

  // If already loaded, return cached value
  if (cached_token_.has_value()) {
    result.token = cached_token_;
    return result;
  }

  // If auth is disabled, return nullopt (no token needed)
  if (!ignore_auth_mode && !RequiresTokenAuthentication()) {
    cached_token_ = std::nullopt;
    result.token = std::nullopt;
    return result;
  }

  // Token auth is enabled, try to load from sources
  result = TryLoadTokenFromSources();
  if (result.hasError()) {
    return result;  // Propagate error
  }

  bool no_token = !result.token.has_value() || result.token->empty();
  if (no_token && ignore_auth_mode) {
    result.token = std::nullopt;
    return result;
  } else if (no_token) {
    result.error_message = kNoTokenErrorMessage;
    return result;
  }
  // Cache and return success
  cached_token_ = result.token;
  return result;
}

TokenLoadResult AuthenticationTokenLoader::TryLoadTokenFromSources() {
  TokenLoadResult result;

  // Precedence 1: RAY_AUTH_TOKEN environment variable
  const char *env_token = std::getenv("RAY_AUTH_TOKEN");
  if (env_token != nullptr) {
    std::string token_str(env_token);
    if (!token_str.empty()) {
      RAY_LOG(DEBUG) << "Loaded authentication token from RAY_AUTH_TOKEN environment "
                        "variable";
      result.token = AuthenticationToken(TrimWhitespace(token_str));
      return result;
    }
  }

  // Precedence 2: RAY_AUTH_TOKEN_PATH environment variable
  const char *env_token_path = std::getenv("RAY_AUTH_TOKEN_PATH");
  if (env_token_path != nullptr) {
    std::string path_str(env_token_path);
    if (!path_str.empty()) {
      std::string token_str = TrimWhitespace(ReadTokenFromFile(path_str));
      if (token_str.empty()) {
        // Return error message instead of crashing
        result.error_message =
            "RAY_AUTH_TOKEN_PATH is set but file cannot be opened or is empty: " +
            path_str;
        return result;
      }
      RAY_LOG(DEBUG) << "Loaded authentication token from file (RAY_AUTH_TOKEN_PATH): "
                     << path_str;
      result.token = AuthenticationToken(token_str);
      return result;
    }
  }

  // Precedence 3 (auth_mode=k8s only): Load Kubernetes service account token
  if (GetAuthenticationMode() == AuthenticationMode::K8S) {
    std::string token_str = TrimWhitespace(ReadTokenFromFile(k8s::kK8sSaTokenPath));
    if (!token_str.empty()) {
      RAY_LOG(DEBUG)
          << "Loaded authentication token from Kubernetes service account path: "
          << k8s::kK8sSaTokenPath;
      result.token = AuthenticationToken(token_str);
      return result;
    }
    RAY_LOG(DEBUG) << "Kubernetes service account token not found or empty at: "
                   << k8s::kK8sSaTokenPath;
  }

  // Precedence 4: Default token path ~/.ray/auth_token
  std::string default_path = GetDefaultTokenPath();
  std::string token_str = TrimWhitespace(ReadTokenFromFile(default_path));
  if (!token_str.empty()) {
    RAY_LOG(DEBUG) << "Loaded authentication token from default path: " << default_path;
    result.token = AuthenticationToken(token_str);
    return result;
  }

  // No token found - return empty result (caller decides if error)
  RAY_LOG(DEBUG) << "No authentication token found in any source";
  result.token = AuthenticationToken();  // Empty token
  return result;
}

// Read token from the first line of the file. trim whitespace.
// Returns empty string if file cannot be opened or is empty.
std::string AuthenticationTokenLoader::ReadTokenFromFile(const std::string &file_path) {
  std::ifstream token_file(file_path);
  if (!token_file.is_open()) {
    return "";
  }

  std::string token;
  std::getline(token_file, token);
  token_file.close();
  return token;
}

std::string AuthenticationTokenLoader::GetDefaultTokenPath() {
  std::string home_dir;

#ifdef _WIN32
  const char *path_separator = "\\";
  const char *userprofile = std::getenv("USERPROFILE");
  if (userprofile != nullptr) {
    home_dir = userprofile;
  } else {
    const char *homedrive = std::getenv("HOMEDRIVE");
    const char *homepath = std::getenv("HOMEPATH");
    if (homedrive != nullptr && homepath != nullptr) {
      home_dir = std::string(homedrive) + std::string(homepath);
    }
  }
#else
  const char *path_separator = "/";
  const char *home = std::getenv("HOME");
  if (home != nullptr) {
    home_dir = home;
  }
#endif

  const std::string token_subpath =
      std::string(path_separator) + ".ray" + std::string(path_separator) + "auth_token";

  if (home_dir.empty()) {
    RAY_LOG(WARNING) << "Cannot determine home directory for token storage";
    return "." + token_subpath;
  }

  return home_dir + token_subpath;
}

std::string AuthenticationTokenLoader::TrimWhitespace(const std::string &str) {
  std::string whitespace = " \t\n\r\f\v";
  std::string trimmed_str = str;
  trimmed_str.erase(0, trimmed_str.find_first_not_of(whitespace));

  // if the string is empty, return it
  if (trimmed_str.empty()) {
    return trimmed_str;
  }

  trimmed_str.erase(trimmed_str.find_last_not_of(whitespace) + 1);
  return trimmed_str;
}

}  // namespace rpc
}  // namespace ray
