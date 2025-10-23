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

#include "ray/util/logging.h"

#if defined(__APPLE__) || defined(__linux__)
#include <sys/stat.h>
#include <unistd.h>
#endif

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

AuthenticationTokenLoader &AuthenticationTokenLoader::instance() {
  static AuthenticationTokenLoader instance;
  return instance;
}

std::optional<AuthenticationToken> AuthenticationTokenLoader::GetToken() {
  std::lock_guard<std::mutex> lock(token_mutex_);

  // If already loaded, return cached value
  if (cached_token_.has_value()) {
    return cached_token_;
  }

  // If token auth is not enabled, return std::nullopt
  if (GetAuthenticationMode() != AuthenticationMode::TOKEN) {
    cached_token_ = std::nullopt;
    return std::nullopt;
  }

  // Token auth is enabled, try to load from sources
  AuthenticationToken token = LoadTokenFromSources();

  // If no token found and auth is enabled, fail with RAY_CHECK
  RAY_CHECK(!token.empty())
      << "Token authentication is enabled but no authentication token was found. "
      << "Please set RAY_AUTH_TOKEN environment variable, RAY_AUTH_TOKEN_PATH to a file "
      << "containing the token, or create a token file at ~/.ray/auth_token";

  // Cache and return the loaded token
  cached_token_ = std::move(token);
  return *cached_token_;
}

bool AuthenticationTokenLoader::HasToken() {
  std::lock_guard<std::mutex> lock(token_mutex_);

  // If already loaded, check if present
  if (cached_token_.has_value()) {
    return cached_token_->has_value();
  }

  // If token auth is not enabled, no token needed
  if (GetAuthenticationMode() != AuthenticationMode::TOKEN) {
    return false;
  }

  // Try to load token
  AuthenticationToken token = LoadTokenFromSources();
  return !token.empty();
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

AuthenticationToken AuthenticationTokenLoader::LoadTokenFromSources() {
  // Precedence 1: RAY_AUTH_TOKEN environment variable
  const char *env_token = std::getenv("RAY_AUTH_TOKEN");
  if (env_token != nullptr && std::string(env_token).length() > 0) {
    RAY_LOG(DEBUG) << "Loaded authentication token from RAY_AUTH_TOKEN environment "
                      "variable";
    return AuthenticationToken(TrimWhitespace(std::string(env_token)));
  }

  // Precedence 2: RAY_AUTH_TOKEN_PATH environment variable
  const char *env_token_path = std::getenv("RAY_AUTH_TOKEN_PATH");
  if (env_token_path != nullptr && std::string(env_token_path).length() > 0) {
    std::string token_str = TrimWhitespace(ReadTokenFromFile(env_token_path));
    if (!token_str.empty()) {
      RAY_LOG(DEBUG) << "Loaded authentication token from file: " << env_token_path;
      return AuthenticationToken(token_str);
    } else {
      RAY_LOG(WARNING) << "RAY_AUTH_TOKEN_PATH is set but file cannot be opened: "
                       << env_token_path;
    }
  }

  // Precedence 3: Default token path ~/.ray/auth_token
  std::string default_path = GetDefaultTokenPath();
  std::string token_str = TrimWhitespace(ReadTokenFromFile(default_path));
  if (!token_str.empty()) {
    RAY_LOG(DEBUG) << "Loaded authentication token from default path: " << default_path;
    return AuthenticationToken(token_str);
  }

  // No token found
  RAY_LOG(DEBUG) << "No authentication token found in any source";
  return AuthenticationToken();
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
  trimmed_str.erase(trimmed_str.find_last_not_of(whitespace) + 1);
  return trimmed_str;
}

}  // namespace rpc
}  // namespace ray
