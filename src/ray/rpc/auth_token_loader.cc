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

#include "ray/rpc/auth_token_loader.h"

#include <fstream>
#include <stdexcept>
#include <string>

#include "ray/common/ray_config.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace ray {
namespace rpc {

RayAuthTokenLoader &RayAuthTokenLoader::instance() {
  static RayAuthTokenLoader instance;
  return instance;
}

const std::string &RayAuthTokenLoader::GetToken() {
  std::lock_guard<std::mutex> lock(token_mutex_);

  if (token_loaded_) {
    return cached_token_;
  }

  // Try to load from sources
  cached_token_ = LoadTokenFromSources();
  token_loaded_ = true;

  // If token auth is enabled but no token is found, throw an error
  if (RayConfig::instance().enable_token_auth() && cached_token_.empty()) {
    RAY_LOG(ERROR) << "Token authentication is enabled but no authentication token was "
                      "found. Please set RAY_AUTH_TOKEN environment variable, "
                      "RAY_AUTH_TOKEN_PATH to a file containing the token, or create a "
                      "token file at ~/.ray/auth_token";
    throw std::runtime_error(
        "Token authentication is enabled but no authentication token was found");
  }

  return cached_token_;
}

bool RayAuthTokenLoader::HasToken() {
  // This will trigger loading if not already loaded
  const std::string &token = GetToken();
  return !token.empty();
}

std::string RayAuthTokenLoader::LoadTokenFromSources() {
  // Precedence 1: RAY_AUTH_TOKEN environment variable
  const char *env_token = std::getenv("RAY_AUTH_TOKEN");
  if (env_token != nullptr && std::string(env_token).length() > 0) {
    RAY_LOG(DEBUG) << "Loaded authentication token from RAY_AUTH_TOKEN environment "
                      "variable";
    return std::string(env_token);
  }

  // Precedence 2: RAY_AUTH_TOKEN_PATH environment variable
  const char *env_token_path = std::getenv("RAY_AUTH_TOKEN_PATH");
  if (env_token_path != nullptr && std::string(env_token_path).length() > 0) {
    std::ifstream token_file(env_token_path);
    if (token_file.is_open()) {
      std::string token;
      std::getline(token_file, token);
      token_file.close();
      // Trim whitespace
      token.erase(0, token.find_first_not_of(" \t\n\r\f\v"));
      token.erase(token.find_last_not_of(" \t\n\r\f\v") + 1);
      if (!token.empty()) {
        RAY_LOG(DEBUG) << "Loaded authentication token from file: " << env_token_path;
        return token;
      }
    } else {
      RAY_LOG(WARNING) << "RAY_AUTH_TOKEN_PATH is set but file cannot be opened: "
                       << env_token_path;
    }
  }

  // Precedence 3: Default token path ~/.ray/auth_token
  std::string default_path = GetDefaultTokenPath();
  std::ifstream token_file(default_path);
  if (token_file.is_open()) {
    std::string token;
    std::getline(token_file, token);
    token_file.close();
    // Trim whitespace
    token.erase(0, token.find_first_not_of(" \t\n\r\f\v"));
    token.erase(token.find_last_not_of(" \t\n\r\f\v") + 1);
    if (!token.empty()) {
      RAY_LOG(DEBUG) << "Loaded authentication token from default path: " << default_path;
      return token;
    }
  }

  // No token found
  RAY_LOG(DEBUG) << "No authentication token found in any source";
  return "";
}

std::string RayAuthTokenLoader::GetDefaultTokenPath() {
  std::string home_dir;

#ifdef _WIN32
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
  const char *home = std::getenv("HOME");
  if (home != nullptr) {
    home_dir = home;
  }
#endif

  if (home_dir.empty()) {
    RAY_LOG(WARNING) << "Cannot determine home directory for token storage";
    return ".ray/auth_token";
  }

  return home_dir + "/.ray/auth_token";
}

}  // namespace rpc
}  // namespace ray
