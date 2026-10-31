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

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/escaping.h"
#include "absl/strings/str_split.h"
#include "nlohmann/json.hpp"
#include "openssl/rand.h"
#include "ray/common/constants.h"
#include "ray/common/ray_config.h"
#include "ray/rpc/authentication/authentication_mode.h"
#include "ray/rpc/authentication/k8s_constants.h"
#include "ray/util/env.h"
#include "ray/util/logging.h"

#ifdef _WIN32
#include <Windows.h>  // Force inclusion of WinGDI here to resolve name conflict
#else
#include <fcntl.h>
#include <pwd.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#endif

namespace ray {
namespace rpc {

// Keep in sync with K8S_TOKEN_AUTH_REQUIRES_TOKEN_MODE_ERROR_MESSAGE in
// python/ray/_private/authentication/authentication_constants.py.
constexpr const char *kK8sTokenAuthRequiresTokenModeErrorMessage =
    "Kubernetes token authentication (ENABLE_K8S_TOKEN_AUTH) requires "
    "RAY_AUTH_MODE=token. Set RAY_AUTH_MODE=token, or turn off "
    "ENABLE_K8S_TOKEN_AUTH to run with auth disabled.";

std::optional<std::chrono::system_clock::time_point>
AuthenticationTokenLoader::GetJWTTokenExpiration(const std::string &token) {
  std::vector<std::string> parts = absl::StrSplit(token, '.');
  if (parts.size() != 3) {
    RAY_LOG(WARNING) << "Invalid JWT token format.";
    return std::nullopt;
  }

  std::string payload;
  if (!absl::WebSafeBase64Unescape(parts[1], &payload)) {
    RAY_LOG(WARNING) << "Unable to base64 decode JWT token.";
    return std::nullopt;
  }

  try {
    auto json = nlohmann::json::parse(payload);
    if (json.contains("exp") && json["exp"].is_number()) {
      int64_t exp = json["exp"].get<int64_t>();
      return std::chrono::system_clock::from_time_t(exp) -
             std::chrono::seconds(ray::rpc::k8s::kRaySATokenExpirationBufferSeconds);
    }
  } catch (...) {
    return std::nullopt;
  }

  return std::chrono::system_clock::now() +
         std::chrono::seconds(ray::rpc::k8s::kRaySATokenDefaultTTLSeconds);
}

AuthenticationTokenLoader &AuthenticationTokenLoader::instance() {
  static AuthenticationTokenLoader instance;
  return instance;
}

std::shared_ptr<const AuthenticationToken> AuthenticationTokenLoader::GetToken(
    bool ignore_auth_mode) {
  // Every Ray process loads its token at startup, so this fails each one. A user
  // config error, so no RAY_CHECK, whose message calls it a bug in Ray.
  if (IsK8sTokenAuthEnabled() && GetAuthenticationMode() != AuthenticationMode::TOKEN) {
    RAY_LOG(FATAL) << kK8sTokenAuthRequiresTokenModeErrorMessage;
  }
  absl::MutexLock lock(&token_mutex_);

  // If k8s token auth is enabled, revoke cached token as Kubelet
  // will expire and auto rotate new service account tokens every hour by default.
  // Use 5 minutes as a default as users can configure the expiration time.
  if (IsK8sTokenAuthEnabled()) {
    if (cached_token_ && cached_token_expiration_time_ &&
        std::chrono::system_clock::now() >= cached_token_expiration_time_) {
      cached_token_ = nullptr;
    }
  }

  // If already loaded, return cached value
  if (cached_token_) {
    return cached_token_;
  }

  // If token auth is not enabled, return std::nullopt (unless ignoring auth mode)
  if (!ignore_auth_mode && GetAuthenticationMode() != AuthenticationMode::TOKEN) {
    return nullptr;
  }

  // Token auth is enabled (or we're ignoring auth mode), try to load from sources
  TokenLoadResult result = TryLoadTokenFromSources();

  // If there was an error loading (e.g., RAY_AUTH_TOKEN_PATH set but file missing), crash
  RAY_CHECK(!result.hasError()) << result.error_message;

  // If no token found and auth is enabled, fail with RAY_CHECK
  bool has_token = result.token.has_value() && !result.token->empty();
  RAY_CHECK(has_token || ignore_auth_mode) << kTokenAuthErrorMessage;

  // Cache and return the loaded token
  if (has_token) {
    cached_token_ = std::make_shared<const AuthenticationToken>(std::move(*result.token));
    if (IsK8sTokenAuthEnabled()) {
      cached_token_expiration_time_ = GetJWTTokenExpiration(cached_token_->GetRawValue());
    }
  }
  return cached_token_;
}

TokenLoadResult AuthenticationTokenLoader::TryLoadToken(bool ignore_auth_mode) {
  absl::MutexLock lock(&token_mutex_);
  TokenLoadResult result;

  // If k8s token auth is enabled, revoke cached token as Kubelet
  // will expire and auto rotate new service account tokens every hour by default.
  // Use 5 minutes as a default as users can configure the expiration time.
  if (IsK8sTokenAuthEnabled()) {
    if (cached_token_ && cached_token_expiration_time_ &&
        std::chrono::system_clock::now() >= cached_token_expiration_time_) {
      cached_token_ = nullptr;
    }
  }

  // If already loaded, return cached value
  if (cached_token_) {
    result.token = *cached_token_;  // Copy from shared_ptr
    return result;
  }

  // If auth is disabled, return nullopt (no token needed)
  if (!ignore_auth_mode && GetAuthenticationMode() != AuthenticationMode::TOKEN) {
    result.token = std::nullopt;
    return result;
  }

  // Token auth is enabled, try to load from sources
  result = TryLoadTokenFromSources();
  if (result.hasError()) {
    return result;  // Propagate error
  }

  bool has_token = result.token.has_value() && !result.token->empty();
  if (!has_token && ignore_auth_mode) {
    result.token = std::nullopt;
    return result;
  } else if (!has_token) {
    result.error_message = kTokenAuthErrorMessage;
    return result;
  }
  // Cache and return success
  cached_token_ = std::make_shared<const AuthenticationToken>(std::move(*result.token));
  if (IsK8sTokenAuthEnabled()) {
    cached_token_expiration_time_ = GetJWTTokenExpiration(cached_token_->GetRawValue());
  }
  result.token = *cached_token_;  // Copy back for return
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

  // Precedence 3 (ENABLE_K8S_TOKEN_AUTH only): Load Ray service account token in
  // /var/run/secrets/ray.io/serviceaccount/token
  if (IsK8sTokenAuthEnabled()) {
    const std::string k8s_token_path(k8s::kRaySaTokenPath);
    std::string token_str = TrimWhitespace(ReadTokenFromFile(k8s_token_path));
    if (!token_str.empty()) {
      RAY_LOG(DEBUG)
          << "Loaded authentication token from Kubernetes service account path: "
          << k8s_token_path;
      result.token = AuthenticationToken(token_str);
      return result;
    }
    RAY_LOG(DEBUG) << "Kubernetes service account token not found or empty at: "
                   << k8s_token_path;
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

// Read the token from the first non-blank line of the file.
// Returns empty string if the file cannot be opened or has no non-blank line.
std::string AuthenticationTokenLoader::ReadTokenFromFile(const std::string &file_path) {
  std::ifstream token_file(file_path);
  if (!token_file.is_open()) {
    return "";
  }

  std::string line;
  while (std::getline(token_file, line)) {
    if (!TrimWhitespace(line).empty()) {
      return line;
    }
  }
  return "";
}

std::string AuthenticationTokenLoader::CreateTokenIfMissing() {
  TokenLoadResult existing = TryLoadToken(/*ignore_auth_mode=*/true);
  if (existing.hasError()) {
    return existing.error_message;
  }
  if (existing.token.has_value() && !existing.token->empty()) {
    return "";
  }

  const std::string token_path = GetDefaultTokenPath();
  if (token_path.empty()) {
    return "Cannot determine the home directory to store the authentication token in.";
  }
  const std::filesystem::path token_dir = std::filesystem::path(token_path).parent_path();
  std::error_code ec;
  const bool created_dir = std::filesystem::create_directories(token_dir, ec);
  if (ec) {
    return "Failed to create " + token_dir.string() + ": " + ec.message();
  }
  if (created_dir) {
    std::filesystem::permissions(token_dir, std::filesystem::perms::owner_all, ec);
    if (ec) {
      // Some filesystems (FAT, some network mounts) don't support Unix modes; the
      // token file itself is still created with mode 0600.
      RAY_LOG(WARNING) << "Failed to restrict " << token_dir.string()
                       << " to its owner: " << ec.message();
    }
  }

  // 256 bits of entropy, hex-encoded: the same format `ray get-auth-token --generate`
  // has always produced.
  unsigned char random_bytes[32];
  if (RAND_bytes(random_bytes, sizeof(random_bytes)) != 1) {
    return "Failed to generate a random authentication token.";
  }
  const std::string token = absl::BytesToHexString(absl::string_view(
      reinterpret_cast<const char *>(random_bytes), sizeof(random_bytes)));

  // Written in place, so a symlinked token file keeps its link. The token is the
  // cluster credential, so the file is owner-only; fchmod covers a file that existed
  // before with a wider mode. Windows has no POSIX modes.
#ifdef _WIN32
  std::ofstream token_file(std::filesystem::path(token_path),
                           std::ios::out | std::ios::trunc);
  token_file << token;
  token_file.close();
  if (!token_file) {
    return "Failed to write the authentication token to " + token_path;
  }
#else
  const int fd = open(token_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
  if (fd < 0) {
    return "Failed to open " + token_path + ": " + std::strerror(errno);
  }
  std::string write_error;
  if (fchmod(fd, 0600) != 0) {
    write_error = std::strerror(errno);
  } else {
    const ssize_t written = write(fd, token.data(), token.size());
    if (written < 0) {
      write_error = std::strerror(errno);
    } else if (static_cast<size_t>(written) != token.size()) {
      write_error = "short write";
    }
  }
  close(fd);
  if (!write_error.empty()) {
    return "Failed to write the authentication token to " + token_path + ": " +
           write_error;
  }
#endif

  ResetCache();
  RAY_LOG(INFO) << "Generated new authentication token and saved to " << token_path;
  return "";
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
  if (home != nullptr && home[0] != '\0') {
    home_dir = home;
  } else {
    // Match Python's Path.home(): when HOME is unset (e.g. CI running as root
    // without HOME), fall back to the passwd database so the C++ and Python
    // sides resolve the same ~/.ray/auth_token path.
    struct passwd pwd;
    struct passwd *result = nullptr;
    char buf[4096];  // A home-dir path fits comfortably; no ERANGE retry needed.
    if (getpwuid_r(getuid(), &pwd, buf, sizeof(buf), &result) == 0 && result != nullptr &&
        result->pw_dir != nullptr) {
      home_dir = result->pw_dir;
    }
  }
#endif

  const std::string token_subpath =
      std::string(path_separator) + ".ray" + std::string(path_separator) + "auth_token";

  if (home_dir.empty()) {
    // No resolvable home: return an empty path rather than an invalid relative
    // "./.ray/auth_token". ReadTokenFromFile("") simply finds no token.
    RAY_LOG(WARNING) << "Cannot determine home directory for token storage";
    return "";
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

void MaybeEnableTokenAuthIfTokenAvailable() {
  if (std::getenv("RAY_AUTH_MODE") != nullptr) {
    return;
  }
  auto result =
      AuthenticationTokenLoader::instance().TryLoadToken(/*ignore_auth_mode=*/true);
  if (result.hasError() || !result.token.has_value() || result.token->empty()) {
    return;
  }
  SetEnv("RAY_AUTH_MODE", "token");
  RayConfig::instance().initialize("");
  RAY_LOG(WARNING) << "Token authentication is enabled for this Ray cluster. Set "
                      "RAY_AUTH_MODE=disabled to opt out. For more information, see "
                      "https://docs.ray.io/en/latest/ray-security/token-auth.html";
}

std::string CreateTokenIfMissingForOwnCluster() {
  if (GetAuthenticationMode() != AuthenticationMode::TOKEN) {
    return "";
  }
  return AuthenticationTokenLoader::instance().CreateTokenIfMissing();
}

}  // namespace rpc
}  // namespace ray
