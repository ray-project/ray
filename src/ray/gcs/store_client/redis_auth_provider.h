// Copyright 2026 The Ray Authors.
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

#include <functional>
#include <memory>
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"
#include "ray/util/clock.h"

namespace ray {
namespace gcs {

/// The credentials used to authenticate a Redis connection with the `AUTH`
/// command. For password-only auth (Redis < 6) `username` is empty.
struct RedisCredentials {
  std::string username;
  std::string password;
  /// The wall-clock time at which `password` stops being valid. Static
  /// credentials never expire and use `absl::InfiniteFuture()`.
  absl::Time expiry = absl::InfiniteFuture();
};

/// Supplies (and, for token-based modes, refreshes) the credentials used to
/// authenticate Ray's GCS connection to an external Redis.
///
/// Implementations must be thread-safe: `GetCredentials` may be called from the
/// connection path and concurrently from a background refresh timer.
class RedisAuthProvider {
 public:
  virtual ~RedisAuthProvider() = default;

  /// Returns the current credentials, refreshing them from the underlying
  /// source first if they are absent or close to expiry. May block on I/O for
  /// refreshable providers.
  virtual StatusOr<RedisCredentials> GetCredentials() = 0;

  /// Whether the credentials expire and therefore require the connection to be
  /// periodically re-authenticated (re-`AUTH`ed) before expiry.
  virtual bool IsRefreshable() const = 0;
};

/// A fixed username/password, matching Ray's historical static-password auth.
class StaticRedisAuthProvider : public RedisAuthProvider {
 public:
  StaticRedisAuthProvider(std::string username, std::string password)
      : username_(std::move(username)), password_(std::move(password)) {}

  StatusOr<RedisCredentials> GetCredentials() override {
    return RedisCredentials{username_, password_, absl::InfiniteFuture()};
  }

  bool IsRefreshable() const override { return false; }

 private:
  const std::string username_;
  const std::string password_;
};

/// Configuration for `EntraRedisAuthProvider`.
struct EntraAuthConfig {
  /// The IMDS token endpoint, e.g.
  /// "http://169.254.169.254/metadata/identity/oauth2/token". Plain HTTP on the
  /// link-local address; overridable for testing.
  std::string imds_endpoint;
  /// The Entra resource/audience to request a token for. For Azure (Managed)
  /// Redis this is "https://redis.azure.com/".
  std::string resource;
  /// Optional user-assigned managed identity client id. Empty selects the
  /// system-assigned managed identity.
  std::string client_id;
  /// Optional Redis `AUTH` username override. When empty, the `oid` claim of
  /// the access token is used (as required by Azure Entra Redis auth).
  std::string username;
  /// How long before expiry to proactively refresh the token.
  absl::Duration refresh_buffer = absl::Seconds(300);
  /// Timeout for a single IMDS HTTP request.
  absl::Duration request_timeout = absl::Seconds(10);
};

/// Fetches a Microsoft Entra ID (Azure AD) access token from the Azure Instance
/// Metadata Service (IMDS) managed-identity endpoint and uses it as the Redis
/// password. The token is cached and transparently refreshed before expiry.
///
/// A pluggable `token_fetcher` performs the raw HTTP GET; the default
/// implementation talks to IMDS over HTTP. Tests inject a fake fetcher.
class EntraRedisAuthProvider : public RedisAuthProvider {
 public:
  /// Performs the IMDS HTTP GET and returns the raw JSON response body.
  using TokenFetcher =
      std::function<StatusOr<std::string>(const EntraAuthConfig &config)>;

  EntraRedisAuthProvider(ClockInterface &clock,
                         EntraAuthConfig config,
                         TokenFetcher token_fetcher = DefaultTokenFetcher);

  StatusOr<RedisCredentials> GetCredentials() override ABSL_LOCKS_EXCLUDED(mu_);

  bool IsRefreshable() const override { return true; }

  /// The default fetcher: an HTTP GET against the IMDS token endpoint.
  static StatusOr<std::string> DefaultTokenFetcher(const EntraAuthConfig &config);

 private:
  StatusOr<RedisCredentials> RefreshLocked() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  ClockInterface &clock_;
  const EntraAuthConfig config_;
  const TokenFetcher token_fetcher_;

  absl::Mutex mu_;
  RedisCredentials cached_ ABSL_GUARDED_BY(mu_);
  bool has_token_ ABSL_GUARDED_BY(mu_) = false;
};

/// Parses the IMDS token JSON response, extracting the access token and its
/// absolute expiry. Handles both the absolute `expires_on` (Unix seconds) and
/// the relative `expires_in` (seconds from `now`) fields, which IMDS may return
/// as either JSON strings or numbers.
Status ParseImdsTokenResponse(const std::string &body,
                              absl::Time now,
                              std::string *access_token,
                              absl::Time *expiry);

/// Extracts the `oid` (object id) claim from a JWT access token, used as the
/// Redis `AUTH` username for Entra-authenticated Azure Redis.
StatusOr<std::string> ExtractOidFromJwt(const std::string &jwt);

}  // namespace gcs
}  // namespace ray
