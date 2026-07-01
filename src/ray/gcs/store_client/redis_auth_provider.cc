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

#include "ray/gcs/store_client/redis_auth_provider.h"

#include <azure/core/context.hpp>
#include <azure/core/credentials/credentials.hpp>
#include <azure/identity/default_azure_credential.hpp>
#include <azure/identity/managed_identity_credential.hpp>
#include <chrono>
#include <exception>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "nlohmann/json.hpp"
#include "ray/util/logging.h"

namespace ray {
namespace gcs {

namespace {

// Converts an Azure resource/audience into the OAuth2 scope expected by the
// Azure Identity SDK, i.e. "<resource>/.default".
std::string ResourceToScope(const std::string &resource) {
  if (!resource.empty() && resource.back() == '/') {
    return absl::StrCat(resource, ".default");
  }
  return absl::StrCat(resource, "/.default");
}

// Converts the SDK's absolute expiry timestamp to an absl::Time. Azure::DateTime
// derives from std::chrono::system_clock::time_point, so second-granularity is
// sufficient for the (multi-minute) refresh buffer we operate with.
absl::Time ToAbslTime(const Azure::DateTime &time) {
  return absl::FromUnixSeconds(
      std::chrono::duration_cast<std::chrono::seconds>(time.time_since_epoch()).count());
}

}  // namespace

StatusOr<std::string> ExtractOidFromJwt(const std::string &jwt) {
  std::vector<std::string> parts = absl::StrSplit(jwt, '.');
  if (parts.size() != 3) {
    return Status::RedisError(
        "Entra access token is not a well-formed JWT (expected 3 segments).");
  }
  std::string payload;
  if (!absl::WebSafeBase64Unescape(parts[1], &payload)) {
    return Status::RedisError("Failed to base64url-decode the JWT payload.");
  }
  nlohmann::json json;
  try {
    json = nlohmann::json::parse(payload);
  } catch (const std::exception &e) {
    return Status::RedisError(
        absl::StrCat("Failed to parse JWT payload as JSON: ", e.what()));
  }
  if (!json.contains("oid") || !json.at("oid").is_string()) {
    return Status::RedisError(
        "JWT payload is missing a string `oid` claim; set RAY_REDIS_USERNAME "
        "to the identity's object id explicitly.");
  }
  return json.at("oid").get<std::string>();
}

StatusOr<EntraAccessToken> EntraRedisAuthProvider::DefaultTokenFetcher(
    const EntraAuthConfig &config) {
  namespace identity = Azure::Identity;
  using Azure::Core::Context;
  using Azure::Core::Credentials::AccessToken;
  using Azure::Core::Credentials::TokenCredential;
  using Azure::Core::Credentials::TokenRequestContext;

  try {
    std::shared_ptr<TokenCredential> credential;
    if (!config.client_id.empty()) {
      // A user-assigned managed identity was requested explicitly.
      credential =
          std::make_shared<identity::ManagedIdentityCredential>(config.client_id);
    } else {
      // Covers system-assigned managed identity, workload identity federation,
      // environment credentials, etc. Deployments select the flow they need
      // through the standard AZURE_* environment variables.
      credential = std::make_shared<identity::DefaultAzureCredential>();
    }

    TokenRequestContext request;
    request.Scopes = {ResourceToScope(config.resource)};
    AccessToken token = credential->GetToken(request, Context{});

    EntraAccessToken result;
    result.token = std::move(token.Token);
    result.expiry = ToAbslTime(token.ExpiresOn);
    return result;
  } catch (const std::exception &e) {
    return Status::RedisError(absl::StrCat(
        "Failed to obtain an Entra ID token via the Azure Identity SDK: ", e.what()));
  }
}

EntraRedisAuthProvider::EntraRedisAuthProvider(ClockInterface &clock,
                                               EntraAuthConfig config,
                                               TokenFetcher token_fetcher)
    : clock_(clock),
      config_(std::move(config)),
      token_fetcher_(std::move(token_fetcher)) {}

StatusOr<RedisCredentials> EntraRedisAuthProvider::GetCredentials() {
  absl::MutexLock lock(&mu_);
  // Refresh if we have no token yet or are within the refresh buffer of expiry.
  if (!has_token_ || clock_.Now() + config_.refresh_buffer >= cached_.expiry) {
    return RefreshLocked();
  }
  return cached_;
}

StatusOr<RedisCredentials> EntraRedisAuthProvider::RefreshLocked() {
  StatusOr<EntraAccessToken> token = token_fetcher_(config_);
  RAY_RETURN_NOT_OK(token.status());

  std::string username = config_.username;
  if (username.empty()) {
    StatusOr<std::string> oid = ExtractOidFromJwt(token->token);
    RAY_RETURN_NOT_OK(oid.status());
    username = *oid;
  }

  cached_ = RedisCredentials{std::move(username), std::move(token->token), token->expiry};
  has_token_ = true;
  RAY_LOG(INFO) << "Refreshed Entra ID Redis token; valid until "
                << absl::FormatTime(cached_.expiry);
  return cached_;
}

}  // namespace gcs
}  // namespace ray
