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

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <cctype>
#include <chrono>
#include <cstdint>
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

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
using tcp = net::ip::tcp;

// Reads a JSON field that IMDS may encode either as a string or as a number,
// returning it as an int64. Returns false if the field is missing or not an
// integer-convertible value.
bool GetJsonInt64(const nlohmann::json &obj, const char *key, int64_t *out) {
  if (!obj.contains(key)) {
    return false;
  }
  const auto &value = obj.at(key);
  try {
    if (value.is_number()) {
      *out = value.get<int64_t>();
      return true;
    }
    if (value.is_string()) {
      *out = std::stoll(value.get<std::string>());
      return true;
    }
  } catch (const std::exception &) {
    return false;
  }
  return false;
}

// Percent-encodes a query-parameter value (RFC 3986 unreserved chars stay).
std::string UrlEncode(const std::string &value) {
  static const char kHex[] = "0123456789ABCDEF";
  std::string out;
  out.reserve(value.size());
  for (unsigned char c : value) {
    if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
      out.push_back(static_cast<char>(c));
    } else {
      out.push_back('%');
      out.push_back(kHex[c >> 4]);
      out.push_back(kHex[c & 0xF]);
    }
  }
  return out;
}

// Splits "http://host[:port]/path" into its components. Returns false if the
// URL is not a plain-HTTP absolute URL.
bool ParseHttpUrl(const std::string &url,
                  std::string *host,
                  std::string *port,
                  std::string *path) {
  const std::string kScheme = "http://";
  if (url.size() < kScheme.size() || url.compare(0, kScheme.size(), kScheme) != 0) {
    return false;
  }
  std::string rest = url.substr(kScheme.size());
  auto slash = rest.find('/');
  std::string authority = slash == std::string::npos ? rest : rest.substr(0, slash);
  *path = slash == std::string::npos ? "/" : rest.substr(slash);
  auto colon = authority.find(':');
  if (colon == std::string::npos) {
    *host = authority;
    *port = "80";
  } else {
    *host = authority.substr(0, colon);
    *port = authority.substr(colon + 1);
  }
  return !host->empty();
}

}  // namespace

Status ParseImdsTokenResponse(const std::string &body,
                              absl::Time now,
                              std::string *access_token,
                              absl::Time *expiry) {
  nlohmann::json json;
  try {
    json = nlohmann::json::parse(body);
  } catch (const std::exception &e) {
    return Status::RedisError(
        absl::StrCat("Failed to parse IMDS token response as JSON: ", e.what()));
  }
  if (!json.contains("access_token") || !json.at("access_token").is_string()) {
    return Status::RedisError(
        "IMDS token response is missing a string `access_token` field.");
  }
  *access_token = json.at("access_token").get<std::string>();
  if (access_token->empty()) {
    return Status::RedisError("IMDS token response contains an empty `access_token`.");
  }

  int64_t seconds = 0;
  if (GetJsonInt64(json, "expires_on", &seconds)) {
    *expiry = absl::FromUnixSeconds(seconds);
  } else if (GetJsonInt64(json, "expires_in", &seconds)) {
    *expiry = now + absl::Seconds(seconds);
  } else {
    return Status::RedisError(
        "IMDS token response is missing both `expires_on` and `expires_in`.");
  }
  return Status::OK();
}

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

StatusOr<std::string> EntraRedisAuthProvider::DefaultTokenFetcher(
    const EntraAuthConfig &config) {
  std::string host, port, path;
  if (!ParseHttpUrl(config.imds_endpoint, &host, &port, &path)) {
    return Status::RedisError(absl::StrCat(
        "Invalid IMDS endpoint (expected http:// URL): ", config.imds_endpoint));
  }

  // IMDS requires api-version and resource; client_id selects a user-assigned
  // managed identity when provided.
  std::string target =
      absl::StrCat(path, "?api-version=2018-02-01&resource=", UrlEncode(config.resource));
  if (!config.client_id.empty()) {
    absl::StrAppend(&target, "&client_id=", UrlEncode(config.client_id));
  }

  try {
    net::io_context ioc;
    tcp::resolver resolver(ioc);
    beast::tcp_stream stream(ioc);
    stream.expires_after(
        std::chrono::milliseconds(absl::ToInt64Milliseconds(config.request_timeout)));

    auto const results = resolver.resolve(host, port);
    stream.connect(results);

    http::request<http::string_body> req{http::verb::get, target, 11};
    req.set(http::field::host, host);
    req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    // IMDS mandates this header as an anti-SSRF measure.
    req.set("Metadata", "true");

    http::write(stream, req);

    beast::flat_buffer buffer;
    http::response<http::string_body> res;
    http::read(stream, buffer, res);

    beast::error_code ec;
    stream.socket().shutdown(tcp::socket::shutdown_both, ec);

    if (res.result() != http::status::ok) {
      return Status::RedisError(absl::StrCat("IMDS token request returned HTTP status ",
                                             res.result_int(),
                                             ". Response: ",
                                             res.body()));
    }
    return res.body();
  } catch (const std::exception &e) {
    return Status::RedisError(absl::StrCat("IMDS token request failed: ", e.what()));
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
  StatusOr<std::string> body = token_fetcher_(config_);
  RAY_RETURN_NOT_OK(body.status());

  std::string access_token;
  absl::Time expiry;
  RAY_RETURN_NOT_OK(ParseImdsTokenResponse(*body, clock_.Now(), &access_token, &expiry));

  std::string username = config_.username;
  if (username.empty()) {
    StatusOr<std::string> oid = ExtractOidFromJwt(access_token);
    RAY_RETURN_NOT_OK(oid.status());
    username = *oid;
  }

  cached_ = RedisCredentials{std::move(username), std::move(access_token), expiry};
  has_token_ = true;
  RAY_LOG(INFO) << "Refreshed Entra ID Redis token; valid until "
                << absl::FormatTime(expiry);
  return cached_;
}

}  // namespace gcs
}  // namespace ray
