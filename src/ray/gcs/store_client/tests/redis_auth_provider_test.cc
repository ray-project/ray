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

#include <string>

#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "gtest/gtest.h"
#include "ray/util/clock.h"

namespace ray {
namespace gcs {

namespace {

// Builds a (signature-less, but structurally valid) JWT whose payload is the
// given JSON object string.
std::string MakeJwt(const std::string &payload_json) {
  std::string header_b64;
  absl::WebSafeBase64Escape(R"({"alg":"none"})", &header_b64);
  std::string payload_b64;
  absl::WebSafeBase64Escape(payload_json, &payload_b64);
  return absl::StrCat(header_b64, ".", payload_b64, ".sig");
}

}  // namespace

TEST(RedisAuthProviderTest, StaticProviderReturnsFixedCredentials) {
  StaticRedisAuthProvider provider("user", "pass");
  EXPECT_FALSE(provider.IsRefreshable());

  auto creds = provider.GetCredentials();
  ASSERT_TRUE(creds.ok());
  EXPECT_EQ(creds->username, "user");
  EXPECT_EQ(creds->password, "pass");
  EXPECT_EQ(creds->expiry, absl::InfiniteFuture());
}

TEST(RedisAuthProviderTest, ExtractOidFromJwt) {
  std::string jwt =
      MakeJwt(R"({"oid":"11111111-2222-3333-4444-555555555555","aud":"x"})");
  auto oid = ExtractOidFromJwt(jwt);
  ASSERT_TRUE(oid.ok());
  EXPECT_EQ(*oid, "11111111-2222-3333-4444-555555555555");
}

TEST(RedisAuthProviderTest, ExtractOidFromJwtErrors) {
  // Not three segments.
  EXPECT_FALSE(ExtractOidFromJwt("only.two").ok());
  // No oid claim.
  EXPECT_FALSE(ExtractOidFromJwt(MakeJwt(R"({"sub":"x"})")).ok());
}

TEST(RedisAuthProviderTest, EntraProviderFetchesCachesAndExtractsOid) {
  FakeClock clock(absl::FromUnixSeconds(1000));
  int fetch_count = 0;
  std::string jwt = MakeJwt(R"({"oid":"the-object-id"})");

  EntraAuthConfig config;
  config.refresh_buffer = absl::Seconds(300);
  // username left empty -> oid claim is used.

  EntraRedisAuthProvider provider(
      clock, config, [&](const EntraAuthConfig &) -> StatusOr<EntraAccessToken> {
        ++fetch_count;
        return EntraAccessToken{jwt, absl::FromUnixSeconds(4600)};
      });

  EXPECT_TRUE(provider.IsRefreshable());

  auto creds = provider.GetCredentials();
  ASSERT_TRUE(creds.ok());
  EXPECT_EQ(creds->username, "the-object-id");
  EXPECT_EQ(creds->password, jwt);
  EXPECT_EQ(creds->expiry, absl::FromUnixSeconds(4600));
  EXPECT_EQ(fetch_count, 1);

  // A second call well before expiry returns the cached token (no new fetch).
  ASSERT_TRUE(provider.GetCredentials().ok());
  EXPECT_EQ(fetch_count, 1);
}

TEST(RedisAuthProviderTest, EntraProviderRefreshesNearExpiry) {
  FakeClock clock(absl::FromUnixSeconds(1000));
  int fetch_count = 0;

  EntraAuthConfig config;
  config.username = "explicit-user";  // skip oid extraction
  config.refresh_buffer = absl::Seconds(300);

  EntraRedisAuthProvider provider(
      clock, config, [&](const EntraAuthConfig &) -> StatusOr<EntraAccessToken> {
        ++fetch_count;
        return EntraAccessToken{absl::StrCat("token-", fetch_count),
                                absl::FromUnixSeconds(1000 + fetch_count * 3600)};
      });

  auto creds = provider.GetCredentials();
  ASSERT_TRUE(creds.ok());
  EXPECT_EQ(creds->username, "explicit-user");
  EXPECT_EQ(creds->password, "token-1");
  EXPECT_EQ(creds->expiry, absl::FromUnixSeconds(4600));
  EXPECT_EQ(fetch_count, 1);

  // Move to just outside the refresh buffer: still cached.
  clock.SetTime(absl::FromUnixSeconds(4299));  // 4299 + 300 < 4600
  ASSERT_TRUE(provider.GetCredentials().ok());
  EXPECT_EQ(fetch_count, 1);

  // Move inside the refresh buffer: a fresh token is fetched.
  clock.SetTime(absl::FromUnixSeconds(4400));  // 4400 + 300 >= 4600
  creds = provider.GetCredentials();
  ASSERT_TRUE(creds.ok());
  EXPECT_EQ(creds->password, "token-2");
  EXPECT_EQ(creds->expiry, absl::FromUnixSeconds(8200));
  EXPECT_EQ(fetch_count, 2);
}

TEST(RedisAuthProviderTest, EntraProviderPropagatesFetchError) {
  FakeClock clock(absl::FromUnixSeconds(1000));
  EntraAuthConfig config;
  config.username = "explicit-user";
  EntraRedisAuthProvider provider(
      clock, config, [&](const EntraAuthConfig &) -> StatusOr<EntraAccessToken> {
        return Status::RedisError("token acquisition failed");
      });
  auto creds = provider.GetCredentials();
  EXPECT_FALSE(creds.ok());
}

}  // namespace gcs
}  // namespace ray
