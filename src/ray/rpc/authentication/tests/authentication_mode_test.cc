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

#include "ray/rpc/authentication/authentication_mode.h"

#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/ray_config.h"

namespace ray {
namespace rpc {

class AuthenticationModeTest : public ::testing::Test {
 protected:
  void TearDown() override { RayConfig::instance().AUTH_MODE() = "token"; }
};

TEST_F(AuthenticationModeTest, AcceptsTokenAndDisabledInAnyCase) {
  const std::vector<std::pair<std::string, AuthenticationMode>> cases = {
      {"token", AuthenticationMode::TOKEN},
      {"TOKEN", AuthenticationMode::TOKEN},
      {"disabled", AuthenticationMode::DISABLED},
      {"Disabled", AuthenticationMode::DISABLED},
  };
  for (const auto &[mode, expected] : cases) {
    RayConfig::instance().AUTH_MODE() = mode;
    EXPECT_EQ(InvalidAuthenticationModeError(), "") << mode;
    EXPECT_EQ(GetAuthenticationMode(), expected) << mode;
  }
}

TEST_F(AuthenticationModeTest, RejectsAnyOtherValue) {
  for (const std::string mode : {"tokn", "none", ""}) {
    RayConfig::instance().AUTH_MODE() = mode;
    EXPECT_EQ(InvalidAuthenticationModeError(),
              "Invalid RAY_AUTH_MODE '" + mode + "': set it to 'token' or 'disabled'.");
    EXPECT_DEATH(GetAuthenticationMode(), "Invalid RAY_AUTH_MODE");
  }
}

}  // namespace rpc
}  // namespace ray
