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

#include "ray/rpc/auth_token_loader.h"

#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/ray_config.h"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {

class RayAuthTokenLoaderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Clean up environment variables before each test
    std::string home_dir = getenv("HOME");
    default_token_path_ = home_dir + "/.ray/auth_token";
    cleanup_env();
    // Reset the singleton's cached state for test isolation
    RayAuthTokenLoader::instance().ResetForTesting();
  }

  void TearDown() override {
    // Clean up after test
    cleanup_env();
    // Reset the singleton's cached state for test isolation
    RayAuthTokenLoader::instance().ResetForTesting();
  }

  void cleanup_env() {
    unsetenv("RAY_AUTH_TOKEN");
    unsetenv("RAY_AUTH_TOKEN_PATH");
    remove(default_token_path_.c_str());
  }

  std::string default_token_path_;
};

TEST_F(RayAuthTokenLoaderTest, TestLoadFromEnvVariable) {
  // Set token in environment variable
  setenv("RAY_AUTH_TOKEN", "test-token-from-env", 1);

  // Create a new instance to avoid cached state
  auto &loader = RayAuthTokenLoader::instance();
  std::string token = loader.GetToken();

  EXPECT_EQ(token, "test-token-from-env");
  EXPECT_TRUE(loader.HasToken());
}

TEST_F(RayAuthTokenLoaderTest, TestLoadFromEnvPath) {
  // Create a temporary token file
  std::string temp_token_path = "/tmp/ray_test_token_" + std::to_string(getpid());
  std::ofstream token_file(temp_token_path);
  token_file << "test-token-from-file";
  token_file.close();

  // Set path in environment variable
  setenv("RAY_AUTH_TOKEN_PATH", temp_token_path.c_str(), 1);

  auto &loader = RayAuthTokenLoader::instance();
  std::string token = loader.GetToken();

  EXPECT_EQ(token, "test-token-from-file");
  EXPECT_TRUE(loader.HasToken());

  // Clean up
  remove(temp_token_path.c_str());
}

TEST_F(RayAuthTokenLoaderTest, TestLoadFromDefaultPath) {
  // Create directory
  std::string ray_dir = std::string(getenv("HOME")) + "/.ray";
  mkdir(ray_dir.c_str(), 0700);

  // Create token file in default location
  std::ofstream token_file(default_token_path_);
  token_file << "test-token-from-default";
  token_file.close();

  auto &loader = RayAuthTokenLoader::instance();
  std::string token = loader.GetToken();

  EXPECT_EQ(token, "test-token-from-default");
  EXPECT_TRUE(loader.HasToken());
}

// Parametrized test for token loading precedence: env var > file > default file

struct TokenSourceConfig {
  bool set_env = false;
  bool set_file = false;
  bool set_default = false;
  std::string expected_token;
  std::string env_token = "token-from-env";
  std::string file_token = "token-from-path";
  std::string default_token = "token-from-default";
};

class RayAuthTokenLoaderPrecedenceTest
    : public RayAuthTokenLoaderTest,
      public ::testing::WithParamInterface<TokenSourceConfig> {};

INSTANTIATE_TEST_SUITE_P(TokenPrecedenceCases,
                         RayAuthTokenLoaderPrecedenceTest,
                         ::testing::Values(
                             // All set: env should win
                             TokenSourceConfig{true, true, true, "token-from-env"},
                             // File and default file set: file should win
                             TokenSourceConfig{false, true, true, "token-from-path"},
                             // Only default file set
                             TokenSourceConfig{
                                 false, false, true, "token-from-default"}));

TEST_P(RayAuthTokenLoaderPrecedenceTest, Precedence) {
  const auto &param = GetParam();

  // Optionally set environment variable
  if (param.set_env) {
    setenv("RAY_AUTH_TOKEN", param.env_token.c_str(), 1);
  } else {
    unsetenv("RAY_AUTH_TOKEN");
  }

  // Optionally create file and set path
  std::string temp_token_path = "/tmp/ray_test_token_" + std::to_string(getpid());
  if (param.set_file) {
    std::ofstream token_file(temp_token_path);
    token_file << param.file_token;
    token_file.close();
    setenv("RAY_AUTH_TOKEN_PATH", temp_token_path.c_str(), 1);
  } else {
    unsetenv("RAY_AUTH_TOKEN_PATH");
  }

  // Optionally create default file
  std::string ray_dir = std::string(getenv("HOME")) + "/.ray";
  mkdir(ray_dir.c_str(), 0700);
  if (param.set_default) {
    std::ofstream default_file(default_token_path_);
    default_file << param.default_token;
    default_file.close();
  } else {
    remove(default_token_path_.c_str());
  }

  // Always create a new instance to avoid cached state
  auto &loader = RayAuthTokenLoader::instance();
  std::string token = loader.GetToken();

  EXPECT_EQ(token, param.expected_token);

  // Clean up token file if it was written
  if (param.set_file) {
    remove(temp_token_path.c_str());
  }
  // Clean up default file if it was written
  if (param.set_default) {
    remove(default_token_path_.c_str());
  }
}

TEST_F(RayAuthTokenLoaderTest, TestNoTokenFoundWhenAuthDisabled) {
  // No token set anywhere, but auth is disabled (default)
  auto &loader = RayAuthTokenLoader::instance();
  std::string token = loader.GetToken();

  EXPECT_EQ(token, "");
  EXPECT_FALSE(loader.HasToken());
}

TEST_F(RayAuthTokenLoaderTest, TestErrorWhenAuthEnabledButNoToken) {
  // Enable token auth
  RayConfig::instance().initialize(R"({"enable_token_auth": true})");

  // No token exists, should throw an error
  auto &loader = RayAuthTokenLoader::instance();
  EXPECT_THROW(loader.GetToken(), std::runtime_error);

  // Reset config for other tests
  RayConfig::instance().initialize(R"({"enable_token_auth": false})");
}

TEST_F(RayAuthTokenLoaderTest, TestCaching) {
  // Set token in environment
  setenv("RAY_AUTH_TOKEN", "cached-token", 1);

  auto &loader = RayAuthTokenLoader::instance();
  std::string token1 = loader.GetToken();

  // Change environment variable (shouldn't affect cached value)
  setenv("RAY_AUTH_TOKEN", "new-token", 1);
  std::string token2 = loader.GetToken();

  // Should still return the cached token
  EXPECT_EQ(token1, token2);
  EXPECT_EQ(token2, "cached-token");
}

TEST_F(RayAuthTokenLoaderTest, TestWhitespaceHandling) {
  // Create token file with whitespace
  std::string ray_dir = std::string(getenv("HOME")) + "/.ray";
  mkdir(ray_dir.c_str(), 0700);
  std::ofstream token_file(default_token_path_);
  token_file << "  token-with-spaces  \n\t";
  token_file.close();

  auto &loader = RayAuthTokenLoader::instance();
  std::string token = loader.GetToken();

  // Whitespace should be trimmed
  EXPECT_EQ(token, "token-with-spaces");
}

}  // namespace rpc
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
