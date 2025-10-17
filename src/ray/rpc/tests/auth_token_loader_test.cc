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
#include <thread>

#include "gtest/gtest.h"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {

class RayAuthTokenLoaderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Clean up environment variables before each test
    unsetenv("RAY_AUTH_TOKEN");
    unsetenv("RAY_AUTH_TOKEN_PATH");

    // Clean up default token file
    std::string home_dir = getenv("HOME");
    default_token_path_ = home_dir + "/.ray/auth_token";
    remove(default_token_path_.c_str());
  }

  void TearDown() override {
    // Clean up after test
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
  std::string token = loader.GetToken(false);

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
  std::string token = loader.GetToken(false);

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
  std::string token = loader.GetToken(false);

  EXPECT_EQ(token, "test-token-from-default");
  EXPECT_TRUE(loader.HasToken());
}

TEST_F(RayAuthTokenLoaderTest, TestPrecedenceOrder) {
  // Set all three sources
  setenv("RAY_AUTH_TOKEN", "token-from-env", 1);

  std::string temp_token_path = "/tmp/ray_test_token_" + std::to_string(getpid());
  std::ofstream temp_file(temp_token_path);
  temp_file << "token-from-path";
  temp_file.close();
  setenv("RAY_AUTH_TOKEN_PATH", temp_token_path.c_str(), 1);

  std::string ray_dir = std::string(getenv("HOME")) + "/.ray";
  mkdir(ray_dir.c_str(), 0700);
  std::ofstream default_file(default_token_path_);
  default_file << "token-from-default";
  default_file.close();

  // Environment variable should have highest precedence
  auto &loader = RayAuthTokenLoader::instance();
  std::string token = loader.GetToken(false);

  EXPECT_EQ(token, "token-from-env");

  // Clean up
  remove(temp_token_path.c_str());
}

TEST_F(RayAuthTokenLoaderTest, TestNoTokenFound) {
  // No token set anywhere
  auto &loader = RayAuthTokenLoader::instance();
  std::string token = loader.GetToken(false);

  EXPECT_EQ(token, "");
  EXPECT_FALSE(loader.HasToken());
}

TEST_F(RayAuthTokenLoaderTest, TestGenerateToken) {
  // No token exists, but request generation
  auto &loader = RayAuthTokenLoader::instance();
  std::string token = loader.GetToken(true);

  // Token should be generated (32 character hex string)
  EXPECT_EQ(token.length(), 32);
  EXPECT_TRUE(loader.HasToken());

  // Token should be saved to default path
  std::ifstream token_file(default_token_path_);
  EXPECT_TRUE(token_file.is_open());
  std::string saved_token;
  std::getline(token_file, saved_token);
  EXPECT_EQ(saved_token, token);
}

TEST_F(RayAuthTokenLoaderTest, TestCaching) {
  // Set token in environment
  setenv("RAY_AUTH_TOKEN", "cached-token", 1);

  auto &loader = RayAuthTokenLoader::instance();
  std::string token1 = loader.GetToken(false);

  // Change environment variable (shouldn't affect cached value)
  setenv("RAY_AUTH_TOKEN", "new-token", 1);
  std::string token2 = loader.GetToken(false);

  // Should still return the cached token
  EXPECT_EQ(token1, token2);
  EXPECT_EQ(token2, "cached-token");
}

TEST_F(RayAuthTokenLoaderTest, TestThreadSafety) {
  // Set a token
  setenv("RAY_AUTH_TOKEN", "thread-safe-token", 1);

  auto &loader = RayAuthTokenLoader::instance();

  // Create multiple threads that try to get token simultaneously
  std::vector<std::thread> threads;
  std::vector<std::string> results(10);

  for (int i = 0; i < 10; i++) {
    threads.emplace_back(
        [&loader, &results, i]() { results[i] = loader.GetToken(false); });
  }

  // Wait for all threads to complete
  for (auto &thread : threads) {
    thread.join();
  }

  // All threads should get the same token
  for (const auto &result : results) {
    EXPECT_EQ(result, "thread-safe-token");
  }
}

TEST_F(RayAuthTokenLoaderTest, TestWhitespaceHandling) {
  // Create token file with whitespace
  std::string ray_dir = std::string(getenv("HOME")) + "/.ray";
  mkdir(ray_dir.c_str(), 0700);
  std::ofstream token_file(default_token_path_);
  token_file << "  token-with-spaces  \n\t";
  token_file.close();

  auto &loader = RayAuthTokenLoader::instance();
  std::string token = loader.GetToken(false);

  // Whitespace should be trimmed
  EXPECT_EQ(token, "token-with-spaces");
}

}  // namespace rpc
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
