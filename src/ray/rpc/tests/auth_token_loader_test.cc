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
#include <direct.h>   // For _mkdir on Windows
#include <process.h>  // For _getpid on Windows
#endif

namespace ray {
namespace rpc {

class RayAuthTokenLoaderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Enable token authentication for tests
    RayConfig::instance().initialize(R"({"enable_token_auth": true})");

    // If HOME is not set (e.g., in Bazel sandbox), set it to a test directory
    // This ensures tests work in environments where HOME isn't provided
#ifdef _WIN32
    if (std::getenv("USERPROFILE") == nullptr) {
      const char *test_tmpdir = std::getenv("TEST_TMPDIR");
      if (test_tmpdir != nullptr) {
        test_home_dir_ = std::string(test_tmpdir) + "\\ray_test_home";
      } else {
        test_home_dir_ = "C:\\Windows\\Temp\\ray_test_home";
      }
      _putenv(("USERPROFILE=" + test_home_dir_).c_str());
    }
    const char *home_dir = std::getenv("USERPROFILE");
    default_token_path_ = std::string(home_dir) + "\\.ray\\auth_token";
#else
    if (std::getenv("HOME") == nullptr) {
      const char *test_tmpdir = std::getenv("TEST_TMPDIR");
      if (test_tmpdir != nullptr) {
        test_home_dir_ = std::string(test_tmpdir) + "/ray_test_home";
      } else {
        test_home_dir_ = "/tmp/ray_test_home";
      }
      setenv("HOME", test_home_dir_.c_str(), 1);
    }
    const char *home_dir = std::getenv("HOME");
    if (home_dir != nullptr) {
      default_token_path_ = std::string(home_dir) + "/.ray/auth_token";
      test_home_dir_ = home_dir;
    } else {
      default_token_path_ = ".ray/auth_token";
    }
#endif
    cleanup_env();
    // Reset the singleton's cached state for test isolation
    RayAuthTokenLoader::instance().ResetCache();
  }

  void TearDown() override {
    // Clean up after test
    cleanup_env();
    // Reset the singleton's cached state for test isolation
    RayAuthTokenLoader::instance().ResetCache();
    // Disable token auth after tests
    RayConfig::instance().initialize(R"({"enable_token_auth": false})");
  }

  void cleanup_env() {
    unset_env_var("RAY_AUTH_TOKEN");
    unset_env_var("RAY_AUTH_TOKEN_PATH");
    remove(default_token_path_.c_str());
  }

  std::string get_temp_token_path() {
#ifdef _WIN32
    return "C:\\Windows\\Temp\\ray_test_token_" + std::to_string(_getpid());
#else
    return "/tmp/ray_test_token_" + std::to_string(getpid());
#endif
  }

  void set_env_var(const char *name, const char *value) {
#ifdef _WIN32
    std::string env_str = std::string(name) + "=" + std::string(value);
    _putenv(env_str.c_str());
#else
    setenv(name, value, 1);
#endif
  }

  void unset_env_var(const char *name) {
#ifdef _WIN32
    std::string env_str = std::string(name) + "=";
    _putenv(env_str.c_str());
#else
    unsetenv(name);
#endif
  }

  void ensure_ray_dir_exists() {
#ifdef _WIN32
    const char *home_dir = std::getenv("USERPROFILE");
    _mkdir(home_dir);  // Create parent directory
    std::string ray_dir = std::string(home_dir) + "\\.ray";
    _mkdir(ray_dir.c_str());
#else
    // Always ensure the home directory exists (it might be a test temp dir we created)
    if (!test_home_dir_.empty()) {
      mkdir(test_home_dir_.c_str(),
            0700);  // Create if it doesn't exist (ignore error if it does)
    }

    const char *home_dir = std::getenv("HOME");
    if (home_dir != nullptr) {
      std::string ray_dir = std::string(home_dir) + "/.ray";
      mkdir(ray_dir.c_str(), 0700);
    }
#endif
  }

  void write_token_file(const std::string &path, const std::string &content) {
    std::ofstream token_file(path);
    token_file << content;
    token_file.close();
  }

  std::string default_token_path_;
  std::string test_home_dir_;  // Fallback home directory for tests
};

TEST_F(RayAuthTokenLoaderTest, TestLoadFromEnvVariable) {
  // Set token in environment variable
  set_env_var("RAY_AUTH_TOKEN", "test-token-from-env");

  // Create a new instance to avoid cached state
  auto &loader = RayAuthTokenLoader::instance();
  std::string token = loader.GetToken();

  EXPECT_EQ(token, "test-token-from-env");
  EXPECT_TRUE(loader.HasToken());
}

TEST_F(RayAuthTokenLoaderTest, TestLoadFromEnvPath) {
  // Create a temporary token file
  std::string temp_token_path = get_temp_token_path();
  write_token_file(temp_token_path, "test-token-from-file");

  // Set path in environment variable
  set_env_var("RAY_AUTH_TOKEN_PATH", temp_token_path.c_str());

  auto &loader = RayAuthTokenLoader::instance();
  std::string token = loader.GetToken();

  EXPECT_EQ(token, "test-token-from-file");
  EXPECT_TRUE(loader.HasToken());

  // Clean up
  remove(temp_token_path.c_str());
}

TEST_F(RayAuthTokenLoaderTest, TestLoadFromDefaultPath) {
  // Create directory and token file in default location
  ensure_ray_dir_exists();
  write_token_file(default_token_path_, "test-token-from-default");

  auto &loader = RayAuthTokenLoader::instance();
  std::string token = loader.GetToken();

  EXPECT_EQ(token, "test-token-from-default");
  EXPECT_TRUE(loader.HasToken());
}

// Parametrized test for token loading precedence: env var > user-specified file > default
// file

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
    set_env_var("RAY_AUTH_TOKEN", param.env_token.c_str());
  } else {
    unset_env_var("RAY_AUTH_TOKEN");
  }

  // Optionally create file and set path
  std::string temp_token_path = get_temp_token_path();
  if (param.set_file) {
    write_token_file(temp_token_path, param.file_token);
    set_env_var("RAY_AUTH_TOKEN_PATH", temp_token_path.c_str());
  } else {
    unset_env_var("RAY_AUTH_TOKEN_PATH");
  }

  // Optionally create default file
  ensure_ray_dir_exists();
  if (param.set_default) {
    write_token_file(default_token_path_, param.default_token);
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
  // Disable auth for this specific test
  RayConfig::instance().initialize(R"({"enable_token_auth": false})");
  RayAuthTokenLoader::instance().ResetCache();

  // No token set anywhere, but auth is disabled
  auto &loader = RayAuthTokenLoader::instance();
  std::string token = loader.GetToken();

  EXPECT_EQ(token, "");
  EXPECT_FALSE(loader.HasToken());

  // Re-enable for other tests
  RayConfig::instance().initialize(R"({"enable_token_auth": true})");
}

TEST_F(RayAuthTokenLoaderTest, TestErrorWhenAuthEnabledButNoToken) {
  // Token auth is already enabled in SetUp()
  // No token exists, should throw an error
  auto &loader = RayAuthTokenLoader::instance();
  EXPECT_THROW(loader.GetToken(), std::runtime_error);
}

TEST_F(RayAuthTokenLoaderTest, TestCaching) {
  // Set token in environment
  set_env_var("RAY_AUTH_TOKEN", "cached-token");

  auto &loader = RayAuthTokenLoader::instance();
  std::string token1 = loader.GetToken();

  // Change environment variable (shouldn't affect cached value)
  set_env_var("RAY_AUTH_TOKEN", "new-token");
  std::string token2 = loader.GetToken();

  // Should still return the cached token
  EXPECT_EQ(token1, token2);
  EXPECT_EQ(token2, "cached-token");
}

TEST_F(RayAuthTokenLoaderTest, TestWhitespaceHandling) {
  // Create token file with whitespace
  ensure_ray_dir_exists();
  write_token_file(default_token_path_, "  token-with-spaces  \n\t");

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
