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

#include "ray/common/runtime_env_redaction.h"

#include <string>
#include <string_view>

#include "gtest/gtest.h"
#include "nlohmann/json.hpp"

namespace ray {

namespace {

constexpr std::string_view kSecret = "supersecretvalue";

/**
 * @brief Builds a serialized runtime env holding secrets under `env_vars`.
 *
 * @return A JSON runtime env with two `env_vars` set to `kSecret` and a
 *         `working_dir` that redaction must leave alone.
 */
std::string MakeSerializedRuntimeEnv() {
  return nlohmann::json{
      {"env_vars",
       {{"AWS_SECRET_ACCESS_KEY", std::string(kSecret)},
        {"DB_PASSWORD", std::string(kSecret)}}},
      {"working_dir", "gcs://_ray_pkg_abc123.zip"},
  }
      .dump();
}

}  // namespace

TEST(RuntimeEnvRedactionTest, MasksEnvVarValuesAndKeepsKeys) {
  auto redacted =
      nlohmann::json::parse(RedactSerializedRuntimeEnv(MakeSerializedRuntimeEnv()));

  EXPECT_EQ(redacted["env_vars"],
            (nlohmann::json{{"AWS_SECRET_ACCESS_KEY", std::string(kRedactedPlaceholder)},
                            {"DB_PASSWORD", std::string(kRedactedPlaceholder)}}));
  EXPECT_EQ(redacted["working_dir"], "gcs://_ray_pkg_abc123.zip");
}

TEST(RuntimeEnvRedactionTest, MasksRuntimeEnvContext) {
  // A serialized `RuntimeEnvContext` keeps `env_vars` at the top level too.
  std::string context =
      nlohmann::json{
          {"command_prefix", nlohmann::json::array()},
          {"env_vars", {{"API_TOKEN", std::string(kSecret)}}},
          {"py_executable", "python"},
      }
          .dump();

  std::string redacted = RedactSerializedRuntimeEnv(context);

  EXPECT_EQ(redacted.find(kSecret), std::string::npos);
  EXPECT_EQ(nlohmann::json::parse(redacted)["py_executable"], "python");
}

TEST(RuntimeEnvRedactionTest, LeavesRuntimeEnvWithoutEnvVarsUnchanged) {
  // The exact input string is returned, not a re-serialized copy.
  for (const std::string &serialized :
       {std::string(R"({})"),
        std::string(R"({"working_dir":"gcs://_ray_pkg_abc123.zip","pip":["requests"]})"),
        std::string(R"({"pip": ["requests"], "env_vars": {}})"),
        std::string(R"({"env_vars": null})"),
        std::string(R"({"env_vars": ["not", "a", "dict"]})")}) {
    EXPECT_EQ(RedactSerializedRuntimeEnv(serialized), serialized);
  }
}

TEST(RuntimeEnvRedactionTest, PassesThroughEmptyString) {
  EXPECT_EQ(RedactSerializedRuntimeEnv(""), "");
}

TEST(RuntimeEnvRedactionTest, FailsClosed) {
  for (const std::string &serialized :
       {std::string("not json"),
        std::string(R"({"env_vars": {"KEY": ")").append(kSecret),
        std::string(R"(["env_vars"])"),
        std::string(R"("a string")")}) {
    EXPECT_EQ(RedactSerializedRuntimeEnv(serialized), kRedactedPlaceholder) << serialized;
  }
}

}  // namespace ray
