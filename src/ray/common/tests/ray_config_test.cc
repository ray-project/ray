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

#include "ray/common/ray_config.h"

#include <cstdlib>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/grpc_util.h"
#include "ray/util/env.h"

namespace ray {
class RayConfigTest : public ::testing::Test {};

TEST_F(RayConfigTest, ConvertValueTrimsVectorElements) {
  const std::string type_string = "std::vector";
  const std::string input = "no_spaces, with spaces ";
  const std::vector<std::string> expected_output{"no_spaces", "with spaces"};
  auto output = ConvertValue<std::vector<std::string>>(type_string, input);
  ASSERT_EQ(output, expected_output);
}

TEST_F(RayConfigTest, RejectsZeroDeadNodeCacheCapacity) {
  EXPECT_DEATH(
      RayConfig::instance().initialize(R"({"maximum_gcs_dead_node_cached_count": 0})"),
      "maximum_gcs_dead_node_cached_count must be greater than zero");
}

TEST_F(RayConfigTest, RejectsZeroDestroyedActorCacheCapacity) {
  EXPECT_DEATH(RayConfig::instance().initialize(
                   R"({"maximum_gcs_destroyed_actor_cached_count": 0})"),
               "maximum_gcs_destroyed_actor_cached_count must be greater than zero");
}

TEST_F(RayConfigTest, PositiveCacheCapacitiesOverrideZeroEnvironmentValues) {
  // Re-execute in a fresh process so the environment is set before first singleton
  // access.
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_EXIT(
      {
        SetEnv("RAY_maximum_gcs_dead_node_cached_count", "0");
        SetEnv("RAY_maximum_gcs_destroyed_actor_cached_count", "0");
        auto &config = RayConfig::instance();
        config.initialize(R"({"maximum_gcs_dead_node_cached_count": 1,
                              "maximum_gcs_destroyed_actor_cached_count": 1})");
        std::_Exit(config.maximum_gcs_dead_node_cached_count() == 1 &&
                           config.maximum_gcs_destroyed_actor_cached_count() == 1
                       ? 0
                       : 1);
      },
      ::testing::ExitedWithCode(0),
      "");
}

TEST_F(RayConfigTest, RejectsZeroDeadNodeCacheCapacityFromEnvironment) {
  for (const auto &config_list : {"", "{}"}) {
    EXPECT_DEATH(
        {
          SetEnv("RAY_maximum_gcs_dead_node_cached_count", "0");
          RayConfig::instance().initialize(config_list);
        },
        "maximum_gcs_dead_node_cached_count must be greater than zero");
  }
}

TEST_F(RayConfigTest, RejectsZeroDestroyedActorCacheCapacityFromEnvironment) {
  for (const auto &config_list : {"", "{}"}) {
    EXPECT_DEATH(
        {
          SetEnv("RAY_maximum_gcs_destroyed_actor_cached_count", "0");
          RayConfig::instance().initialize(config_list);
        },
        "maximum_gcs_destroyed_actor_cached_count must be greater than zero");
  }
}

}  // namespace ray
