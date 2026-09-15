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

#include "ray/util/port_persistence.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <variant>

namespace ray {

class PortPersistenceTest : public ::testing::TestWithParam<const char *> {};

TEST_P(PortPersistenceTest, TimeoutMessageIncludesProcessStartupHint) {
  auto test_dir =
      std::filesystem::temp_directory_path() / "port_persistence_timeout_test";
  std::filesystem::create_directories(test_dir);

  auto result = WaitForPersistedPort(test_dir.string(),
                                     NodeID::FromRandom(),
                                     GetParam(),
                                     /*timeout_ms=*/10,
                                     /*poll_interval_ms=*/5);
  ASSERT_TRUE(result.has_error());
  EXPECT_TRUE(std::holds_alternative<StatusT::TimedOut>(result.error()));

  EXPECT_NE(result.message().find(GetParam()), std::string::npos);
  EXPECT_NE(result.message().find("10 ms"), std::string::npos);
  EXPECT_NE(result.message().find("The corresponding Ray process may be slow to start or "
                                  "may have failed to start."),
            std::string::npos);

  std::filesystem::remove_all(test_dir);
}

INSTANTIATE_TEST_SUITE_P(AgentAndGcsPorts,
                         PortPersistenceTest,
                         ::testing::Values("metrics_agent_port", "gcs_server_port"));

}  // namespace ray
