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

#include "ray/raylet/worker_resource_limits.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/ray_config.h"
#include "ray/common/scheduling/resource_set.h"

namespace ray::raylet {

class WorkerResourceLimitsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    RayConfig::instance().initialize(R"({"worker_resource_limits_enabled": true})");
  }
};

TEST_F(WorkerResourceLimitsTest, PreservesFractionalCpuAndExactMemory) {
  constexpr uint64_t kMemoryBytes = 256 * 1024 * 1024;
  for (const auto &[cpus, expected_quota] : std::vector<std::pair<double, int64_t>>{
           {0.5, 50000}, {1.0, 100000}, {1.5, 150000}}) {
    const ResourceSet resources(absl::flat_hash_map<std::string, double>{
        {"CPU", cpus}, {"memory", kMemoryBytes}});
    const auto limits =
        BuildWorkerResourceLimits(R"({"image_uri":"podman://ray:latest"})", resources);

    EXPECT_EQ(limits.cpu_period_us(), 100000);
    EXPECT_EQ(limits.cpu_quota_us(), expected_quota);
    EXPECT_EQ(limits.memory_bytes(), kMemoryBytes);
  }
}

TEST_F(WorkerResourceLimitsTest, UsesLegalCfsValuesForSmallFractionalCpu) {
  for (const auto &[cpus, expected_quota] : std::vector<std::pair<double, int64_t>>{
           {0.001, 1000}, {0.005, 5000}, {0.0099, 9900}}) {
    const ResourceSet resources(absl::flat_hash_map<std::string, double>{{"CPU", cpus}});
    const auto limits =
        BuildWorkerResourceLimits(R"({"image_uri":"podman://ray:latest"})", resources);

    EXPECT_EQ(limits.cpu_period_us(), 1000000);
    EXPECT_EQ(limits.cpu_quota_us(), expected_quota);
    EXPECT_TRUE(limits.validation_error().empty());
  }

  const ResourceSet resources(absl::flat_hash_map<std::string, double>{{"CPU", 0.01}});
  const auto limits =
      BuildWorkerResourceLimits(R"({"image_uri":"podman://ray:latest"})", resources);
  EXPECT_EQ(limits.cpu_period_us(), 100000);
  EXPECT_EQ(limits.cpu_quota_us(), 1000);
}

TEST_F(WorkerResourceLimitsTest, RejectsUnrepresentableCpu) {
  const ResourceSet resources(absl::flat_hash_map<std::string, double>{{"CPU", 0.0001}});
  const auto limits =
      BuildWorkerResourceLimits(R"({"image_uri":"podman://ray:latest"})", resources);

  EXPECT_EQ(limits.cpu_period_us(), 0);
  EXPECT_EQ(limits.cpu_quota_us(), 0);
  EXPECT_NE(limits.validation_error().find("below 0.001 CPU"), std::string::npos);
  EXPECT_TRUE(HasWorkerResourceLimits(limits));
}

TEST_F(WorkerResourceLimitsTest, DeduplicatesPlacementGroupAliases) {
  constexpr uint64_t kMemoryBytes = 128 * 1024 * 1024;
  const ResourceSet resources(absl::flat_hash_map<std::string, double>{
      {"CPU_group_deadbeef", 1.5},
      {"CPU_group_0_deadbeef", 1.5},
      {"memory_group_deadbeef", kMemoryBytes},
      {"memory_group_0_deadbeef", kMemoryBytes},
  });

  const auto limits =
      BuildWorkerResourceLimits(R"({"image_uri":"podman://ray:latest"})", resources);
  EXPECT_EQ(limits.cpu_quota_us(), 150000);
  EXPECT_EQ(limits.memory_bytes(), kMemoryBytes);
}

TEST_F(WorkerResourceLimitsTest, LeavesNonImageRuntimeEnvUnchanged) {
  const ResourceSet resources(
      absl::flat_hash_map<std::string, double>{{"CPU", 1}, {"memory", 1024}});

  EXPECT_FALSE(HasWorkerResourceLimits(
      BuildWorkerResourceLimits(R"({"env_vars":{"KEY":"value"}})", resources)));
  EXPECT_FALSE(HasWorkerResourceLimits(BuildWorkerResourceLimits("{}", resources)));
}

TEST_F(WorkerResourceLimitsTest, DisabledLeavesImageRuntimeEnvUnchanged) {
  RayConfig::instance().initialize(R"({"worker_resource_limits_enabled": false})");
  const ResourceSet resources(
      absl::flat_hash_map<std::string, double>{{"CPU", 1}, {"memory", 1024}});

  EXPECT_FALSE(HasWorkerResourceLimits(
      BuildWorkerResourceLimits(R"({"image_uri":"podman://ray:latest"})", resources)));
}

}  // namespace ray::raylet
