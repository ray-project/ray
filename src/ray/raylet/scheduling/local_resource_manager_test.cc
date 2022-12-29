// Copyright 2022 The Ray Authors.
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

#include "ray/raylet/scheduling/local_resource_manager.h"

#include "gtest/gtest.h"

namespace ray {

class LocalResourceManagerTest : public ::testing::Test {
 public:
  void SetUp() {
    ::testing::Test::SetUp();
    manager = nullptr;
  }

  NodeResources CreateNodeResources(
      absl::flat_hash_map<std::string, double> resource_usage_map) {
    NodeResources resources;
    for (auto &[resource, total] : resource_usage_map) {
      auto resource_id = ResourceID(resource);
      resources.available.Set(resource_id, total);
      resources.total.Set(resource_id, total);
    }
    return resources;
  }

  void ResourceUsageMapDebugString(
      absl::flat_hash_map<std::string, LocalResourceManager::ResourceUsage>
          resource_usage_map) {
    for (auto &[resource, usage] : resource_usage_map) {
      RAY_LOG(INFO) << resource << ":"
                    << "\n\tAvailable: " << usage.avail << "\n\tUsed: " << usage.used;
    }
  }

  scheduling::NodeID local_node_id = scheduling::NodeID(0);
  std::unique_ptr<LocalResourceManager> manager;
};

TEST_F(LocalResourceManagerTest, BasicGetResourceUsageMapTest) {
  /*
    Test `GetResourceUsageMap`. This method is used to record metrics.
  */
  auto node_ip_resource = "node:127.0.0.1";
  auto pg_wildcard_resource = "CPU_group_4482dec0faaf5ead891ff1659a9501000000";
  auto pg_index_0_resource = "CPU_group_0_4482dec0faaf5ead891ff1659a9501000000";
  auto pg_index_1_resource = "CPU_group_1_4482dec0faaf5ead891ff1659a9501000000";
  manager = std::make_unique<LocalResourceManager>(
      local_node_id,
      CreateNodeResources({{"CPU", 8.0},
                           {"GPU", 2.0},
                           {"CUSTOM", 4.0},
                           {node_ip_resource, 1.0},
                           {pg_wildcard_resource, 4.0},
                           {pg_index_0_resource, 2.0},
                           {pg_index_1_resource, 2.0}}),
      nullptr,
      nullptr,
      nullptr);

  ///
  /// Test when there's no allocation.
  ///
  {
    auto resource_usage_map = manager->GetResourceUsageMap();
    ResourceUsageMapDebugString(resource_usage_map);
    ASSERT_TRUE(resource_usage_map.find("CPU") != resource_usage_map.end());
    ASSERT_TRUE(resource_usage_map.find("GPU") != resource_usage_map.end());
    ASSERT_TRUE(resource_usage_map.find("CUSTOM") != resource_usage_map.end());
    ASSERT_EQ(resource_usage_map["CPU"].used, 0.0);
    ASSERT_EQ(resource_usage_map["CPU"].avail, 8.0);
    ASSERT_EQ(resource_usage_map["GPU"].used, 0.0);
    ASSERT_EQ(resource_usage_map["GPU"].avail, 2.0);
    ASSERT_EQ(resource_usage_map["CUSTOM"].used, 0.0);
    ASSERT_EQ(resource_usage_map["CUSTOM"].avail, 4.0);
    // Verify node ip is not reported.
    ASSERT_TRUE(resource_usage_map.find(node_ip_resource) == resource_usage_map.end());
    // Verify pg resources are not reported.
    ASSERT_TRUE(resource_usage_map.find(pg_wildcard_resource) ==
                resource_usage_map.end());
    ASSERT_TRUE(resource_usage_map.find(pg_index_0_resource) == resource_usage_map.end());
    ASSERT_TRUE(resource_usage_map.find(pg_index_1_resource) == resource_usage_map.end());
  }

  ///
  /// Test when there's the allocation.
  ///
  {
    const absl::flat_hash_map<std::string, double> task_spec = {
        {"CPU", 1.}, {"GPU", 0.5}, {"CUSTOM", 2.0}, {node_ip_resource, 0.01}};
    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    ASSERT_TRUE(manager->AllocateLocalTaskResources(task_spec, task_allocation));
    auto resource_usage_map = manager->GetResourceUsageMap();
    ResourceUsageMapDebugString(resource_usage_map);

    ASSERT_EQ(resource_usage_map["CPU"].used, 1.0);
    ASSERT_EQ(resource_usage_map["CPU"].avail, 7.0);
    ASSERT_EQ(resource_usage_map["GPU"].used, 0.5);
    ASSERT_EQ(resource_usage_map["GPU"].avail, 1.5);
    ASSERT_EQ(resource_usage_map["CUSTOM"].used, 2.0);
    ASSERT_EQ(resource_usage_map["CUSTOM"].avail, 2.0);
    // Verify node ip is not reported.
    ASSERT_TRUE(resource_usage_map.find(node_ip_resource) == resource_usage_map.end());
    // Verify pg resources are not reported.
    ASSERT_TRUE(resource_usage_map.find(pg_wildcard_resource) ==
                resource_usage_map.end());
    ASSERT_TRUE(resource_usage_map.find(pg_index_0_resource) == resource_usage_map.end());
    ASSERT_TRUE(resource_usage_map.find(pg_index_1_resource) == resource_usage_map.end());
  }
}

}  // namespace ray
