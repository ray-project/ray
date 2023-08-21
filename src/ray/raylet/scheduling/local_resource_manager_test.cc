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
      absl::flat_hash_map<ResourceID, double> resource_usage_map) {
    NodeResources resources;
    for (auto &[resource_id, total] : resource_usage_map) {
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

  rpc::ResourcesData GetSyncMessageForResourceReport() {
    auto msg = manager->CreateSyncMessage(0, syncer::MessageType::RESOURCE_VIEW);
    rpc::ResourcesData resources_data;
    resources_data.ParseFromString(msg->sync_message());
    return resources_data;
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
      CreateNodeResources({{ResourceID::CPU(), 8.0},
                           {ResourceID::GPU(), 2.0},
                           {ResourceID("CUSTOM"), 4.0},
                           {ResourceID(node_ip_resource), 1.0},
                           {ResourceID(pg_wildcard_resource), 4.0},
                           {ResourceID(pg_index_0_resource), 2.0},
                           {ResourceID(pg_index_1_resource), 2.0}}),
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
    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();

    ResourceRequest resource_request =
        ResourceMapToResourceRequest({{ResourceID::CPU(), 1.},
                                      {ResourceID::GPU(), 0.5},
                                      {ResourceID("CUSTOM"), 2.0},
                                      {ResourceID(node_ip_resource), 0.01}},
                                     false);

    ASSERT_TRUE(manager->AllocateLocalTaskResources(resource_request, task_allocation));
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

TEST_F(LocalResourceManagerTest, NodeDrainingTest) {
  manager = std::make_unique<LocalResourceManager>(
      local_node_id,
      CreateNodeResources({{ResourceID::CPU(), 8.0}}),
      nullptr,
      nullptr,
      nullptr);

  // Make the node non-idle.
  {
    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    ResourceRequest resource_request =
        ResourceMapToResourceRequest({{ResourceID::CPU(), 1.0}}, false);
    manager->AllocateLocalTaskResources(resource_request, task_allocation);
  }

  manager->SetLocalNodeDraining();
  ASSERT_TRUE(manager->IsLocalNodeDraining());

  // Make the node idle so that the node is drained and terminated.
  std::shared_ptr<TaskResourceInstances> task_allocation =
      std::make_shared<TaskResourceInstances>(
          ResourceSet({{ResourceID::CPU(), FixedPoint(1.0)}}));
  EXPECT_DEATH(manager->ReleaseWorkerResources(task_allocation), ".*");
}

TEST_F(LocalResourceManagerTest, ObjectStoreMemoryDrainingTest) {
  // Test to make sure the node is drained when object store memory is free.
  auto used_object_store = std::make_unique<int64_t>(0);
  manager = std::make_unique<LocalResourceManager>(
      local_node_id,
      CreateNodeResources({{ResourceID::ObjectStoreMemory(), 100.0}}),
      /* get_used_object_store_memory */
      [&used_object_store]() { return *used_object_store; },
      nullptr,
      nullptr);

  // Make the node non-idle.
  *used_object_store = 1;
  manager->UpdateAvailableObjectStoreMemResource();

  manager->SetLocalNodeDraining();
  ASSERT_TRUE(manager->IsLocalNodeDraining());

  // Free object store memory so that the node is drained and terminated.
  *used_object_store = 0;
  EXPECT_DEATH(manager->UpdateAvailableObjectStoreMemResource(), ".*");
}

TEST_F(LocalResourceManagerTest, IdleResourceTimeTest) {
  auto node_ip_resource = "node:127.0.0.1";
  auto pg_wildcard_resource = "CPU_group_4482dec0faaf5ead891ff1659a9501000000";
  auto pg_index_0_resource = "CPU_group_0_4482dec0faaf5ead891ff1659a9501000000";
  auto pg_index_1_resource = "CPU_group_1_4482dec0faaf5ead891ff1659a9501000000";
  auto used_object_store = std::make_unique<int64_t>(0);
  manager = std::make_unique<LocalResourceManager>(
      local_node_id,
      CreateNodeResources({{ResourceID::CPU(), 8.0},
                           {ResourceID::GPU(), 2.0},
                           {ResourceID("CUSTOM"), 4.0},
                           {ResourceID::ObjectStoreMemory(), 100.0},
                           {ResourceID(node_ip_resource), 1.0},
                           {ResourceID(pg_wildcard_resource), 4.0},
                           {ResourceID(pg_index_0_resource), 2.0},
                           {ResourceID(pg_index_1_resource), 2.0}}),
      /* get_used_object_store_memory */
      [&used_object_store]() { return *used_object_store; },
      nullptr,
      nullptr);

  /// Test when the resource is all idle when initialized.
  {
    auto idle_time = manager->GetResourceIdleTime();
    // Sleep for a while.
    absl::SleepFor(absl::Seconds(1));

    ASSERT_NE(idle_time, absl::nullopt);
    ASSERT_NE(*idle_time, absl::InfinitePast());
    auto dur = absl::ToInt64Seconds(absl::Now() - *idle_time);
    ASSERT_GE(dur, 1);
  }

  /// Test that allocate some resources make it non-idle.
  {
    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    ResourceRequest resource_request = ResourceMapToResourceRequest(
        {{ResourceID::CPU(), 1.}, {ResourceID("CUSTOM"), 1.0}}, false);

    manager->AllocateLocalTaskResources(resource_request, task_allocation);

    auto idle_time = manager->GetResourceIdleTime();
    ASSERT_EQ(idle_time, absl::nullopt);
  }

  /// Test that deallocate some resources (not all) should not make it idle.
  {
    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>(
            ResourceSet({{ResourceID::CPU(), FixedPoint(1.0)}}));
    manager->FreeTaskResourceInstances(task_allocation, /* record_idle_resource */ true);

    auto idle_time = manager->GetResourceIdleTime();
    ASSERT_EQ(idle_time, absl::nullopt);
  }

  // Test that deallocate all used resources make it idle.
  {
    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>(
            ResourceSet({{ResourceID("CUSTOM"), FixedPoint(1.)}}));
    manager->FreeTaskResourceInstances(task_allocation, /* record_idle_resource */
                                       true);

    auto idle_time = manager->GetResourceIdleTime();
    ASSERT_TRUE(idle_time.has_value());
    auto dur = absl::Now() - *idle_time;
    ASSERT_GE(dur, absl::ZeroDuration());
  }

  {
    // Sleep for a while should have the right idle time.
    absl::SleepFor(absl::Seconds(1));
    {
      // Test allocates same resource have the right idle time.
      auto idle_time = manager->GetResourceIdleTime();
      ASSERT_TRUE(idle_time.has_value());
      ASSERT_GE(absl::Now() - *idle_time, absl::Seconds(1));
    }

    // Allocate the resource
    {
      std::shared_ptr<TaskResourceInstances> task_allocation =
          std::make_shared<TaskResourceInstances>();
      ResourceRequest resource_request =
          ResourceMapToResourceRequest({{ResourceID::CPU(), 1.}}, false);

      manager->AllocateLocalTaskResources(resource_request, task_allocation);
    }

    // Should not be idle.
    {
      auto idle_time = manager->GetResourceIdleTime();
      ASSERT_EQ(idle_time, absl::nullopt);

      const auto &resources_data = GetSyncMessageForResourceReport();
      ASSERT_EQ(resources_data.idle_duration_ms(), 0);
    }

    // Deallocate the resource
    {
      std::shared_ptr<TaskResourceInstances> task_allocation =
          std::make_shared<TaskResourceInstances>(
              ResourceSet({{ResourceID::CPU(), FixedPoint(1.)}}));
      manager->FreeTaskResourceInstances(task_allocation, /* record_idle_resource */
                                         true);
    }

    // Check the idle time should be reset (not longer than 1 secs).
    {
      auto idle_time = manager->GetResourceIdleTime();
      ASSERT_TRUE(idle_time.has_value());
      auto dur = absl::Now() - *idle_time;
      ASSERT_GE(dur, absl::ZeroDuration());
      ASSERT_LE(dur, absl::Seconds(1));

      const auto &resources_data = GetSyncMessageForResourceReport();
      ASSERT_GE(resources_data.idle_duration_ms(), 0);
      ASSERT_LE(resources_data.idle_duration_ms(), 1 * 1000);
    }
  }

  // Test object store resource is also making node non-idle when used.
  {
    *used_object_store = 1;
    manager->UpdateAvailableObjectStoreMemResource();
    auto idle_time = manager->GetResourceIdleTime();
    ASSERT_EQ(idle_time, absl::nullopt);

    const auto &resources_data = GetSyncMessageForResourceReport();
    ASSERT_EQ(resources_data.idle_duration_ms(), 0);
  }

  // Free object store memory usage should make node resource idle.
  {
    *used_object_store = 0;
    manager->UpdateAvailableObjectStoreMemResource();
    auto idle_time = manager->GetResourceIdleTime();
    ASSERT_TRUE(idle_time.has_value());
    auto dur = absl::Now() - *idle_time;
    ASSERT_GE(dur, absl::ZeroDuration());

    // And syncer messages should be created correctly for resource reporting.
    const auto &resources_data = GetSyncMessageForResourceReport();
    ASSERT_GE(resources_data.idle_duration_ms(), 0);
  }
}

}  // namespace ray
