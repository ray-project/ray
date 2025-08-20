// Copyright 2021 The Ray Authors.
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

#include "ray/raylet/scheduling/cluster_resource_manager.h"

#include <memory>

#include "gtest/gtest.h"

namespace ray {

NodeResources CreateNodeResources(double available_cpu,
                                  double total_cpu,
                                  double available_custom_resource = 0,
                                  double total_custom_resource = 0,
                                  bool object_pulls_queued = false) {
  NodeResources resources;
  resources.available.Set(ResourceID::CPU(), available_cpu);
  resources.total.Set(ResourceID::CPU(), total_cpu);
  resources.available.Set(scheduling::ResourceID("CUSTOM"), available_custom_resource);
  resources.total.Set(scheduling::ResourceID("CUSTOM"), total_custom_resource);
  resources.object_pulls_queued = object_pulls_queued;
  return resources;
}

struct ClusterResourceManagerTest : public ::testing::Test {
  void SetUp() {
    ::testing::Test::SetUp();
    static instrumented_io_context io_context;
    manager = std::make_unique<ClusterResourceManager>(io_context);
    manager->AddOrUpdateNode(node0,
                             CreateNodeResources(/*available_cpu*/ 1, /*total_cpu*/ 1));
    manager->AddOrUpdateNode(node1,
                             CreateNodeResources(/*available_cpu*/ 0,
                                                 /*total_cpu*/ 0,
                                                 /*available_custom*/ 1,
                                                 /*total_custom*/ 1));
    manager->AddOrUpdateNode(node2,
                             CreateNodeResources(/*available_cpu*/ 1,
                                                 /*total_cpu*/ 1,
                                                 /*available_custom*/ 1,
                                                 /*total_custom*/ 1,
                                                 /*object_pulls_queued*/ true));
  }
  scheduling::NodeID node0 = scheduling::NodeID(0);
  scheduling::NodeID node1 = scheduling::NodeID(1);
  scheduling::NodeID node2 = scheduling::NodeID(2);
  scheduling::NodeID node3 = scheduling::NodeID(3);
  std::unique_ptr<ClusterResourceManager> manager;
};

TEST_F(ClusterResourceManagerTest, UpdateNode) {
  // Prepare a sync message with updated totals/available, labels and flags.
  syncer::ResourceViewSyncMessage payload;
  payload.mutable_resources_total()->insert({"CPU", 10.0});
  payload.mutable_resources_available()->insert({"CPU", 5.0});
  payload.mutable_labels()->insert({"zone", "us-east-1a"});
  payload.set_object_pulls_queued(true);
  payload.set_idle_duration_ms(42);
  payload.set_is_draining(true);
  payload.set_draining_deadline_timestamp_ms(123456);

  // Update existing node and validate the local view reflects the payload.
  ASSERT_TRUE(manager->UpdateNode(node0, payload));

  const auto &node_resources = manager->GetNodeResources(node0);
  ASSERT_EQ(node_resources.total.Get(scheduling::ResourceID("CPU")), 10);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU")), 5);
  ASSERT_EQ(node_resources.labels.at("zone"), "us-east-1a");
  ASSERT_TRUE(node_resources.object_pulls_queued);
  ASSERT_EQ(node_resources.idle_resource_duration_ms, 42);
  ASSERT_TRUE(node_resources.is_draining);
  ASSERT_EQ(node_resources.draining_deadline_timestamp_ms, 123456);
  ASSERT_TRUE(node_resources.last_resource_update_time.has_value());
}

TEST_F(ClusterResourceManagerTest, DebugStringTest) {
  // Test max_num_nodes_to_include parameter is working.
  ASSERT_EQ(std::vector<std::string>(absl::StrSplit(manager->DebugString(), "node id:"))
                    .size() -
                1,
            3);
  ASSERT_EQ(std::vector<std::string>(
                absl::StrSplit(manager->DebugString(/*max_num_nodes_to_include=*/5),
                               "node id:"))
                    .size() -
                1,
            3);
  ASSERT_EQ(std::vector<std::string>(
                absl::StrSplit(manager->DebugString(/*max_num_nodes_to_include=*/2),
                               "node id:"))
                    .size() -
                1,
            2);
}

TEST_F(ClusterResourceManagerTest, HasFeasibleResourcesTest) {
  ASSERT_FALSE(manager->HasFeasibleResources(node3, {}));
  ASSERT_FALSE(manager->HasFeasibleResources(
      node0,
      ResourceMapToResourceRequest({{"GPU", 1}},
                                   /*requires_object_store_memory=*/false)));
  ASSERT_TRUE(manager->HasFeasibleResources(
      node0,
      ResourceMapToResourceRequest({{"CPU", 1}},
                                   /*requires_object_store_memory=*/false)));
  manager->SubtractNodeAvailableResources(
      node0,
      ResourceMapToResourceRequest({{"CPU", 1}},
                                   /*requires_object_store_memory=*/false));
  // node0 has no available CPU resource but it's still feasible.
  ASSERT_TRUE(manager->HasFeasibleResources(
      node0,
      ResourceMapToResourceRequest({{"CPU", 1}},
                                   /*requires_object_store_memory=*/false)));
}

TEST_F(ClusterResourceManagerTest, HasAvailableResourcesTest) {
  ASSERT_FALSE(manager->HasAvailableResources(
      node3, {}, /*ignore_object_store_memory_requirement*/ false));
  ASSERT_TRUE(manager->HasAvailableResources(
      node0,
      ResourceMapToResourceRequest({{"CPU", 1}},
                                   /*requires_object_store_memory=*/true),
      /*ignore_object_store_memory_requirement*/ false));
  ASSERT_FALSE(manager->HasAvailableResources(
      node0,
      ResourceMapToResourceRequest({{"CUSTOM", 1}},
                                   /*requires_object_store_memory=*/true),
      /*ignore_object_store_memory_requirement*/ false));
  ASSERT_TRUE(manager->HasAvailableResources(
      node1,
      ResourceMapToResourceRequest({{"CUSTOM", 1}},
                                   /*requires_object_store_memory=*/true),
      /*ignore_object_store_memory_requirement*/ false));
  ASSERT_TRUE(manager->HasAvailableResources(
      node2,
      ResourceMapToResourceRequest({{"CPU", 1}},
                                   /*requires_object_store_memory=*/false),
      /*ignore_object_store_memory_requirement*/ false));
  ASSERT_FALSE(manager->HasAvailableResources(
      node2,
      ResourceMapToResourceRequest({{"CPU", 1}},
                                   /*requires_object_store_memory=*/true),
      /*ignore_object_store_memory_requirement*/ false));
  ASSERT_TRUE(manager->HasAvailableResources(
      node2,
      ResourceMapToResourceRequest({{"CPU", 1}},
                                   /*requires_object_store_memory=*/true),
      /*ignore_object_store_memory_requirement*/ true));
}

TEST_F(ClusterResourceManagerTest, SubtractAndAddNodeAvailableResources) {
  const auto &node_resources = manager->GetNodeResources(node0);
  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 1);

  manager->SubtractNodeAvailableResources(
      node0,
      ResourceMapToResourceRequest({{"CPU", 1}},
                                   /*requires_object_store_memory=*/false));
  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 0);
  // Subtract again and make sure the available == 0.
  manager->SubtractNodeAvailableResources(
      node0,
      ResourceMapToResourceRequest({{"CPU", 1}},
                                   /*requires_object_store_memory=*/false));
  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 0);

  // Add resources back.
  manager->AddNodeAvailableResources(node0, ResourceSet({{"CPU", FixedPoint(1)}}));
  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 1);
  // Add again and make sure the available == 1 (<= total).
  manager->AddNodeAvailableResources(node0, ResourceSet({{"CPU", FixedPoint(1)}}));
  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 1);
}

TEST_F(ClusterResourceManagerTest, UpdateNodeNormalTaskResources) {
  const auto &node_resources = manager->GetNodeResources(node0);
  ASSERT_TRUE(node_resources.normal_task_resources.IsEmpty());

  rpc::ResourcesData resources_data;
  resources_data.set_resources_normal_task_changed(true);
  resources_data.set_resources_normal_task_timestamp(absl::GetCurrentTimeNanos());
  resources_data.mutable_resources_normal_task()->insert({"CPU", 0.5});

  manager->UpdateNodeNormalTaskResources(node0, resources_data);
  ASSERT_TRUE(node_resources.normal_task_resources.Get(ResourceID::CPU()) == 0.5);

  (*resources_data.mutable_resources_normal_task())["CPU"] = 0.8;
  resources_data.set_resources_normal_task_changed(false);
  resources_data.set_resources_normal_task_timestamp(absl::GetCurrentTimeNanos());
  manager->UpdateNodeNormalTaskResources(node0, resources_data);
  ASSERT_TRUE(node_resources.normal_task_resources.Get(ResourceID::CPU()) == 0.5);

  resources_data.set_resources_normal_task_changed(true);
  resources_data.set_resources_normal_task_timestamp(0);
  manager->UpdateNodeNormalTaskResources(node0, resources_data);
  ASSERT_TRUE(node_resources.normal_task_resources.Get(ResourceID::CPU()) == 0.5);

  resources_data.set_resources_normal_task_changed(true);
  resources_data.set_resources_normal_task_timestamp(0);
  manager->UpdateNodeNormalTaskResources(node0, resources_data);
  ASSERT_TRUE(node_resources.normal_task_resources.Get(ResourceID::CPU()) == 0.5);

  resources_data.set_resources_normal_task_changed(true);
  resources_data.set_resources_normal_task_timestamp(absl::GetCurrentTimeNanos());
  manager->UpdateNodeNormalTaskResources(node0, resources_data);
  ASSERT_TRUE(node_resources.normal_task_resources.Get(ResourceID::CPU()) == 0.8);
}

}  // namespace ray
