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
    manager = std::make_unique<ClusterResourceManager>();
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

TEST_F(ClusterResourceManagerTest, HasSufficientResourceTest) {
  ASSERT_FALSE(manager->HasSufficientResource(
      node3, {}, /*ignore_object_store_memory_requirement*/ false));
  ASSERT_TRUE(manager->HasSufficientResource(
      node0,
      ResourceMapToResourceRequest({{"CPU", 1}},
                                   /*requires_object_store_memory=*/true),
      /*ignore_object_store_memory_requirement*/ false));
  ASSERT_FALSE(manager->HasSufficientResource(
      node0,
      ResourceMapToResourceRequest({{"CUSTOM", 1}},
                                   /*requires_object_store_memory=*/true),
      /*ignore_object_store_memory_requirement*/ false));
  ASSERT_TRUE(manager->HasSufficientResource(
      node1,
      ResourceMapToResourceRequest({{"CUSTOM", 1}},
                                   /*requires_object_store_memory=*/true),
      /*ignore_object_store_memory_requirement*/ false));
  ASSERT_TRUE(manager->HasSufficientResource(
      node2,
      ResourceMapToResourceRequest({{"CPU", 1}},
                                   /*requires_object_store_memory=*/false),
      /*ignore_object_store_memory_requirement*/ false));
  ASSERT_FALSE(manager->HasSufficientResource(
      node2,
      ResourceMapToResourceRequest({{"CPU", 1}},
                                   /*requires_object_store_memory=*/true),
      /*ignore_object_store_memory_requirement*/ false));
  ASSERT_TRUE(manager->HasSufficientResource(
      node2,
      ResourceMapToResourceRequest({{"CPU", 1}},
                                   /*requires_object_store_memory=*/true),
      /*ignore_object_store_memory_requirement*/ true));
}

TEST_F(ClusterResourceManagerTest, SubtractAndAddNodeAvailableResources) {
  const auto &node_resources = manager->GetNodeResources(node0);
  ASSERT_TRUE(node_resources.available.Get(ResourceID::CPU()) == 1);

  manager->SubtractNodeAvailableResources(
      node0,
      ResourceMapToResourceRequest({{"CPU", 1}},
                                   /*requires_object_store_memory=*/false));
  ASSERT_TRUE(node_resources.available.Get(ResourceID::CPU()) == 0);
  // Subtract again and make sure the available == 0.
  manager->SubtractNodeAvailableResources(
      node0,
      ResourceMapToResourceRequest({{"CPU", 1}},
                                   /*requires_object_store_memory=*/false));
  ASSERT_TRUE(node_resources.available.Get(ResourceID::CPU()) == 0);

  // Add resources back.
  manager->AddNodeAvailableResources(
      node0,
      ResourceMapToResourceRequest({{"CPU", 1}},
                                   /*requires_object_store_memory=*/false));
  ASSERT_TRUE(node_resources.available.Get(ResourceID::CPU()) == 1);

  // Add again and make sure the available == 1 (<= total).
  manager->AddNodeAvailableResources(
      node0,
      ResourceMapToResourceRequest({{"CPU", 1}},
                                   /*requires_object_store_memory=*/false));
  ASSERT_TRUE(node_resources.available.Get(ResourceID::CPU()) == 1);
}

TEST_F(ClusterResourceManagerTest, UpdateNodeAvailableResourcesIfExist) {
  const auto &node_resources = manager->GetNodeResources(node0);
  ASSERT_TRUE(node_resources.available.Get(ResourceID::CPU()) == 1);

  rpc::ResourcesData resources_data;
  resources_data.set_resources_available_changed(true);
  (*resources_data.mutable_resources_available())["CPU"] = 0;

  manager->UpdateNodeAvailableResourcesIfExist(node0, resources_data);
  ASSERT_TRUE(node_resources.available.Get(ResourceID::CPU()) == 0);

  (*resources_data.mutable_resources_available())["CUSTOM_RESOURCE"] = 1;
  manager->UpdateNodeAvailableResourcesIfExist(node0, resources_data);
  ASSERT_FALSE(node_resources.total.Has(ResourceID("CUSTOM_RESOURCE")));
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
