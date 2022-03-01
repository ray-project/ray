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

NodeResources CreateNodeResources(StringIdMap &map, double available_cpu,
                                  double total_cpu, double available_custom_resource = 0,
                                  double total_custom_resource = 0,
                                  bool object_pulls_queued = false) {
  NodeResources resources;
  resources.predefined_resources = {{available_cpu, total_cpu}, {0, 0}, {0, 0}, {0, 0}};
  resources.custom_resources[map.Insert("CUSTOM")] = {available_custom_resource,
                                                      total_custom_resource};
  resources.object_pulls_queued = object_pulls_queued;
  return resources;
}

struct ClusterResourceManagerTest : public ::testing::Test {
  void SetUp() {
    ::testing::Test::SetUp();
    manager = std::make_unique<ClusterResourceManager>(map);
    manager->AddOrUpdateNode(
        node0, CreateNodeResources(map, /*available_cpu*/ 1, /*total_cpu*/ 1));
    manager->AddOrUpdateNode(
        node1, CreateNodeResources(map, /*available_cpu*/ 0, /*total_cpu*/ 0,
                                   /*available_custom*/ 1, /*total_custom*/ 1));
    manager->AddOrUpdateNode(
        node2, CreateNodeResources(map, /*available_cpu*/ 1, /*total_cpu*/ 1,
                                   /*available_custom*/ 1, /*total_custom*/ 1,
                                   /*object_pulls_queued*/ true));
  }
  StringIdMap map;
  int64_t node0 = 0;
  int64_t node1 = 1;
  int64_t node2 = 2;
  int64_t node3 = 3;
  std::unique_ptr<ClusterResourceManager> manager;
};

TEST_F(ClusterResourceManagerTest, HasSufficientResourceTest) {
  ASSERT_FALSE(manager->HasSufficientResource(
      node3, {}, /*ignore_object_store_memory_requirement*/ false));
  ASSERT_TRUE(manager->HasSufficientResource(
      node0,
      ResourceMapToResourceRequest(map, {{"CPU", 1}},
                                   /*requires_object_store_memory=*/true),
      /*ignore_object_store_memory_requirement*/ false));
  ASSERT_FALSE(manager->HasSufficientResource(
      node0,
      ResourceMapToResourceRequest(map, {{"CUSTOM", 1}},
                                   /*requires_object_store_memory=*/true),
      /*ignore_object_store_memory_requirement*/ false));
  ASSERT_TRUE(manager->HasSufficientResource(
      node1,
      ResourceMapToResourceRequest(map, {{"CUSTOM", 1}},
                                   /*requires_object_store_memory=*/true),
      /*ignore_object_store_memory_requirement*/ false));
  ASSERT_TRUE(manager->HasSufficientResource(
      node2,
      ResourceMapToResourceRequest(map, {{"CPU", 1}},
                                   /*requires_object_store_memory=*/false),
      /*ignore_object_store_memory_requirement*/ false));
  ASSERT_FALSE(manager->HasSufficientResource(
      node2,
      ResourceMapToResourceRequest(map, {{"CPU", 1}},
                                   /*requires_object_store_memory=*/true),
      /*ignore_object_store_memory_requirement*/ false));
  ASSERT_TRUE(manager->HasSufficientResource(
      node2,
      ResourceMapToResourceRequest(map, {{"CPU", 1}},
                                   /*requires_object_store_memory=*/true),
      /*ignore_object_store_memory_requirement*/ true));
}
}  // namespace ray
