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

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "ray/common/scheduling/cluster_resource_storage_interface.h"

namespace ray {

/// this fake class exists so we can test the resource conversion functions
class FakeClusterResourceStorage : public ClusterResourceStorageInterface {
 public:
  FakeClusterResourceStorage() = default;

  ~FakeClusterResourceStorage() = default;

  void UpdateStoredResources(const scheduling::NodeID node_id,
                             const NodeResources &node_resources) override {}

  void DeleteStoredResources(const scheduling::NodeID node_id) override {}
};

class ClusterResourceStorageTest : public ::testing::Test {
 public:
  void SetUp() override {
    cluster_resource_storage_ = std::make_unique<FakeClusterResourceStorage>();
  }

 protected:
  std::unique_ptr<FakeClusterResourceStorage> cluster_resource_storage_;
};

TEST_F(ClusterResourceStorageTest, ResourcesDataConversion) {
  auto resource_data = rpc::ResourcesData();
  auto node_id = NodeID::FromRandom();
  absl::flat_hash_map<std::string, double> total = {{"CPU", 23.0}, {"GPU", 17.0}};
  absl::flat_hash_map<std::string, double> available = {{"CPU", 13.0}, {"GPU", 7.0}};
  absl::flat_hash_map<std::string, std::string> labels = {{"l1", "v1"}, {"l2", "v2"}};

  auto node_resources = ResourceMapToNodeResources(total, available, labels);
  node_resources.is_draining = true;
  node_resources.object_pulls_queued = true;
  node_resources.draining_deadline_timestamp_ms = 888;
  node_resources.idle_resource_duration_ms = 999;

  cluster_resource_storage_.get()->FillResourceUsage(
      node_id, node_resources, &resource_data);

  auto new_resources =
      cluster_resource_storage_.get()->NodeResourcesFromResourcesData(resource_data);
  // sadly, this only checks total, available, and labels
  ASSERT_EQ(node_resources, new_resources);
  ASSERT_EQ(node_resources.is_draining, new_resources.is_draining);
  ASSERT_EQ(node_resources.object_pulls_queued, new_resources.object_pulls_queued);
  ASSERT_EQ(node_resources.draining_deadline_timestamp_ms,
            new_resources.draining_deadline_timestamp_ms);
  ASSERT_EQ(node_resources.idle_resource_duration_ms,
            new_resources.idle_resource_duration_ms);
}

}  // namespace ray
