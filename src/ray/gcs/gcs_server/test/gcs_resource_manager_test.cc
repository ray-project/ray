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

#include <memory>

#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {

using ::testing::_;

class GcsResourceManagerTest : public ::testing::Test {
 public:
  GcsResourceManagerTest() {
    gcs_resource_manager_ = std::make_shared<gcs::GcsResourceManager>(nullptr, nullptr);
  }

  std::shared_ptr<gcs::GcsResourceManager> gcs_resource_manager_;
};

TEST_F(GcsResourceManagerTest, TestBasic) {
  // Add node resources.
  auto node_id = NodeID::FromRandom();
  const std::string cpu_resource = "CPU";
  std::unordered_map<std::string, double> resource_map;
  resource_map[cpu_resource] = 10;
  ResourceSet resource_set(resource_map);
  gcs_resource_manager_->UpdateResourceCapacity(node_id, resource_map);

  // Get and check cluster resources.
  const auto &cluster_resource = gcs_resource_manager_->GetClusterResources();
  ASSERT_EQ(1, cluster_resource.size());

  // Test `AcquireResources`.
  ASSERT_TRUE(gcs_resource_manager_->AcquireResources(node_id, resource_set));
  ASSERT_FALSE(gcs_resource_manager_->AcquireResources(node_id, resource_set));

  // Test `ReleaseResources`.
  ASSERT_TRUE(
      gcs_resource_manager_->ReleaseResources(NodeID::FromRandom(), resource_set));
  ASSERT_TRUE(gcs_resource_manager_->ReleaseResources(node_id, resource_set));
  ASSERT_TRUE(gcs_resource_manager_->AcquireResources(node_id, resource_set));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
