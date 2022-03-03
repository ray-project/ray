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

#include "ray/gcs/gcs_server/gcs_resource_manager.h"

#include <memory>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {

using ::testing::_;

class GcsResourceManagerTest : public ::testing::Test {
 public:
  GcsResourceManagerTest() {
    gcs_resource_manager_ =
        std::make_shared<gcs::GcsResourceManager>(io_service_, nullptr, nullptr);
  }

  instrumented_io_context io_service_;
  std::shared_ptr<gcs::GcsResourceManager> gcs_resource_manager_;
};

TEST_F(GcsResourceManagerTest, TestBasic) {
  const std::string cpu_resource = "CPU";
  absl::flat_hash_map<std::string, double> resource_map;
  resource_map[cpu_resource] = 10;

  auto node = Mocker::GenNodeInfo();
  node->mutable_resources_total()->insert(resource_map.begin(), resource_map.end());
  // Add node resources.
  gcs_resource_manager_->OnNodeAdd(*node);

  // Get and check cluster resources.
  const auto &cluster_resource = gcs_resource_manager_->GetClusterResources();
  ASSERT_EQ(1, cluster_resource.size());

  const auto &node_id = NodeID::FromBinary(node->node_id());
  ResourceSet resource_set(resource_map);

  // Test `AcquireResources`.
  ASSERT_TRUE(gcs_resource_manager_->AcquireResources(node_id, resource_set));
  ASSERT_FALSE(gcs_resource_manager_->AcquireResources(node_id, resource_set));

  // Test `ReleaseResources`.
  ASSERT_TRUE(
      gcs_resource_manager_->ReleaseResources(NodeID::FromRandom(), resource_set));
  ASSERT_TRUE(gcs_resource_manager_->ReleaseResources(node_id, resource_set));
  ASSERT_TRUE(gcs_resource_manager_->AcquireResources(node_id, resource_set));
}

TEST_F(GcsResourceManagerTest, TestResourceUsageAPI) {
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  rpc::GetAllResourceUsageRequest get_all_request;
  rpc::GetAllResourceUsageReply get_all_reply;
  auto send_reply_callback = [](ray::Status status, std::function<void()> f1,
                                std::function<void()> f2) {};
  gcs_resource_manager_->HandleGetAllResourceUsage(get_all_request, &get_all_reply,
                                                   send_reply_callback);
  ASSERT_EQ(get_all_reply.resource_usage_data().batch().size(), 0);

  rpc::ReportResourceUsageRequest report_request;
  (*report_request.mutable_resources()->mutable_resources_available())["CPU"] = 2;
  (*report_request.mutable_resources()->mutable_resources_total())["CPU"] = 2;
  gcs_resource_manager_->UpdateNodeResourceUsage(node_id, report_request.resources());

  gcs_resource_manager_->HandleGetAllResourceUsage(get_all_request, &get_all_reply,
                                                   send_reply_callback);
  ASSERT_EQ(get_all_reply.resource_usage_data().batch().size(), 1);

  gcs_resource_manager_->OnNodeDead(node_id);
  rpc::GetAllResourceUsageReply get_all_reply2;
  gcs_resource_manager_->HandleGetAllResourceUsage(get_all_request, &get_all_reply2,
                                                   send_reply_callback);
  ASSERT_EQ(get_all_reply2.resource_usage_data().batch().size(), 0);
}

TEST_F(GcsResourceManagerTest, TestSetAvailableResourcesWhenNodeDead) {
  auto node = Mocker::GenNodeInfo();
  node->mutable_resources_total()->insert({"CPU", 10});

  gcs_resource_manager_->OnNodeAdd(*node);
  ASSERT_EQ(gcs_resource_manager_->GetClusterResources().size(), 1);

  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_resource_manager_->OnNodeDead(node_id);
  ASSERT_EQ(gcs_resource_manager_->GetClusterResources().size(), 0);

  rpc::ResourcesData resources_data;
  resources_data.set_node_id(node->node_id());
  resources_data.mutable_resources_available()->insert({"CPU", 5});
  resources_data.set_resources_available_changed(true);
  gcs_resource_manager_->UpdateFromResourceReport(resources_data);
  ASSERT_EQ(gcs_resource_manager_->GetClusterResources().size(), 0);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
