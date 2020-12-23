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
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
class GcsNodeManagerTest : public ::testing::Test {
 public:
  GcsNodeManagerTest() {
    gcs_pub_sub_ = std::make_shared<GcsServerMocker::MockGcsPubSub>(redis_client_);
    gcs_resource_manager_ = std::make_shared<gcs::GcsResourceManager>(nullptr, nullptr);
  }

 protected:
  std::shared_ptr<GcsServerMocker::MockGcsPubSub> gcs_pub_sub_;
  std::shared_ptr<gcs::RedisClient> redis_client_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<gcs::GcsResourceManager> gcs_resource_manager_;
};

TEST_F(GcsNodeManagerTest, TestManagement) {
  boost::asio::io_service io_service;
  gcs::GcsNodeManager node_manager(io_service, gcs_pub_sub_, gcs_table_storage_);
  // Test Add/Get/Remove functionality.
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());

  {
    rpc::GetAllResourceUsageRequest request;
    rpc::GetAllResourceUsageReply reply;
    auto send_reply_callback = [](ray::Status status, std::function<void()> f1,
                                  std::function<void()> f2) {};
    node_manager.HandleGetAllResourceUsage(request, &reply, send_reply_callback);
    ASSERT_EQ(reply.resource_usage_data().batch().size(), 0);
  }

  node_manager.AddNode(node);
  ASSERT_EQ(node, node_manager.GetAliveNode(node_id).value());

  rpc::ReportResourceUsageRequest report_request;
  (*report_request.mutable_resources()->mutable_resources_available())["CPU"] = 2;
  (*report_request.mutable_resources()->mutable_resources_total())["CPU"] = 2;
  node_manager.UpdateNodeResourceUsage(node_id, report_request);

  {
    rpc::GetAllResourceUsageRequest request;
    rpc::GetAllResourceUsageReply reply;
    auto send_reply_callback = [](ray::Status status, std::function<void()> f1,
                                  std::function<void()> f2) {};
    node_manager.HandleGetAllResourceUsage(request, &reply, send_reply_callback);
    ASSERT_EQ(reply.resource_usage_data().batch().size(), 1);
  }

  node_manager.RemoveNode(node_id);
  ASSERT_TRUE(!node_manager.GetAliveNode(node_id).has_value());

  {
    rpc::GetAllResourceUsageRequest request;
    rpc::GetAllResourceUsageReply reply;
    auto send_reply_callback = [](ray::Status status, std::function<void()> f1,
                                  std::function<void()> f2) {};
    node_manager.HandleGetAllResourceUsage(request, &reply, send_reply_callback);
    ASSERT_EQ(reply.resource_usage_data().batch().size(), 0);
  }
}

TEST_F(GcsNodeManagerTest, TestListener) {
  boost::asio::io_service io_service;
  gcs::GcsNodeManager node_manager(io_service, gcs_pub_sub_, gcs_table_storage_);
  // Test AddNodeAddedListener.
  int node_count = 1000;
  std::vector<std::shared_ptr<rpc::GcsNodeInfo>> added_nodes;
  node_manager.AddNodeAddedListener(
      [&added_nodes](std::shared_ptr<rpc::GcsNodeInfo> node) {
        added_nodes.emplace_back(std::move(node));
      });
  for (int i = 0; i < node_count; ++i) {
    auto node = Mocker::GenNodeInfo();
    node_manager.AddNode(node);
  }
  ASSERT_EQ(node_count, added_nodes.size());

  // Test GetAllAliveNodes.
  auto &alive_nodes = node_manager.GetAllAliveNodes();
  ASSERT_EQ(added_nodes.size(), alive_nodes.size());
  for (const auto &node : added_nodes) {
    ASSERT_EQ(1, alive_nodes.count(NodeID::FromBinary(node->node_id())));
  }

  // Test AddNodeRemovedListener.
  std::vector<std::shared_ptr<rpc::GcsNodeInfo>> removed_nodes;
  node_manager.AddNodeRemovedListener(
      [&removed_nodes](std::shared_ptr<rpc::GcsNodeInfo> node) {
        removed_nodes.emplace_back(std::move(node));
      });
  for (int i = 0; i < node_count; ++i) {
    node_manager.RemoveNode(NodeID::FromBinary(added_nodes[i]->node_id()));
  }
  ASSERT_EQ(node_count, removed_nodes.size());
  ASSERT_TRUE(node_manager.GetAllAliveNodes().empty());
  for (int i = 0; i < node_count; ++i) {
    ASSERT_EQ(added_nodes[i], removed_nodes[i]);
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
