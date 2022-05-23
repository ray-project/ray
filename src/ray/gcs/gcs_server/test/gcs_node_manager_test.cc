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

// clang-format off
#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "ray/rpc/node_manager/node_manager_client.h"
#include "ray/rpc/node_manager/node_manager_client_pool.h"
#include "mock/ray/pubsub/publisher.h"
// clang-format on

namespace ray {
class GcsNodeManagerTest : public ::testing::Test {
 public:
  GcsNodeManagerTest() {
    raylet_client_ = std::make_shared<GcsServerMocker::MockRayletClient>();
    client_pool_ = std::make_shared<rpc::NodeManagerClientPool>(
        [this](const rpc::Address &) { return raylet_client_; });
    gcs_publisher_ = std::make_shared<gcs::GcsPublisher>(
        std::make_unique<ray::pubsub::MockPublisher>());
  }

 protected:
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<GcsServerMocker::MockRayletClient> raylet_client_;
  std::shared_ptr<rpc::NodeManagerClientPool> client_pool_;
  std::shared_ptr<gcs::GcsPublisher> gcs_publisher_;
};

TEST_F(GcsNodeManagerTest, TestManagement) {
  gcs::GcsNodeManager node_manager(gcs_publisher_, gcs_table_storage_, client_pool_);
  // Test Add/Get/Remove functionality.
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());

  node_manager.AddNode(node);
  ASSERT_EQ(node, node_manager.GetAliveNode(node_id).value());

  node_manager.RemoveNode(node_id);
  ASSERT_TRUE(!node_manager.GetAliveNode(node_id).has_value());
}

TEST_F(GcsNodeManagerTest, TestListener) {
  gcs::GcsNodeManager node_manager(gcs_publisher_, gcs_table_storage_, client_pool_);
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
