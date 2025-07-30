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
#include <utility>
#include <vector>

// clang-format off
#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "ray/rpc/node_manager/node_manager_client.h"
#include "ray/rpc/node_manager/raylet_client_pool.h"
#include "mock/ray/pubsub/publisher.h"
#include "ray/common/asio/asio_util.h"
#include "ray/common/ray_syncer/ray_syncer.h"
// clang-format on

namespace ray {
class GcsNodeManagerTest : public ::testing::Test {
 public:
  GcsNodeManagerTest() {
    raylet_client_ = std::make_shared<GcsServerMocker::MockRayletClient>();
    client_pool_ = std::make_unique<rpc::RayletClientPool>(
        [this](const rpc::Address &) { return raylet_client_; });
    gcs_publisher_ = std::make_unique<gcs::GcsPublisher>(
        std::make_unique<ray::pubsub::MockPublisher>());
    io_context_ = std::make_unique<InstrumentedIOContextWithThread>("GcsNodeManagerTest");
  }

 protected:
  std::unique_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<GcsServerMocker::MockRayletClient> raylet_client_;
  std::unique_ptr<rpc::RayletClientPool> client_pool_;
  std::unique_ptr<gcs::GcsPublisher> gcs_publisher_;
  std::unique_ptr<InstrumentedIOContextWithThread> io_context_;
};

TEST_F(GcsNodeManagerTest, TestManagement) {
  gcs::GcsNodeManager node_manager(gcs_publisher_.get(),
                                   gcs_table_storage_.get(),
                                   io_context_->GetIoService(),
                                   client_pool_.get(),
                                   ClusterID::Nil());
  // Test Add/Get/Remove functionality.
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());

  node_manager.AddNode(node);
  ASSERT_EQ(node, node_manager.GetAliveNode(node_id).value());

  rpc::NodeDeathInfo death_info;
  node_manager.RemoveNode(node_id, death_info);
  ASSERT_TRUE(!node_manager.GetAliveNode(node_id).has_value());
}

TEST_F(GcsNodeManagerTest, TestListener) {
  gcs::GcsNodeManager node_manager(gcs_publisher_.get(),
                                   gcs_table_storage_.get(),
                                   io_context_->GetIoService(),
                                   client_pool_.get(),
                                   ClusterID::Nil());
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
  rpc::NodeDeathInfo death_info;
  for (int i = 0; i < node_count; ++i) {
    node_manager.RemoveNode(NodeID::FromBinary(added_nodes[i]->node_id()), death_info);
  }
  ASSERT_EQ(node_count, removed_nodes.size());
  ASSERT_TRUE(node_manager.GetAllAliveNodes().empty());
  for (int i = 0; i < node_count; ++i) {
    ASSERT_EQ(added_nodes[i], removed_nodes[i]);
  }
}

TEST_F(GcsNodeManagerTest, TestUpdateAliveNode) {
  gcs::GcsNodeManager node_manager(gcs_publisher_.get(),
                                   gcs_table_storage_.get(),
                                   io_context_->GetIoService(),
                                   client_pool_.get(),
                                   ClusterID::Nil());

  // Create a test node
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());

  // Add the node to the manager
  node_manager.AddNode(node);

  // Test 1: Update node with idle state
  {
    rpc::syncer::ResourceViewSyncMessage sync_message;
    sync_message.set_idle_duration_ms(5000);

    node_manager.UpdateAliveNode(node_id, sync_message);

    auto updated_node = node_manager.GetAliveNode(node_id);
    EXPECT_TRUE(updated_node.has_value());
    EXPECT_EQ(updated_node.value()->state_snapshot().state(), rpc::NodeSnapshot::IDLE);
    EXPECT_EQ(updated_node.value()->state_snapshot().idle_duration_ms(), 5000);
  }

  // Test 2: Update node with active state (idle_duration_ms = 0)
  {
    rpc::syncer::ResourceViewSyncMessage sync_message;
    sync_message.set_idle_duration_ms(0);
    sync_message.add_node_activity("Busy workers on node.");

    node_manager.UpdateAliveNode(node_id, sync_message);

    auto updated_node = node_manager.GetAliveNode(node_id);
    EXPECT_TRUE(updated_node.has_value());
    EXPECT_EQ(updated_node.value()->state_snapshot().state(), rpc::NodeSnapshot::ACTIVE);
    EXPECT_EQ(updated_node.value()->state_snapshot().node_activity_size(), 1);
    EXPECT_EQ(updated_node.value()->state_snapshot().node_activity(0),
              "Busy workers on node.");
  }

  // Test 3: Update node with draining state
  {
    rpc::syncer::ResourceViewSyncMessage sync_message;
    sync_message.set_idle_duration_ms(0);
    sync_message.set_is_draining(true);

    node_manager.UpdateAliveNode(node_id, sync_message);

    auto updated_node = node_manager.GetAliveNode(node_id);
    EXPECT_TRUE(updated_node.has_value());
    EXPECT_EQ(updated_node.value()->state_snapshot().state(),
              rpc::NodeSnapshot::DRAINING);
  }
}

}  // namespace ray
