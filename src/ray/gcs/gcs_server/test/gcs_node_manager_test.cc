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

#include <ray/gcs/test/gcs_test_util.h>

#include <memory>
#include "gtest/gtest.h"

namespace ray {
class GcsNodeManagerTest : public ::testing::Test {};

TEST_F(GcsNodeManagerTest, TestManagement) {
  boost::asio::io_service io_service;
  auto node_info_accessor = Mocker::MockedNodeInfoAccessor();
  auto error_info_accessor = Mocker::MockedErrorInfoAccessor();
  gcs::GcsNodeManager node_manager(io_service, node_info_accessor, error_info_accessor);
  // Test Add/Get/Remove functionality.
  auto node = Mocker::GenNodeInfo();
  auto node_id = ClientID::FromBinary(node->node_id());

  node_manager.AddNode(node);
  ASSERT_EQ(node, node_manager.GetNode(node_id));

  node_manager.RemoveNode(node_id);
  ASSERT_EQ(nullptr, node_manager.GetNode(node_id));
}

TEST_F(GcsNodeManagerTest, TestListener) {
  boost::asio::io_service io_service;
  auto node_info_accessor = Mocker::MockedNodeInfoAccessor();
  auto error_info_accessor = Mocker::MockedErrorInfoAccessor();
  gcs::GcsNodeManager node_manager(io_service, node_info_accessor, error_info_accessor);
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
    ASSERT_EQ(1, alive_nodes.count(ClientID::FromBinary(node->node_id())));
  }

  // Test AddNodeRemovedListener.
  std::vector<std::shared_ptr<rpc::GcsNodeInfo>> removed_nodes;
  node_manager.AddNodeRemovedListener(
      [&removed_nodes](std::shared_ptr<rpc::GcsNodeInfo> node) {
        removed_nodes.emplace_back(std::move(node));
      });
  for (int i = 0; i < node_count; ++i) {
    node_manager.RemoveNode(ClientID::FromBinary(added_nodes[i]->node_id()));
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
