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

#include "ray/gcs_rpc_client/accessor.h"

#include "gtest/gtest.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

TEST(NodeInfoAccessorTest, TestHandleNotification) {
  // First handle notification that node is alive.
  // Then handle notification that node is dead.
  // Then handle notification that node is alive, should be ignored though because node
  // can only go from alive to dead, never back to alive again.

  NodeInfoAccessor accessor;
  int num_notifications = 0;
  accessor.node_change_callback_address_and_liveness_ =
      [&](NodeID, const rpc::GcsNodeAddressAndLiveness &) { num_notifications++; };
  NodeID node_id = NodeID::FromRandom();

  rpc::GcsNodeAddressAndLiveness node_info;
  node_info.set_node_id(node_id.Binary());
  node_info.set_state(rpc::GcsNodeInfo::ALIVE);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info));
  auto gotten_node_info =
      accessor.GetNodeAddressAndLiveness(node_id, /*filter_dead_nodes=*/false);
  ASSERT_TRUE(gotten_node_info.has_value());
  ASSERT_EQ(gotten_node_info->node_id(), node_id.Binary());
  ASSERT_EQ(gotten_node_info->state(), rpc::GcsNodeInfo::ALIVE);

  node_info.set_state(rpc::GcsNodeInfo::DEAD);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info));
  gotten_node_info =
      accessor.GetNodeAddressAndLiveness(node_id, /*filter_dead_nodes=*/false);
  ASSERT_TRUE(gotten_node_info.has_value());
  ASSERT_EQ(gotten_node_info->state(), rpc::GcsNodeInfo::DEAD);
  ASSERT_FALSE(accessor.GetNodeAddressAndLiveness(node_id, /*filter_dead_nodes=*/true)
                   .has_value());

  node_info.set_state(rpc::GcsNodeInfo::ALIVE);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info));
  gotten_node_info =
      accessor.GetNodeAddressAndLiveness(node_id, /*filter_dead_nodes=*/false);
  ASSERT_TRUE(gotten_node_info.has_value());
  ASSERT_EQ(gotten_node_info->state(), rpc::GcsNodeInfo::DEAD);

  ASSERT_EQ(num_notifications, 2);
}

TEST(NodeInfoAccessorTest, TestHandleNotificationDeathInfo) {
  NodeInfoAccessor accessor;
  rpc::GcsNodeAddressAndLiveness node_info;
  node_info.set_state(rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_DEAD);
  NodeID node_id = NodeID::FromRandom();
  node_info.set_node_id(node_id.Binary());

  auto death_info = node_info.mutable_death_info();
  death_info->set_reason(rpc::NodeDeathInfo::EXPECTED_TERMINATION);
  death_info->set_reason_message("Test termination reason");

  accessor.HandleNotification(std::move(node_info));

  auto cached_node = accessor.GetNodeAddressAndLiveness(node_id, false);
  ASSERT_TRUE(cached_node.has_value());
  ASSERT_EQ(cached_node->node_id(), node_id.Binary());
  ASSERT_EQ(cached_node->state(),
            rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_DEAD);

  ASSERT_TRUE(cached_node->has_death_info());
  ASSERT_EQ(cached_node->death_info().reason(), rpc::NodeDeathInfo::EXPECTED_TERMINATION);
  ASSERT_EQ(cached_node->death_info().reason_message(), "Test termination reason");
}

TEST(NodeInfoAccessorTest, TestGetAliveNodeCountInitiallyZero) {
  // Test that the alive node count is 0 initially.
  NodeInfoAccessor accessor;
  ASSERT_EQ(accessor.GetAliveNodeCount(), 0);
}

TEST(NodeInfoAccessorTest, TestGetAliveNodeCountIncrementsOnAliveNode) {
  // Test that adding an alive node increments the count.
  NodeInfoAccessor accessor;
  ASSERT_EQ(accessor.GetAliveNodeCount(), 0);

  NodeID node_id = NodeID::FromRandom();
  rpc::GcsNodeAddressAndLiveness node_info;
  node_info.set_node_id(node_id.Binary());
  node_info.set_state(rpc::GcsNodeInfo::ALIVE);

  accessor.HandleNotification(std::move(node_info));
  ASSERT_EQ(accessor.GetAliveNodeCount(), 1);
}

TEST(NodeInfoAccessorTest, TestGetAliveNodeCountMultipleNodes) {
  // Test that adding multiple alive nodes increments the count correctly.
  NodeInfoAccessor accessor;
  ASSERT_EQ(accessor.GetAliveNodeCount(), 0);

  // Add 3 alive nodes
  for (int i = 0; i < 3; i++) {
    NodeID node_id = NodeID::FromRandom();
    rpc::GcsNodeAddressAndLiveness node_info;
    node_info.set_node_id(node_id.Binary());
    node_info.set_state(rpc::GcsNodeInfo::ALIVE);
    accessor.HandleNotification(std::move(node_info));
  }

  ASSERT_EQ(accessor.GetAliveNodeCount(), 3);
}

TEST(NodeInfoAccessorTest, TestGetAliveNodeCountDecrementsOnDeath) {
  // Test that a node going from alive to dead decrements the count.
  NodeInfoAccessor accessor;
  NodeID node_id = NodeID::FromRandom();

  // First add an alive node
  rpc::GcsNodeAddressAndLiveness node_info;
  node_info.set_node_id(node_id.Binary());
  node_info.set_state(rpc::GcsNodeInfo::ALIVE);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info));
  ASSERT_EQ(accessor.GetAliveNodeCount(), 1);

  // Then mark it as dead
  node_info.set_state(rpc::GcsNodeInfo::DEAD);
  accessor.HandleNotification(std::move(node_info));
  ASSERT_EQ(accessor.GetAliveNodeCount(), 0);
}

TEST(NodeInfoAccessorTest, TestGetAliveNodeCountDeadNodeDirectlyAdded) {
  // Test that adding a dead node directly doesn't increment the count.
  NodeInfoAccessor accessor;
  ASSERT_EQ(accessor.GetAliveNodeCount(), 0);

  NodeID node_id = NodeID::FromRandom();
  rpc::GcsNodeAddressAndLiveness node_info;
  node_info.set_node_id(node_id.Binary());
  node_info.set_state(rpc::GcsNodeInfo::DEAD);

  accessor.HandleNotification(std::move(node_info));
  ASSERT_EQ(accessor.GetAliveNodeCount(), 0);
}

TEST(NodeInfoAccessorTest, TestGetAliveNodeCountResurrectionIgnored) {
  // Test that attempting to resurrect a dead node doesn't affect the count.
  // This corresponds to the TestHandleNotification test which verifies that
  // a dead node cannot become alive again.
  NodeInfoAccessor accessor;
  NodeID node_id = NodeID::FromRandom();

  // Add alive node
  rpc::GcsNodeAddressAndLiveness node_info;
  node_info.set_node_id(node_id.Binary());
  node_info.set_state(rpc::GcsNodeInfo::ALIVE);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info));
  ASSERT_EQ(accessor.GetAliveNodeCount(), 1);

  // Mark as dead
  node_info.set_state(rpc::GcsNodeInfo::DEAD);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info));
  ASSERT_EQ(accessor.GetAliveNodeCount(), 0);

  // Try to resurrect - should be ignored
  node_info.set_state(rpc::GcsNodeInfo::ALIVE);
  accessor.HandleNotification(std::move(node_info));
  ASSERT_EQ(accessor.GetAliveNodeCount(), 0);  // Still 0, resurrection is ignored
}

TEST(NodeInfoAccessorTest, TestGetAliveNodeCountMultipleNodesWithDeaths) {
  // Test the count with multiple nodes where some die.
  NodeInfoAccessor accessor;

  // Add 5 alive nodes and track their IDs
  std::vector<NodeID> node_ids;
  for (int i = 0; i < 5; i++) {
    NodeID node_id = NodeID::FromRandom();
    node_ids.push_back(node_id);
    rpc::GcsNodeAddressAndLiveness node_info;
    node_info.set_node_id(node_id.Binary());
    node_info.set_state(rpc::GcsNodeInfo::ALIVE);
    accessor.HandleNotification(std::move(node_info));
  }
  ASSERT_EQ(accessor.GetAliveNodeCount(), 5);

  // Kill 2 nodes
  for (int i = 0; i < 2; i++) {
    rpc::GcsNodeAddressAndLiveness node_info;
    node_info.set_node_id(node_ids[i].Binary());
    node_info.set_state(rpc::GcsNodeInfo::DEAD);
    accessor.HandleNotification(std::move(node_info));
  }
  ASSERT_EQ(accessor.GetAliveNodeCount(), 3);

  // Add 2 more alive nodes
  for (int i = 0; i < 2; i++) {
    NodeID node_id = NodeID::FromRandom();
    rpc::GcsNodeAddressAndLiveness node_info;
    node_info.set_node_id(node_id.Binary());
    node_info.set_state(rpc::GcsNodeInfo::ALIVE);
    accessor.HandleNotification(std::move(node_info));
  }
  ASSERT_EQ(accessor.GetAliveNodeCount(), 5);
}

TEST(NodeInfoAccessorTest, TestGetAliveNodeCountDuplicateAliveNotification) {
  // Test that receiving duplicate alive notifications for the same node
  // doesn't double-count.
  NodeInfoAccessor accessor;
  NodeID node_id = NodeID::FromRandom();

  rpc::GcsNodeAddressAndLiveness node_info;
  node_info.set_node_id(node_id.Binary());
  node_info.set_state(rpc::GcsNodeInfo::ALIVE);

  // Send the same alive notification twice
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info));
  ASSERT_EQ(accessor.GetAliveNodeCount(), 1);

  accessor.HandleNotification(std::move(node_info));
  ASSERT_EQ(accessor.GetAliveNodeCount(), 1);  // Still 1, not double-counted
}

TEST(NodeInfoAccessorTest, TestGetAliveNodeCountDuplicateDeadNotification) {
  // Test that receiving duplicate dead notifications for the same node
  // doesn't incorrectly decrement the count.
  // This can happen during bootstrap when pubsub messages arrive out of order
  // relative to the initial state query.
  NodeInfoAccessor accessor;
  NodeID node_id = NodeID::FromRandom();

  rpc::GcsNodeAddressAndLiveness node_info;
  node_info.set_node_id(node_id.Binary());
  node_info.set_state(rpc::GcsNodeInfo::DEAD);

  // First dead notification (e.g., from pubsub during bootstrap)
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info));
  ASSERT_EQ(accessor.GetAliveNodeCount(), 0);

  // Second dead notification (e.g., from initial state query)
  accessor.HandleNotification(std::move(node_info));
  ASSERT_EQ(accessor.GetAliveNodeCount(), 0);  // Still 0, not decremented below 0
}

TEST(NodeInfoAccessorTest, TestGetAliveNodeCountResurrectionOfInitiallyDeadNode) {
  // Test that a node that was first seen as DEAD cannot be resurrected to ALIVE.
  // This can happen during bootstrap when:
  // 1. Initial state query returns a node as DEAD (node died before we started)
  // 2. A late/out-of-order pubsub message arrives saying the node is ALIVE
  // The resurrection should be ignored.
  NodeInfoAccessor accessor;
  NodeID node_id = NodeID::FromRandom();

  rpc::GcsNodeAddressAndLiveness node_info;
  node_info.set_node_id(node_id.Binary());

  // First notification: node is DEAD (e.g., from initial state bootstrap)
  node_info.set_state(rpc::GcsNodeInfo::DEAD);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info));
  ASSERT_EQ(accessor.GetAliveNodeCount(), 0);
  ASSERT_TRUE(accessor.IsNodeDead(node_id));

  // Second notification: late ALIVE message arrives (should be ignored)
  node_info.set_state(rpc::GcsNodeInfo::ALIVE);
  accessor.HandleNotification(std::move(node_info));
  ASSERT_EQ(accessor.GetAliveNodeCount(), 0);  // Still 0, resurrection ignored
  ASSERT_TRUE(accessor.IsNodeDead(node_id));   // Node is still dead
}

TEST(NodeInfoAccessorTest, TestGetAliveNodeCountBootstrapRaceCondition) {
  // Comprehensive test simulating bootstrap race conditions.
  // During bootstrap, pubsub subscription starts before initial state is fetched,
  // so messages can arrive in any order.
  NodeInfoAccessor accessor;

  // Scenario: Multiple nodes with various race conditions
  NodeID node1 = NodeID::FromRandom();  // Will be: pubsub ALIVE -> initial ALIVE
  NodeID node2 = NodeID::FromRandom();  // Will be: pubsub ALIVE -> initial DEAD
  NodeID node3 = NodeID::FromRandom();  // Will be: pubsub DEAD -> initial ALIVE
  NodeID node4 = NodeID::FromRandom();  // Will be: pubsub DEAD -> initial DEAD
  NodeID node5 = NodeID::FromRandom();  // Will be: only in initial state as ALIVE

  // Simulate pubsub messages arriving first
  rpc::GcsNodeAddressAndLiveness node_info;

  // Node1: pubsub says ALIVE
  node_info.set_node_id(node1.Binary());
  node_info.set_state(rpc::GcsNodeInfo::ALIVE);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info));

  // Node2: pubsub says ALIVE
  node_info.set_node_id(node2.Binary());
  node_info.set_state(rpc::GcsNodeInfo::ALIVE);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info));

  // Node3: pubsub says DEAD
  node_info.set_node_id(node3.Binary());
  node_info.set_state(rpc::GcsNodeInfo::DEAD);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info));

  // Node4: pubsub says DEAD
  node_info.set_node_id(node4.Binary());
  node_info.set_state(rpc::GcsNodeInfo::DEAD);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info));

  ASSERT_EQ(accessor.GetAliveNodeCount(), 2);  // node1 and node2 are alive

  // Now simulate initial state bootstrap arriving
  // Node1: initial state says ALIVE (duplicate, should be ignored)
  node_info.set_node_id(node1.Binary());
  node_info.set_state(rpc::GcsNodeInfo::ALIVE);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info));

  // Node2: initial state says DEAD (node died, count should decrement)
  node_info.set_node_id(node2.Binary());
  node_info.set_state(rpc::GcsNodeInfo::DEAD);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info));

  // Node3: initial state says ALIVE (resurrection attempt, should be ignored)
  node_info.set_node_id(node3.Binary());
  node_info.set_state(rpc::GcsNodeInfo::ALIVE);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info));

  // Node4: initial state says DEAD (duplicate dead, should be ignored)
  node_info.set_node_id(node4.Binary());
  node_info.set_state(rpc::GcsNodeInfo::DEAD);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info));

  // Node5: only appears in initial state as ALIVE
  node_info.set_node_id(node5.Binary());
  node_info.set_state(rpc::GcsNodeInfo::ALIVE);
  accessor.HandleNotification(std::move(node_info));

  // Final state:
  // node1: ALIVE (was alive, got duplicate alive)
  // node2: DEAD (was alive, then died)
  // node3: DEAD (was dead, resurrection ignored)
  // node4: DEAD (was dead, duplicate dead)
  // node5: ALIVE (newly added)
  ASSERT_EQ(accessor.GetAliveNodeCount(), 2);  // node1 and node5

  ASSERT_TRUE(accessor.IsNodeAlive(node1));
  ASSERT_TRUE(accessor.IsNodeDead(node2));
  ASSERT_TRUE(accessor.IsNodeDead(node3));
  ASSERT_TRUE(accessor.IsNodeDead(node4));
  ASSERT_TRUE(accessor.IsNodeAlive(node5));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace gcs
}  // namespace ray
