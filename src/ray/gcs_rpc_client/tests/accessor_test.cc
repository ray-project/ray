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
      [&](NodeID, const rpc::GcsNodeAddressAndLiveness &, const bool) {
        num_notifications++;
      };
  NodeID node_id = NodeID::FromRandom();

  rpc::GcsNodeAddressAndLiveness node_info;
  node_info.set_node_id(node_id.Binary());
  node_info.set_state(rpc::GcsNodeInfo::ALIVE);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info));
  const auto *gotten_node_info =
      accessor.GetNodeAddressAndLiveness(node_id, /*filter_dead_nodes=*/false);
  ASSERT_EQ(gotten_node_info->node_id(), node_id.Binary());
  ASSERT_EQ(gotten_node_info->state(), rpc::GcsNodeInfo::ALIVE);

  node_info.set_state(rpc::GcsNodeInfo::DEAD);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info));
  gotten_node_info =
      accessor.GetNodeAddressAndLiveness(node_id, /*filter_dead_nodes=*/false);
  ASSERT_EQ(gotten_node_info->state(), rpc::GcsNodeInfo::DEAD);
  ASSERT_EQ(accessor.GetNodeAddressAndLiveness(node_id, /*filter_dead_nodes=*/true),
            nullptr);

  node_info.set_state(rpc::GcsNodeInfo::ALIVE);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info));
  gotten_node_info =
      accessor.GetNodeAddressAndLiveness(node_id, /*filter_dead_nodes=*/false);
  ASSERT_EQ(gotten_node_info->state(), rpc::GcsNodeInfo::DEAD);

  ASSERT_EQ(num_notifications, 2);
}

TEST(NodeInfoAccessorTest, TestHandleNotificationIsInitializingOverride) {
  // Test that when a DEAD notification comes during initialization, but the cache
  // already has the node as ALIVE (meaning polling saw it first), the callback
  // receives is_initializing=false so that counters get decremented correctly.
  //
  // This fixes a bug where:
  // 1. Poll returns ALIVE → counter increments
  // 2. Init returns DEAD with is_initializing=true → counter skips decrement (BUG)
  // 3. Later poll DEAD is filtered by cache
  // 4. Counter stuck at wrong value

  NodeInfoAccessor accessor;
  std::vector<bool> is_initializing_values;
  accessor.node_change_callback_address_and_liveness_ =
      [&](NodeID, const rpc::GcsNodeAddressAndLiveness &, const bool is_initializing) {
        is_initializing_values.push_back(is_initializing);
      };
  NodeID node_id = NodeID::FromRandom();

  rpc::GcsNodeAddressAndLiveness node_info;
  node_info.set_node_id(node_id.Binary());

  // Simulate poll seeing ALIVE first (is_initializing=false)
  node_info.set_state(rpc::GcsNodeInfo::ALIVE);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info), false);
  ASSERT_EQ(is_initializing_values.size(), 1);
  ASSERT_EQ(is_initializing_values[0], false);

  // Simulate init seeing DEAD (passed is_initializing=true, but should be overridden)
  node_info.set_state(rpc::GcsNodeInfo::DEAD);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info), true);
  ASSERT_EQ(is_initializing_values.size(), 2);
  // Key assertion: even though we passed is_initializing=true, the callback should
  // receive false because the cache had ALIVE and we're transitioning to DEAD
  ASSERT_EQ(is_initializing_values[1], false);
}

TEST(NodeInfoAccessorTest, TestHandleNotificationIsInitializingPreserved) {
  // Test that when a DEAD notification comes during initialization and the cache
  // does NOT have the node (no prior ALIVE), is_initializing=true is preserved.

  NodeInfoAccessor accessor;
  std::vector<bool> is_initializing_values;
  accessor.node_change_callback_address_and_liveness_ =
      [&](NodeID, const rpc::GcsNodeAddressAndLiveness &, const bool is_initializing) {
        is_initializing_values.push_back(is_initializing);
      };
  NodeID node_id = NodeID::FromRandom();

  rpc::GcsNodeAddressAndLiveness node_info;
  node_info.set_node_id(node_id.Binary());

  // Simulate init seeing DEAD first (no prior ALIVE in cache)
  node_info.set_state(rpc::GcsNodeInfo::DEAD);
  accessor.HandleNotification(rpc::GcsNodeAddressAndLiveness(node_info), true);
  ASSERT_EQ(is_initializing_values.size(), 1);
  // is_initializing should be preserved as true since there was no ALIVE→DEAD transition
  ASSERT_EQ(is_initializing_values[0], true);
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
  ASSERT_NE(cached_node, nullptr);
  ASSERT_EQ(cached_node->node_id(), node_id.Binary());
  ASSERT_EQ(cached_node->state(),
            rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_DEAD);

  ASSERT_TRUE(cached_node->has_death_info());
  ASSERT_EQ(cached_node->death_info().reason(), rpc::NodeDeathInfo::EXPECTED_TERMINATION);
  ASSERT_EQ(cached_node->death_info().reason_message(), "Test termination reason");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace gcs
}  // namespace ray
