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
  accessor.node_change_callback_ = [&](NodeID, const rpc::GcsNodeInfo &) {
    num_notifications++;
  };
  NodeID node_id = NodeID::FromRandom();

  rpc::GcsNodeInfo node_info;
  node_info.set_node_id(node_id.Binary());
  node_info.set_state(rpc::GcsNodeInfo::ALIVE);
  accessor.HandleNotification(rpc::GcsNodeInfo(node_info));
  const auto *gotten_node_info = accessor.Get(node_id, /*filter_dead_nodes=*/false);
  ASSERT_EQ(gotten_node_info->node_id(), node_id.Binary());
  ASSERT_EQ(gotten_node_info->state(), rpc::GcsNodeInfo::ALIVE);

  node_info.set_state(rpc::GcsNodeInfo::DEAD);
  accessor.HandleNotification(rpc::GcsNodeInfo(node_info));
  gotten_node_info = accessor.Get(node_id, /*filter_dead_nodes=*/false);
  ASSERT_EQ(gotten_node_info->state(), rpc::GcsNodeInfo::DEAD);
  ASSERT_EQ(accessor.Get(node_id, /*filter_dead_nodes=*/true), nullptr);

  node_info.set_state(rpc::GcsNodeInfo::ALIVE);
  accessor.HandleNotification(rpc::GcsNodeInfo(node_info));
  gotten_node_info = accessor.Get(node_id, /*filter_dead_nodes=*/false);
  ASSERT_EQ(gotten_node_info->state(), rpc::GcsNodeInfo::DEAD);

  ASSERT_EQ(num_notifications, 2);
}

TEST(NodeInfoAccessorTest, TestHandleNotificationDeathInfo) {
  NodeInfoAccessor accessor;
  rpc::GcsNodeInfo node_info;
  node_info.set_state(rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_DEAD);
  NodeID node_id = NodeID::FromRandom();
  node_info.set_node_id(node_id.Binary());

  auto death_info = node_info.mutable_death_info();
  death_info->set_reason(rpc::NodeDeathInfo::EXPECTED_TERMINATION);
  death_info->set_reason_message("Test termination reason");

  node_info.set_end_time_ms(12345678);

  accessor.HandleNotification(std::move(node_info));

  auto cached_node = accessor.Get(node_id, false);
  ASSERT_NE(cached_node, nullptr);
  ASSERT_EQ(cached_node->node_id(), node_id.Binary());
  ASSERT_EQ(cached_node->state(),
            rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_DEAD);

  ASSERT_TRUE(cached_node->has_death_info());
  ASSERT_EQ(cached_node->death_info().reason(), rpc::NodeDeathInfo::EXPECTED_TERMINATION);
  ASSERT_EQ(cached_node->death_info().reason_message(), "Test termination reason");
  ASSERT_EQ(cached_node->end_time_ms(), 12345678);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace gcs
}  // namespace ray
