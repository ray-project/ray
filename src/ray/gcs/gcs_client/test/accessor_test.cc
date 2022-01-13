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

#include "ray/gcs/gcs_client/accessor.h"

#include "gtest/gtest.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
using namespace ray::gcs;
using namespace ray::rpc;

TEST(NodeInfoAccessorTest, TestHandleNotification) {
  NodeInfoAccessor accessor;
  GcsNodeInfo node_info;
  node_info.set_state(rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_DEAD);
  NodeID node_id = NodeID::FromRandom();
  node_info.set_node_id(node_id.Binary());
  accessor.HandleNotification(node_info);
  ASSERT_EQ(accessor.Get(node_id, false)->node_id(), node_id.Binary());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
}  // namespace ray
