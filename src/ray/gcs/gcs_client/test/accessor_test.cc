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