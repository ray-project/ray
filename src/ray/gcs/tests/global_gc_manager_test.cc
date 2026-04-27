// Copyright 2025 The Ray Authors.
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

#include "ray/gcs/global_gc_manager.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "mock/ray/gcs/gcs_node_manager.h"
#include "mock/ray/raylet_client/raylet_client.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/common/test_utils.h"

namespace ray {
namespace gcs {

namespace {

constexpr int64_t kSecondNs = 1'000'000'000LL;
constexpr int64_t kIntervalNs = 30 * kSecondNs;

std::shared_ptr<rpc::GcsNodeInfo> MakeNodeInfo(const NodeID &node_id,
                                               const std::string &ip,
                                               int port) {
  auto node = std::make_shared<rpc::GcsNodeInfo>();
  node->set_node_id(node_id.Binary());
  node->set_node_manager_address(ip);
  node->set_node_manager_port(port);
  node->set_state(rpc::GcsNodeInfo::ALIVE);
  return node;
}

}  // namespace

class GlobalGCManagerTest : public ::testing::Test {
 protected:
  GlobalGCManagerTest()
      : raylet_client_pool_([this](const rpc::Address &address) {
          auto client = std::make_shared<MockRayletClientInterface>();
          mock_clients_.push_back(client);
          // Each call to GlobalGC just bumps a counter on the test fixture so
          // the assertions don't depend on which specific mock client served the
          // request.
          ON_CALL(*client, GlobalGC(::testing::_))
              .WillByDefault([this](const rpc::ClientCallback<rpc::GlobalGCReply> &) {
                ++global_gc_calls_;
              });
          return client;
        }) {
    gcs_node_manager_ = std::make_unique<MockGcsNodeManager>();
    manager_ = std::make_unique<GlobalGCManager>(
        *gcs_node_manager_,
        raylet_client_pool_,
        kIntervalNs,
        [this]() { return fake_now_ns_; });
  }

  // Returns the registered NodeID so the caller can later remove it if needed.
  NodeID AddAliveNode(const std::string &ip, int port) {
    auto node_id = NodeID::FromRandom();
    gcs_node_manager_->AddNode(MakeNodeInfo(node_id, ip, port));
    return node_id;
  }

  void TriggerOnce() {
    rpc::TriggerGlobalGCBestEffortRequest request;
    rpc::TriggerGlobalGCBestEffortReply reply;
    Status reply_status = Status::Invalid("not set");
    manager_->HandleTriggerGlobalGCBestEffort(
        request, &reply, [&reply_status](Status s, auto, auto) { reply_status = s; });
    EXPECT_TRUE(reply_status.ok());
  }

  int64_t fake_now_ns_ = 0;
  int global_gc_calls_ = 0;
  std::vector<std::shared_ptr<MockRayletClientInterface>> mock_clients_;
  rpc::RayletClientPool raylet_client_pool_;
  std::unique_ptr<MockGcsNodeManager> gcs_node_manager_;
  std::unique_ptr<GlobalGCManager> manager_;
};

TEST_F(GlobalGCManagerTest, FirstRequestBroadcastsToAllAliveNodes) {
  AddAliveNode("1.1.1.1", 1001);
  AddAliveNode("2.2.2.2", 2002);

  TriggerOnce();

  EXPECT_EQ(global_gc_calls_, 2);
}

TEST_F(GlobalGCManagerTest, RequestInsideThrottleWindowIsDropped) {
  AddAliveNode("1.1.1.1", 1001);
  TriggerOnce();
  EXPECT_EQ(global_gc_calls_, 1);

  // Still inside the throttle window — request must be dropped.
  fake_now_ns_ = kIntervalNs - 1;
  TriggerOnce();
  EXPECT_EQ(global_gc_calls_, 1);
}

TEST_F(GlobalGCManagerTest, RequestAfterThrottleWindowFiresAgain) {
  AddAliveNode("1.1.1.1", 1001);
  TriggerOnce();
  EXPECT_EQ(global_gc_calls_, 1);

  // Exactly at the boundary should also pass (>= comparison).
  fake_now_ns_ = kIntervalNs;
  TriggerOnce();
  EXPECT_EQ(global_gc_calls_, 2);

  // Well past the window — fires again.
  fake_now_ns_ = 5 * kIntervalNs;
  TriggerOnce();
  EXPECT_EQ(global_gc_calls_, 3);
}

TEST_F(GlobalGCManagerTest, EmptyClusterIsNoOp) {
  TriggerOnce();
  EXPECT_EQ(global_gc_calls_, 0);

  // Throttle still updates: the next request inside the window is dropped even
  // though no broadcast happened. This is the simpler/safer semantic.
  AddAliveNode("1.1.1.1", 1001);
  fake_now_ns_ = kIntervalNs - 1;
  TriggerOnce();
  EXPECT_EQ(global_gc_calls_, 0);
}

TEST_F(GlobalGCManagerTest, ReplyIsOkWhenThrottled) {
  AddAliveNode("1.1.1.1", 1001);
  TriggerOnce();

  // Inside the window — reply is still OK and no broadcast.
  rpc::TriggerGlobalGCBestEffortRequest request;
  rpc::TriggerGlobalGCBestEffortReply reply;
  Status reply_status = Status::Invalid("not set");
  manager_->HandleTriggerGlobalGCBestEffort(
      request, &reply, [&reply_status](Status s, auto, auto) { reply_status = s; });
  EXPECT_TRUE(reply_status.ok());
  EXPECT_EQ(global_gc_calls_, 1);
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
