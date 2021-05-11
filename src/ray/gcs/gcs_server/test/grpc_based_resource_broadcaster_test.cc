
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
#include "ray/gcs/gcs_server/grpc_based_resource_broadcaster.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
namespace gcs {

using ::testing::_;

class GrpcBasedResourceBroadcasterTest : public ::testing::Test {
 public:
  GrpcBasedResourceBroadcasterTest()
      : num_batches_sent_(0),
        broadcaster_(
            /*raylet_client_pool*/ nullptr,
            /*get_resource_usage_batch_for_broadcast*/
            [](rpc::ResourceUsageBatchData &batch) {}

            ,
            /*send_batch*/
            [this](const rpc::Address &address,
                   std::shared_ptr<rpc::NodeManagerClientPool> &pool, std::string &data,
                   const rpc::ClientCallback<rpc::UpdateResourceUsageReply> &callback) {
              num_batches_sent_++;
              callbacks_.push_back(callback);
            }) {}

  void SendBroadcast() { broadcaster_.SendBroadcast(); }

  void AssertNoLeaks() {
    absl::MutexLock guard(&broadcaster_.mutex_);
    ASSERT_EQ(broadcaster_.nodes_.size(), 0);
    ASSERT_EQ(broadcaster_.inflight_updates_.size(), 0);
  }

  int num_batches_sent_;
  std::deque<rpc::ClientCallback<rpc::UpdateResourceUsageReply>> callbacks_;

  GrpcBasedResourceBroadcaster broadcaster_;
};

TEST_F(GrpcBasedResourceBroadcasterTest, TestBasic) {
  auto node_info = Mocker::GenNodeInfo();
  broadcaster_.HandleNodeAdded(*node_info);
  SendBroadcast();
  ASSERT_EQ(callbacks_.size(), 1);
  ASSERT_EQ(num_batches_sent_, 1);
}

TEST_F(GrpcBasedResourceBroadcasterTest, TestStragglerNodes) {
  // When a node doesn't ACK a batch update, drop future requests to that node to prevent
  // a queue from building up.
  for (int i = 0; i < 10; i++) {
    auto node_info = Mocker::GenNodeInfo();
    broadcaster_.HandleNodeAdded(*node_info);
  }

  SendBroadcast();
  ASSERT_EQ(callbacks_.size(), 10);
  ASSERT_EQ(num_batches_sent_, 10);

  // Only 7 nodes reply.
  for (int i = 0; i < 7; i++) {
    rpc::UpdateResourceUsageReply reply;
    auto &callback = callbacks_.front();
    callback(Status::OK(), reply);
    callbacks_.pop_front();
  }
  ASSERT_EQ(callbacks_.size(), 3);
  ASSERT_EQ(num_batches_sent_, 10);

  // We should only send a new rpc to the 7 nodes that haven't received one yet.
  SendBroadcast();
  ASSERT_EQ(callbacks_.size(), 10);
  ASSERT_EQ(num_batches_sent_, 17);

  // Now clear the queue and resume sending broadcasts to everyone.
  while (callbacks_.size()) {
    rpc::UpdateResourceUsageReply reply;
    auto &callback = callbacks_.front();
    callback(Status::OK(), reply);
    callbacks_.pop_front();
  }
  ASSERT_EQ(callbacks_.size(), 0);
  ASSERT_EQ(num_batches_sent_, 17);

  SendBroadcast();
  ASSERT_EQ(callbacks_.size(), 10);
  ASSERT_EQ(num_batches_sent_, 27);
}

TEST_F(GrpcBasedResourceBroadcasterTest, TestNodeRemoval) {
  auto node_info = Mocker::GenNodeInfo();
  broadcaster_.HandleNodeAdded(*node_info);
  SendBroadcast();
  ASSERT_EQ(callbacks_.size(), 1);
  ASSERT_EQ(num_batches_sent_, 1);

  broadcaster_.HandleNodeRemoved(*node_info);
  AssertNoLeaks();

  {
    // Respond to the rpc
    rpc::UpdateResourceUsageReply reply;
    callbacks_.front()(Status::OK(), reply);
    callbacks_.pop_front();
  }
  AssertNoLeaks();
}

}  // namespace gcs
}  // namespace ray
