
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

#include "ray/gcs/gcs_server/grpc_based_resource_broadcaster.h"

#include <memory>

#include "gtest/gtest.h"
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
            /*send_batch*/
            [this](const rpc::Address &address,
                   std::shared_ptr<rpc::NodeManagerClientPool> &pool,
                   std::string &data,
                   const rpc::ClientCallback<rpc::UpdateResourceUsageReply> &callback) {
              num_batches_sent_++;
              callbacks_.push_back(callback);
            }) {}

  void SendBroadcast() {
    rpc::ResourceUsageBroadcastData batch;
    rpc::ResourceUpdate update;
    NodeID node_id = NodeID::FromRandom();
    update.mutable_data()->set_node_id(node_id.Binary());
    batch.add_batch()->Swap(&update);
    broadcaster_.SendBroadcast(std::move(batch));
  }

  void AssertNoLeaks() {
    absl::MutexLock guard(&broadcaster_.mutex_);
    ASSERT_EQ(broadcaster_.nodes_.size(), 0);
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
