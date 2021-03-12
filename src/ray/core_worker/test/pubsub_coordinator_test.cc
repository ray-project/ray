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

#include "ray/core_worker/pubsub/pubsub_coordinator.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace ray {

using ::testing::_;

class PubsubCoordinatorTest : public ::testing::Test {
 public:
  PubsubCoordinatorTest() {}

  ~PubsubCoordinatorTest() {}

  void SetUp() {
    dead_nodes_.clear();
    pubsub_coordinator_ = std::shared_ptr<PubsubCoordinator>(new PubsubCoordinator(
        [this](const NodeID &node_id) { return dead_nodes_.count(node_id) == 1; }));
  }

  void RegisterDeadNode(const NodeID &node_id) { dead_nodes_.emplace(node_id); }

  std::unordered_set<NodeID> dead_nodes_;
  std::shared_ptr<PubsubCoordinator> pubsub_coordinator_;
};

TEST_F(PubsubCoordinatorTest, TestSubscriptionIndex) {}

TEST_F(PubsubCoordinatorTest, TestSubscriber) {}

TEST_F(PubsubCoordinatorTest, TestBasicSingleSubscriber) {
  std::vector<ObjectID> batched_ids;
  auto long_polling_connect = [&batched_ids](std::vector<ObjectID> &object_ids) {
    auto it = batched_ids.begin();
    batched_ids.insert(it, object_ids.begin(), object_ids.end());
  };

  const auto subscriber_node_id = NodeID::FromRandom();
  const auto oid = ObjectID::FromRandom();

  pubsub_coordinator_->Connect(subscriber_node_id, long_polling_connect);
  pubsub_coordinator_->RegisterSubscription(subscriber_node_id, oid);
  pubsub_coordinator_->Publish(oid);
  RAY_LOG(ERROR) << "abc";
  ASSERT_EQ(batched_ids[0], oid);
}

// No connection after registration.
// Multi object subscription from a single node.
// Single object subscription from multi nodes.
// Multi object subscription from multi nodes.
// Node failure from multi nodes.
// Node failure when connection initiated.
// Node failure when not connected.
// Unregistration an entry.
// Unregistration a subscriber.

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
