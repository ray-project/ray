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

TEST_F(PubsubCoordinatorTest, TestSubscriptionIndexSingeNodeSingleObject) {
  std::unordred_map<ObjectID, std::unordered_set<NodeID>> subscribers_map;

  auto node_id = NodeID::FromRandom();
  auto oid = ObjectID::FromRandom();
  auto &subscribers = subscribers_map[oid];
  subscribers.emplace(node_id);

  ///
  /// Test single node id & object id
  ///
  /// oid1 -> [nid1]
  SubscriptionIndex subscription_index;
  subscription_index.AddEntry(oid, node_id);
  const auto &subscribers_from_index = subscription_index.GetSubscriberIdsByObjectId(oid);
  for (const auto &it : subscribers) {
    const auto node_id = *it;
    ASSERT_TRUE(subscribers_from_index.count(node_id) > 0);
  }
}

TEST_F(PubsubCoordinatorTest, TestSubscriptionIndexMultiNodeSingleObject) {
  ///
  /// Test single object id & multi nodes
  ///
  /// oid1 -> [nid1~nid5]
  auto oid = ObjectID::FromRandom();
  auto &subscribers = subscribers_map[oid];

  for (int i = 0; i < 5; i++) {
    node_id = NodeID::FromRandom();
    subscribers.emplace(node_id);
    subscription_index.AddEntry(oid, node_id);
  }
  const auto &subscribers_from_index = subscription_index.GetSubscriberIdsByObjectId(oid);
  for (const auto &it : subscribers) {
    const auto node_id = *it;
    ASSERT_TRUE(subscribers_from_index.count(node_id) > 0);
  }

  ///
  /// Test multi node id & multi object ids
  ///
  /// oid1 -> [nid1~nid5]
  /// oid2 -> [nid1~nid5]
  auto oid2 = ObjectID::FromRandom()
  subscribers = object_ids_map[oid2];
  for (int i = 0; i < 5; i++) {
    node_id = NodeID::FromRandom();
    subscribers.emplace(node_id);
    subscription_index.AddEntry(oid2, node_id);
  }
  const auto &subscribers_from_index3 = subscription_index.GetSubscriberIdsByObjectId(oid2);
  for (const auto &it : subscribers) {
    const auto node_id = *it;
    ASSERT_TRUE(subscribers_from_index3.count(node_id) > 0);
  }
  // Make sure oid1 entries are not corrupted.
  const auto &subscribers_from_index4 = subscription_index.GetSubscriberIdsByObjectId(oid);
  subscribers = object_ids_map[oid];
  for (const auto &it : subscribers) {
    const auto node_id = *it;
    ASSERT_TRUE(subscribers_from_index4.count(node_id) > 0);
  }
}

TEST_F(PubsubCoordinatorTest, TestSubscriptionIndexErase) {
  ///
  /// Test erase entry.
  ///
  /// oid1 -> [nid1~nid5]
  /// oid2 -> [nid1~nid5]
  auto oid = ObjectID::FromRandom();
  auto &subscribers = subscribers_map[oid];

  // Add entries.
  for (int i = 0; i < 6; i++) {
    node_id = NodeID::FromRandom();
    subscribers.emplace(node_id);
    subscription_index.AddEntry(oid, node_id);
  }
  auto oid2 = ObjectID::FromRandom()
  subscribers = object_ids_map[oid2];
  for (int i = 0; i < 6; i++) {
    node_id = NodeID::FromRandom();
    subscribers.emplace(node_id);
    subscription_index.AddEntry(oid2, node_id);
  }

  // Verify.
  subscribers = object_ids_map[oid];
  std::vector<NodeID> subscribers_to_delete;
  int i = 0;
  int entries_to_delete = 3;
  for (auto it : subscribers) {
    if (i == entries_to_delete) {
      break;
    }
    subscribers_to_delete.push_back(*it);
    i++;
  }
  for (auto it : subscribers_to_delete) {
    auto &node_id = *it;
    subscribers.erase(node_id);
    subscription_index.EraseEntry(oid, node_id);
  }
  // Verify.
  const auto &subscribers_from_index = subscription_index.GetSubscriberIdsByObjectId(oid);
  subscribers = object_ids_map[oid];
  for (const auto &it : subscribers) {
    const auto node_id = *it;
    ASSERT_TRUE(subscribers_from_index.count(node_id) > 0);
  }

  // Delete all entries and make sure the oid is removed from the index.
  int i = 0;
  int entries_to_delete = 3;
  for (auto it : subscribers) {
    if (i == entries_to_delete) {
      break;
    }
    subscribers_to_delete.push_back(*it);
    i++;
  }
  for (auto it : subscribers_to_delete) {
    auto &node_id = *it;
    subscribers.erase(node_id);
    subscription_index.EraseEntry(oid, node_id);
  }
  ASSERT_FALSE(subscription_index.IsObjectIdExist(oid));
}

TEST_F(PubsubCoordinatorTest, TestSubscriptionIndexEraseSubscriber) {
  ///
  /// Test erase subscriber.
  ///
  auto oid = ObjectID::FromRandom();
  auto &subscribers = subscribers_map[oid];
  std::vector<NodeID> nodes_ids;

  // Add entries.
  for (int i = 0; i < 6; i++) {
    node_id = NodeID::FromRandom();
    nodes_ids.emplace(node_id);
    subscribers.emplace(node_id);
    subscription_index.AddEntry(oid, node_id);
  }
  subscription_index.EraseSubscriber(nodes_ids[0]);
  ASSERT_FALSE(subscription_index.IsSubscriberExist(nodes_ids[0]));
  const auto &subscribers_from_index = subscription_index.GetSubscriberIdsByObjectId(oid);
  ASSERT_TRUE(subscribers_from_index.count(node_ids[0]) == 0);
}

TEST_F(PubsubCoordinatorTest, TestSubscriber) {
  std::vector<ObjectID> object_ids_published;
  auto reply = [&object_ids_published](std::vector<ObjectID> &object_ids) {
    object_ids_published.insert(object_ids_published.end(), object_ids.begin(), object_ids.end());
  }

  Subscriber subscriber;
  // If there's no connection, it will return false.
  ASSERT_FALSE(subscriber.PublishIfPossible());
  ASSERT_TRUE(subscriber.Connect(reply));
  ASSERT_FALSE(subscriber.Connect(reply));
  // Since there's no published objects, it should return false.
  ASSERT_FALSE(subscriber.PublishIfPossible());

  unordered_map<ObjectID> published_objects;
  // Make sure publishing one object works as expected.
  auto oid = ObjectID::FromRandom();
  subscriber.QueueMessage(oid);
  published_objects.emplace(oid);
  ASSERT_TRUE(subscriber.PublishIfPossible());
  ASSERT_TRUE(object_ids_published.count(oid) > 0);
  // Since the object is published, and there's no connection, it should return false.
  ASSERT_FALSE(subscriber.PublishIfPossible());

  // Add 3 oids and see if it works properly.
  for (int i = 0; i < 3; i++) {
    oid = ObjectID::FromRandom();
    subscriber.QueueMessage(oid);
    published_objects.emplace(oid);
  }
  // Since there's no connection, objects won't be published.
  ASSERT_FALSE(subscriber.PublishIfPossible());
  ASSERT_TRUE(subscriber.Connect(reply));
  ASSERT_TRUE(subscriber.PublishIfPossible());
  for (auto it : published_objects) {
    oid = *it;
    ASSERT_TRUE(object_ids_published.count(oid) > 0);
  }
}

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
