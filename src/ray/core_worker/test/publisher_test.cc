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

#include "ray/core_worker/pubsub/publisher.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace ray {

using ::testing::_;

class PublisherTest : public ::testing::Test {
 public:
  PublisherTest() {}

  ~PublisherTest() {}

  void SetUp() {
    dead_nodes_.clear();
    object_status_publisher_ = std::shared_ptr<Publisher>(new Publisher(
        [this](const NodeID &node_id) { return dead_nodes_.count(node_id) == 1; }));
  }

  void TearDown() { subscribers_map_.clear(); }

  void RegisterDeadNode(const NodeID &node_id) { dead_nodes_.emplace(node_id); }

  std::unordered_set<NodeID> dead_nodes_;
  std::shared_ptr<Publisher> object_status_publisher_;
  std::unordered_map<ObjectID, std::unordered_set<NodeID>> subscribers_map_;
};

TEST_F(PublisherTest, TestSubscriptionIndexSingeNodeSingleObject) {
  auto node_id = NodeID::FromRandom();
  auto oid = ObjectID::FromRandom();
  auto &subscribers = subscribers_map_[oid];
  subscribers.emplace(node_id);

  ///
  /// Test single node id & object id
  ///
  /// oid1 -> [nid1]
  SubscriptionIndex subscription_index;
  subscription_index.AddEntry(oid, node_id);
  const auto &subscribers_from_index =
      subscription_index.GetSubscriberIdsByObjectId(oid).value().get();
  for (const auto &node_id : subscribers) {
    ASSERT_TRUE(subscribers_from_index.count(node_id) > 0);
  }
}

TEST_F(PublisherTest, TestSubscriptionIndexMultiNodeSingleObject) {
  ///
  /// Test single object id & multi nodes
  ///
  /// oid1 -> [nid1~nid5]
  SubscriptionIndex subscription_index;
  const auto oid = ObjectID::FromRandom();
  std::unordered_set<NodeID> empty_set;
  subscribers_map_.emplace(oid, empty_set);

  for (int i = 0; i < 5; i++) {
    auto node_id = NodeID::FromRandom();
    subscribers_map_.at(oid).emplace(node_id);
    subscription_index.AddEntry(oid, node_id);
  }
  const auto &subscribers_from_index =
      subscription_index.GetSubscriberIdsByObjectId(oid).value().get();
  for (const auto &node_id : subscribers_map_.at(oid)) {
    ASSERT_TRUE(subscribers_from_index.count(node_id) > 0);
  }

  ///
  /// Test multi node id & multi object ids
  ///
  /// oid1 -> [nid1~nid5]
  /// oid2 -> [nid1~nid5]
  const auto oid2 = ObjectID::FromRandom();
  subscribers_map_.emplace(oid2, empty_set);
  for (int i = 0; i < 5; i++) {
    auto node_id = NodeID::FromRandom();
    subscribers_map_.at(oid2).emplace(node_id);
    subscription_index.AddEntry(oid2, node_id);
  }
  const auto &subscribers_from_index2 =
      subscription_index.GetSubscriberIdsByObjectId(oid2).value().get();
  for (const auto &node_id : subscribers_map_.at(oid2)) {
    ASSERT_TRUE(subscribers_from_index2.count(node_id) > 0);
  }

  // Make sure oid1 entries are not corrupted.
  const auto &subscribers_from_index3 =
      subscription_index.GetSubscriberIdsByObjectId(oid).value().get();
  for (const auto &node_id : subscribers_map_.at(oid)) {
    ASSERT_TRUE(subscribers_from_index3.count(node_id) > 0);
  }
}

TEST_F(PublisherTest, TestSubscriptionIndexErase) {
  ///
  /// Test erase entry.
  ///
  /// oid1 -> [nid1~nid5]
  /// oid2 -> [nid1~nid5]
  SubscriptionIndex subscription_index;
  int total_entries = 6;
  int entries_to_delete_at_each_time = 3;
  auto oid = ObjectID::FromRandom();
  std::unordered_set<NodeID> empty_set;
  subscribers_map_.emplace(oid, empty_set);

  // Add entries.
  for (int i = 0; i < total_entries; i++) {
    auto node_id = NodeID::FromRandom();
    subscribers_map_.at(oid).emplace(node_id);
    subscription_index.AddEntry(oid, node_id);
  }

  // Verify that the first 3 entries are deleted properly.
  int i = 0;
  auto &oid_subscribers = subscribers_map_[oid];
  for (auto it = oid_subscribers.begin(); it != oid_subscribers.end();) {
    if (i == entries_to_delete_at_each_time) {
      break;
    }
    auto current = it++;
    auto node_id = *current;
    oid_subscribers.erase(current);
    ASSERT_EQ(subscription_index.EraseEntry(oid, node_id), 1);
    i++;
  }
  const auto &subscribers_from_index =
      subscription_index.GetSubscriberIdsByObjectId(oid).value().get();
  for (const auto &node_id : subscribers_map_.at(oid)) {
    ASSERT_TRUE(subscribers_from_index.count(node_id) > 0);
  }

  // Delete all entries and make sure the oid is removed from the index.
  for (auto it = oid_subscribers.begin(); it != oid_subscribers.end();) {
    auto current = it++;
    auto node_id = *current;
    oid_subscribers.erase(current);
    subscription_index.EraseEntry(oid, node_id);
  }
  ASSERT_FALSE(subscription_index.HasObjectId(oid));
}

TEST_F(PublisherTest, TestSubscriptionIndexEraseSubscriber) {
  ///
  /// Test erase subscriber.
  ///
  SubscriptionIndex subscription_index;
  auto oid = ObjectID::FromRandom();
  auto &subscribers = subscribers_map_[oid];
  std::vector<NodeID> node_ids;

  // Add entries.
  for (int i = 0; i < 6; i++) {
    auto node_id = NodeID::FromRandom();
    node_ids.push_back(node_id);
    subscribers.emplace(node_id);
    subscription_index.AddEntry(oid, node_id);
  }
  subscription_index.EraseSubscriber(node_ids[0]);
  ASSERT_FALSE(subscription_index.HasSubscriber(node_ids[0]));
  const auto &subscribers_from_index =
      subscription_index.GetSubscriberIdsByObjectId(oid).value().get();
  ASSERT_TRUE(subscribers_from_index.count(node_ids[0]) == 0);
}

TEST_F(PublisherTest, TestSubscriber) {
  std::unordered_set<ObjectID> object_ids_published;
  LongPollConnectCallback reply =
      [&object_ids_published](const std::vector<ObjectID> &object_ids) {
        for (auto &oid : object_ids) {
          object_ids_published.emplace(oid);
        }
      };

  Subscriber subscriber;
  // If there's no connection, it will return false.
  ASSERT_FALSE(subscriber.PublishIfPossible());
  // Try connecting it. Should return true.
  ASSERT_TRUE(subscriber.Connect(reply));
  // If connecting it again, it should fail the request.
  ASSERT_FALSE(subscriber.Connect(reply));
  // Since there's no published objects, it should return false.
  ASSERT_FALSE(subscriber.PublishIfPossible());

  std::unordered_set<ObjectID> published_objects;
  // Make sure publishing one object works as expected.
  auto oid = ObjectID::FromRandom();
  subscriber.QueueMessage(oid, /*try_publish=*/false);
  published_objects.emplace(oid);
  ASSERT_TRUE(subscriber.PublishIfPossible());
  ASSERT_TRUE(object_ids_published.count(oid) > 0);
  // Since the object is published, and there's no connection, it should return false.
  ASSERT_FALSE(subscriber.PublishIfPossible());

  // Add 3 oids and see if it works properly.
  for (int i = 0; i < 3; i++) {
    oid = ObjectID::FromRandom();
    subscriber.QueueMessage(oid, /*try_publish=*/false);
    published_objects.emplace(oid);
  }
  // Since there's no connection, objects won't be published.
  ASSERT_FALSE(subscriber.PublishIfPossible());
  ASSERT_TRUE(subscriber.Connect(reply));
  ASSERT_TRUE(subscriber.PublishIfPossible());
  for (auto oid : published_objects) {
    ASSERT_TRUE(object_ids_published.count(oid) > 0);
  }
}

TEST_F(PublisherTest, TestBasicSingleSubscriber) {
  std::vector<ObjectID> batched_ids;
  auto long_polling_connect = [&batched_ids](const std::vector<ObjectID> &object_ids) {
    auto it = batched_ids.begin();
    batched_ids.insert(it, object_ids.begin(), object_ids.end());
  };

  const auto subscriber_node_id = NodeID::FromRandom();
  const auto oid = ObjectID::FromRandom();

  object_status_publisher_->Connect(subscriber_node_id, long_polling_connect);
  object_status_publisher_->RegisterSubscription(subscriber_node_id, oid);
  object_status_publisher_->Publish(oid);
  ASSERT_EQ(batched_ids[0], oid);
}

TEST_F(PublisherTest, TestNoConnectionWhenRegistered) {
  std::vector<ObjectID> batched_ids;
  auto long_polling_connect = [&batched_ids](const std::vector<ObjectID> &object_ids) {
    auto it = batched_ids.begin();
    batched_ids.insert(it, object_ids.begin(), object_ids.end());
  };

  const auto subscriber_node_id = NodeID::FromRandom();
  const auto oid = ObjectID::FromRandom();

  object_status_publisher_->RegisterSubscription(subscriber_node_id, oid);
  object_status_publisher_->Publish(oid);
  // Nothing has been published because there's no connection.
  ASSERT_EQ(batched_ids.size(), 0);
  object_status_publisher_->Connect(subscriber_node_id, long_polling_connect);
  // When the connection is coming, it should be published.
  ASSERT_EQ(batched_ids[0], oid);
}

TEST_F(PublisherTest, TestMultiObjectsFromSingleNode) {
  std::vector<ObjectID> batched_ids;
  auto long_polling_connect = [&batched_ids](const std::vector<ObjectID> &object_ids) {
    auto it = batched_ids.begin();
    batched_ids.insert(it, object_ids.begin(), object_ids.end());
  };

  const auto subscriber_node_id = NodeID::FromRandom();
  std::vector<ObjectID> oids;
  int num_oids = 5;
  for (int i = 0; i < num_oids; i++) {
    const auto oid = ObjectID::FromRandom();
    oids.push_back(oid);
    object_status_publisher_->RegisterSubscription(subscriber_node_id, oid);
    object_status_publisher_->Publish(oid);
  }
  ASSERT_EQ(batched_ids.size(), 0);

  // Now connection is initiated, and all oids are published.
  object_status_publisher_->Connect(subscriber_node_id, long_polling_connect);
  for (int i = 0; i < num_oids; i++) {
    const auto oid_test = oids[i];
    const auto published_oid = batched_ids[i];
    ASSERT_EQ(oid_test, published_oid);
  }
}

TEST_F(PublisherTest, TestMultiObjectsFromMultiNodes) {
  std::vector<ObjectID> batched_ids;
  auto long_polling_connect = [&batched_ids](const std::vector<ObjectID> &object_ids) {
    auto it = batched_ids.end();
    batched_ids.insert(it, object_ids.begin(), object_ids.end());
  };

  std::vector<NodeID> subscribers;
  std::vector<ObjectID> oids;
  int num_nodes = 5;
  for (int i = 0; i < num_nodes; i++) {
    oids.push_back(ObjectID::FromRandom());
    subscribers.push_back(NodeID::FromRandom());
  }

  // There will be one object per node.
  for (int i = 0; i < num_nodes; i++) {
    const auto oid = oids[i];
    const auto subscriber_node_id = subscribers[i];
    object_status_publisher_->RegisterSubscription(subscriber_node_id, oid);
    object_status_publisher_->Publish(oid);
  }
  ASSERT_EQ(batched_ids.size(), 0);

  // Check all of nodes are publishing objects properly.
  for (int i = 0; i < num_nodes; i++) {
    const auto subscriber_node_id = subscribers[i];
    object_status_publisher_->Connect(subscriber_node_id, long_polling_connect);
    const auto oid_test = oids[i];
    const auto published_oid = batched_ids[i];
    ASSERT_EQ(oid_test, published_oid);
  }
}

TEST_F(PublisherTest, TestBatch) {
  // Test if published objects are batched properly.
  std::vector<ObjectID> batched_ids;
  auto long_polling_connect = [&batched_ids](const std::vector<ObjectID> &object_ids) {
    auto it = batched_ids.begin();
    batched_ids.insert(it, object_ids.begin(), object_ids.end());
  };

  const auto subscriber_node_id = NodeID::FromRandom();
  std::vector<ObjectID> oids;
  int num_oids = 5;
  for (int i = 0; i < num_oids; i++) {
    const auto oid = ObjectID::FromRandom();
    oids.push_back(oid);
    object_status_publisher_->RegisterSubscription(subscriber_node_id, oid);
    object_status_publisher_->Publish(oid);
  }
  ASSERT_EQ(batched_ids.size(), 0);

  // Now connection is initiated, and all oids are published.
  object_status_publisher_->Connect(subscriber_node_id, long_polling_connect);
  for (int i = 0; i < num_oids; i++) {
    const auto oid_test = oids[i];
    const auto published_oid = batched_ids[i];
    ASSERT_EQ(oid_test, published_oid);
  }

  batched_ids.clear();
  oids.clear();

  for (int i = 0; i < num_oids; i++) {
    const auto oid = ObjectID::FromRandom();
    oids.push_back(oid);
    object_status_publisher_->RegisterSubscription(subscriber_node_id, oid);
    object_status_publisher_->Publish(oid);
  }
  object_status_publisher_->Connect(subscriber_node_id, long_polling_connect);
  for (int i = 0; i < num_oids; i++) {
    const auto oid_test = oids[i];
    const auto published_oid = batched_ids[i];
    ASSERT_EQ(oid_test, published_oid);
  }
}

TEST_F(PublisherTest, TestNodeFailureWhenConnectionExisted) {
  bool long_polling_connection_replied = false;
  auto long_polling_connect =
      [&long_polling_connection_replied](const std::vector<ObjectID> &object_ids) {
        long_polling_connection_replied = true;
      };

  const auto subscriber_node_id = NodeID::FromRandom();
  const auto oid = ObjectID::FromRandom();
  object_status_publisher_->Connect(subscriber_node_id, long_polling_connect);
  dead_nodes_.emplace(subscriber_node_id);
  // All these ops should be no-op.
  object_status_publisher_->RegisterSubscription(subscriber_node_id, oid);
  // Note if the Publish function is called here, it will cause check failure. Application
  // code must ensure publish won't be called without registration.
  ASSERT_EQ(long_polling_connection_replied, false);

  // Connection should be replied (removed) when the subscriber is unregistered.
  int erased = object_status_publisher_->UnregisterSubscriber(subscriber_node_id);
  ASSERT_FALSE(erased);
  ASSERT_EQ(long_polling_connection_replied, true);
}

TEST_F(PublisherTest, TestNodeFailureWhenConnectionDoesntExist) {
  bool long_polling_connection_replied = false;
  auto long_polling_connect =
      [&long_polling_connection_replied](const std::vector<ObjectID> &object_ids) {
        long_polling_connection_replied = true;
      };

  ///
  /// Test the case where there was a registration, but no connection.
  ///
  auto subscriber_node_id = NodeID::FromRandom();
  auto oid = ObjectID::FromRandom();
  object_status_publisher_->RegisterSubscription(subscriber_node_id, oid);
  object_status_publisher_->Publish(oid);

  // Node is dead before connection is made.
  dead_nodes_.emplace(subscriber_node_id);
  ASSERT_EQ(long_polling_connection_replied, false);

  // Connect should reply right away to avoid memory leak.
  object_status_publisher_->Connect(subscriber_node_id, long_polling_connect);
  ASSERT_EQ(long_polling_connection_replied, true);
  long_polling_connection_replied = false;

  // Connection should be replied (removed) when the subscriber is unregistered.
  auto erased = object_status_publisher_->UnregisterSubscriber(subscriber_node_id);
  // Since there was no connection, long polling shouldn't have been replied.
  ASSERT_EQ(long_polling_connection_replied, false);
  ASSERT_TRUE(erased);

  ///
  /// Test the case where there was a connection, but no registration.
  ///
  subscriber_node_id = NodeID::FromRandom();
  oid = ObjectID::FromRandom();
  object_status_publisher_->Connect(subscriber_node_id, long_polling_connect);
  dead_nodes_.emplace(subscriber_node_id);
  erased = object_status_publisher_->UnregisterSubscriber(subscriber_node_id);
  ASSERT_EQ(long_polling_connection_replied, true);
  // Since there was no registration, nothing was erased.
  ASSERT_FALSE(erased);
}

// Unregistration an entry.
TEST_F(PublisherTest, TestUnregisterSubscription) {
  bool long_polling_connection_replied = false;
  auto long_polling_connect =
      [&long_polling_connection_replied](const std::vector<ObjectID> &object_ids) {
        long_polling_connection_replied = true;
      };

  const auto subscriber_node_id = NodeID::FromRandom();
  const auto oid = ObjectID::FromRandom();
  object_status_publisher_->Connect(subscriber_node_id, long_polling_connect);
  object_status_publisher_->RegisterSubscription(subscriber_node_id, oid);
  ASSERT_EQ(long_polling_connection_replied, false);

  // Connection should be replied (removed) when the subscriber is unregistered.
  int erased = object_status_publisher_->UnregisterSubscription(subscriber_node_id, oid);
  ASSERT_EQ(erased, 1);
  ASSERT_EQ(long_polling_connection_replied, false);

  // Make sure when the entries don't exist, it doesn't delete anything.
  ASSERT_EQ(object_status_publisher_->UnregisterSubscription(subscriber_node_id,
                                                        ObjectID::FromRandom()),
            0);
  ASSERT_EQ(object_status_publisher_->UnregisterSubscription(NodeID::FromRandom(), oid), 0);
  ASSERT_EQ(object_status_publisher_->UnregisterSubscription(NodeID::FromRandom(),
                                                        ObjectID::FromRandom()),
            0);
  ASSERT_EQ(long_polling_connection_replied, false);
}

// Unregistration a subscriber.
TEST_F(PublisherTest, TestUnregisterSubscriber) {
  bool long_polling_connection_replied = false;
  auto long_polling_connect =
      [&long_polling_connection_replied](const std::vector<ObjectID> &object_ids) {
        long_polling_connection_replied = true;
      };

  // Test basic.
  const auto subscriber_node_id = NodeID::FromRandom();
  const auto oid = ObjectID::FromRandom();
  object_status_publisher_->Connect(subscriber_node_id, long_polling_connect);
  object_status_publisher_->RegisterSubscription(subscriber_node_id, oid);
  ASSERT_EQ(long_polling_connection_replied, false);
  int erased = object_status_publisher_->UnregisterSubscriber(subscriber_node_id);
  ASSERT_TRUE(erased);
  // Make sure the long polling request is replied to avoid memory leak.
  ASSERT_EQ(long_polling_connection_replied, true);

  // Test when registration wasn't done.
  long_polling_connection_replied = false;
  object_status_publisher_->Connect(subscriber_node_id, long_polling_connect);
  erased = object_status_publisher_->UnregisterSubscriber(subscriber_node_id);
  ASSERT_FALSE(erased);
  ASSERT_EQ(long_polling_connection_replied, true);

  // Test when connect wasn't done.
  long_polling_connection_replied = false;
  object_status_publisher_->RegisterSubscription(subscriber_node_id, oid);
  erased = object_status_publisher_->UnregisterSubscriber(subscriber_node_id);
  ASSERT_TRUE(erased);
  ASSERT_EQ(long_polling_connection_replied, false);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
