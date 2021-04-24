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

#include "ray/pubsub/publisher.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"

namespace ray {

using ::testing::_;
using namespace pubsub;
using namespace pub_internal;

class PublisherTest : public ::testing::Test {
 public:
  PublisherTest() { periodic_runner_.reset(new PeriodicalRunner(io_service_)); }

  ~PublisherTest() {}

  void SetUp() {
    object_status_publisher_ = std::shared_ptr<Publisher>(new Publisher(
        /*periodic_runner=*/periodic_runner_.get(),
        /*get_time_ms=*/[this]() { return current_time_; },
        /*subscriber_timeout_ms=*/subscriber_timeout_ms_,
        /*batch_size*/ 100));
    current_time_ = 0;
  }

  void TearDown() { subscribers_map_.clear(); }

  rpc::PubMessage GeneratePubMessage(const ObjectID &object_id) {
    rpc::PubMessage pub_message;
    auto *wait_for_object_eviction_msg =
        pub_message.mutable_wait_for_object_eviction_message();
    wait_for_object_eviction_msg->set_object_id(object_id.Binary());
    pub_message.set_message_id(object_id.Binary());
    pub_message.set_channel_type(rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION);
    return pub_message;
  }

  instrumented_io_context io_service_;
  std::shared_ptr<PeriodicalRunner> periodic_runner_;
  std::shared_ptr<Publisher> object_status_publisher_;
  std::unordered_map<ObjectID, std::unordered_set<NodeID>> subscribers_map_;
  const uint64_t subscriber_timeout_ms_ = 30000;
  double current_time_;
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
  SubscriptionIndex<ObjectID> subscription_index;
  subscription_index.AddEntry(oid.Binary(), node_id);
  const auto &subscribers_from_index =
      subscription_index.GetSubscriberIdsByMessageId(oid.Binary()).value().get();
  for (const auto &node_id : subscribers) {
    ASSERT_TRUE(subscribers_from_index.count(node_id) > 0);
  }
}

TEST_F(PublisherTest, TestSubscriptionIndexMultiNodeSingleObject) {
  ///
  /// Test single object id & multi nodes
  ///
  /// oid1 -> [nid1~nid5]
  SubscriptionIndex<ObjectID> subscription_index;
  const auto oid = ObjectID::FromRandom();
  std::unordered_set<NodeID> empty_set;
  subscribers_map_.emplace(oid, empty_set);

  for (int i = 0; i < 5; i++) {
    auto node_id = NodeID::FromRandom();
    subscribers_map_.at(oid).emplace(node_id);
    subscription_index.AddEntry(oid.Binary(), node_id);
  }
  const auto &subscribers_from_index =
      subscription_index.GetSubscriberIdsByMessageId(oid.Binary()).value().get();
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
    subscription_index.AddEntry(oid2.Binary(), node_id);
  }
  const auto &subscribers_from_index2 =
      subscription_index.GetSubscriberIdsByMessageId(oid2.Binary()).value().get();
  for (const auto &node_id : subscribers_map_.at(oid2)) {
    ASSERT_TRUE(subscribers_from_index2.count(node_id) > 0);
  }

  // Make sure oid1 entries are not corrupted.
  const auto &subscribers_from_index3 =
      subscription_index.GetSubscriberIdsByMessageId(oid.Binary()).value().get();
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
  SubscriptionIndex<ObjectID> subscription_index;
  int total_entries = 6;
  int entries_to_delete_at_each_time = 3;
  auto oid = ObjectID::FromRandom();
  std::unordered_set<NodeID> empty_set;
  subscribers_map_.emplace(oid, empty_set);

  // Add entries.
  for (int i = 0; i < total_entries; i++) {
    auto node_id = NodeID::FromRandom();
    subscribers_map_.at(oid).emplace(node_id);
    subscription_index.AddEntry(oid.Binary(), node_id);
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
    ASSERT_EQ(subscription_index.EraseEntry(oid.Binary(), node_id), 1);
    i++;
  }
  const auto &subscribers_from_index =
      subscription_index.GetSubscriberIdsByMessageId(oid.Binary()).value().get();
  for (const auto &node_id : subscribers_map_.at(oid)) {
    ASSERT_TRUE(subscribers_from_index.count(node_id) > 0);
  }

  // Delete all entries and make sure the oid is removed from the index.
  for (auto it = oid_subscribers.begin(); it != oid_subscribers.end();) {
    auto current = it++;
    auto node_id = *current;
    oid_subscribers.erase(current);
    subscription_index.EraseEntry(oid.Binary(), node_id);
  }
  ASSERT_FALSE(subscription_index.HasMessageId(oid.Binary()));
  ASSERT_TRUE(subscription_index.CheckNoLeaks());
}

TEST_F(PublisherTest, TestSubscriptionIndexEraseSubscriber) {
  ///
  /// Test erase subscriber.
  ///
  SubscriptionIndex<ObjectID> subscription_index;
  auto oid = ObjectID::FromRandom();
  auto &subscribers = subscribers_map_[oid];
  std::vector<NodeID> node_ids;

  // Add entries.
  for (int i = 0; i < 6; i++) {
    auto node_id = NodeID::FromRandom();
    node_ids.push_back(node_id);
    subscribers.emplace(node_id);
    subscription_index.AddEntry(oid.Binary(), node_id);
  }
  subscription_index.EraseSubscriber(node_ids[0]);
  ASSERT_FALSE(subscription_index.HasSubscriber(node_ids[0]));
  const auto &subscribers_from_index =
      subscription_index.GetSubscriberIdsByMessageId(oid.Binary()).value().get();
  ASSERT_TRUE(subscribers_from_index.count(node_ids[0]) == 0);

  for (int i = 1; i < 6; i++) {
    subscription_index.EraseSubscriber(node_ids[i]);
  }
  ASSERT_TRUE(subscription_index.CheckNoLeaks());
}

TEST_F(PublisherTest, TestSubscriber) {
  std::unordered_set<ObjectID> object_ids_published;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&reply, &object_ids_published](Status status, std::function<void()> success,
                                      std::function<void()> failure) {
        for (int i = 0; i < reply.pub_messages_size(); i++) {
          const auto &msg = reply.pub_messages(i);
          const auto oid =
              ObjectID::FromBinary(msg.wait_for_object_eviction_message().object_id());
          object_ids_published.emplace(oid);
        }
        reply = rpc::PubsubLongPollingReply();
      };

  std::shared_ptr<Subscriber> subscriber = std::make_shared<Subscriber>(
      [this]() { return current_time_; }, subscriber_timeout_ms_, 10);
  // If there's no connection, it will return false.
  ASSERT_FALSE(subscriber->PublishIfPossible());
  // Try connecting it. Should return true.
  ASSERT_TRUE(subscriber->ConnectToSubscriber(&reply, send_reply_callback));
  // If connecting it again, it should fail the request.
  ASSERT_FALSE(subscriber->ConnectToSubscriber(&reply, send_reply_callback));
  // Since there's no published objects, it should return false.
  ASSERT_FALSE(subscriber->PublishIfPossible());

  std::unordered_set<ObjectID> published_objects;
  // Make sure publishing one object works as expected.
  auto oid = ObjectID::FromRandom();
  subscriber->QueueMessage(absl::make_unique<rpc::PubMessage>(GeneratePubMessage(oid)),
                           /*try_publish=*/false);
  published_objects.emplace(oid);
  ASSERT_TRUE(subscriber->PublishIfPossible());
  ASSERT_TRUE(object_ids_published.count(oid) > 0);
  // Since the object is published, and there's no connection, it should return false.
  ASSERT_FALSE(subscriber->PublishIfPossible());

  // Add 3 oids and see if it works properly.
  for (int i = 0; i < 3; i++) {
    oid = ObjectID::FromRandom();
    subscriber->QueueMessage(absl::make_unique<rpc::PubMessage>(GeneratePubMessage(oid)),
                             /*try_publish=*/false);
    published_objects.emplace(oid);
  }
  // Since there's no connection, objects won't be published.
  ASSERT_FALSE(subscriber->PublishIfPossible());
  ASSERT_TRUE(subscriber->ConnectToSubscriber(&reply, send_reply_callback));
  ASSERT_TRUE(subscriber->PublishIfPossible());
  for (auto oid : published_objects) {
    ASSERT_TRUE(object_ids_published.count(oid) > 0);
  }
  ASSERT_TRUE(subscriber->CheckNoLeaks());
}

TEST_F(PublisherTest, TestSubscriberBatchSize) {
  std::unordered_set<ObjectID> object_ids_published;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&reply, &object_ids_published](Status status, std::function<void()> success,
                                      std::function<void()> failure) {
        for (int i = 0; i < reply.pub_messages_size(); i++) {
          const auto &msg = reply.pub_messages(i);
          const auto oid =
              ObjectID::FromBinary(msg.wait_for_object_eviction_message().object_id());
          object_ids_published.emplace(oid);
        }
        reply = rpc::PubsubLongPollingReply();
      };

  auto max_publish_size = 5;
  std::shared_ptr<Subscriber> subscriber = std::make_shared<Subscriber>(
      [this]() { return current_time_; }, subscriber_timeout_ms_, max_publish_size);
  ASSERT_TRUE(subscriber->ConnectToSubscriber(&reply, send_reply_callback));

  std::unordered_set<ObjectID> published_objects;
  std::vector<ObjectID> oids;
  for (int i = 0; i < 10; i++) {
    auto oid = ObjectID::FromRandom();
    oids.push_back(oid);
    subscriber->QueueMessage(absl::make_unique<rpc::PubMessage>(GeneratePubMessage(oid)),
                             /*try_publish=*/false);
    published_objects.emplace(oid);
  }

  // Make sure only up to batch size is published.
  ASSERT_TRUE(subscriber->PublishIfPossible());

  for (int i = 0; i < max_publish_size; i++) {
    ASSERT_TRUE(object_ids_published.count(oids[i]) > 0);
  }
  for (int i = max_publish_size; i < 10; i++) {
    ASSERT_FALSE(object_ids_published.count(oids[i]) > 0);
  }

  // Remainings are published.
  ASSERT_TRUE(subscriber->ConnectToSubscriber(&reply, send_reply_callback));
  ASSERT_TRUE(subscriber->PublishIfPossible());
  for (int i = 0; i < 10; i++) {
    ASSERT_TRUE(object_ids_published.count(oids[i]) > 0);
  }
}

TEST_F(PublisherTest, TestSubscriberActiveTimeout) {
  ///
  /// Test the active connection timeout.
  ///

  auto reply_cnt = 0;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&reply_cnt](Status status, std::function<void()> success,
                   std::function<void()> failure) { reply_cnt++; };

  std::shared_ptr<Subscriber> subscriber = std::make_shared<Subscriber>(
      [this]() { return current_time_; }, subscriber_timeout_ms_, 10);

  ASSERT_TRUE(subscriber->ConnectToSubscriber(&reply, send_reply_callback));

  // Connection is not timed out yet.
  ASSERT_FALSE(subscriber->IsActiveConnectionTimedOut());
  ASSERT_FALSE(subscriber->IsDisconnected());

  // Some time has passed, but it is not timed out yet.
  current_time_ += subscriber_timeout_ms_ / 2;
  ASSERT_FALSE(subscriber->IsActiveConnectionTimedOut());
  ASSERT_FALSE(subscriber->IsDisconnected());

  // Timeout is reached, and the long polling connection should've been refreshed.
  current_time_ += subscriber_timeout_ms_ / 2;
  ASSERT_TRUE(subscriber->IsActiveConnectionTimedOut());
  ASSERT_FALSE(subscriber->IsDisconnected());

  // Refresh the connection.
  subscriber->PublishIfPossible(/*force*/ true);
  ASSERT_EQ(reply_cnt, 1);

  // New connection is established.
  ASSERT_TRUE(subscriber->ConnectToSubscriber(&reply, send_reply_callback));
  ASSERT_FALSE(subscriber->IsActiveConnectionTimedOut());
  ASSERT_FALSE(subscriber->IsDisconnected());

  // Some time has passed, but it is not timed out yet.
  current_time_ += subscriber_timeout_ms_ / 2;
  ASSERT_FALSE(subscriber->IsActiveConnectionTimedOut());
  ASSERT_FALSE(subscriber->IsDisconnected());

  // A message is published, so the connection is refreshed.
  auto oid = ObjectID::FromRandom();
  subscriber->QueueMessage(absl::make_unique<rpc::PubMessage>(GeneratePubMessage(oid)));
  ASSERT_FALSE(subscriber->IsActiveConnectionTimedOut());
  ASSERT_FALSE(subscriber->IsDisconnected());
  ASSERT_EQ(reply_cnt, 2);

  // Although time has passed, since the connection was refreshed, timeout shouldn't
  // happen.
  current_time_ += subscriber_timeout_ms_ / 2;
  ASSERT_FALSE(subscriber->IsActiveConnectionTimedOut());
  ASSERT_FALSE(subscriber->IsDisconnected());

  ASSERT_TRUE(subscriber->CheckNoLeaks());
}

TEST_F(PublisherTest, TestSubscriberDisconnected) {
  ///
  /// Test the subscriber is considered as dead due to the disconnection timeout.
  ///

  auto reply_cnt = 0;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&reply_cnt](Status status, std::function<void()> success,
                   std::function<void()> failure) { reply_cnt++; };

  std::shared_ptr<Subscriber> subscriber = std::make_shared<Subscriber>(
      [this]() { return current_time_; }, subscriber_timeout_ms_, 10);

  // Suppose the new connection is removed.
  ASSERT_TRUE(subscriber->ConnectToSubscriber(&reply, send_reply_callback));
  subscriber->PublishIfPossible(/*force*/ true);
  ASSERT_EQ(reply_cnt, 1);
  ASSERT_FALSE(subscriber->IsActiveConnectionTimedOut());
  ASSERT_FALSE(subscriber->IsDisconnected());

  // Some time has passed, but it is not timed out yet.
  current_time_ += subscriber_timeout_ms_ / 2;
  ASSERT_FALSE(subscriber->IsActiveConnectionTimedOut());
  ASSERT_FALSE(subscriber->IsDisconnected());

  // Timeout is reached. Since there was no new long polling connection, it is considered
  // as disconnected.
  current_time_ += subscriber_timeout_ms_ / 2;
  ASSERT_FALSE(subscriber->IsActiveConnectionTimedOut());
  ASSERT_TRUE(subscriber->IsDisconnected());

  // New connection is coming in.
  ASSERT_TRUE(subscriber->ConnectToSubscriber(&reply, send_reply_callback));
  subscriber->PublishIfPossible(/*force*/ true);
  ASSERT_EQ(reply_cnt, 2);

  // Some time has passed, but it is not timed out yet.
  current_time_ += subscriber_timeout_ms_ / 2;
  ASSERT_FALSE(subscriber->IsActiveConnectionTimedOut());
  ASSERT_FALSE(subscriber->IsDisconnected());

  // Another connection is made, so it shouldn't timeout until the next timeout is
  // reached.
  ASSERT_TRUE(subscriber->ConnectToSubscriber(&reply, send_reply_callback));
  subscriber->PublishIfPossible(/*force*/ true);
  ASSERT_EQ(reply_cnt, 3);
  current_time_ += subscriber_timeout_ms_ / 2;
  ASSERT_FALSE(subscriber->IsActiveConnectionTimedOut());
  ASSERT_FALSE(subscriber->IsDisconnected());

  // IF there's no new connection for a long time it should eventually timeout.
  current_time_ += subscriber_timeout_ms_ / 2;
  ASSERT_FALSE(subscriber->IsActiveConnectionTimedOut());
  ASSERT_TRUE(subscriber->IsDisconnected());

  ASSERT_TRUE(subscriber->CheckNoLeaks());
}

TEST_F(PublisherTest, TestSubscriberTimeoutComplicated) {
  ///
  /// Test the subscriber timeout in more complicated scenario.
  ///

  auto reply_cnt = 0;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&reply_cnt](Status status, std::function<void()> success,
                   std::function<void()> failure) { reply_cnt++; };

  std::shared_ptr<Subscriber> subscriber = std::make_shared<Subscriber>(
      [this]() { return current_time_; }, subscriber_timeout_ms_, 10);

  // Suppose the new connection is removed.
  ASSERT_TRUE(subscriber->ConnectToSubscriber(&reply, send_reply_callback));
  subscriber->PublishIfPossible(/*force*/ true);
  ASSERT_EQ(reply_cnt, 1);
  ASSERT_FALSE(subscriber->IsActiveConnectionTimedOut());
  ASSERT_FALSE(subscriber->IsDisconnected());

  // Some time has passed, and the connection is removed.
  current_time_ += subscriber_timeout_ms_ - 1;
  ASSERT_TRUE(subscriber->ConnectToSubscriber(&reply, send_reply_callback));
  current_time_ += 2;
  // Timeout shouldn't happen because the connection has been refreshed.
  ASSERT_FALSE(subscriber->IsActiveConnectionTimedOut());
  ASSERT_FALSE(subscriber->IsDisconnected());

  // Right before the timeout, connection is removed. In this case, timeout shouldn't also
  // happen.
  current_time_ += subscriber_timeout_ms_ - 1;
  subscriber->PublishIfPossible(/*force*/ true);
  current_time_ += 2;
  ASSERT_FALSE(subscriber->IsActiveConnectionTimedOut());
  ASSERT_FALSE(subscriber->IsDisconnected());

  // Timeout is reached. Since there was no connection, it should be considered
  // disconnected.
  current_time_ += subscriber_timeout_ms_;
  ASSERT_FALSE(subscriber->IsActiveConnectionTimedOut());
  ASSERT_TRUE(subscriber->IsDisconnected());

  ASSERT_TRUE(subscriber->CheckNoLeaks());
}

TEST_F(PublisherTest, TestBasicSingleSubscriber) {
  std::vector<ObjectID> batched_ids;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&reply, &batched_ids](Status status, std::function<void()> success,
                             std::function<void()> failure) {
        for (int i = 0; i < reply.pub_messages_size(); i++) {
          const auto &msg = reply.pub_messages(i);
          const auto oid =
              ObjectID::FromBinary(msg.wait_for_object_eviction_message().object_id());
          batched_ids.push_back(oid);
        }
        reply = rpc::PubsubLongPollingReply();
      };

  const auto subscriber_node_id = NodeID::FromRandom();
  const auto oid = ObjectID::FromRandom();

  object_status_publisher_->ConnectToSubscriber(subscriber_node_id, &reply,
                                                send_reply_callback);
  object_status_publisher_->RegisterSubscription(
      rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION, subscriber_node_id, oid.Binary());
  object_status_publisher_->Publish(
      rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION,
      absl::make_unique<rpc::PubMessage>(GeneratePubMessage(oid)), oid.Binary());
  ASSERT_EQ(batched_ids[0], oid);
}

TEST_F(PublisherTest, TestNoConnectionWhenRegistered) {
  std::vector<ObjectID> batched_ids;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&reply, &batched_ids](Status status, std::function<void()> success,
                             std::function<void()> failure) {
        for (int i = 0; i < reply.pub_messages_size(); i++) {
          const auto &msg = reply.pub_messages(i);
          const auto oid =
              ObjectID::FromBinary(msg.wait_for_object_eviction_message().object_id());
          batched_ids.push_back(oid);
        }
        reply = rpc::PubsubLongPollingReply();
      };

  const auto subscriber_node_id = NodeID::FromRandom();
  const auto oid = ObjectID::FromRandom();

  object_status_publisher_->RegisterSubscription(
      rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION, subscriber_node_id, oid.Binary());
  object_status_publisher_->Publish(
      rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION,
      absl::make_unique<rpc::PubMessage>(GeneratePubMessage(oid)), oid.Binary());
  // Nothing has been published because there's no connection.
  ASSERT_EQ(batched_ids.size(), 0);
  object_status_publisher_->ConnectToSubscriber(subscriber_node_id, &reply,
                                                send_reply_callback);
  // When the connection is coming, it should be published.
  ASSERT_EQ(batched_ids[0], oid);
}

TEST_F(PublisherTest, TestMultiObjectsFromSingleNode) {
  std::vector<ObjectID> batched_ids;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&reply, &batched_ids](Status status, std::function<void()> success,
                             std::function<void()> failure) {
        for (int i = 0; i < reply.pub_messages_size(); i++) {
          const auto &msg = reply.pub_messages(i);
          const auto oid =
              ObjectID::FromBinary(msg.wait_for_object_eviction_message().object_id());
          batched_ids.push_back(oid);
        }
        reply = rpc::PubsubLongPollingReply();
      };

  const auto subscriber_node_id = NodeID::FromRandom();
  std::vector<ObjectID> oids;
  int num_oids = 5;
  for (int i = 0; i < num_oids; i++) {
    const auto oid = ObjectID::FromRandom();
    oids.push_back(oid);
    object_status_publisher_->RegisterSubscription(
        rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION, subscriber_node_id, oid.Binary());
    object_status_publisher_->Publish(
        rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION,
        absl::make_unique<rpc::PubMessage>(GeneratePubMessage(oid)), oid.Binary());
  }
  ASSERT_EQ(batched_ids.size(), 0);

  // Now connection is initiated, and all oids are published.
  object_status_publisher_->ConnectToSubscriber(subscriber_node_id, &reply,
                                                send_reply_callback);
  for (int i = 0; i < num_oids; i++) {
    const auto oid_test = oids[i];
    const auto published_oid = batched_ids[i];
    ASSERT_EQ(oid_test, published_oid);
  }
}

TEST_F(PublisherTest, TestMultiObjectsFromMultiNodes) {
  std::vector<ObjectID> batched_ids;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&reply, &batched_ids](Status status, std::function<void()> success,
                             std::function<void()> failure) {
        for (int i = 0; i < reply.pub_messages_size(); i++) {
          const auto &msg = reply.pub_messages(i);
          const auto oid =
              ObjectID::FromBinary(msg.wait_for_object_eviction_message().object_id());
          batched_ids.push_back(oid);
        }
        reply = rpc::PubsubLongPollingReply();
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
    object_status_publisher_->RegisterSubscription(
        rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION, subscriber_node_id, oid.Binary());
    object_status_publisher_->Publish(
        rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION,
        absl::make_unique<rpc::PubMessage>(GeneratePubMessage(oid)), oid.Binary());
  }
  ASSERT_EQ(batched_ids.size(), 0);

  // Check all of nodes are publishing objects properly.
  for (int i = 0; i < num_nodes; i++) {
    const auto subscriber_node_id = subscribers[i];
    object_status_publisher_->ConnectToSubscriber(subscriber_node_id, &reply,
                                                  send_reply_callback);
    const auto oid_test = oids[i];
    const auto published_oid = batched_ids[i];
    ASSERT_EQ(oid_test, published_oid);
  }
}

TEST_F(PublisherTest, TestBatch) {
  // Test if published objects are batched properly.
  std::vector<ObjectID> batched_ids;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&reply, &batched_ids](Status status, std::function<void()> success,
                             std::function<void()> failure) {
        for (int i = 0; i < reply.pub_messages_size(); i++) {
          const auto &msg = reply.pub_messages(i);
          const auto oid =
              ObjectID::FromBinary(msg.wait_for_object_eviction_message().object_id());
          batched_ids.push_back(oid);
        }
        reply = rpc::PubsubLongPollingReply();
      };

  const auto subscriber_node_id = NodeID::FromRandom();
  std::vector<ObjectID> oids;
  int num_oids = 5;
  for (int i = 0; i < num_oids; i++) {
    const auto oid = ObjectID::FromRandom();
    oids.push_back(oid);
    object_status_publisher_->RegisterSubscription(
        rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION, subscriber_node_id, oid.Binary());
    object_status_publisher_->Publish(
        rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION,
        absl::make_unique<rpc::PubMessage>(GeneratePubMessage(oid)), oid.Binary());
  }
  ASSERT_EQ(batched_ids.size(), 0);

  // Now connection is initiated, and all oids are published.
  object_status_publisher_->ConnectToSubscriber(subscriber_node_id, &reply,
                                                send_reply_callback);
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
    object_status_publisher_->RegisterSubscription(
        rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION, subscriber_node_id, oid.Binary());
    object_status_publisher_->Publish(
        rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION,
        absl::make_unique<rpc::PubMessage>(GeneratePubMessage(oid)), oid.Binary());
  }
  object_status_publisher_->ConnectToSubscriber(subscriber_node_id, &reply,
                                                send_reply_callback);
  for (int i = 0; i < num_oids; i++) {
    const auto oid_test = oids[i];
    const auto published_oid = batched_ids[i];
    ASSERT_EQ(oid_test, published_oid);
  }
}

TEST_F(PublisherTest, TestNodeFailureWhenConnectionExisted) {
  bool long_polling_connection_replied = false;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&long_polling_connection_replied](Status status, std::function<void()> success,
                                         std::function<void()> failure) {
        long_polling_connection_replied = true;
      };

  const auto subscriber_node_id = NodeID::FromRandom();
  const auto oid = ObjectID::FromRandom();
  object_status_publisher_->ConnectToSubscriber(subscriber_node_id, &reply,
                                                send_reply_callback);
  // This information should be cleaned up as the subscriber is dead.
  object_status_publisher_->RegisterSubscription(
      rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION, subscriber_node_id, oid.Binary());
  // Timeout is reached. The connection should've been refreshed. Since the subscriber is
  // dead, no new connection is made.
  current_time_ += subscriber_timeout_ms_;
  object_status_publisher_->CheckDeadSubscribers();
  ASSERT_EQ(long_polling_connection_replied, true);

  // More time has passed, and since there was no new long polling connection, this
  // subscriber is considered as dead.
  current_time_ += subscriber_timeout_ms_;
  object_status_publisher_->CheckDeadSubscribers();

  // Connection should be replied (removed) when the subscriber is unregistered.
  int erased = object_status_publisher_->UnregisterSubscriber(subscriber_node_id);
  ASSERT_EQ(erased, 0);
  ASSERT_TRUE(object_status_publisher_->CheckNoLeaks());

  // New subscriber is registsered for some reason. Since there's no new long polling
  // connection for the timeout, it should be removed.
  long_polling_connection_replied = false;
  object_status_publisher_->RegisterSubscription(
      rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION, subscriber_node_id, oid.Binary());
  current_time_ += subscriber_timeout_ms_;
  object_status_publisher_->CheckDeadSubscribers();
  erased = object_status_publisher_->UnregisterSubscriber(subscriber_node_id);
  ASSERT_EQ(erased, 0);
  ASSERT_TRUE(object_status_publisher_->CheckNoLeaks());
}

TEST_F(PublisherTest, TestNodeFailureWhenConnectionDoesntExist) {
  bool long_polling_connection_replied = false;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&long_polling_connection_replied](Status status, std::function<void()> success,
                                         std::function<void()> failure) {
        long_polling_connection_replied = true;
      };

  ///
  /// Test the case where there was a registration, but no connection.
  ///
  auto subscriber_node_id = NodeID::FromRandom();
  auto oid = ObjectID::FromRandom();
  object_status_publisher_->RegisterSubscription(
      rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION, subscriber_node_id, oid.Binary());
  object_status_publisher_->Publish(
      rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION,
      absl::make_unique<rpc::PubMessage>(GeneratePubMessage(oid)), oid.Binary());
  // There was no long polling connection yet.
  ASSERT_EQ(long_polling_connection_replied, false);

  // Connect should be removed eventually to avoid having a memory leak.
  object_status_publisher_->ConnectToSubscriber(subscriber_node_id, &reply,
                                                send_reply_callback);
  ASSERT_EQ(long_polling_connection_replied, true);
  // Nothing happens at first.
  object_status_publisher_->CheckDeadSubscribers();

  // After the timeout, the subscriber should be considered as dead because there was no
  // new long polling connection.
  current_time_ += subscriber_timeout_ms_;
  object_status_publisher_->CheckDeadSubscribers();
  // Make sure the registration is cleaned up.
  ASSERT_TRUE(object_status_publisher_->CheckNoLeaks());

  /// Test the case where there's no connection coming at all when there was a
  /// registration.
  object_status_publisher_->RegisterSubscription(
      rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION, subscriber_node_id, oid.Binary());
  object_status_publisher_->Publish(
      rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION,
      absl::make_unique<rpc::PubMessage>(GeneratePubMessage(oid)), oid.Binary());

  // No new long polling connection was made until timeout.
  current_time_ += subscriber_timeout_ms_;
  object_status_publisher_->CheckDeadSubscribers();
  // Make sure the registration is cleaned up.
  ASSERT_TRUE(object_status_publisher_->CheckNoLeaks());
}

// Unregistration an entry.
TEST_F(PublisherTest, TestUnregisterSubscription) {
  bool long_polling_connection_replied = false;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&long_polling_connection_replied](Status status, std::function<void()> success,
                                         std::function<void()> failure) {
        long_polling_connection_replied = true;
      };

  const auto subscriber_node_id = NodeID::FromRandom();
  const auto oid = ObjectID::FromRandom();
  object_status_publisher_->ConnectToSubscriber(subscriber_node_id, &reply,
                                                send_reply_callback);
  object_status_publisher_->RegisterSubscription(
      rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION, subscriber_node_id, oid.Binary());
  ASSERT_EQ(long_polling_connection_replied, false);

  // Connection should be replied (removed) when the subscriber is unregistered.
  int erased = object_status_publisher_->UnregisterSubscription(
      rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION, subscriber_node_id, oid.Binary());
  ASSERT_EQ(erased, 1);
  ASSERT_EQ(long_polling_connection_replied, false);

  // Make sure when the entries don't exist, it doesn't delete anything.
  ASSERT_EQ(object_status_publisher_->UnregisterSubscription(
                rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION, subscriber_node_id,
                ObjectID::FromRandom().Binary()),
            0);
  ASSERT_EQ(
      object_status_publisher_->UnregisterSubscription(
          rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION, NodeID::FromRandom(), oid.Binary()),
      0);
  ASSERT_EQ(object_status_publisher_->UnregisterSubscription(
                rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION, NodeID::FromRandom(),
                ObjectID::FromRandom().Binary()),
            0);
  ASSERT_EQ(long_polling_connection_replied, false);
  // Metadata won't be removed until we unregsiter the subscriber.
  object_status_publisher_->UnregisterSubscriber(subscriber_node_id);
  ASSERT_TRUE(object_status_publisher_->CheckNoLeaks());
}

// Unregistration a subscriber.
TEST_F(PublisherTest, TestUnregisterSubscriber) {
  bool long_polling_connection_replied = false;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&long_polling_connection_replied](Status status, std::function<void()> success,
                                         std::function<void()> failure) {
        long_polling_connection_replied = true;
      };

  // Test basic.
  const auto subscriber_node_id = NodeID::FromRandom();
  const auto oid = ObjectID::FromRandom();
  object_status_publisher_->ConnectToSubscriber(subscriber_node_id, &reply,
                                                send_reply_callback);
  object_status_publisher_->RegisterSubscription(
      rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION, subscriber_node_id, oid.Binary());
  ASSERT_EQ(long_polling_connection_replied, false);
  int erased = object_status_publisher_->UnregisterSubscriber(subscriber_node_id);
  ASSERT_TRUE(erased);
  // Make sure the long polling request is replied to avoid memory leak.
  ASSERT_EQ(long_polling_connection_replied, true);

  // Test when registration wasn't done.
  long_polling_connection_replied = false;
  object_status_publisher_->ConnectToSubscriber(subscriber_node_id, &reply,
                                                send_reply_callback);
  erased = object_status_publisher_->UnregisterSubscriber(subscriber_node_id);
  ASSERT_FALSE(erased);
  ASSERT_EQ(long_polling_connection_replied, true);

  // Test when connect wasn't done.
  long_polling_connection_replied = false;
  object_status_publisher_->RegisterSubscription(
      rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION, subscriber_node_id, oid.Binary());
  erased = object_status_publisher_->UnregisterSubscriber(subscriber_node_id);
  ASSERT_TRUE(erased);
  ASSERT_EQ(long_polling_connection_replied, false);
  ASSERT_TRUE(object_status_publisher_->CheckNoLeaks());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
