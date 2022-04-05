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
#include "ray/common/ray_config.h"

namespace ray {

namespace pubsub {

using namespace pub_internal;

class PublisherTest : public ::testing::Test {
 public:
  PublisherTest() { periodic_runner_.reset(new PeriodicalRunner(io_service_)); }

  ~PublisherTest() {}

  void SetUp() {
    publisher_ = std::make_shared<Publisher>(
        /*channels=*/
        std::vector<rpc::ChannelType>{
            rpc::ChannelType::WORKER_OBJECT_EVICTION,
            rpc::ChannelType::WORKER_REF_REMOVED_CHANNEL,
            rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL,
            rpc::ChannelType::RAY_ERROR_INFO_CHANNEL,
        },
        /*periodic_runner=*/periodic_runner_.get(),
        /*get_time_ms=*/[this]() { return current_time_; },
        /*subscriber_timeout_ms=*/subscriber_timeout_ms_,
        /*batch_size*/ 100);
    current_time_ = 0;
    request_.set_subscriber_id(subscriber_id_.Binary());
  }

  void TearDown() { subscribers_map_.clear(); }

  const rpc::PubMessage GeneratePubMessage(const ObjectID &object_id) {
    rpc::PubMessage pub_message;
    auto *object_eviction_msg = pub_message.mutable_worker_object_eviction_message();
    object_eviction_msg->set_object_id(object_id.Binary());
    pub_message.set_key_id(object_id.Binary());
    pub_message.set_channel_type(rpc::ChannelType::WORKER_OBJECT_EVICTION);
    return pub_message;
  }

  const rpc::PubMessage GenerateErrorInfoMessage(const std::string &id,
                                                 const std::string &text) {
    rpc::PubMessage pub_message;
    auto *error_msg = pub_message.mutable_error_info_message();
    error_msg->set_error_message(text);
    pub_message.set_key_id(id);
    pub_message.set_channel_type(rpc::ChannelType::RAY_ERROR_INFO_CHANNEL);
    return pub_message;
  }

  bool HasSubscriber(const std::vector<SubscriberID> &subscribers,
                     const SubscriberID &subscriber) {
    return std::find(subscribers.begin(), subscribers.end(), subscriber) !=
           subscribers.end();
  }

  SubscriberState *CreateSubscriber() {
    subscribers_.push_back(std::make_unique<SubscriberState>(
        NodeID::FromRandom(),
        /*get_time_ms=*/[]() { return 1.0; },
        /*subscriber_timeout_ms=*/1000,
        /*publish_batch_size=*/1000));
    return subscribers_.back().get();
  }

  rpc::PubsubLongPollingReply FlushSubscriber(SubscriberState *subscriber) {
    rpc::PubsubLongPollingRequest request;
    rpc::PubsubLongPollingReply reply;
    rpc::SendReplyCallback send_reply_callback = [](Status status,
                                                    std::function<void()> success,
                                                    std::function<void()> failure) {};
    subscriber->ConnectToSubscriber(request, &reply, send_reply_callback);
    subscriber->PublishIfPossible();
    return reply;
  }

  instrumented_io_context io_service_;
  std::shared_ptr<PeriodicalRunner> periodic_runner_;
  std::shared_ptr<Publisher> publisher_;
  absl::flat_hash_map<ObjectID, absl::flat_hash_set<NodeID>> subscribers_map_;
  const uint64_t subscriber_timeout_ms_ = 30000;
  double current_time_;
  const SubscriberID subscriber_id_ = SubscriberID::FromRandom();
  rpc::PubsubLongPollingRequest request_;
  std::vector<std::unique_ptr<SubscriberState>> subscribers_;
};

TEST_F(PublisherTest, TestSubscriptionIndexSingeNodeSingleObject) {
  auto oid = ObjectID::FromRandom();
  auto *subscriber = CreateSubscriber();

  ///
  /// Test single node id & object id
  ///
  /// oid1 -> [nid1]
  SubscriptionIndex subscription_index;
  subscription_index.AddEntry(oid.Binary(), subscriber);
  const auto &subscribers_from_index =
      subscription_index.GetSubscriberIdsByKeyId(oid.Binary());
  ASSERT_TRUE(HasSubscriber(subscribers_from_index, subscriber->id()));
}

TEST_F(PublisherTest, TestSubscriptionIndexMultiNodeSingleObject) {
  ///
  /// Test single object id & multi nodes
  ///
  /// oid1 -> [nid1~nid5]
  SubscriptionIndex subscription_index;
  const auto oid = ObjectID::FromRandom();
  absl::flat_hash_set<NodeID> empty_set;
  subscribers_map_.emplace(oid, empty_set);

  for (int i = 0; i < 5; i++) {
    auto *subscriber = CreateSubscriber();
    auto subscriber_id = subscriber->id();
    subscribers_map_.at(oid).emplace(subscriber_id);
    subscription_index.AddEntry(oid.Binary(), subscriber);
  }
  const auto &subscribers_from_index =
      subscription_index.GetSubscriberIdsByKeyId(oid.Binary());
  for (const auto &subscriber_id : subscribers_map_.at(oid)) {
    ASSERT_TRUE(HasSubscriber(subscribers_from_index, subscriber_id));
  }

  ///
  /// Test multi node id & multi object ids
  ///
  /// oid1 -> [nid1~nid5]
  /// oid2 -> [nid1~nid5]
  const auto oid2 = ObjectID::FromRandom();
  subscribers_map_.emplace(oid2, empty_set);
  for (int i = 0; i < 5; i++) {
    auto *subscriber = CreateSubscriber();
    auto subscriber_id = subscriber->id();
    subscribers_map_.at(oid2).emplace(subscriber_id);
    subscription_index.AddEntry(oid2.Binary(), subscriber);
  }
  const auto &subscribers_from_index2 =
      subscription_index.GetSubscriberIdsByKeyId(oid2.Binary());
  for (const auto &subscriber_id : subscribers_map_.at(oid2)) {
    ASSERT_TRUE(HasSubscriber(subscribers_from_index2, subscriber_id));
  }

  // Make sure oid1 entries are not corrupted.
  const auto &subscribers_from_index3 =
      subscription_index.GetSubscriberIdsByKeyId(oid.Binary());
  for (const auto &subscriber_id : subscribers_map_.at(oid)) {
    ASSERT_TRUE(HasSubscriber(subscribers_from_index3, subscriber_id));
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
  absl::flat_hash_set<NodeID> empty_set;
  subscribers_map_.emplace(oid, empty_set);

  // Add entries.
  for (int i = 0; i < total_entries; i++) {
    auto *subscriber = CreateSubscriber();
    auto subscriber_id = subscriber->id();
    subscribers_map_.at(oid).emplace(subscriber_id);
    subscription_index.AddEntry(oid.Binary(), subscriber);
  }

  // Verify that the first 3 entries are deleted properly.
  int i = 0;
  auto &oid_subscribers = subscribers_map_[oid];
  for (auto it = oid_subscribers.begin(); it != oid_subscribers.end();) {
    if (i == entries_to_delete_at_each_time) {
      break;
    }
    auto current = it++;
    auto subscriber_id = *current;
    oid_subscribers.erase(current);
    ASSERT_EQ(subscription_index.EraseEntry(oid.Binary(), subscriber_id), 1);
    i++;
  }
  const auto &subscribers_from_index =
      subscription_index.GetSubscriberIdsByKeyId(oid.Binary());
  for (const auto &subscriber_id : subscribers_map_.at(oid)) {
    ASSERT_TRUE(HasSubscriber(subscribers_from_index, subscriber_id));
  }

  // Delete all entries and make sure the oid is removed from the index.
  for (auto it = oid_subscribers.begin(); it != oid_subscribers.end();) {
    auto current = it++;
    auto subscriber_id = *current;
    oid_subscribers.erase(current);
    subscription_index.EraseEntry(oid.Binary(), subscriber_id);
  }
  ASSERT_FALSE(subscription_index.HasKeyId(oid.Binary()));
  ASSERT_TRUE(subscription_index.CheckNoLeaks());
}

TEST_F(PublisherTest, TestSubscriptionIndexEraseMultiSubscribers) {
  ///
  /// Test erase the duplicated entries with multi subscribers.
  ///
  SubscriptionIndex subscription_index;
  auto oid = ObjectID::FromRandom();
  auto oid2 = ObjectID::FromRandom();
  absl::flat_hash_set<NodeID> empty_set;
  subscribers_map_.emplace(oid, empty_set);
  subscribers_map_.emplace(oid2, empty_set);

  // Add entries.
  auto *subscriber_1 = CreateSubscriber();
  auto subscriber_id = subscriber_1->id();
  auto *subscriber_2 = CreateSubscriber();
  subscribers_map_.at(oid).emplace(subscriber_id);
  subscribers_map_.at(oid2).emplace(subscriber_id);
  subscription_index.AddEntry(oid.Binary(), subscriber_1);
  subscription_index.AddEntry(oid2.Binary(), subscriber_1);
  subscription_index.AddEntry(oid.Binary(), subscriber_2);
  ASSERT_TRUE(subscription_index.EraseEntry(oid.Binary(), subscriber_id));
  ASSERT_FALSE(subscription_index.EraseEntry(oid.Binary(), subscriber_id));
}

TEST_F(PublisherTest, TestSubscriptionIndexEraseSubscriber) {
  ///
  /// Test erase subscriber.
  ///
  SubscriptionIndex subscription_index;
  auto oid = ObjectID::FromRandom();
  auto &subscribers = subscribers_map_[oid];
  std::vector<SubscriberID> subscriber_ids;

  // Add entries.
  for (int i = 0; i < 6; i++) {
    auto *subscriber = CreateSubscriber();
    auto subscriber_id = subscriber->id();
    subscriber_ids.push_back(subscriber_id);
    subscribers.emplace(subscriber_id);
    subscription_index.AddEntry(oid.Binary(), subscriber);
  }
  subscription_index.EraseSubscriber(subscriber_ids[0]);
  ASSERT_FALSE(subscription_index.HasSubscriber(subscriber_ids[0]));
  const auto &subscribers_from_index =
      subscription_index.GetSubscriberIdsByKeyId(oid.Binary());
  ASSERT_FALSE(HasSubscriber(subscribers_from_index, subscriber_ids[0]));

  for (int i = 1; i < 6; i++) {
    subscription_index.EraseSubscriber(subscriber_ids[i]);
  }
  ASSERT_TRUE(subscription_index.CheckNoLeaks());
}

TEST_F(PublisherTest, TestSubscriptionIndexIdempotency) {
  ///
  /// Test the subscription index is idempotent.
  ///
  auto *subscriber = CreateSubscriber();
  auto subscriber_id = subscriber->id();
  auto oid = ObjectID::FromRandom();
  SubscriptionIndex subscription_index;

  // Add the same entry many times.
  for (int i = 0; i < 5; i++) {
    subscription_index.AddEntry(oid.Binary(), subscriber);
  }
  ASSERT_TRUE(subscription_index.HasKeyId(oid.Binary()));
  ASSERT_TRUE(subscription_index.HasSubscriber(subscriber_id));

  // Erase it and make sure it is erased.
  for (int i = 0; i < 5; i++) {
    subscription_index.EraseEntry(oid.Binary(), subscriber_id);
  }
  ASSERT_TRUE(subscription_index.CheckNoLeaks());

  // Random mix.
  subscription_index.AddEntry(oid.Binary(), subscriber);
  subscription_index.AddEntry(oid.Binary(), subscriber);
  subscription_index.EraseEntry(oid.Binary(), subscriber_id);
  subscription_index.EraseEntry(oid.Binary(), subscriber_id);
  ASSERT_TRUE(subscription_index.CheckNoLeaks());

  subscription_index.AddEntry(oid.Binary(), subscriber);
  subscription_index.AddEntry(oid.Binary(), subscriber);
  ASSERT_TRUE(subscription_index.HasKeyId(oid.Binary()));
  ASSERT_TRUE(subscription_index.HasSubscriber(subscriber_id));
}

TEST_F(PublisherTest, TestSubscriber) {
  absl::flat_hash_set<ObjectID> object_ids_published;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&reply, &object_ids_published](
          Status status, std::function<void()> success, std::function<void()> failure) {
        for (int i = 0; i < reply.pub_messages_size(); i++) {
          const auto &msg = reply.pub_messages(i);
          const auto oid =
              ObjectID::FromBinary(msg.worker_object_eviction_message().object_id());
          object_ids_published.emplace(oid);
        }
        reply = rpc::PubsubLongPollingReply();
      };

  auto subscriber = std::make_shared<SubscriberState>(
      subscriber_id_, [this]() { return current_time_; }, subscriber_timeout_ms_, 10);
  // If there's no connection, it will return false.
  ASSERT_FALSE(subscriber->PublishIfPossible());
  // Try connecting.
  subscriber->ConnectToSubscriber(request_, &reply, send_reply_callback);
  // Reconnection should still succeed.
  subscriber->ConnectToSubscriber(request_, &reply, send_reply_callback);
  // No result should have been returned.
  ASSERT_TRUE(object_ids_published.empty());
  // Since there's no objects pending to be published, it should return false.
  ASSERT_FALSE(subscriber->PublishIfPossible());

  absl::flat_hash_set<ObjectID> published_objects;
  // Make sure publishing one object works as expected.
  auto oid = ObjectID::FromRandom();
  subscriber->QueueMessage(std::make_shared<rpc::PubMessage>(GeneratePubMessage(oid)),
                           /*try_publish=*/false);
  published_objects.emplace(oid);
  ASSERT_TRUE(subscriber->PublishIfPossible());
  ASSERT_TRUE(object_ids_published.contains(oid));
  // No object is pending to be published, and there's no connection.
  ASSERT_FALSE(subscriber->PublishIfPossible());

  // Add 3 oids and see if it works properly.
  for (int i = 0; i < 3; i++) {
    oid = ObjectID::FromRandom();
    subscriber->QueueMessage(std::make_shared<rpc::PubMessage>(GeneratePubMessage(oid)),
                             /*try_publish=*/false);
    published_objects.emplace(oid);
  }
  // Since there's no connection, objects won't be published.
  ASSERT_FALSE(subscriber->PublishIfPossible());
  subscriber->ConnectToSubscriber(request_, &reply, send_reply_callback);
  for (auto oid : published_objects) {
    ASSERT_TRUE(object_ids_published.contains(oid));
  }
  ASSERT_TRUE(subscriber->CheckNoLeaks());
}

TEST_F(PublisherTest, TestSubscriberBatchSize) {
  absl::flat_hash_set<ObjectID> object_ids_published;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&reply, &object_ids_published](
          Status status, std::function<void()> success, std::function<void()> failure) {
        for (int i = 0; i < reply.pub_messages_size(); i++) {
          const auto &msg = reply.pub_messages(i);
          const auto oid =
              ObjectID::FromBinary(msg.worker_object_eviction_message().object_id());
          object_ids_published.emplace(oid);
        }
        reply = rpc::PubsubLongPollingReply();
      };

  auto max_publish_size = 5;
  auto subscriber = std::make_shared<SubscriberState>(
      subscriber_id_,
      [this]() { return current_time_; },
      subscriber_timeout_ms_,
      max_publish_size);
  subscriber->ConnectToSubscriber(request_, &reply, send_reply_callback);

  absl::flat_hash_set<ObjectID> published_objects;
  std::vector<ObjectID> oids;
  for (int i = 0; i < 10; i++) {
    auto oid = ObjectID::FromRandom();
    oids.push_back(oid);
    subscriber->QueueMessage(std::make_shared<rpc::PubMessage>(GeneratePubMessage(oid)),
                             /*try_publish=*/false);
    published_objects.emplace(oid);
  }

  // Make sure only up to batch size is published.
  ASSERT_TRUE(subscriber->PublishIfPossible());

  for (int i = 0; i < max_publish_size; i++) {
    ASSERT_TRUE(object_ids_published.contains(oids[i]));
  }
  for (int i = max_publish_size; i < 10; i++) {
    ASSERT_FALSE(object_ids_published.contains(oids[i]));
  }

  // Remaining messages are published upon polling.
  subscriber->ConnectToSubscriber(request_, &reply, send_reply_callback);
  for (int i = 0; i < 10; i++) {
    ASSERT_TRUE(object_ids_published.contains(oids[i]));
  }
}

TEST_F(PublisherTest, TestSubscriberActiveTimeout) {
  ///
  /// Test the active connection timeout.
  ///

  auto reply_cnt = 0;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&reply_cnt](Status status,
                   std::function<void()> success,
                   std::function<void()> failure) { reply_cnt++; };

  auto subscriber = std::make_shared<SubscriberState>(
      subscriber_id_, [this]() { return current_time_; }, subscriber_timeout_ms_, 10);

  subscriber->ConnectToSubscriber(request_, &reply, send_reply_callback);

  // Connection is not timed out yet.
  ASSERT_TRUE(subscriber->IsActive());
  ASSERT_TRUE(subscriber->ConnectionExists());

  // Some time has passed, but it is not timed out yet.
  current_time_ += subscriber_timeout_ms_ / 2;
  ASSERT_TRUE(subscriber->IsActive());
  ASSERT_TRUE(subscriber->ConnectionExists());

  // Timeout is reached, and the long polling connection should've been refreshed.
  current_time_ += subscriber_timeout_ms_ / 2;
  ASSERT_FALSE(subscriber->IsActive());
  ASSERT_TRUE(subscriber->ConnectionExists());

  // Refresh the connection.
  subscriber->PublishIfPossible(/*force_noop=*/true);
  ASSERT_EQ(reply_cnt, 1);

  // New connection is established.
  subscriber->ConnectToSubscriber(request_, &reply, send_reply_callback);
  ASSERT_TRUE(subscriber->IsActive());
  ASSERT_TRUE(subscriber->ConnectionExists());

  // Some time has passed, but it is not timed out yet.
  current_time_ += subscriber_timeout_ms_ / 2;
  ASSERT_TRUE(subscriber->IsActive());
  ASSERT_TRUE(subscriber->ConnectionExists());

  // A message is published, so the connection is refreshed.
  auto oid = ObjectID::FromRandom();
  subscriber->QueueMessage(std::make_shared<rpc::PubMessage>(GeneratePubMessage(oid)));
  ASSERT_TRUE(subscriber->IsActive());
  ASSERT_FALSE(subscriber->ConnectionExists());
  ASSERT_EQ(reply_cnt, 2);

  // Although time has passed, since the connection was refreshed, timeout shouldn't
  // happen.
  current_time_ += subscriber_timeout_ms_ / 2;
  ASSERT_TRUE(subscriber->IsActive());
  ASSERT_FALSE(subscriber->ConnectionExists());

  ASSERT_TRUE(subscriber->CheckNoLeaks());
}

TEST_F(PublisherTest, TestSubscriberDisconnected) {
  ///
  /// Test the subscriber is considered as dead due to the disconnection timeout.
  ///

  auto reply_cnt = 0;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&reply_cnt](Status status,
                   std::function<void()> success,
                   std::function<void()> failure) { reply_cnt++; };

  auto subscriber = std::make_shared<SubscriberState>(
      subscriber_id_, [this]() { return current_time_; }, subscriber_timeout_ms_, 10);

  // Suppose the new connection is removed.
  subscriber->ConnectToSubscriber(request_, &reply, send_reply_callback);
  subscriber->PublishIfPossible(/*force*/ true);
  ASSERT_EQ(reply_cnt, 1);
  ASSERT_TRUE(subscriber->IsActive());
  ASSERT_FALSE(subscriber->ConnectionExists());

  // Some time has passed, but it is not timed out yet.
  current_time_ += subscriber_timeout_ms_ / 2;
  ASSERT_TRUE(subscriber->IsActive());
  ASSERT_FALSE(subscriber->ConnectionExists());

  // Timeout is reached. Since there was no new long polling connection, it is considered
  // as disconnected.
  current_time_ += subscriber_timeout_ms_ / 2;
  ASSERT_FALSE(subscriber->IsActive());
  ASSERT_FALSE(subscriber->ConnectionExists());

  // New connection is coming in.
  subscriber->ConnectToSubscriber(request_, &reply, send_reply_callback);
  subscriber->PublishIfPossible(/*force*/ true);
  ASSERT_EQ(reply_cnt, 2);

  // Some time has passed, but it is not timed out yet.
  current_time_ += subscriber_timeout_ms_ / 2;
  ASSERT_TRUE(subscriber->IsActive());
  ASSERT_FALSE(subscriber->ConnectionExists());

  // Another connection is made, so it shouldn't timeout until the next timeout is
  // reached.
  subscriber->ConnectToSubscriber(request_, &reply, send_reply_callback);
  subscriber->PublishIfPossible(/*force*/ true);
  ASSERT_EQ(reply_cnt, 3);
  current_time_ += subscriber_timeout_ms_ / 2;
  ASSERT_TRUE(subscriber->IsActive());
  ASSERT_FALSE(subscriber->ConnectionExists());

  // IF there's no new connection for a long time it should eventually timeout.
  current_time_ += subscriber_timeout_ms_ / 2;
  ASSERT_FALSE(subscriber->IsActive());
  ASSERT_FALSE(subscriber->ConnectionExists());

  ASSERT_TRUE(subscriber->CheckNoLeaks());
}

TEST_F(PublisherTest, TestSubscriberTimeoutComplicated) {
  ///
  /// Test the subscriber timeout in more complicated scenario.
  ///

  auto reply_cnt = 0;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&reply_cnt](Status status,
                   std::function<void()> success,
                   std::function<void()> failure) { reply_cnt++; };

  auto subscriber = std::make_shared<SubscriberState>(
      subscriber_id_, [this]() { return current_time_; }, subscriber_timeout_ms_, 10);

  // Suppose the new connection is removed.
  subscriber->ConnectToSubscriber(request_, &reply, send_reply_callback);
  subscriber->PublishIfPossible(/*force*/ true);
  ASSERT_EQ(reply_cnt, 1);
  ASSERT_TRUE(subscriber->IsActive());
  ASSERT_FALSE(subscriber->ConnectionExists());

  // Some time has passed, and the connection is removed.
  current_time_ += subscriber_timeout_ms_ - 1;
  subscriber->ConnectToSubscriber(request_, &reply, send_reply_callback);
  current_time_ += 2;
  // Timeout shouldn't happen because the connection has been refreshed.
  ASSERT_TRUE(subscriber->IsActive());
  ASSERT_TRUE(subscriber->ConnectionExists());

  // Right before the timeout, connection is removed. In this case, timeout shouldn't also
  // happen.
  current_time_ += subscriber_timeout_ms_ - 1;
  subscriber->PublishIfPossible(/*force*/ true);
  current_time_ += 2;
  ASSERT_TRUE(subscriber->IsActive());
  ASSERT_FALSE(subscriber->ConnectionExists());

  // Timeout is reached. Since there was no connection, it should be considered
  // disconnected.
  current_time_ += subscriber_timeout_ms_;
  ASSERT_FALSE(subscriber->IsActive());
  ASSERT_FALSE(subscriber->ConnectionExists());

  ASSERT_TRUE(subscriber->CheckNoLeaks());
}

TEST_F(PublisherTest, TestBasicSingleSubscriber) {
  std::vector<ObjectID> batched_ids;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&reply, &batched_ids](
          Status status, std::function<void()> success, std::function<void()> failure) {
        for (int i = 0; i < reply.pub_messages_size(); i++) {
          const auto &msg = reply.pub_messages(i);
          const auto oid =
              ObjectID::FromBinary(msg.worker_object_eviction_message().object_id());
          batched_ids.push_back(oid);
        }
        reply = rpc::PubsubLongPollingReply();
      };

  const auto oid = ObjectID::FromRandom();

  publisher_->ConnectToSubscriber(request_, &reply, send_reply_callback);
  publisher_->RegisterSubscription(
      rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary());
  publisher_->Publish(GeneratePubMessage(oid));
  ASSERT_EQ(batched_ids[0], oid);
}

TEST_F(PublisherTest, TestNoConnectionWhenRegistered) {
  std::vector<ObjectID> batched_ids;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&reply, &batched_ids](
          Status status, std::function<void()> success, std::function<void()> failure) {
        for (int i = 0; i < reply.pub_messages_size(); i++) {
          const auto &msg = reply.pub_messages(i);
          const auto oid =
              ObjectID::FromBinary(msg.worker_object_eviction_message().object_id());
          batched_ids.push_back(oid);
        }
        reply = rpc::PubsubLongPollingReply();
      };

  const auto oid = ObjectID::FromRandom();

  publisher_->RegisterSubscription(
      rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary());
  publisher_->Publish(GeneratePubMessage(oid));
  // Nothing has been published because there's no connection.
  ASSERT_EQ(batched_ids.size(), 0);
  publisher_->ConnectToSubscriber(request_, &reply, send_reply_callback);
  // When the connection is coming, it should be published.
  ASSERT_EQ(batched_ids[0], oid);
}

TEST_F(PublisherTest, TestMultiObjectsFromSingleNode) {
  std::vector<ObjectID> batched_ids;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&reply, &batched_ids](
          Status status, std::function<void()> success, std::function<void()> failure) {
        for (int i = 0; i < reply.pub_messages_size(); i++) {
          const auto &msg = reply.pub_messages(i);
          const auto oid =
              ObjectID::FromBinary(msg.worker_object_eviction_message().object_id());
          batched_ids.push_back(oid);
        }
        reply = rpc::PubsubLongPollingReply();
      };

  std::vector<ObjectID> oids;
  int num_oids = 5;
  for (int i = 0; i < num_oids; i++) {
    const auto oid = ObjectID::FromRandom();
    oids.push_back(oid);
    publisher_->RegisterSubscription(
        rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary());
    publisher_->Publish(GeneratePubMessage(oid));
  }
  ASSERT_EQ(batched_ids.size(), 0);

  // Now connection is initiated, and all oids are published.
  publisher_->ConnectToSubscriber(request_, &reply, send_reply_callback);
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
      [&reply, &batched_ids](
          Status status, std::function<void()> success, std::function<void()> failure) {
        for (int i = 0; i < reply.pub_messages_size(); i++) {
          const auto &msg = reply.pub_messages(i);
          const auto oid =
              ObjectID::FromBinary(msg.worker_object_eviction_message().object_id());
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
    publisher_->RegisterSubscription(
        rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary());
    publisher_->Publish(GeneratePubMessage(oid));
  }
  ASSERT_EQ(batched_ids.size(), 0);

  // Check all of nodes are publishing objects properly.
  for (int i = 0; i < num_nodes; i++) {
    publisher_->ConnectToSubscriber(request_, &reply, send_reply_callback);
    const auto oid_test = oids[i];
    const auto published_oid = batched_ids[i];
    ASSERT_EQ(oid_test, published_oid);
  }
}

TEST_F(PublisherTest, TestMultiSubscribers) {
  absl::flat_hash_set<ObjectID> batched_ids;
  rpc::PubsubLongPollingReply reply;
  int reply_invoked = 0;
  rpc::SendReplyCallback send_reply_callback =
      [&reply, &batched_ids, &reply_invoked](
          Status status, std::function<void()> success, std::function<void()> failure) {
        for (int i = 0; i < reply.pub_messages_size(); i++) {
          const auto &msg = reply.pub_messages(i);
          const auto oid =
              ObjectID::FromBinary(msg.worker_object_eviction_message().object_id());
          batched_ids.emplace(oid);
        }
        reply = rpc::PubsubLongPollingReply();
        reply_invoked += 1;
      };

  std::vector<NodeID> subscribers;
  const auto oid = ObjectID::FromRandom();
  int num_nodes = 5;
  for (int i = 0; i < num_nodes; i++) {
    subscribers.push_back(NodeID::FromRandom());
  }

  // There will be one object per node.
  for (int i = 0; i < num_nodes; i++) {
    publisher_->RegisterSubscription(
        rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary());
  }
  ASSERT_EQ(batched_ids.size(), 0);

  // Check all of nodes are publishing objects properly.
  for (int i = 0; i < num_nodes; i++) {
    publisher_->ConnectToSubscriber(request_, &reply, send_reply_callback);
  }
  publisher_->Publish(GeneratePubMessage(oid));
  ASSERT_EQ(batched_ids.size(), 1);
  ASSERT_EQ(reply_invoked, 5);
}

TEST_F(PublisherTest, TestBatch) {
  // Test if published objects are batched properly.
  std::vector<ObjectID> batched_ids;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&reply, &batched_ids](
          Status status, std::function<void()> success, std::function<void()> failure) {
        for (int i = 0; i < reply.pub_messages_size(); i++) {
          const auto &msg = reply.pub_messages(i);
          const auto oid =
              ObjectID::FromBinary(msg.worker_object_eviction_message().object_id());
          batched_ids.push_back(oid);
        }
        reply = rpc::PubsubLongPollingReply();
      };

  std::vector<ObjectID> oids;
  int num_oids = 5;
  for (int i = 0; i < num_oids; i++) {
    const auto oid = ObjectID::FromRandom();
    oids.push_back(oid);
    publisher_->RegisterSubscription(
        rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary());
    publisher_->Publish(GeneratePubMessage(oid));
  }
  ASSERT_EQ(batched_ids.size(), 0);

  // Now connection is initiated, and all oids are published.
  publisher_->ConnectToSubscriber(request_, &reply, send_reply_callback);
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
    publisher_->RegisterSubscription(
        rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary());
    publisher_->Publish(GeneratePubMessage(oid));
  }
  publisher_->ConnectToSubscriber(request_, &reply, send_reply_callback);
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
      [&long_polling_connection_replied](
          Status status, std::function<void()> success, std::function<void()> failure) {
        long_polling_connection_replied = true;
      };

  const auto oid = ObjectID::FromRandom();
  publisher_->ConnectToSubscriber(request_, &reply, send_reply_callback);
  // This information should be cleaned up as the subscriber is dead.
  publisher_->RegisterSubscription(
      rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary());
  // Timeout is reached. The connection should've been refreshed. Since the subscriber is
  // dead, no new connection is made.
  current_time_ += subscriber_timeout_ms_;
  publisher_->CheckDeadSubscribers();
  ASSERT_EQ(long_polling_connection_replied, true);

  // More time has passed, and since there was no new long polling connection, this
  // subscriber is considered as dead.
  current_time_ += subscriber_timeout_ms_;
  publisher_->CheckDeadSubscribers();

  // Connection should be replied (removed) when the subscriber is unregistered.
  int erased = publisher_->UnregisterSubscriber(subscriber_id_);
  ASSERT_EQ(erased, 0);
  ASSERT_TRUE(publisher_->CheckNoLeaks());

  // New subscriber is registsered for some reason. Since there's no new long polling
  // connection for the timeout, it should be removed.
  long_polling_connection_replied = false;
  publisher_->RegisterSubscription(
      rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary());
  current_time_ += subscriber_timeout_ms_;
  publisher_->CheckDeadSubscribers();
  erased = publisher_->UnregisterSubscriber(subscriber_id_);
  ASSERT_EQ(erased, 0);
  ASSERT_TRUE(publisher_->CheckNoLeaks());
}

TEST_F(PublisherTest, TestNodeFailureWhenConnectionDoesntExist) {
  bool long_polling_connection_replied = false;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&long_polling_connection_replied](
          Status status, std::function<void()> success, std::function<void()> failure) {
        long_polling_connection_replied = true;
      };

  ///
  /// Test the case where there was a registration, but no connection.
  ///
  auto oid = ObjectID::FromRandom();
  publisher_->RegisterSubscription(
      rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary());
  publisher_->Publish(GeneratePubMessage(oid));
  // There was no long polling connection yet.
  ASSERT_EQ(long_polling_connection_replied, false);

  // Connect should be removed eventually to avoid having a memory leak.
  publisher_->ConnectToSubscriber(request_, &reply, send_reply_callback);
  ASSERT_EQ(long_polling_connection_replied, true);
  // Nothing happens at first.
  publisher_->CheckDeadSubscribers();

  // After the timeout, the subscriber should be considered as dead because there was no
  // new long polling connection.
  current_time_ += subscriber_timeout_ms_;
  publisher_->CheckDeadSubscribers();
  // Make sure the registration is cleaned up.
  ASSERT_TRUE(publisher_->CheckNoLeaks());

  /// Test the case where there's no connection coming at all when there was a
  /// registration.
  publisher_->RegisterSubscription(
      rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary());
  publisher_->Publish(GeneratePubMessage(oid));

  // No new long polling connection was made until timeout.
  current_time_ += subscriber_timeout_ms_;
  publisher_->CheckDeadSubscribers();
  // Make sure the registration is cleaned up.
  ASSERT_TRUE(publisher_->CheckNoLeaks());
}

// Unregistration an entry.
TEST_F(PublisherTest, TestUnregisterSubscription) {
  bool long_polling_connection_replied = false;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&long_polling_connection_replied](
          Status status, std::function<void()> success, std::function<void()> failure) {
        long_polling_connection_replied = true;
      };

  const auto oid = ObjectID::FromRandom();
  publisher_->ConnectToSubscriber(request_, &reply, send_reply_callback);
  publisher_->RegisterSubscription(
      rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary());
  ASSERT_EQ(long_polling_connection_replied, false);

  // Connection should be replied (removed) when the subscriber is unregistered.
  int erased = publisher_->UnregisterSubscription(
      rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary());
  ASSERT_EQ(erased, 1);
  ASSERT_EQ(long_polling_connection_replied, false);

  // Make sure when the entries don't exist, it doesn't delete anything.
  ASSERT_EQ(publisher_->UnregisterSubscription(rpc::ChannelType::WORKER_OBJECT_EVICTION,
                                               subscriber_id_,
                                               ObjectID::FromRandom().Binary()),
            0);
  ASSERT_EQ(
      publisher_->UnregisterSubscription(
          rpc::ChannelType::WORKER_OBJECT_EVICTION, NodeID::FromRandom(), oid.Binary()),
      0);
  ASSERT_EQ(publisher_->UnregisterSubscription(rpc::ChannelType::WORKER_OBJECT_EVICTION,
                                               NodeID::FromRandom(),
                                               ObjectID::FromRandom().Binary()),
            0);
  ASSERT_EQ(long_polling_connection_replied, false);
  // Metadata won't be removed until we unregsiter the subscriber.
  publisher_->UnregisterSubscriber(subscriber_id_);
  ASSERT_TRUE(publisher_->CheckNoLeaks());
}

// Unregistration a subscriber.
TEST_F(PublisherTest, TestUnregisterSubscriber) {
  bool long_polling_connection_replied = false;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&long_polling_connection_replied](
          Status status, std::function<void()> success, std::function<void()> failure) {
        long_polling_connection_replied = true;
      };

  // Test basic.
  const auto oid = ObjectID::FromRandom();
  publisher_->ConnectToSubscriber(request_, &reply, send_reply_callback);
  publisher_->RegisterSubscription(
      rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary());
  ASSERT_EQ(long_polling_connection_replied, false);
  int erased = publisher_->UnregisterSubscriber(subscriber_id_);
  ASSERT_TRUE(erased);
  // Make sure the long polling request is replied to avoid memory leak.
  ASSERT_EQ(long_polling_connection_replied, true);

  // Test when registration wasn't done.
  long_polling_connection_replied = false;
  publisher_->ConnectToSubscriber(request_, &reply, send_reply_callback);
  erased = publisher_->UnregisterSubscriber(subscriber_id_);
  ASSERT_FALSE(erased);
  ASSERT_EQ(long_polling_connection_replied, true);

  // Test when connect wasn't done.
  long_polling_connection_replied = false;
  publisher_->RegisterSubscription(
      rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary());
  erased = publisher_->UnregisterSubscriber(subscriber_id_);
  ASSERT_TRUE(erased);
  ASSERT_EQ(long_polling_connection_replied, false);
  ASSERT_TRUE(publisher_->CheckNoLeaks());
}

// Test if registration / unregistration is idempotent.
TEST_F(PublisherTest, TestRegistrationIdempotency) {
  const auto oid = ObjectID::FromRandom();
  ASSERT_TRUE(publisher_->RegisterSubscription(
      rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary()));
  ASSERT_FALSE(publisher_->RegisterSubscription(
      rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary()));
  ASSERT_FALSE(publisher_->RegisterSubscription(
      rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary()));
  ASSERT_FALSE(publisher_->RegisterSubscription(
      rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary()));
  ASSERT_FALSE(publisher_->CheckNoLeaks());
  ASSERT_TRUE(publisher_->UnregisterSubscription(
      rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary()));
  ASSERT_FALSE(publisher_->UnregisterSubscription(
      rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary()));
  ASSERT_TRUE(publisher_->CheckNoLeaks());
  ASSERT_TRUE(publisher_->RegisterSubscription(
      rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary()));
  ASSERT_FALSE(publisher_->CheckNoLeaks());
  ASSERT_TRUE(publisher_->UnregisterSubscription(
      rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary()));
}

TEST_F(PublisherTest, TestPublishFailure) {
  ///
  /// Test the publish failure API.
  ///
  std::vector<ObjectID> failed_ids;
  rpc::PubsubLongPollingReply reply;
  rpc::SendReplyCallback send_reply_callback =
      [&reply, &failed_ids](
          Status status, std::function<void()> success, std::function<void()> failure) {
        for (int i = 0; i < reply.pub_messages_size(); i++) {
          const auto &msg = reply.pub_messages(i);
          RAY_LOG(ERROR) << "ha";
          if (msg.has_failure_message()) {
            const auto oid = ObjectID::FromBinary(msg.key_id());
            failed_ids.push_back(oid);
          }
        }
        reply = rpc::PubsubLongPollingReply();
      };

  const auto oid = ObjectID::FromRandom();

  publisher_->ConnectToSubscriber(request_, &reply, send_reply_callback);
  publisher_->RegisterSubscription(
      rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_id_, oid.Binary());
  publisher_->PublishFailure(rpc::ChannelType::WORKER_OBJECT_EVICTION, oid.Binary());
  ASSERT_EQ(failed_ids[0], oid);
}

class ScopedEntityBufferMaxBytes {
 public:
  ScopedEntityBufferMaxBytes(int64_t max_bytes)
      : prev_max_bytes_(RayConfig::instance().publisher_entity_buffer_max_bytes()) {
    RayConfig::instance().publisher_entity_buffer_max_bytes() = max_bytes;
  }

  ~ScopedEntityBufferMaxBytes() {
    RayConfig::instance().publisher_entity_buffer_max_bytes() = prev_max_bytes_;
  }

 private:
  const int64_t prev_max_bytes_;
};

TEST_F(PublisherTest, TestMaxBufferSizePerEntity) {
  ScopedEntityBufferMaxBytes max_bytes(10000);

  SubscriptionIndex subscription_index;
  auto job_id = JobID::FromInt(1234);
  auto *subscriber = CreateSubscriber();
  // Subscribe to job_id.
  subscription_index.AddEntry(job_id.Binary(), subscriber);

  rpc::PubMessage pub_message;
  pub_message.set_key_id(job_id.Binary());
  pub_message.set_channel_type(rpc::ChannelType::RAY_ERROR_INFO_CHANNEL);
  pub_message.mutable_error_info_message()->set_error_message(std::string(4000, 'a'));

  // Buffer is available.
  EXPECT_TRUE(subscription_index.Publish(pub_message));

  // Buffer is still available.
  pub_message.mutable_error_info_message()->set_error_message(std::string(4000, 'b'));
  EXPECT_TRUE(subscription_index.Publish(pub_message));

  // Buffer is full.
  pub_message.mutable_error_info_message()->set_error_message(std::string(4000, 'c'));
  EXPECT_TRUE(subscription_index.Publish(pub_message));

  // Subscriber receives the last two messages. 1st message is dropped.
  auto reply = FlushSubscriber(subscriber);
  ASSERT_EQ(reply.pub_messages().size(), 2);
  EXPECT_EQ(reply.pub_messages(0).error_info_message().error_message(),
            std::string(4000, 'b'));
  EXPECT_EQ(reply.pub_messages(1).error_info_message().error_message(),
            std::string(4000, 'c'));

  // A message larger than the buffer limit can still be published.
  pub_message.mutable_error_info_message()->set_error_message(std::string(14000, 'd'));
  EXPECT_TRUE(subscription_index.Publish(pub_message));
  reply = FlushSubscriber(subscriber);
  ASSERT_EQ(reply.pub_messages().size(), 1);
  EXPECT_EQ(reply.pub_messages(0).error_info_message().error_message(),
            std::string(14000, 'd'));
}

TEST_F(PublisherTest, TestMaxBufferSizeAllEntities) {
  ScopedEntityBufferMaxBytes max_bytes(10000);

  SubscriptionIndex subscription_index;
  auto *subscriber = CreateSubscriber();
  // Subscribe to all entities.
  subscription_index.AddEntry("", subscriber);

  rpc::PubMessage pub_message;
  pub_message.set_key_id("aaa");
  pub_message.set_channel_type(rpc::ChannelType::RAY_ERROR_INFO_CHANNEL);
  pub_message.mutable_error_info_message()->set_error_message(std::string(4000, 'a'));

  // Buffer is available.
  EXPECT_TRUE(subscription_index.Publish(pub_message));

  // Buffer is still available.
  pub_message.set_key_id("bbb");
  pub_message.mutable_error_info_message()->set_error_message(std::string(4000, 'b'));
  EXPECT_TRUE(subscription_index.Publish(pub_message));

  // Buffer is full.
  pub_message.set_key_id("ccc");
  pub_message.mutable_error_info_message()->set_error_message(std::string(4000, 'c'));
  EXPECT_TRUE(subscription_index.Publish(pub_message));

  auto reply = FlushSubscriber(subscriber);
  ASSERT_EQ(reply.pub_messages().size(), 2);
  EXPECT_EQ(reply.pub_messages(0).error_info_message().error_message(),
            std::string(4000, 'b'));
  EXPECT_EQ(reply.pub_messages(1).error_info_message().error_message(),
            std::string(4000, 'c'));
}

}  // namespace pubsub

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
