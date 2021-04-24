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

#include "ray/pubsub/subscriber.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace ray {

using ::testing::_;
using namespace pubsub;

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  void PubsubLongPolling(
      const rpc::PubsubLongPollingRequest &request,
      const rpc::ClientCallback<rpc::PubsubLongPollingReply> &callback) override {
    long_polling_callbacks.push_back(callback);
  }

  bool ReplyLongPolling(rpc::ChannelType channel_type, std::vector<ObjectID> &object_ids,
                        Status status = Status::OK()) {
    if (long_polling_callbacks.empty()) {
      return false;
    }
    auto callback = long_polling_callbacks.front();
    auto reply = rpc::PubsubLongPollingReply();

    for (const auto &object_id : object_ids) {
      auto *new_pub_message = reply.add_pub_messages();
      new_pub_message->set_message_id(object_id.Binary());
      new_pub_message->set_channel_type(channel_type);
    }
    callback(status, reply);
    long_polling_callbacks.pop_front();
    return true;
  }

  int GetNumberOfInFlightLongPollingRequests() { return long_polling_callbacks.size(); }

  std::deque<rpc::ClientCallback<rpc::PubsubLongPollingReply>> long_polling_callbacks;
};

class SubscriberTest : public ::testing::Test {
 public:
  SubscriberTest()
      : self_node_id_(NodeID::FromRandom()),
        self_node_address_("address"),
        self_node_port_(1234),
        owner_client(std::make_shared<MockWorkerClient>()),
        client_pool([&](const rpc::Address &addr) { return owner_client; }),
        channel(rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION) {}
  ~SubscriberTest() {}

  void SetUp() {
    object_subscribed_.clear();
    object_failed_to_subscribe_.clear();
    subscriber_ = std::shared_ptr<Subscriber>(
        new Subscriber(self_node_id_, self_node_address_, self_node_port_, client_pool));
  }

  const rpc::Address GenerateOwnerAddress(
      const std::string node_id = NodeID::FromRandom().Binary(),
      const std::string worker_id = WorkerID::FromRandom().Binary(),
      const std::string address = "abc", const int port = 1234) {
    rpc::Address addr;
    addr.set_raylet_id(node_id);
    addr.set_ip_address(address);
    addr.set_port(port);
    addr.set_worker_id(worker_id);
    return addr;
  }

  void TearDown() {}

  const NodeID self_node_id_;
  const std::string self_node_address_;
  const int self_node_port_;
  std::shared_ptr<MockWorkerClient> owner_client;
  rpc::CoreWorkerClientPool client_pool;
  std::shared_ptr<Subscriber> subscriber_;
  std::unordered_set<ObjectID> object_subscribed_;
  std::unordered_set<ObjectID> object_failed_to_subscribe_;
  rpc::ChannelType channel;
};

TEST_F(SubscriberTest, TestBasicSubscription) {
  auto subscription_callback = [this](const rpc::PubMessage &msg) {
    object_subscribed_.emplace(ObjectID::FromBinary(msg.message_id()));
  };
  auto failure_callback = []() {};

  const auto owner_addr = GenerateOwnerAddress();
  const auto object_id = ObjectID::FromRandom();
  ASSERT_FALSE(subscriber_->Unsubscribe(channel, owner_addr, object_id.Binary()));
  subscriber_->Subscribe(channel, owner_addr, object_id.Binary(), subscription_callback,
                         failure_callback);
  std::vector<ObjectID> objects_batched;
  objects_batched.push_back(object_id);
  ASSERT_TRUE(owner_client->ReplyLongPolling(channel, objects_batched));
  ASSERT_TRUE(subscriber_->Unsubscribe(channel, owner_addr, object_id.Binary()));

  // Make sure the long polling batch works as expected.
  for (const auto &object_id : objects_batched) {
    ASSERT_TRUE(object_subscribed_.count(object_id) > 0);
  }

  // Here, once the long polling request is replied, the metadata is cleaned up.
  ASSERT_TRUE(owner_client->ReplyLongPolling(channel, objects_batched));
  ASSERT_TRUE(subscriber_->CheckNoLeaks());
}

TEST_F(SubscriberTest, TestSingleLongPollingWithMultipleSubscriptions) {
  ///
  /// Make sure long polling is called only once with multiple subscription.
  ///

  auto subscription_callback = [this](const rpc::PubMessage &msg) {
    object_subscribed_.emplace(ObjectID::FromBinary(msg.message_id()));
  };
  auto failure_callback = []() {};

  const auto owner_addr = GenerateOwnerAddress();
  std::vector<ObjectID> object_ids;
  std::vector<ObjectID> objects_batched;
  for (int i = 0; i < 5; i++) {
    const auto object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    subscriber_->Subscribe(channel, owner_addr, object_id.Binary(), subscription_callback,
                           failure_callback);
    objects_batched.push_back(object_id);
  }
  ASSERT_EQ(owner_client->GetNumberOfInFlightLongPollingRequests(), 1);
  ASSERT_TRUE(owner_client->ReplyLongPolling(channel, objects_batched));
  ASSERT_EQ(owner_client->GetNumberOfInFlightLongPollingRequests(), 1);

  // Make sure the long polling batch works as expected.
  for (const auto &object_id : objects_batched) {
    ASSERT_TRUE(object_subscribed_.count(object_id) > 0);
  }
}

TEST_F(SubscriberTest, TestMultiLongPollingWithTheSameSubscription) {
  ///
  /// Make sure long polling will keep working as long as subscription is not removed.
  ///

  auto subscription_callback = [this](const rpc::PubMessage &msg) {
    object_subscribed_.emplace(ObjectID::FromBinary(msg.message_id()));
  };
  auto failure_callback = []() {};

  const auto owner_addr = GenerateOwnerAddress();
  const auto object_id = ObjectID::FromRandom();
  subscriber_->Subscribe(channel, owner_addr, object_id.Binary(), subscription_callback,
                         failure_callback);
  ASSERT_EQ(owner_client->GetNumberOfInFlightLongPollingRequests(), 1);

  // The object information is published.
  std::vector<ObjectID> objects_batched;
  objects_batched.push_back(object_id);
  ASSERT_TRUE(owner_client->ReplyLongPolling(channel, objects_batched));
  ASSERT_TRUE(object_subscribed_.count(object_id) > 0);
  objects_batched.clear();
  object_subscribed_.clear();

  // New long polling should be made because the subscription is still alive.
  ASSERT_EQ(owner_client->GetNumberOfInFlightLongPollingRequests(), 1);
  objects_batched.push_back(object_id);
  ASSERT_TRUE(owner_client->ReplyLongPolling(channel, objects_batched));
  ASSERT_TRUE(object_subscribed_.count(object_id) > 0);
}

TEST_F(SubscriberTest, TestCallbackNotInvokedForNonSubscribedObject) {
  ///
  /// Make sure the non-subscribed object's subscription callback is not called.
  ///

  auto subscription_callback = [this](const rpc::PubMessage &msg) {
    object_subscribed_.emplace(ObjectID::FromBinary(msg.message_id()));
  };
  auto failure_callback = []() {};

  const auto owner_addr = GenerateOwnerAddress();
  const auto object_id = ObjectID::FromRandom();
  const auto object_id_not_subscribed = ObjectID::FromRandom();
  subscriber_->Subscribe(channel, owner_addr, object_id.Binary(), subscription_callback,
                         failure_callback);

  // The object information is published.
  std::vector<ObjectID> objects_batched;
  objects_batched.push_back(object_id_not_subscribed);
  ASSERT_TRUE(owner_client->ReplyLongPolling(channel, objects_batched));
  ASSERT_EQ(object_subscribed_.count(object_id), 0);
  // Since this object id wasn't subscribed, the callback shouldn't be called.
  ASSERT_EQ(object_subscribed_.count(object_id_not_subscribed), 0);
}

TEST_F(SubscriberTest, TestIgnoreBatchAfterUnsubscription) {
  ///
  /// Make sure long polling is ignored after unsubscription.
  ///

  auto subscription_callback = [this](const rpc::PubMessage &msg) {
    object_subscribed_.emplace(ObjectID::FromBinary(msg.message_id()));
  };
  auto failure_callback = []() {};

  const auto owner_addr = GenerateOwnerAddress();
  const auto object_id = ObjectID::FromRandom();
  subscriber_->Subscribe(channel, owner_addr, object_id.Binary(), subscription_callback,
                         failure_callback);
  ASSERT_TRUE(subscriber_->Unsubscribe(channel, owner_addr, object_id.Binary()));
  std::vector<ObjectID> objects_batched;
  objects_batched.push_back(object_id);
  ASSERT_TRUE(owner_client->ReplyLongPolling(channel, objects_batched));
  // Make sure the batched object won't invoke the callback since it is already
  // unsubscribed before long polling is replied.
  ASSERT_EQ(object_subscribed_.count(object_id), 0);
  // Make sure the long polling is not invoked since there's no more subscribed object to
  // this owner.
  ASSERT_EQ(owner_client->GetNumberOfInFlightLongPollingRequests(), 0);
  ASSERT_TRUE(subscriber_->CheckNoLeaks());
}

TEST_F(SubscriberTest, TestLongPollingFailure) {
  auto subscription_callback = [this](const rpc::PubMessage &msg) {
    object_subscribed_.emplace(ObjectID::FromBinary(msg.message_id()));
  };

  const auto owner_addr = GenerateOwnerAddress();
  const auto object_id = ObjectID::FromRandom();
  auto failure_callback = [this, object_id]() {
    object_failed_to_subscribe_.emplace(object_id);
  };
  subscriber_->Subscribe(channel, owner_addr, object_id.Binary(), subscription_callback,
                         failure_callback);

  // Long polling failed.
  std::vector<ObjectID> objects_batched;
  ASSERT_TRUE(
      owner_client->ReplyLongPolling(channel, objects_batched, Status::NotFound("")));
  // Callback is not invoked.
  ASSERT_EQ(object_subscribed_.count(object_id), 0);
  // Failure callback is invoked.
  ASSERT_EQ(object_failed_to_subscribe_.count(object_id), 1);
  // Since the long polling is failed due to the publisher failure, we shouldn't have any
  // outstanding long polling request.
  ASSERT_EQ(owner_client->GetNumberOfInFlightLongPollingRequests(), 0);
}

TEST_F(SubscriberTest, TestUnsubscribeInSubscriptionCallback) {
  const auto owner_addr = GenerateOwnerAddress();
  const auto object_id = ObjectID::FromRandom();
  // Test unsubscription call inside the subscription callback doesn't break raylet.
  auto subscription_callback = [this, owner_addr](const rpc::PubMessage &msg) {
    const auto object_id = ObjectID::FromBinary(msg.message_id());
    subscriber_->Unsubscribe(channel, owner_addr, object_id.Binary());
    object_subscribed_.emplace(object_id);
  };
  auto failure_callback = []() {
    // This shouldn't be invoked in this test.
    ASSERT_TRUE(false);
  };

  subscriber_->Subscribe(channel, owner_addr, object_id.Binary(), subscription_callback,
                         failure_callback);
  std::vector<ObjectID> objects_batched;
  objects_batched.push_back(object_id);
  ASSERT_TRUE(owner_client->ReplyLongPolling(channel, objects_batched));
  // Since we unsubscribe the object in the subscription callback, there shouldn't be any
  // long polling request in flight.
  ASSERT_EQ(owner_client->GetNumberOfInFlightLongPollingRequests(), 0);
  ASSERT_TRUE(subscriber_->CheckNoLeaks());
}
// TODO(sang): Need to add a network failure test once we support network failure
// properly.

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
