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
#include "ray/common/asio/instrumented_io_context.h"

namespace ray {

#define EMPTY_FAILURE_CALLBACK [](const std::string &, const Status &) {}

class MockWorkerClient : public pubsub::SubscriberClientInterface {
 public:
  void PubsubLongPolling(
      const rpc::PubsubLongPollingRequest &request,
      const rpc::ClientCallback<rpc::PubsubLongPollingReply> &callback) override {
    long_polling_callbacks.push_back(callback);
  }

  void PubsubCommandBatch(
      const rpc::PubsubCommandBatchRequest &request,
      const rpc::ClientCallback<rpc::PubsubCommandBatchReply> &callback) override {
    requests_.push(request);
    command_batch_callbacks.push_back(callback);
  }

  std::shared_ptr<rpc::PubsubCommandBatchRequest> ReplyCommandBatch(
      Status status = Status::OK()) {
    RAY_CHECK(command_batch_callbacks.size() == requests_.size());
    if (command_batch_callbacks.empty()) {
      return nullptr;
    }
    auto callback = command_batch_callbacks.front();
    auto reply = rpc::PubsubCommandBatchReply();
    callback(status, reply);
    command_batch_callbacks.pop_front();
    auto r = std::make_shared<rpc::PubsubCommandBatchRequest>(requests_.front());
    requests_.pop();
    return r;
  }

  bool ReplyLongPolling(rpc::ChannelType channel_type,
                        std::vector<ObjectID> &object_ids,
                        Status status = Status::OK()) {
    if (long_polling_callbacks.empty()) {
      return false;
    }
    auto callback = long_polling_callbacks.front();
    auto reply = rpc::PubsubLongPollingReply();

    for (const auto &object_id : object_ids) {
      auto *new_pub_message = reply.add_pub_messages();
      new_pub_message->set_key_id(object_id.Binary());
      new_pub_message->set_channel_type(channel_type);
    }
    callback(status, reply);
    long_polling_callbacks.pop_front();
    return true;
  }

  bool FailureMessagePublished(rpc::ChannelType channel_type,
                               std::vector<ObjectID> &object_ids) {
    if (long_polling_callbacks.empty()) {
      return false;
    }

    auto callback = long_polling_callbacks.front();
    auto reply = rpc::PubsubLongPollingReply();

    for (const auto &object_id : object_ids) {
      auto new_pub_message = reply.add_pub_messages();
      new_pub_message->set_key_id(object_id.Binary());
      new_pub_message->set_channel_type(channel_type);
      new_pub_message->mutable_failure_message();
    }
    callback(Status::OK(), reply);
    long_polling_callbacks.pop_front();
    return true;
  }

  int GetNumberOfInFlightLongPollingRequests() { return long_polling_callbacks.size(); }

  ~MockWorkerClient(){};

  std::deque<rpc::ClientCallback<rpc::PubsubLongPollingReply>> long_polling_callbacks;
  std::deque<rpc::ClientCallback<rpc::PubsubCommandBatchReply>> command_batch_callbacks;
  std::queue<rpc::PubsubCommandBatchRequest> requests_;
};

namespace pubsub {

class SubscriberTest : public ::testing::Test {
 public:
  SubscriberTest()
      : self_node_id_(NodeID::FromRandom()),
        owner_client(std::make_shared<MockWorkerClient>()),
        client_pool([&](const rpc::Address &addr) {
          std::shared_ptr<SubscriberClientInterface> t = owner_client;
          return owner_client;
        }),
        channel(rpc::ChannelType::WORKER_OBJECT_EVICTION) {}
  ~SubscriberTest() {}

  void SetUp() {
    object_subscribed_.clear();
    object_failed_to_subscribe_.clear();
    subscriber_ = std::make_shared<Subscriber>(
        self_node_id_,
        /*channels=*/
        std::vector<rpc::ChannelType>{rpc::ChannelType::WORKER_OBJECT_EVICTION,
                                      rpc::ChannelType::WORKER_REF_REMOVED_CHANNEL,
                                      rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL},
        /*max_command_batch_size*/ 3,
        client_pool,
        &callback_service_);
  }

  const rpc::Address GenerateOwnerAddress(
      const std::string node_id = NodeID::FromRandom().Binary(),
      const std::string worker_id = WorkerID::FromRandom().Binary(),
      const std::string address = "abc",
      const int port = 1234) {
    rpc::Address addr;
    addr.set_raylet_id(node_id);
    addr.set_ip_address(address);
    addr.set_port(port);
    addr.set_worker_id(worker_id);
    return addr;
  }

  std::unique_ptr<rpc::SubMessage> GenerateSubMessage(const ObjectID &object_id) {
    auto sub_message = std::make_unique<rpc::SubMessage>();
    auto *object_eviction_msg = sub_message->mutable_worker_object_eviction_message();
    object_eviction_msg->set_object_id(object_id.Binary());
    return sub_message;
  }

  bool ReplyLongPolling(rpc::ChannelType channel_type,
                        std::vector<ObjectID> &object_ids,
                        Status status = Status::OK()) {
    auto success = owner_client->ReplyLongPolling(channel_type, object_ids, status);
    // Need to call this to invoke callback when the reply comes.
    // The io service basically executes the queued handler in a blocking manner, and
    // reset should be called in order to run the poll_one again.
    callback_service_.poll();
    callback_service_.reset();
    return success;
  }

  bool FailureMessagePublished(rpc::ChannelType channel_type,
                               std::vector<ObjectID> &object_ids) {
    auto published = owner_client->FailureMessagePublished(channel_type, object_ids);
    // reset should be called in order to run the poll_one again.
    callback_service_.poll();
    callback_service_.reset();
    return published;
  }

  void TearDown() {}

  instrumented_io_context callback_service_;
  const NodeID self_node_id_;
  std::shared_ptr<MockWorkerClient> owner_client;
  std::function<std::shared_ptr<SubscriberClientInterface>(const rpc::Address &)>
      client_pool;
  std::shared_ptr<Subscriber> subscriber_;
  std::unordered_set<ObjectID> object_subscribed_;
  std::unordered_set<ObjectID> object_failed_to_subscribe_;
  rpc::ChannelType channel;
};

TEST_F(SubscriberTest, TestBasicSubscription) {
  auto subscription_callback = [this](const rpc::PubMessage &msg) {
    object_subscribed_.emplace(ObjectID::FromBinary(msg.key_id()));
  };
  auto failure_callback = EMPTY_FAILURE_CALLBACK;

  const auto owner_addr = GenerateOwnerAddress();
  const auto object_id = ObjectID::FromRandom();
  ASSERT_FALSE(subscriber_->Unsubscribe(channel, owner_addr, object_id.Binary()));
  ASSERT_TRUE(owner_client->ReplyCommandBatch());
  subscriber_->Subscribe(GenerateSubMessage(object_id),
                         channel,
                         owner_addr,
                         object_id.Binary(),
                         /*subscribe_done_callback=*/nullptr,
                         subscription_callback,
                         failure_callback);
  ASSERT_TRUE(owner_client->ReplyCommandBatch());
  ASSERT_TRUE(subscriber_->IsSubscribed(channel, owner_addr, object_id.Binary()));

  std::vector<ObjectID> objects_batched;
  objects_batched.push_back(object_id);
  ASSERT_TRUE(ReplyLongPolling(channel, objects_batched));
  ASSERT_TRUE(subscriber_->Unsubscribe(channel, owner_addr, object_id.Binary()));
  ASSERT_TRUE(owner_client->ReplyCommandBatch());
  ASSERT_FALSE(subscriber_->IsSubscribed(channel, owner_addr, object_id.Binary()));

  // Make sure the long polling batch works as expected.
  for (const auto &object_id : objects_batched) {
    ASSERT_TRUE(object_subscribed_.count(object_id) > 0);
  }

  // Here, once the long polling request is replied, the metadata is cleaned up.
  ASSERT_TRUE(ReplyLongPolling(channel, objects_batched));
  ASSERT_TRUE(subscriber_->CheckNoLeaks());
}

TEST_F(SubscriberTest, TestSingleLongPollingWithMultipleSubscriptions) {
  ///
  /// Make sure long polling is called only once with multiple subscription.
  ///

  auto subscription_callback = [this](const rpc::PubMessage &msg) {
    object_subscribed_.emplace(ObjectID::FromBinary(msg.key_id()));
  };
  auto failure_callback = EMPTY_FAILURE_CALLBACK;

  const auto owner_addr = GenerateOwnerAddress();
  std::vector<ObjectID> object_ids;
  std::vector<ObjectID> objects_batched;
  for (int i = 0; i < 5; i++) {
    const auto object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    subscriber_->Subscribe(GenerateSubMessage(object_id),
                           channel,
                           owner_addr,
                           object_id.Binary(),
                           /*subscribe_done_callback=*/nullptr,
                           subscription_callback,
                           failure_callback);
    ASSERT_TRUE(owner_client->ReplyCommandBatch());
    ASSERT_TRUE(subscriber_->IsSubscribed(channel, owner_addr, object_id.Binary()));
    objects_batched.push_back(object_id);
  }

  ASSERT_EQ(owner_client->GetNumberOfInFlightLongPollingRequests(), 1);
  ASSERT_TRUE(ReplyLongPolling(channel, objects_batched));
  ASSERT_EQ(owner_client->GetNumberOfInFlightLongPollingRequests(), 1);

  // Make sure the long polling batch works as expected.
  for (const auto &object_id : objects_batched) {
    // RAY_LOG(ERROR) << "haha " << object_subscribed_.count(object_id);
    ASSERT_TRUE(object_subscribed_.count(object_id) > 0);
  }
}

TEST_F(SubscriberTest, TestMultiLongPollingWithTheSameSubscription) {
  ///
  /// Make sure long polling will keep working as long as subscription is not removed.
  ///

  auto subscription_callback = [this](const rpc::PubMessage &msg) {
    object_subscribed_.emplace(ObjectID::FromBinary(msg.key_id()));
  };
  auto failure_callback = EMPTY_FAILURE_CALLBACK;

  const auto owner_addr = GenerateOwnerAddress();
  const auto object_id = ObjectID::FromRandom();
  subscriber_->Subscribe(GenerateSubMessage(object_id),
                         channel,
                         owner_addr,
                         object_id.Binary(),
                         /*subscribe_done_callback=*/nullptr,
                         subscription_callback,
                         failure_callback);
  ASSERT_TRUE(owner_client->ReplyCommandBatch());
  ASSERT_EQ(owner_client->GetNumberOfInFlightLongPollingRequests(), 1);
  ASSERT_TRUE(subscriber_->IsSubscribed(channel, owner_addr, object_id.Binary()));

  // The object information is published.
  std::vector<ObjectID> objects_batched;
  objects_batched.push_back(object_id);
  ASSERT_TRUE(ReplyLongPolling(channel, objects_batched));
  ASSERT_TRUE(object_subscribed_.count(object_id) > 0);
  objects_batched.clear();
  object_subscribed_.clear();

  // New long polling should be made because the subscription is still alive.
  ASSERT_EQ(owner_client->GetNumberOfInFlightLongPollingRequests(), 1);
  objects_batched.push_back(object_id);
  ASSERT_TRUE(ReplyLongPolling(channel, objects_batched));
  ASSERT_TRUE(object_subscribed_.count(object_id) > 0);
}

TEST_F(SubscriberTest, TestCallbackNotInvokedForNonSubscribedObject) {
  ///
  /// Make sure the non-subscribed object's subscription callback is not called.
  ///

  auto subscription_callback = [this](const rpc::PubMessage &msg) {
    object_subscribed_.emplace(ObjectID::FromBinary(msg.key_id()));
  };
  auto failure_callback = EMPTY_FAILURE_CALLBACK;

  const auto owner_addr = GenerateOwnerAddress();
  const auto object_id = ObjectID::FromRandom();
  const auto object_id_not_subscribed = ObjectID::FromRandom();
  subscriber_->Subscribe(GenerateSubMessage(object_id),
                         channel,
                         owner_addr,
                         object_id.Binary(),
                         /*subscribe_done_callback=*/nullptr,
                         subscription_callback,
                         failure_callback);
  ASSERT_TRUE(owner_client->ReplyCommandBatch());

  // The object information is published.
  std::vector<ObjectID> objects_batched;
  objects_batched.push_back(object_id_not_subscribed);
  ASSERT_TRUE(ReplyLongPolling(channel, objects_batched));
  ASSERT_EQ(object_subscribed_.count(object_id), 0);
}

TEST_F(SubscriberTest, TestSubscribeChannelEntities) {
  ///
  /// Make sure SubscribeChannel() can receive all entities from a channel.
  ///

  auto subscription_callback = [this](const rpc::PubMessage &msg) {
    object_subscribed_.emplace(ObjectID::FromBinary(msg.key_id()));
  };
  auto failure_callback = EMPTY_FAILURE_CALLBACK;

  const auto owner_addr = GenerateOwnerAddress();
  subscriber_->SubscribeChannel(std::make_unique<rpc::SubMessage>(),
                                channel,
                                owner_addr,
                                /*subscribe_done_callback=*/nullptr,
                                subscription_callback,
                                failure_callback);
  ASSERT_TRUE(owner_client->ReplyCommandBatch());
  ASSERT_EQ(owner_client->GetNumberOfInFlightLongPollingRequests(), 1);

  // The object information is published.
  std::vector<ObjectID> objects_batched;
  for (int i = 0; i < 5; ++i) {
    objects_batched.push_back(ObjectID::FromRandom());
  }
  ASSERT_TRUE(ReplyLongPolling(channel, objects_batched));
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(object_subscribed_.count(objects_batched[i]), 1);
  }
  objects_batched.clear();
  object_subscribed_.clear();

  // New long polling should be made because the subscription is still alive.
  ASSERT_EQ(owner_client->GetNumberOfInFlightLongPollingRequests(), 1);

  // The object information is published.
  for (int i = 0; i < 10; ++i) {
    objects_batched.push_back(ObjectID::FromRandom());
  }
  ASSERT_TRUE(ReplyLongPolling(channel, objects_batched));
  for (int i = 0; i < 10; ++i) {
    ASSERT_EQ(object_subscribed_.count(objects_batched[i]), 1);
  }

  // Unsubscribe from the channel.
  ASSERT_TRUE(subscriber_->UnsubscribeChannel(channel, owner_addr));
}

TEST_F(SubscriberTest, TestIgnoreBatchAfterUnsubscription) {
  ///
  /// Make sure long polling is ignored after unsubscription.
  ///

  auto subscription_callback = [this](const rpc::PubMessage &msg) {
    object_subscribed_.emplace(ObjectID::FromBinary(msg.key_id()));
  };
  auto failure_callback = EMPTY_FAILURE_CALLBACK;

  const auto owner_addr = GenerateOwnerAddress();
  const auto object_id = ObjectID::FromRandom();
  subscriber_->Subscribe(GenerateSubMessage(object_id),
                         channel,
                         owner_addr,
                         object_id.Binary(),
                         /*subscribe_done_callback=*/nullptr,
                         subscription_callback,
                         failure_callback);
  ASSERT_TRUE(owner_client->ReplyCommandBatch());
  ASSERT_TRUE(subscriber_->Unsubscribe(channel, owner_addr, object_id.Binary()));
  ASSERT_TRUE(owner_client->ReplyCommandBatch());
  std::vector<ObjectID> objects_batched;
  objects_batched.push_back(object_id);
  ASSERT_TRUE(ReplyLongPolling(channel, objects_batched));
  // Make sure the batched object won't invoke the callback since it is already
  // unsubscribed before long polling is replied.
  ASSERT_EQ(object_subscribed_.count(object_id), 0);
  // Make sure the long polling is not invoked since there's no more subscribed object to
  // this owner.
  ASSERT_EQ(owner_client->GetNumberOfInFlightLongPollingRequests(), 0);
  ASSERT_TRUE(subscriber_->CheckNoLeaks());
}

TEST_F(SubscriberTest, TestIgnoreBatchAfterUnsubscribeFromAll) {
  ///
  /// Make sure long polling is ignored after unsubscription from channel.
  ///

  auto subscription_callback = [this](const rpc::PubMessage &msg) {
    object_subscribed_.emplace(ObjectID::FromBinary(msg.key_id()));
  };
  auto failure_callback = EMPTY_FAILURE_CALLBACK;

  const auto owner_addr = GenerateOwnerAddress();
  subscriber_->SubscribeChannel(std::make_unique<rpc::SubMessage>(),
                                channel,
                                owner_addr,
                                /*subscribe_done_callback=*/nullptr,
                                subscription_callback,
                                failure_callback);
  ASSERT_TRUE(owner_client->ReplyCommandBatch());
  ASSERT_TRUE(subscriber_->UnsubscribeChannel(channel, owner_addr));
  ASSERT_TRUE(owner_client->ReplyCommandBatch());

  const auto object_id = ObjectID::FromRandom();
  std::vector<ObjectID> objects_batched;
  objects_batched.push_back(object_id);
  ASSERT_TRUE(ReplyLongPolling(channel, objects_batched));
  // Make sure the returned object won't invoke the callback since the channel is already
  // unsubscribed before long polling is replied.
  ASSERT_EQ(object_subscribed_.count(object_id), 0);
  // After the previous reply, no new long polling is invoked since the channel has been
  // unsubscribed.
  ASSERT_EQ(owner_client->GetNumberOfInFlightLongPollingRequests(), 0);
  ASSERT_TRUE(subscriber_->CheckNoLeaks());
}

TEST_F(SubscriberTest, TestLongPollingFailure) {
  auto subscription_callback = [this](const rpc::PubMessage &msg) {
    object_subscribed_.emplace(ObjectID::FromBinary(msg.key_id()));
  };

  const auto owner_addr = GenerateOwnerAddress();
  const auto object_id = ObjectID::FromRandom();
  auto failure_callback = [this, object_id](const std::string &key_id, const Status &) {
    object_failed_to_subscribe_.emplace(object_id);
  };
  subscriber_->Subscribe(GenerateSubMessage(object_id),
                         channel,
                         owner_addr,
                         object_id.Binary(),
                         /*subscribe_done_callback=*/nullptr,
                         subscription_callback,
                         failure_callback);
  ASSERT_TRUE(owner_client->ReplyCommandBatch());

  // Long polling failed.
  std::vector<ObjectID> objects_batched;
  ASSERT_TRUE(ReplyLongPolling(channel, objects_batched, Status::NotFound("")));
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
    const auto object_id = ObjectID::FromBinary(msg.key_id());
    subscriber_->Unsubscribe(channel, owner_addr, object_id.Binary());
    ASSERT_TRUE(owner_client->ReplyCommandBatch());
    object_subscribed_.emplace(object_id);
  };
  auto failure_callback = [](const std::string &key_id, const Status &) {
    // This shouldn't be invoked in this test.
    ASSERT_TRUE(false);
  };

  subscriber_->Subscribe(GenerateSubMessage(object_id),
                         channel,
                         owner_addr,
                         object_id.Binary(),
                         /*subscribe_done_callback=*/nullptr,
                         subscription_callback,
                         failure_callback);
  ASSERT_TRUE(owner_client->ReplyCommandBatch());

  std::vector<ObjectID> objects_batched;
  objects_batched.push_back(object_id);
  ASSERT_TRUE(ReplyLongPolling(channel, objects_batched));

  // Since we unsubscribe the object in the subscription callback, there shouldn't be any
  // long polling request in flight.
  // NOTE(sang): Since the callback is called asynchronously, the long polling request is
  // sent "one more time" before unsubscribe API is called. To ensure it is unsubscribed,
  // one more long polling request should be replied.
  ASSERT_TRUE(ReplyLongPolling(channel, objects_batched));
  ASSERT_EQ(owner_client->GetNumberOfInFlightLongPollingRequests(), 0);
  ASSERT_TRUE(subscriber_->CheckNoLeaks());
}

TEST_F(SubscriberTest, TestSubUnsubCommandBatchSingleEntry) {
  ///
  /// Verify the command batch works as expected when there's a single publisher.
  ///
  auto subscription_callback = [](const rpc::PubMessage &msg) {};
  auto failure_callback = EMPTY_FAILURE_CALLBACK;

  const auto owner_addr = GenerateOwnerAddress();
  const auto object_id = ObjectID::FromRandom();
  subscriber_->Subscribe(GenerateSubMessage(object_id),
                         channel,
                         owner_addr,
                         object_id.Binary(),
                         /*subscribe_done_callback=*/nullptr,
                         subscription_callback,
                         failure_callback);
  auto r = owner_client->ReplyCommandBatch();
  auto commands = r->commands();

  // Only a single command was batched.
  ASSERT_EQ(commands.size(), 1);
  for (const auto &command : commands) {
    ASSERT_EQ(command.channel_type(), channel);
    ASSERT_EQ(command.key_id(), object_id.Binary());
    ASSERT_EQ(command.subscribe_message().worker_object_eviction_message().object_id(),
              object_id.Binary());
  }
  // No more request.
  ASSERT_EQ(owner_client->ReplyCommandBatch(), nullptr);

  std::vector<ObjectID> objects_batched;
  ASSERT_TRUE(ReplyLongPolling(channel, objects_batched));
  // Regardless of the long polling request, there's no more command batch.
  // Note that the long polling request here is independent from the
  // command batch request.
  ASSERT_EQ(owner_client->ReplyCommandBatch(), nullptr);
  ASSERT_FALSE(owner_client->ReplyCommandBatch());
}

TEST_F(SubscriberTest, TestSubUnsubCommandBatchMultiEntries) {
  ///
  /// Verify the command batch works when there are multiple entries in the FIFO order.
  ///
  auto subscription_callback = [](const rpc::PubMessage &msg) {};
  auto failure_callback = EMPTY_FAILURE_CALLBACK;

  const auto owner_addr = GenerateOwnerAddress();
  const auto object_id = ObjectID::FromRandom();
  const auto object_id_2 = ObjectID::FromRandom();
  // The first batch is always processed right away.
  subscriber_->Subscribe(GenerateSubMessage(object_id),
                         channel,
                         owner_addr,
                         object_id.Binary(),
                         /*subscribe_done_callback=*/nullptr,
                         subscription_callback,
                         failure_callback);

  // Test multiple entries in the batch before new reply is coming.
  subscriber_->Unsubscribe(channel, owner_addr, object_id.Binary());
  subscriber_->Subscribe(GenerateSubMessage(object_id),
                         channel,
                         owner_addr,
                         object_id.Binary(),
                         /*subscribe_done_callback=*/nullptr,
                         subscription_callback,
                         failure_callback);
  subscriber_->Subscribe(GenerateSubMessage(object_id_2),
                         channel,
                         owner_addr,
                         object_id_2.Binary(),
                         /*subscribe_done_callback=*/nullptr,
                         subscription_callback,
                         failure_callback);

  // The long polling request is replied. New batch will be sent.
  std::vector<ObjectID> objects_batched;
  ASSERT_TRUE(ReplyLongPolling(channel, objects_batched));

  // The first batch is always processed right away.
  auto first_batch = owner_client->ReplyCommandBatch();
  auto first_batch_command = first_batch->commands();
  ASSERT_EQ(first_batch_command.size(), 1);

  auto r = owner_client->ReplyCommandBatch();
  auto commands = r->commands();

  // There are total 3 commands batched.
  ASSERT_EQ(commands.size(), 3);

  // First entry unsubscribe.
  ASSERT_EQ(commands[0].channel_type(), channel);
  ASSERT_EQ(commands[0].key_id(), object_id.Binary());
  ASSERT_TRUE(commands[0].has_unsubscribe_message());

  // Second entry subscribe object 1.
  ASSERT_EQ(commands[1].channel_type(), channel);
  ASSERT_EQ(commands[1].key_id(), object_id.Binary());
  ASSERT_TRUE(commands[1].has_subscribe_message());
  ASSERT_EQ(commands[1].subscribe_message().worker_object_eviction_message().object_id(),
            object_id.Binary());

  // Third entry subscribe object 2.
  ASSERT_EQ(commands[2].channel_type(), channel);
  ASSERT_EQ(commands[2].key_id(), object_id_2.Binary());
  ASSERT_TRUE(commands[2].has_subscribe_message());
  ASSERT_EQ(commands[2].subscribe_message().worker_object_eviction_message().object_id(),
            object_id_2.Binary());

  // No more request after that.
  ASSERT_EQ(owner_client->ReplyCommandBatch(), nullptr);
}

TEST_F(SubscriberTest, TestSubUnsubCommandBatchMultiBatch) {
  ///
  /// Verify when there are multi batches, they are sent properly.
  ///
  auto subscription_callback = [](const rpc::PubMessage &msg) {};
  auto failure_callback = EMPTY_FAILURE_CALLBACK;

  const auto owner_addr = GenerateOwnerAddress();
  const auto object_id = ObjectID::FromRandom();
  const auto object_id_2 = ObjectID::FromRandom();
  // The first batch is always processed right away.
  subscriber_->Unsubscribe(channel, owner_addr, object_id.Binary());

  // The first 3 will be in the first batch.
  subscriber_->Unsubscribe(channel, owner_addr, object_id.Binary());
  subscriber_->Subscribe(GenerateSubMessage(object_id),
                         channel,
                         owner_addr,
                         object_id.Binary(),
                         /*subscribe_done_callback=*/nullptr,
                         subscription_callback,
                         failure_callback);
  subscriber_->Unsubscribe(channel, owner_addr, object_id.Binary());
  // Note that this request will be batched in the second batch.
  subscriber_->Subscribe(GenerateSubMessage(object_id_2),
                         channel,
                         owner_addr,
                         object_id_2.Binary(),
                         /*subscribe_done_callback=*/nullptr,
                         subscription_callback,
                         failure_callback);

  // The long polling request is replied.
  std::vector<ObjectID> objects_batched;
  ASSERT_TRUE(ReplyLongPolling(channel, objects_batched));

  // The first batch is always processed right away.
  ASSERT_EQ(owner_client->ReplyCommandBatch()->commands().size(), 1);

  // There are total 3 commands batched. The last batch will be sent.
  auto r = owner_client->ReplyCommandBatch();
  auto commands = r->commands();
  ASSERT_EQ(commands.size(), 3);

  // Long polling is replied again.
  ASSERT_TRUE(ReplyLongPolling(channel, objects_batched));
  r = owner_client->ReplyCommandBatch();
  commands = r->commands();
  ASSERT_EQ(commands.size(), 1);

  ASSERT_EQ(commands[0].channel_type(), channel);
  ASSERT_EQ(commands[0].key_id(), object_id_2.Binary());
  ASSERT_TRUE(commands[0].has_subscribe_message());
  ASSERT_EQ(commands[0].subscribe_message().worker_object_eviction_message().object_id(),
            object_id_2.Binary());

  // No more request after that.
  ASSERT_FALSE(owner_client->ReplyCommandBatch());
}

TEST_F(SubscriberTest, TestOnlyOneInFlightCommandBatch) {
  ///
  /// In order to guarantee the command ordering, we only allow to have
  /// 1 in-flight command batch RPC at a time.
  ///
  auto subscription_callback = [](const rpc::PubMessage &msg) {};
  auto failure_callback = EMPTY_FAILURE_CALLBACK;

  const auto owner_addr = GenerateOwnerAddress();
  const auto object_id = ObjectID::FromRandom();
  // The first batch is sent right away. There should be no more in flight request until
  // is is replied.
  subscriber_->Subscribe(GenerateSubMessage(object_id),
                         channel,
                         owner_addr,
                         object_id.Binary(),
                         /*subscribe_done_callback=*/nullptr,
                         subscription_callback,
                         failure_callback);

  // These two subscribe requests are sent in the next batch.
  for (int i = 0; i < 2; i++) {
    const auto object_id = ObjectID::FromRandom();
    subscriber_->Subscribe(GenerateSubMessage(object_id),
                           channel,
                           owner_addr,
                           object_id.Binary(),
                           /*subscribe_done_callback=*/nullptr,
                           subscription_callback,
                           failure_callback);
  }

  // The first batch is replied. The second batch should be sent.
  auto r = owner_client->ReplyCommandBatch();
  auto commands = r->commands();
  // Only a single command was batched.
  ASSERT_EQ(commands.size(), 1);

  // The second batch is replied.
  r = owner_client->ReplyCommandBatch();
  commands = r->commands();
  ASSERT_EQ(commands.size(), 2);
  // No more request.
  ASSERT_FALSE(owner_client->ReplyCommandBatch());
}

TEST_F(SubscriberTest, TestCommandsCleanedUponPublishFailure) {
  ///
  /// Commands are queued per publisher until they are sent from the command batch RPC.
  /// If the publisher is failed, the queue should be cleaned up.
  ///
  auto subscription_callback = [](const rpc::PubMessage &msg) {};
  auto failure_callback = EMPTY_FAILURE_CALLBACK;

  const auto owner_addr = GenerateOwnerAddress();
  const auto object_id = ObjectID::FromRandom();
  subscriber_->Subscribe(GenerateSubMessage(object_id),
                         channel,
                         owner_addr,
                         object_id.Binary(),
                         /*subscribe_done_callback=*/nullptr,
                         subscription_callback,
                         failure_callback);

  // These two subscribe requests are sent to the next batch.
  for (int i = 0; i < 2; i++) {
    const auto object_id = ObjectID::FromRandom();
    subscriber_->Subscribe(GenerateSubMessage(object_id),
                           channel,
                           owner_addr,
                           object_id.Binary(),
                           /*subscribe_done_callback=*/nullptr,
                           subscription_callback,
                           failure_callback);
  }

  std::vector<ObjectID> objects_batched;
  // The publisher failed. In this case, the queue should be cleaned up.
  ASSERT_TRUE(ReplyLongPolling(channel, objects_batched, Status::Invalid("")));
  // The reply from the first batch.
  ASSERT_TRUE(owner_client->ReplyCommandBatch());
  // We shouldn't have the second batch request because the publisher is already dead and
  // the queue is cleaned up.
  ASSERT_FALSE(owner_client->ReplyCommandBatch());
  // Make sure entries are cleaned up.
  ASSERT_TRUE(subscriber_->CheckNoLeaks());
}

TEST_F(SubscriberTest, TestFailureMessagePublished) {
  ///
  /// The publisher can publish the failure message to indicate that
  /// the key id is not available. This test ensures that the failure callback
  /// is properly called in this scenario.
  ///
  auto subscription_callback = [this](const rpc::PubMessage &msg) {
    object_subscribed_.emplace(ObjectID::FromBinary(msg.key_id()));
  };

  const auto owner_addr = GenerateOwnerAddress();
  const auto object_id = ObjectID::FromRandom();
  const auto object_id2 = ObjectID::FromRandom();
  auto failure_callback = [this](const std::string &key_id, const Status &) {
    const auto id = ObjectID::FromBinary(key_id);
    object_failed_to_subscribe_.emplace(id);
  };
  subscriber_->Subscribe(GenerateSubMessage(object_id),
                         channel,
                         owner_addr,
                         object_id.Binary(),
                         /*subscribe_done_callback=*/nullptr,
                         subscription_callback,
                         failure_callback);
  subscriber_->Subscribe(GenerateSubMessage(object_id),
                         channel,
                         owner_addr,
                         object_id2.Binary(),
                         /*subscribe_done_callback=*/nullptr,
                         subscription_callback,
                         failure_callback);
  ASSERT_TRUE(owner_client->ReplyCommandBatch());

  // Failure message is published.
  std::vector<ObjectID> objects_batched;
  objects_batched.push_back(object_id);
  ASSERT_TRUE(FailureMessagePublished(channel, objects_batched));
  // Callback is not invoked.
  ASSERT_EQ(object_subscribed_.count(object_id), 0);
  // Failure callback is invoked.
  ASSERT_EQ(object_failed_to_subscribe_.count(object_id), 1);
  // Since object2 is still subscribed, we should have the long polling requests.
  ASSERT_EQ(owner_client->GetNumberOfInFlightLongPollingRequests(), 1);

  // Test the second failure is published, and there's no more long polling.
  objects_batched.clear();
  objects_batched.push_back(object_id2);
  ASSERT_TRUE(FailureMessagePublished(channel, objects_batched));
  ASSERT_EQ(object_subscribed_.count(object_id2), 0);
  ASSERT_EQ(object_failed_to_subscribe_.count(object_id2), 1);
  ASSERT_EQ(owner_client->GetNumberOfInFlightLongPollingRequests(), 0);
}

TEST_F(SubscriberTest, TestIsSubscribed) {
  auto subscription_callback = [this](const rpc::PubMessage &msg) {
    object_subscribed_.emplace(ObjectID::FromBinary(msg.key_id()));
  };
  auto failure_callback = EMPTY_FAILURE_CALLBACK;
  const auto owner_addr = GenerateOwnerAddress();
  const auto object_id = ObjectID::FromRandom();

  ASSERT_FALSE(subscriber_->Unsubscribe(channel, owner_addr, object_id.Binary()));
  ASSERT_FALSE(subscriber_->IsSubscribed(channel, owner_addr, object_id.Binary()));

  subscriber_->Subscribe(GenerateSubMessage(object_id),
                         channel,
                         owner_addr,
                         object_id.Binary(),
                         /*subscribe_done_callback=*/nullptr,
                         subscription_callback,
                         failure_callback);
  ASSERT_TRUE(subscriber_->IsSubscribed(channel, owner_addr, object_id.Binary()));

  ASSERT_TRUE(subscriber_->Unsubscribe(channel, owner_addr, object_id.Binary()));
  ASSERT_FALSE(subscriber_->IsSubscribed(channel, owner_addr, object_id.Binary()));
}

// TODO(sang): Need to add a network failure test once we support network failure
// properly.

}  // namespace pubsub

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
