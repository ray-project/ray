// Copyright 2021 The Ray Authors.
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

namespace ray {
namespace pubsub {

class MockSubscriberClientInterface : public SubscriberClientInterface {
 public:
  MOCK_METHOD(void,
              PubsubLongPolling,
              (const rpc::PubsubLongPollingRequest &request,
               const rpc::ClientCallback<rpc::PubsubLongPollingReply> &callback),
              (override));
  MOCK_METHOD(void,
              PubsubCommandBatch,
              (const rpc::PubsubCommandBatchRequest &request,
               const rpc::ClientCallback<rpc::PubsubCommandBatchReply> &callback),
              (override));
};

class MockSubscriber : public SubscriberInterface {
 public:
  MOCK_METHOD(bool,
              Subscribe,
              (std::unique_ptr<rpc::SubMessage> sub_message,
               const rpc::ChannelType channel_type,
               const rpc::Address &owner_address,
               const std::string &key_id,
               pubsub::SubscribeDoneCallback subscribe_done_callback,
               pubsub::SubscriptionItemCallback subscription_callback,
               pubsub::SubscriptionFailureCallback subscription_failure_callback),
              (override));

  MOCK_METHOD(bool,
              SubscribeChannel,
              (std::unique_ptr<rpc::SubMessage> sub_message,
               const rpc::ChannelType channel_type,
               const rpc::Address &owner_address,
               pubsub::SubscribeDoneCallback subscribe_done_callback,
               pubsub::SubscriptionItemCallback subscription_callback,
               pubsub::SubscriptionFailureCallback subscription_failure_callback),
              (override));

  MOCK_METHOD(bool,
              Unsubscribe,
              (const rpc::ChannelType channel_type,
               const rpc::Address &publisher_address,
               const std::string &key_id),
              (override));

  MOCK_METHOD(bool,
              UnsubscribeChannel,
              (const rpc::ChannelType channel_type,
               const rpc::Address &publisher_address),
              (override));

  MOCK_METHOD(bool,
              IsSubscribed,
              (const rpc::ChannelType channel_type,
               const rpc::Address &publisher_address,
               const std::string &key_id),
              (const, override));

  MOCK_METHOD(std::string, DebugString, (), (const, override));
};

}  // namespace pubsub
}  // namespace ray
