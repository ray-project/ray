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

template <typename KeyIdType>
class MockSubscriptionInfo : public SubscriptionInfo<KeyIdType> {
 public:
};

}  // namespace pubsub
}  // namespace ray

namespace ray {
namespace pubsub {

class MockSubscribeChannelInterface : public SubscribeChannelInterface {
 public:
  MOCK_METHOD(void, Subscribe,
              (const rpc::Address &publisher_address, const std::string &key_id_binary,
               SubscriptionCallback subscription_callback,
               SubscriptionFailureCallback subscription_failure_callback),
              (override));
  MOCK_METHOD(bool, Unsubscribe,
              (const rpc::Address &publisher_address, const std::string &key_id_binary),
              (override));
  MOCK_METHOD(void, HandlePublishedMessage,
              (const rpc::Address &publisher_address, const rpc::PubMessage &pub_message),
              (const, override));
  MOCK_METHOD(void, HandlePublisherFailure, (const rpc::Address &publisher_address),
              (override));
  MOCK_METHOD(void, HandlePublisherFailure,
              (const rpc::Address &publisher_address, const std::string &key_id_binary),
              (override));
  MOCK_METHOD(bool, SubscriptionExists, (const PublisherID &publisher_id), (override));
  MOCK_METHOD(const rpc::ChannelType, GetChannelType, (), (const, override));
  MOCK_METHOD(bool, CheckNoLeaks, (), (const, override));
  MOCK_METHOD(std::string, DebugString, (), (const, override));
};

}  // namespace pubsub
}  // namespace ray

namespace ray {
namespace pubsub {

template <typename KeyIdType>
class MockSubscriberChannel : public SubscriberChannel<KeyIdType> {
 public:
  MOCK_METHOD(void, Subscribe,
              (const rpc::Address &publisher_address, const std::string &key_id,
               SubscriptionCallback subscription_callback,
               SubscriptionFailureCallback subscription_failure_callback),
              (override));
  MOCK_METHOD(bool, Unsubscribe,
              (const rpc::Address &publisher_address, const std::string &key_id),
              (override));
  MOCK_METHOD(bool, CheckNoLeaks, (), (const, override));
  MOCK_METHOD(void, HandlePublishedMessage,
              (const rpc::Address &publisher_address, const rpc::PubMessage &pub_message),
              (const, override));
  MOCK_METHOD(void, HandlePublisherFailure, (const rpc::Address &publisher_address),
              (override));
  MOCK_METHOD(void, HandlePublisherFailure,
              (const rpc::Address &publisher_address, const std::string &key_id_binary),
              (override));
  MOCK_METHOD(bool, SubscriptionExists, (const PublisherID &publisher_id), (override));
  MOCK_METHOD(const rpc::ChannelType, GetChannelType, (), (const, override));
  MOCK_METHOD(std::string, DebugString, (), (const, override));
};

}  // namespace pubsub
}  // namespace ray

namespace ray {
namespace pubsub {

class MockWaitForObjectEvictionChannel : public WaitForObjectEvictionChannel {
 public:
};

}  // namespace pubsub
}  // namespace ray

namespace ray {
namespace pubsub {

class MockWaitForRefRemovedChannel : public WaitForRefRemovedChannel {
 public:
};

}  // namespace pubsub
}  // namespace ray

namespace ray {
namespace pubsub {

class MockObjectLocationsChannel : public ObjectLocationsChannel {
 public:
};

}  // namespace pubsub
}  // namespace ray

namespace ray {
namespace pubsub {

class MockSubscriberInterface : public SubscriberInterface {
 public:
  MOCK_METHOD(void, Subscribe,
              (std::unique_ptr<rpc::SubMessage> sub_message,
               const rpc::ChannelType channel_type, const rpc::Address &publisher_address,
               const std::string &key_id_binary,
               SubscriptionCallback subscription_callback,
               SubscriptionFailureCallback subscription_failure_callback),
              (override));
  MOCK_METHOD(bool, Unsubscribe,
              (const rpc::ChannelType channel_type, const rpc::Address &publisher_address,
               const std::string &key_id_binary),
              (override));
  MOCK_METHOD(std::string, DebugString, (), (const, override));
};

}  // namespace pubsub
}  // namespace ray

namespace ray {
namespace pubsub {

class MockSubscriberClientInterface : public SubscriberClientInterface {
 public:
  MOCK_METHOD(void, PubsubLongPolling,
              (const rpc::PubsubLongPollingRequest &request,
               const rpc::ClientCallback<rpc::PubsubLongPollingReply> &callback),
              (override));
  MOCK_METHOD(void, PubsubCommandBatch,
              (const rpc::PubsubCommandBatchRequest &request,
               const rpc::ClientCallback<rpc::PubsubCommandBatchReply> &callback),
              (override));
};

}  // namespace pubsub
}  // namespace ray
