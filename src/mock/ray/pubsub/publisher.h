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
namespace pub_internal {

class MockSubscriptionIndex : public SubscriptionIndex {
 public:
};

}  // namespace pub_internal
}  // namespace pubsub
}  // namespace ray

namespace ray {
namespace pubsub {
namespace pub_internal {

class MockLongPollConnection : public LongPollConnection {
 public:
};

}  // namespace pub_internal
}  // namespace pubsub
}  // namespace ray

namespace ray {
namespace pubsub {
namespace pub_internal {

class MockSubscriberState : public SubscriberState {
 public:
};

}  // namespace pub_internal
}  // namespace pubsub
}  // namespace ray

namespace ray {
namespace pubsub {

class MockPublisherInterface : public PublisherInterface {
 public:
  MOCK_METHOD(bool,
              RegisterSubscription,
              (const rpc::ChannelType channel_type,
               const SubscriberID &subscriber_id,
               const std::optional<std::string> &key_id),
              (override));
  MOCK_METHOD(void, Publish, (const rpc::PubMessage &pub_message), (override));
  MOCK_METHOD(void,
              PublishFailure,
              (const rpc::ChannelType channel_type, const std::string &key_id),
              (override));
  MOCK_METHOD(bool,
              UnregisterSubscription,
              (const rpc::ChannelType channel_type,
               const SubscriberID &subscriber_id,
               const std::optional<std::string> &key_id),
              (override));
};

}  // namespace pubsub
}  // namespace ray

namespace ray {
namespace pubsub {

class MockPublisher : public Publisher {
 public:
  MOCK_METHOD(bool,
              RegisterSubscription,
              (const rpc::ChannelType channel_type,
               const SubscriberID &subscriber_id,
               const std::optional<std::string> &key_id),
              (override));
  MOCK_METHOD(void, Publish, (const rpc::PubMessage &pub_message), (override));
  MOCK_METHOD(void,
              PublishFailure,
              (const rpc::ChannelType channel_type, const std::string &key_id),
              (override));
  MOCK_METHOD(bool,
              UnregisterSubscription,
              (const rpc::ChannelType channel_type,
               const SubscriberID &subscriber_id,
               const std::optional<std::string> &key_id),
              (override));

 private:
};

}  // namespace pubsub
}  // namespace ray
