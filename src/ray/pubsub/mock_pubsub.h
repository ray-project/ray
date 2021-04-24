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
#include "ray/pubsub/subscriber.h"

namespace ray {

namespace mock_pubsub {

/// NOTE: When you use these classes, please include #include "gmock/gmock.h" in the test
/// file.

class MockSubscriber : public pubsub::SubscriberInterface {
 public:
  MOCK_METHOD5(Subscribe,
               void(const rpc::ChannelType channel_type,
                    const rpc::Address &owner_address,
                    const std::string &message_id_binary,
                    pubsub::SubscriptionCallback subscription_callback,
                    pubsub::SubscriptionFailureCallback subscription_failure_callback));

  MOCK_METHOD3(Unsubscribe, bool(const rpc::ChannelType channel_type,
                                 const rpc::Address &publisher_address,
                                 const std::string &message_id_binary));

  bool CheckNoLeaks() const override { return true; };
};

class MockPublisher : public pubsub::PublisherInterface {
 public:
  MOCK_METHOD3(RegisterSubscription, void(const rpc::ChannelType channel_type,
                                          const pubsub::SubscriberID &subscriber_id,
                                          const std::string &message_id_binary));

  MOCK_METHOD3(Publish, void(const rpc::ChannelType channel_type,
                             std::unique_ptr<rpc::PubMessage> pub_message,
                             const std::string &message_id_binary));

  MOCK_METHOD3(UnregisterSubscription, bool(const rpc::ChannelType channel_type,
                                            const pubsub::SubscriberID &subscriber_id,
                                            const std::string &message_id_binary));
};

}  // namespace mock_pubsub

}  // namespace ray
