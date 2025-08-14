// Copyright 2025 The Ray Authors.
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

#pragma once

#include "ray/pubsub/subscriber.h"

namespace ray {
namespace pubsub {

class FakeSubscriberClient : public SubscriberClientInterface {
 public:
  void PubsubLongPolling(
      const rpc::PubsubLongPollingRequest &request,
      const rpc::ClientCallback<rpc::PubsubLongPollingReply> &callback) override {}

  void PubsubCommandBatch(
      const rpc::PubsubCommandBatchRequest &request,
      const rpc::ClientCallback<rpc::PubsubCommandBatchReply> &callback) override {}
};

class FakeSubscriber : public SubscriberInterface {
 public:
  bool Subscribe(
      std::unique_ptr<rpc::SubMessage> sub_message,
      const rpc::ChannelType channel_type,
      const rpc::Address &owner_address,
      const std::string &key_id,
      pubsub::SubscribeDoneCallback subscribe_done_callback,
      pubsub::SubscriptionItemCallback subscription_callback,
      pubsub::SubscriptionFailureCallback subscription_failure_callback) override {
    return true;
  }

  bool SubscribeChannel(
      std::unique_ptr<rpc::SubMessage> sub_message,
      const rpc::ChannelType channel_type,
      const rpc::Address &owner_address,
      pubsub::SubscribeDoneCallback subscribe_done_callback,
      pubsub::SubscriptionItemCallback subscription_callback,
      pubsub::SubscriptionFailureCallback subscription_failure_callback) override {
    return true;
  }

  bool Unsubscribe(const rpc::ChannelType channel_type,
                   const rpc::Address &publisher_address,
                   const std::string &key_id) override {
    return true;
  }

  bool UnsubscribeChannel(const rpc::ChannelType channel_type,
                          const rpc::Address &publisher_address) override {
    return true;
  }

  bool IsSubscribed(const rpc::ChannelType channel_type,
                    const rpc::Address &publisher_address,
                    const std::string &key_id) const override {
    return false;
  }

  std::string DebugString() const override { return "FakeSubscriber"; }
};

}  // namespace pubsub
}  // namespace ray
