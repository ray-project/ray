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

#include <deque>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "ray/pubsub/publisher.h"

namespace ray {
namespace pubsub {

struct FakeSubscriberState {
  std::deque<std::shared_ptr<rpc::PubMessage>> mailbox;
  std::unordered_set<rpc::ChannelType> subscribed_channels;
  std::unordered_set<std::string> subscribed_keys;
};

class FakePublisher : public PublisherInterface {
 public:
  FakePublisher() : publisher_id_(UniqueID::FromRandom()) {}

  std::unordered_map<UniqueID, FakeSubscriberState> subscribers_;
  int64_t next_sequence_id_ = 0;
  UniqueID publisher_id_;

  void RegisterSubscription(const rpc::ChannelType channel_type,
                            const UniqueID &subscriber_id,
                            const std::optional<std::string> &key_id) override {
    auto &subscriber = subscribers_[subscriber_id];
    subscriber.subscribed_channels.insert(channel_type);
    if (key_id.has_value()) {
      subscriber.subscribed_keys.insert(key_id.value());
    }
  }

  void Publish(rpc::PubMessage pub_message) override {
    pub_message.set_sequence_id(++next_sequence_id_);
    auto message_ptr = std::make_shared<rpc::PubMessage>(std::move(pub_message));

    for (auto &[subscriber_id, subscriber_state] : subscribers_) {
      if (subscriber_state.subscribed_channels.count(message_ptr->channel_type()) > 0) {
        bool key_matches =
            subscriber_state.subscribed_keys.empty() ||
            subscriber_state.subscribed_keys.count(message_ptr->key_id()) > 0;

        if (key_matches) {
          subscriber_state.mailbox.push_back(message_ptr);
        }
      }
    }
  }

  void PublishFailure(const rpc::ChannelType channel_type,
                      const std::string &key_id) override {
    rpc::PubMessage failure_message;
    failure_message.set_channel_type(channel_type);
    failure_message.set_key_id(key_id);
    failure_message.mutable_failure_message();
    Publish(std::move(failure_message));
  }

  void ConnectToSubscriber(
      const rpc::PubsubLongPollingRequest &request,
      std::string *publisher_id,
      google::protobuf::RepeatedPtrField<rpc::PubMessage> *pub_messages,
      rpc::SendReplyCallback send_reply_callback) override {
    const auto subscriber_id = UniqueID::FromBinary(request.subscriber_id());
    auto &subscriber_state = subscribers_[subscriber_id];
    int64_t max_processed_sequence_id = request.max_processed_sequence_id();
    while (!subscriber_state.mailbox.empty() &&
           subscriber_state.mailbox.front()->sequence_id() <= max_processed_sequence_id) {
      subscriber_state.mailbox.pop_front();
    }

    *publisher_id = publisher_id_.Binary();

    for (const auto &message : subscriber_state.mailbox) {
      *pub_messages->Add() = *message;
    }
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  void UnregisterSubscription(const rpc::ChannelType channel_type,
                              const UniqueID &subscriber_id,
                              const std::optional<std::string> &key_id) override {
    auto it = subscribers_.find(subscriber_id);
    if (it == subscribers_.end()) {
      return;
    }

    auto &subscriber = it->second;
    subscriber.subscribed_channels.erase(channel_type);

    if (key_id.has_value()) {
      subscriber.subscribed_keys.erase(key_id.value());
    }
  }

  void UnregisterSubscriber(const UniqueID &subscriber_id) override {
    subscribers_.erase(subscriber_id);
  }

  std::string DebugString() const override { return "FakePublisher"; }
};

}  // namespace pubsub
}  // namespace ray
