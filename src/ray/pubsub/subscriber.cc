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

namespace ray {

namespace pubsub {

///////////////////////////////////////////////////////////////////////////////
/// SubscriberChannel
///////////////////////////////////////////////////////////////////////////////

template <typename KeyIdType>
void SubscriberChannel<KeyIdType>::Subscribe(
    const rpc::Address &publisher_address, const std::string &key_id_binary,
    SubscriptionCallback subscription_callback,
    SubscriptionFailureCallback subscription_failure_callback) {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  const auto key_id = KeyIdType::FromBinary(key_id_binary);

  auto subscription_it = subscription_map_.find(publisher_id);
  if (subscription_it == subscription_map_.end()) {
    subscription_it =
        subscription_map_.emplace(publisher_id, SubscriptionInfo<KeyIdType>()).first;
  }
  RAY_CHECK(subscription_it != subscription_map_.end());
  RAY_CHECK(subscription_it->second.subscription_callback_map
                .emplace(key_id, std::make_pair(std::move(subscription_callback),
                                                std::move(subscription_failure_callback)))
                .second);
}

template <typename KeyIdType>
bool SubscriberChannel<KeyIdType>::Unsubscribe(const rpc::Address &publisher_address,
                                               const std::string &key_id_binary) {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  const auto key_id = KeyIdType::FromBinary(key_id_binary);

  auto subscription_it = subscription_map_.find(publisher_id);
  if (subscription_it == subscription_map_.end()) {
    return false;
  }
  auto &subscription_callback_map = subscription_it->second.subscription_callback_map;
  auto subscription_callback_it = subscription_callback_map.find(key_id);
  if (subscription_callback_it == subscription_callback_map.end()) {
    return false;
  }
  subscription_callback_map.erase(subscription_callback_it);
  subscription_it = subscription_map_.find(publisher_id);
  if (subscription_it->second.subscription_callback_map.size() == 0) {
    subscription_map_.erase(subscription_it);
  }
  return true;
}

template <typename KeyIdType>
bool SubscriberChannel<KeyIdType>::CheckNoLeaks() const {
  for (const auto &subscription : subscription_map_) {
    if (subscription.second.subscription_callback_map.size() != 0) {
      return false;
    }
  }
  return subscription_map_.size() == 0;
}

template <typename KeyIdType>
void SubscriberChannel<KeyIdType>::HandlePublishedMessage(
    const rpc::Address &publisher_address, const rpc::PubMessage &pub_message) {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  auto subscription_it = subscription_map_.find(publisher_id);
  // If there's no more subscription, do nothing.
  if (subscription_it == subscription_map_.end()) {
    return;
  }

  const auto channel_type = pub_message.channel_type();
  const auto key_id = KeyIdType::FromBinary(pub_message.key_id());
  RAY_CHECK(channel_type == channel_type_);
  RAY_LOG(DEBUG) << "key id " << key_id << " information was published from "
                 << publisher_id;

  auto maybe_subscription_callback = GetSubscriptionCallback(publisher_address, key_id);
  if (maybe_subscription_callback.has_value()) {
    // If the object id is still subscribed, invoke a subscription callback.
    const auto &subscription_callback = maybe_subscription_callback.value();
    subscription_callback(pub_message);
  }
}

template <typename KeyIdType>
void SubscriberChannel<KeyIdType>::HandlePublisherFailure(
    const rpc::Address &publisher_address) {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  const auto &subscription_it = subscription_map_.find(publisher_id);
  // If there's no more subscription, do nothing.
  if (subscription_it == subscription_map_.end()) {
    return;
  }
  auto &subscription_callback_map = subscription_it->second.subscription_callback_map;

  std::vector<KeyIdType> key_ids_to_unsubscribe;
  for (const auto &key_id_it : subscription_callback_map) {
    const auto &key_id = key_id_it.first;
    key_ids_to_unsubscribe.push_back(key_id);

    auto maybe_failure_callback = GetFailureCallback(publisher_address, key_id);
    if (maybe_failure_callback.has_value()) {
      // If the object id is still subscribed, invoke a failure callback.
      const auto &failure_callback = maybe_failure_callback.value();
      failure_callback();
    }
  }

  for (const auto &key_id : key_ids_to_unsubscribe) {
    // If the publisher is failed, we automatically unsubscribe objects from this
    // publishers. If the failure callback called UnsubscribeObject, this will raise
    // check failures.
    RAY_CHECK(Unsubscribe(publisher_address, key_id.Binary()))
        << "Calling UnsubscribeObject inside a failure callback is not allowed.";
  }
}

///////////////////////////////////////////////////////////////////////////////
/// Subscriber
///////////////////////////////////////////////////////////////////////////////

void Subscriber::Subscribe(const rpc::ChannelType channel_type,
                           const rpc::Address &publisher_address,
                           const std::string &key_id_binary,
                           SubscriptionCallback subscription_callback,
                           SubscriptionFailureCallback subscription_failure_callback) {
  Channel(channel_type)
      ->Subscribe(publisher_address, key_id_binary, std::move(subscription_callback),
                  std::move(subscription_failure_callback));

  // Make a long polling connection if we never made the one with this publisher for
  // pubsub operations.
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  auto publishers_connected_it = publishers_connected_.find(publisher_id);
  if (publishers_connected_it == publishers_connected_.end()) {
    publishers_connected_.emplace(publisher_id);
    rpc::Address subscriber_address;
    subscriber_address.set_raylet_id(subscriber_id_.Binary());
    subscriber_address.set_ip_address(subscriber_address_);
    subscriber_address.set_port(subscriber_port_);
    MakeLongPollingPubsubConnection(publisher_address, subscriber_address);
  }
}

bool Subscriber::Unsubscribe(const rpc::ChannelType channel_type,
                             const rpc::Address &publisher_address,
                             const std::string &key_id_binary) {
  return Channel(channel_type)->Unsubscribe(publisher_address, key_id_binary);
}

void Subscriber::MakeLongPollingPubsubConnection(const rpc::Address &publisher_address,
                                                 const rpc::Address &subscriber_address) {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  RAY_LOG(DEBUG) << "Make a long polling request to " << publisher_id;
  auto publisher_client = publisher_client_pool_.GetOrConnect(publisher_address);
  rpc::PubsubLongPollingRequest long_polling_request;
  long_polling_request.mutable_subscriber_address()->CopyFrom(subscriber_address);

  publisher_client->PubsubLongPolling(
      long_polling_request, [this, publisher_address, subscriber_address](
                                Status status, const rpc::PubsubLongPollingReply &reply) {
        HandleLongPollingResponse(publisher_address, subscriber_address, status, reply);
      });
}

void Subscriber::HandleLongPollingResponse(const rpc::Address &publisher_address,
                                           const rpc::Address &subscriber_address,
                                           const Status &status,
                                           const rpc::PubsubLongPollingReply &reply) {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  RAY_LOG(DEBUG) << "Long polling request has replied from " << publisher_id;
  auto publishers_connected_it = publishers_connected_.find(publisher_id);
  RAY_CHECK(publishers_connected_it != publishers_connected_.end());
  if (!status.ok()) {
    // If status is not okay, we treat that the publisher is dead.
    RAY_LOG(DEBUG) << "A worker is dead. subscription_failure_callback will be invoked. "
                      "Publisher id: "
                   << publisher_id;
    for (const auto &channel_it : channels_) {
      channel_it.second->HandlePublisherFailure(publisher_address);
    }
  } else {
    // Otherwise, iterate on the reply and pass published messages to channels.
    for (int i = 0; i < reply.pub_messages_size(); i++) {
      const auto &msg = reply.pub_messages(i);
      const auto channel_type = msg.channel_type();
      Channel(channel_type)->HandlePublishedMessage(publisher_address, msg);
    }
  }

  if (SubscriptionExists(publisher_id)) {
    MakeLongPollingPubsubConnection(publisher_address, subscriber_address);
  } else {
    publishers_connected_.erase(publisher_id);
  }
}

bool Subscriber::CheckNoLeaks() const {
  bool leaks = false;
  for (const auto &channel_it : channels_) {
    if (!channel_it.second->CheckNoLeaks()) {
      leaks = true;
    }
  }
  return !leaks && publishers_connected_.size() == 0;
}

/// Per each key id, we need to define templates for these functions/classes here so
/// that symbols are discoverable.
template class SubscriberChannel<ObjectID>;

}  // namespace pubsub

}  // namespace ray
