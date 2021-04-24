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

template <typename MessageID>
void SubscriberChannel<MessageID>::Subscribe(
    const rpc::Address &publisher_address, const std::string &message_id,
    SubscriptionCallback subscription_callback,
    SubscriptionFailureCallback subscription_failure_callback) {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  const auto mid = MessageID::FromBinary(message_id);

  auto subscription_it = subscription_map_.find(publisher_id);
  if (subscription_it == subscription_map_.end()) {
    subscription_it =
        subscription_map_.emplace(publisher_id, SubscriptionInfo<MessageID>()).first;
  }
  RAY_CHECK(subscription_it != subscription_map_.end());
  RAY_CHECK(subscription_it->second.subscription_callback_map
                .emplace(mid, std::make_pair(std::move(subscription_callback),
                                             std::move(subscription_failure_callback)))
                .second);
}

template <typename MessageID>
bool SubscriberChannel<MessageID>::Unsubscribe(const rpc::Address &publisher_address,
                                               const std::string &message_id) {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  const auto mid = MessageID::FromBinary(message_id);

  auto subscription_it = subscription_map_.find(publisher_id);
  if (subscription_it == subscription_map_.end()) {
    return false;
  }
  auto &subscription_callback_map = subscription_it->second.subscription_callback_map;
  auto subscription_callback_it = subscription_callback_map.find(mid);
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

template <typename MessageID>
bool SubscriberChannel<MessageID>::CheckNoLeaks() const {
  for (const auto &subscription : subscription_map_) {
    if (subscription.second.subscription_callback_map.size() != 0) {
      return false;
    }
  }
  return subscription_map_.size() == 0;
}

template <typename MessageID>
void SubscriberChannel<MessageID>::HandlePublishedMessage(
    const rpc::Address &publisher_address, const rpc::PubMessage &pub_message) {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  auto subscription_it = subscription_map_.find(publisher_id);
  // If there's no more subscription, do nothing.
  if (subscription_it == subscription_map_.end()) {
    return;
  }

  const auto channel_type = pub_message.channel_type();
  const auto message_id = ParseMessageID(pub_message);
  RAY_CHECK(channel_type == channel_type_);
  RAY_LOG(DEBUG) << "Message id " << message_id << " information was published from "
                 << publisher_id;

  auto maybe_subscription_callback =
      GetSubscriptionCallback(publisher_address, message_id);
  if (maybe_subscription_callback.has_value()) {
    // If the object id is still subscribed, invoke a subscription callback.
    const auto &subscription_callback = maybe_subscription_callback.value();
    subscription_callback(pub_message);
  }
}

template <typename MessageID>
void SubscriberChannel<MessageID>::HandlePublisherFailure(
    const rpc::Address &publisher_address) {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  const auto &subscription_it = subscription_map_.find(publisher_id);
  // If there's no more subscription, do nothing.
  if (subscription_it == subscription_map_.end()) {
    return;
  }
  auto &subscription_callback_map = subscription_it->second.subscription_callback_map;

  std::vector<MessageID> message_ids_to_unsubscribe;
  for (const auto &message_id_it : subscription_callback_map) {
    const auto &message_id = message_id_it.first;
    message_ids_to_unsubscribe.push_back(message_id);

    auto maybe_failure_callback = GetFailureCallback(publisher_address, message_id);
    if (maybe_failure_callback.has_value()) {
      // If the object id is still subscribed, invoke a failure callback.
      const auto &failure_callback = maybe_failure_callback.value();
      failure_callback();
    }
  }

  for (const auto &message_id : message_ids_to_unsubscribe) {
    // If the publisher is failed, we automatically unsubscribe objects from this
    // publishers. If the failure callback called UnsubscribeObject, this will raise
    // check failures.
    RAY_CHECK(Unsubscribe(publisher_address, message_id.Binary()))
        << "Calling UnsubscribeObject inside a failure callback is not allowed.";
  }
}

template <typename MessageID>
inline absl::optional<SubscriptionCallback>
SubscriberChannel<MessageID>::GetSubscriptionCallback(
    const rpc::Address &publisher_address, const MessageID &message_id) const {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  auto subscription_it = subscription_map_.find(publisher_id);
  if (subscription_it == subscription_map_.end()) {
    return absl::nullopt;
  }
  auto callback_it = subscription_it->second.subscription_callback_map.find(message_id);
  bool exist = callback_it != subscription_it->second.subscription_callback_map.end();
  if (!exist) {
    return absl::nullopt;
  }
  auto subscription_callback = callback_it->second.first;
  return absl::optional<SubscriptionCallback>{subscription_callback};
}

template <typename MessageID>
inline absl::optional<SubscriptionFailureCallback>
SubscriberChannel<MessageID>::GetFailureCallback(const rpc::Address &publisher_address,
                                                 const MessageID &message_id) const {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  auto subscription_it = subscription_map_.find(publisher_id);
  if (subscription_it == subscription_map_.end()) {
    return absl::nullopt;
  }
  auto callback_it = subscription_it->second.subscription_callback_map.find(message_id);
  bool exist = callback_it != subscription_it->second.subscription_callback_map.end();
  if (!exist) {
    return absl::nullopt;
  }
  auto subscription_failure_callback = callback_it->second.second;
  return absl::optional<SubscriptionFailureCallback>{subscription_failure_callback};
}

template <typename MessageID>
bool SubscriberChannel<MessageID>::SubscriptionExists(const PublisherID &publisher_id) {
  return subscription_map_.count(publisher_id);
}

///////////////////////////////////////////////////////////////////////////////
/// Subscriber
///////////////////////////////////////////////////////////////////////////////

inline std::shared_ptr<SubscribeChannelInterface> Subscriber::Channel(
    const rpc::ChannelType channel_type) const {
  const auto it = channels_.find(channel_type);
  RAY_CHECK(it != channels_.end()) << "Unknown channel: " << channel_type;
  return it->second;
}

void Subscriber::Subscribe(const rpc::ChannelType channel_type,
                           const rpc::Address &publisher_address,
                           const std::string &message_id_binary,
                           SubscriptionCallback subscription_callback,
                           SubscriptionFailureCallback subscription_failure_callback) {
  Channel(channel_type)
      ->Subscribe(publisher_address, message_id_binary, std::move(subscription_callback),
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
                             const std::string &message_id_binary) {
  return Channel(channel_type)->Unsubscribe(publisher_address, message_id_binary);
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
                      "Publisher id:"
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

inline bool Subscriber::SubscriptionExists(const PublisherID &publisher_id) {
  for (const auto &channel_it : channels_) {
    if (channel_it.second->SubscriptionExists(publisher_id)) {
      return true;
    }
  }
  return false;
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

/// Per each message id, we need to define templates for these functions/classes here so
/// that symbols are discoverable.
template class SubscriberChannel<ObjectID>;

}  // namespace pubsub

}  // namespace ray
