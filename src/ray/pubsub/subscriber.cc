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
  cum_subscribe_requests_++;
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
  cum_unsubscribe_requests_++;
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
    const rpc::Address &publisher_address, const rpc::PubMessage &pub_message) const {
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
  cum_published_messages_++;
  if (!maybe_subscription_callback.has_value()) {
    return;
  }
  cum_processed_messages_++;
  // If the object id is still subscribed, run a callback to the callback io service.
  const auto &channel_name =
      rpc::ChannelType_descriptor()->FindValueByNumber(channel_type_)->name();
  callback_service_->post(
      [subscription_callback = std::move(maybe_subscription_callback.value()),
       msg = std::move(pub_message)]() { subscription_callback(msg); },
      "Subscriber.HandlePublishedMessage_" + channel_name);
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

  std::vector<std::string> key_ids_to_unsubscribe;
  for (const auto &key_id_it : subscription_callback_map) {
    const auto &key_id = key_id_it.first;
    auto unsubscribe_needed = HandlePublisherFailureInternal(publisher_address, key_id);
    if (unsubscribe_needed) {
      key_ids_to_unsubscribe.push_back(key_id.Binary());
    }
  }

  for (const auto &key_id : key_ids_to_unsubscribe) {
    // If the publisher is failed, we automatically unsubscribe objects from this
    // publishers. If the failure callback called UnsubscribeObject, this will raise
    // check failures.
    RAY_CHECK(Unsubscribe(publisher_address, key_id))
        << "Calling UnsubscribeObject inside a failure callback is not allowed.";
  }
}

template <typename KeyIdType>
void SubscriberChannel<KeyIdType>::HandlePublisherFailure(
    const rpc::Address &publisher_address, const std::string &key_id_binary) {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  const auto &subscription_it = subscription_map_.find(publisher_id);
  // If there's no more subscription, do nothing.
  if (subscription_it == subscription_map_.end()) {
    return;
  }
  const auto key_id = KeyIdType::FromBinary(key_id_binary);
  auto unsubscribe_needed = HandlePublisherFailureInternal(publisher_address, key_id);
  if (unsubscribe_needed) {
    RAY_CHECK(Unsubscribe(publisher_address, key_id_binary))
        << "Calling UnsubscribeObject inside a failure callback is not allowed.";
  }
}

template <typename KeyIdType>
bool SubscriberChannel<KeyIdType>::HandlePublisherFailureInternal(
    const rpc::Address &publisher_address, const KeyIdType &key_id) {
  auto maybe_failure_callback = GetFailureCallback(publisher_address, key_id);
  if (maybe_failure_callback.has_value()) {
    const auto &channel_name =
        rpc::ChannelType_descriptor()->FindValueByNumber(channel_type_)->name();
    callback_service_->post([failure_callback = std::move(maybe_failure_callback.value()),
                             key_id]() { failure_callback(key_id.Binary()); },
                            "Subscriber.HandleFailureCallback_" + channel_name);
    return true;
  } else {
    return false;
  }
}

template <typename KeyIdType>
std::string SubscriberChannel<KeyIdType>::DebugString() const {
  std::stringstream result;
  const google::protobuf::EnumDescriptor *descriptor = rpc::ChannelType_descriptor();
  const auto &channel_name = descriptor->FindValueByNumber(channel_type_)->name();
  result << "Channel " << channel_name;
  result << "\n- cumulative subscribe requests: " << cum_subscribe_requests_;
  result << "\n- cumulative unsubscribe requests: " << cum_unsubscribe_requests_;
  result << "\n- active subscribed publishers: " << subscription_map_.size();
  result << "\n- cumulative published messages: " << cum_published_messages_;
  result << "\n- cumulative processed messages: " << cum_processed_messages_;
  return result.str();
}

///////////////////////////////////////////////////////////////////////////////
/// Subscriber
///////////////////////////////////////////////////////////////////////////////

void Subscriber::Subscribe(std::unique_ptr<rpc::SubMessage> sub_message,
                           const rpc::ChannelType channel_type,
                           const rpc::Address &publisher_address,
                           const std::string &key_id_binary,
                           SubscriptionCallback subscription_callback,
                           SubscriptionFailureCallback subscription_failure_callback) {
  // Batch a subscribe command.
  auto command = std::make_unique<rpc::Command>();
  command->set_channel_type(channel_type);
  command->set_key_id(key_id_binary);
  command->mutable_subscribe_message()->Swap(sub_message.get());

  absl::MutexLock lock(&mutex_);
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  commands_[publisher_id].emplace(std::move(command));
  SendCommandBatchIfPossible(publisher_address);
  MakeLongPollingConnectionIfNotConnected(publisher_address);

  Channel(channel_type)
      ->Subscribe(publisher_address, key_id_binary, std::move(subscription_callback),
                  std::move(subscription_failure_callback));
}

bool Subscriber::Unsubscribe(const rpc::ChannelType channel_type,
                             const rpc::Address &publisher_address,
                             const std::string &key_id_binary) {
  // Batch the unsubscribe command.
  auto command = std::make_unique<rpc::Command>();
  command->set_channel_type(channel_type);
  command->set_key_id(key_id_binary);
  rpc::UnsubscribeMessage unsub_message;
  command->mutable_unsubscribe_message()->CopyFrom(unsub_message);

  absl::MutexLock lock(&mutex_);
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  commands_[publisher_id].emplace(std::move(command));
  SendCommandBatchIfPossible(publisher_address);

  return Channel(channel_type)->Unsubscribe(publisher_address, key_id_binary);
}

void Subscriber::MakeLongPollingConnectionIfNotConnected(
    const rpc::Address &publisher_address) {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  auto publishers_connected_it = publishers_connected_.find(publisher_id);
  if (publishers_connected_it == publishers_connected_.end()) {
    publishers_connected_.emplace(publisher_id);
    MakeLongPollingPubsubConnection(publisher_address);
  }
}

void Subscriber::MakeLongPollingPubsubConnection(const rpc::Address &publisher_address) {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  RAY_LOG(DEBUG) << "Make a long polling request to " << publisher_id;
  auto publisher_client = get_client_(publisher_address);
  rpc::PubsubLongPollingRequest long_polling_request;
  long_polling_request.set_subscriber_id(subscriber_id_.Binary());

  publisher_client->PubsubLongPolling(
      long_polling_request,
      [this, publisher_address](Status status, const rpc::PubsubLongPollingReply &reply) {
        absl::MutexLock lock(&mutex_);
        HandleLongPollingResponse(publisher_address, status, reply);
      });
}

void Subscriber::HandleLongPollingResponse(const rpc::Address &publisher_address,
                                           const Status &status,
                                           const rpc::PubsubLongPollingReply &reply) {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  RAY_LOG(DEBUG) << "Long polling request has replied from " << publisher_id;
  RAY_CHECK(publishers_connected_.count(publisher_id));

  if (!status.ok()) {
    // If status is not okay, we treat that the publisher is dead.
    RAY_LOG(DEBUG) << "A worker is dead. subscription_failure_callback will be invoked. "
                      "Publisher id: "
                   << publisher_id;

    for (const auto &channel_it : channels_) {
      channel_it.second->HandlePublisherFailure(publisher_address);
    }
    // Empty the command queue because we cannot send commands anymore.
    commands_.erase(publisher_id);
  } else {
    for (int i = 0; i < reply.pub_messages_size(); i++) {
      const auto &msg = reply.pub_messages(i);
      const auto channel_type = msg.channel_type();
      const auto &key_id = msg.key_id();
      // If the published message is a failure message, the publisher indicates
      // this key id is failed. Invoke the failure callback. At this time, we should not
      // unsubscribe the publisher because there are other entries that subscribe from the
      // publisher.
      if (msg.has_failure_message()) {
        RAY_LOG(DEBUG) << "Failure message has published from a channel " << channel_type;
        Channel(channel_type)->HandlePublisherFailure(publisher_address, key_id);
        continue;
      }

      // Otherwise, invoke the subscribe callback.
      Channel(channel_type)->HandlePublishedMessage(publisher_address, msg);
    }
  }

  if (SubscriptionExists(publisher_id)) {
    MakeLongPollingPubsubConnection(publisher_address);
  } else {
    publishers_connected_.erase(publisher_id);
  }
}

void Subscriber::SendCommandBatchIfPossible(const rpc::Address &publisher_address) {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  auto command_batch_sent_it = command_batch_sent_.find(publisher_id);

  // If there's no in flight command batch request to the publisher,
  // send it. Otherwise, this function will be called when the in flight
  // request is replied.
  if (command_batch_sent_it == command_batch_sent_.end()) {
    // Obtain the command queue.
    auto command_queue_it = commands_.find(publisher_id);
    if (command_queue_it == commands_.end()) {
      return;
    }
    auto &command_queue = command_queue_it->second;

    // Update the command in the FIFO order.
    rpc::PubsubCommandBatchRequest command_batch_request;
    command_batch_request.set_subscriber_id(subscriber_id_.Binary());
    int64_t updated_commands = 0;
    while (!command_queue.empty() && updated_commands < max_command_batch_size_) {
      auto new_command = command_batch_request.add_commands();
      new_command->Swap(command_queue.front().get());
      command_queue.pop();
      updated_commands += 1;
    }

    if (command_queue.size() == 0) {
      commands_.erase(command_queue_it);
    }

    if (updated_commands == 0) {
      return;
    }

    command_batch_sent_.emplace(publisher_id);
    auto publisher_client = get_client_(publisher_address);
    publisher_client->PubsubCommandBatch(
        command_batch_request,
        [this, publisher_address, publisher_id](
            Status status, const rpc::PubsubCommandBatchReply &reply) {
          absl::MutexLock lock(&mutex_);
          auto command_batch_sent_it = command_batch_sent_.find(publisher_id);
          RAY_CHECK(command_batch_sent_it != command_batch_sent_.end());
          command_batch_sent_.erase(command_batch_sent_it);
          if (!status.ok()) {
            // This means the publisher has failed.
            // The publisher dead detection & command clean up will be done
            // from the long polling request.
            RAY_LOG(DEBUG) << "The command batch request to " << publisher_id
                           << " has failed";
          } else {
            SendCommandBatchIfPossible(publisher_address);
          }
        });
  }
}

bool Subscriber::CheckNoLeaks() const {
  absl::MutexLock lock(&mutex_);
  bool leaks = false;
  for (const auto &channel_it : channels_) {
    if (!channel_it.second->CheckNoLeaks()) {
      leaks = true;
    }
  }
  bool command_batch_leak = command_batch_sent_.size() != 0;
  bool long_polling_leak = publishers_connected_.size() != 0;
  bool command_queue_leak = commands_.size() != 0;
  return !leaks && publishers_connected_.size() == 0 && !command_batch_leak &&
         !long_polling_leak && !command_queue_leak;
}

std::string Subscriber::DebugString() const {
  absl::MutexLock lock(&mutex_);
  std::stringstream result;
  result << "Subscriber:";
  for (const auto &channel_it : channels_) {
    result << "\n" << channel_it.second->DebugString();
  }
  return result.str();
}

/// Per each key id, we need to define templates for these functions/classes here so
/// that symbols are discoverable.
template class SubscriberChannel<ObjectID>;

}  // namespace pubsub

}  // namespace ray
