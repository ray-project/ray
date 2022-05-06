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

bool SubscriberChannel::Subscribe(
    const rpc::Address &publisher_address,
    const std::optional<std::string> &key_id,
    SubscriptionItemCallback subscription_callback,
    SubscriptionFailureCallback subscription_failure_callback) {
  cum_subscribe_requests_++;
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());

  if (key_id) {
    return subscription_map_[publisher_id]
        .per_entity_subscription
        .try_emplace(*key_id,
                     SubscriptionInfo(std::move(subscription_callback),
                                      std::move(subscription_failure_callback)))
        .second;
  }
  auto &all_entities_subscription =
      subscription_map_[publisher_id].all_entities_subscription;
  if (all_entities_subscription != nullptr) {
    return false;
  }
  all_entities_subscription = std::make_unique<SubscriptionInfo>(
      std::move(subscription_callback), std::move(subscription_failure_callback));
  return true;
}

bool SubscriberChannel::Unsubscribe(const rpc::Address &publisher_address,
                                    const std::optional<std::string> &key_id) {
  cum_unsubscribe_requests_++;
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());

  // Find subscription info.
  auto subscription_it = subscription_map_.find(publisher_id);
  if (subscription_it == subscription_map_.end()) {
    return false;
  }
  auto &subscription_index = subscription_it->second;

  // Unsubscribing from the channel.
  if (!key_id) {
    RAY_CHECK(subscription_index.per_entity_subscription.empty());
    const bool unsubscribed = subscription_index.all_entities_subscription != nullptr;
    subscription_index.all_entities_subscription.reset();
    subscription_map_.erase(subscription_it);
    return unsubscribed;
  }

  // Unsubscribing from a single key.
  RAY_CHECK(subscription_index.all_entities_subscription == nullptr);
  auto &per_entity_subscription = subscription_index.per_entity_subscription;

  auto subscription_callback_it = per_entity_subscription.find(*key_id);
  if (subscription_callback_it == per_entity_subscription.end()) {
    return false;
  }
  per_entity_subscription.erase(subscription_callback_it);
  if (per_entity_subscription.empty()) {
    subscription_map_.erase(subscription_it);
  }
  return true;
}

bool SubscriberChannel::IsSubscribed(const rpc::Address &publisher_address,
                                     const std::string &key_id) const {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  auto subscription_it = subscription_map_.find(publisher_id);
  if (subscription_it == subscription_map_.end()) {
    return false;
  }
  RAY_CHECK(subscription_it->second.all_entities_subscription == nullptr);
  auto &per_entity_subscription = subscription_it->second.per_entity_subscription;
  auto subscription_callback_it = per_entity_subscription.find(key_id);
  if (subscription_callback_it == per_entity_subscription.end()) {
    return false;
  }
  return true;
}

bool SubscriberChannel::CheckNoLeaks() const {
  for (const auto &subscription : subscription_map_) {
    if (subscription.second.all_entities_subscription != nullptr) {
      return false;
    }
    if (subscription.second.per_entity_subscription.size() != 0) {
      return false;
    }
  }
  return subscription_map_.size() == 0;
}

void SubscriberChannel::HandlePublishedMessage(const rpc::Address &publisher_address,
                                               const rpc::PubMessage &pub_message) const {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  auto subscription_it = subscription_map_.find(publisher_id);
  // If there's no more subscription, do nothing.
  if (subscription_it == subscription_map_.end()) {
    return;
  }

  const auto channel_type = pub_message.channel_type();
  const auto &key_id = pub_message.key_id();
  RAY_CHECK(channel_type == channel_type_)
      << "Message from " << rpc::ChannelType_Name(channel_type) << ", this channel is "
      << rpc::ChannelType_Name(channel_type_);

  auto maybe_subscription_callback =
      GetSubscriptionItemCallback(publisher_address, key_id);
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

void SubscriberChannel::HandlePublisherFailure(const rpc::Address &publisher_address,
                                               const Status &status) {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  const auto &subscription_it = subscription_map_.find(publisher_id);
  // If there's no more subscription, do nothing.
  if (subscription_it == subscription_map_.end()) {
    return;
  }
  auto &per_entity_subscription = subscription_it->second.per_entity_subscription;

  std::vector<std::string> key_ids_to_unsubscribe;
  for (const auto &key_id_it : per_entity_subscription) {
    const auto &key_id = key_id_it.first;
    auto unsubscribe_needed =
        HandlePublisherFailureInternal(publisher_address, key_id, status);
    if (unsubscribe_needed) {
      key_ids_to_unsubscribe.push_back(key_id);
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

void SubscriberChannel::HandlePublisherFailure(const rpc::Address &publisher_address,
                                               const std::string &key_id) {
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  const auto &subscription_it = subscription_map_.find(publisher_id);
  // If there's no more subscription, do nothing.
  if (subscription_it == subscription_map_.end()) {
    return;
  }
  auto unsubscribe_needed =
      HandlePublisherFailureInternal(publisher_address, key_id, Status::OK());
  if (unsubscribe_needed) {
    RAY_CHECK(Unsubscribe(publisher_address, key_id))
        << "Calling UnsubscribeObject inside a failure callback is not allowed.";
  }
}

bool SubscriberChannel::HandlePublisherFailureInternal(
    const rpc::Address &publisher_address,
    const std::string &key_id,
    const Status &status) {
  auto maybe_failure_callback = GetFailureCallback(publisher_address, key_id);
  if (maybe_failure_callback.has_value()) {
    const auto &channel_name =
        rpc::ChannelType_descriptor()->FindValueByNumber(channel_type_)->name();
    callback_service_->post(
        [failure_callback = std::move(maybe_failure_callback.value()), key_id, status]() {
          failure_callback(key_id, status);
        },
        "Subscriber.HandleFailureCallback_" + channel_name);
    return true;
  }
  return false;
}

std::string SubscriberChannel::DebugString() const {
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

Subscriber::~Subscriber() {
  // TODO(mwtian): flush Subscriber and ensure there is no leak during destruction.
}

bool Subscriber::Subscribe(std::unique_ptr<rpc::SubMessage> sub_message,
                           const rpc::ChannelType channel_type,
                           const rpc::Address &publisher_address,
                           const std::string &key_id,
                           SubscribeDoneCallback subscribe_done_callback,
                           SubscriptionItemCallback subscription_callback,
                           SubscriptionFailureCallback subscription_failure_callback) {
  return SubscribeInternal(std::move(sub_message),
                           channel_type,
                           publisher_address,
                           key_id,
                           std::move(subscribe_done_callback),
                           std::move(subscription_callback),
                           std::move(subscription_failure_callback));
}

bool Subscriber::SubscribeChannel(
    std::unique_ptr<rpc::SubMessage> sub_message,
    const rpc::ChannelType channel_type,
    const rpc::Address &publisher_address,
    SubscribeDoneCallback subscribe_done_callback,
    SubscriptionItemCallback subscription_callback,
    SubscriptionFailureCallback subscription_failure_callback) {
  return SubscribeInternal(std::move(sub_message),
                           channel_type,
                           publisher_address,
                           std::nullopt,
                           std::move(subscribe_done_callback),
                           std::move(subscription_callback),
                           std::move(subscription_failure_callback));
}

bool Subscriber::Unsubscribe(const rpc::ChannelType channel_type,
                             const rpc::Address &publisher_address,
                             const std::string &key_id) {
  // Batch the unsubscribe command.
  auto command = std::make_unique<CommandItem>();
  command->cmd.set_channel_type(channel_type);
  command->cmd.set_key_id(key_id);
  command->cmd.mutable_unsubscribe_message();

  absl::MutexLock lock(&mutex_);
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  commands_[publisher_id].emplace(std::move(command));
  SendCommandBatchIfPossible(publisher_address);

  return Channel(channel_type)->Unsubscribe(publisher_address, key_id);
}

bool Subscriber::UnsubscribeChannel(const rpc::ChannelType channel_type,
                                    const rpc::Address &publisher_address) {
  // Batch the unsubscribe command.
  auto command = std::make_unique<CommandItem>();
  command->cmd.set_channel_type(channel_type);
  command->cmd.mutable_unsubscribe_message();

  absl::MutexLock lock(&mutex_);
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
  commands_[publisher_id].emplace(std::move(command));
  SendCommandBatchIfPossible(publisher_address);

  return Channel(channel_type)->Unsubscribe(publisher_address, std::nullopt);
}

bool Subscriber::IsSubscribed(const rpc::ChannelType channel_type,
                              const rpc::Address &publisher_address,
                              const std::string &key_id) const {
  absl::MutexLock lock(&mutex_);
  auto *channel = Channel(channel_type);
  if (channel == nullptr) {
    return false;
  }
  return channel->IsSubscribed(publisher_address, key_id);
}

bool Subscriber::SubscribeInternal(
    std::unique_ptr<rpc::SubMessage> sub_message,
    const rpc::ChannelType channel_type,
    const rpc::Address &publisher_address,
    const std::optional<std::string> &key_id,
    SubscribeDoneCallback subscribe_done_callback,
    SubscriptionItemCallback subscription_callback,
    SubscriptionFailureCallback subscription_failure_callback) {
  // Batch a subscribe command.
  auto command = std::make_unique<CommandItem>();
  command->cmd.set_channel_type(channel_type);
  if (key_id) {
    command->cmd.set_key_id(*key_id);
  }
  if (sub_message != nullptr) {
    command->cmd.mutable_subscribe_message()->Swap(sub_message.get());
  }
  command->done_cb = std::move(subscribe_done_callback);
  const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());

  absl::MutexLock lock(&mutex_);
  commands_[publisher_id].emplace(std::move(command));
  SendCommandBatchIfPossible(publisher_address);
  MakeLongPollingConnectionIfNotConnected(publisher_address);
  return Channel(channel_type)
      ->Subscribe(publisher_address,
                  key_id,
                  std::move(subscription_callback),
                  std::move(subscription_failure_callback));
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
  auto subscriber_client = get_client_(publisher_address);
  rpc::PubsubLongPollingRequest long_polling_request;
  long_polling_request.set_subscriber_id(subscriber_id_.Binary());

  subscriber_client->PubsubLongPolling(
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
      channel_it.second->HandlePublisherFailure(publisher_address, status);
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

      // Otherwise, invoke the subscription callback.
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
    std::vector<SubscribeDoneCallback> done_cb;
    while (!command_queue.empty() &&
           done_cb.size() < static_cast<size_t>(max_command_batch_size_)) {
      auto new_command = command_batch_request.add_commands();
      new_command->Swap(&command_queue.front()->cmd);
      done_cb.push_back(std::move(command_queue.front()->done_cb));
      command_queue.pop();
    }

    if (command_queue.size() == 0) {
      commands_.erase(command_queue_it);
    }

    if (done_cb.size() == 0) {
      return;
    }

    command_batch_sent_.emplace(publisher_id);
    auto subscriber_client = get_client_(publisher_address);
    subscriber_client->PubsubCommandBatch(
        command_batch_request,
        [this, publisher_address, publisher_id, done_cb = std::move(done_cb)](
            Status status, const rpc::PubsubCommandBatchReply &reply) {
          {
            absl::MutexLock lock(&mutex_);
            auto command_batch_sent_it = command_batch_sent_.find(publisher_id);
            RAY_CHECK(command_batch_sent_it != command_batch_sent_.end());
            command_batch_sent_.erase(command_batch_sent_it);
          }
          for (const auto &done : done_cb) {
            if (done) {
              done(status);
            }
          }
          if (!status.ok()) {
            // This means the publisher has failed.
            // The publisher dead detection & command clean up will be done
            // from the long polling request.
            RAY_LOG(DEBUG) << "The command batch request to " << publisher_id
                           << " has failed";
          }
          {
            absl::MutexLock lock(&mutex_);
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
  return !leaks && publishers_connected_.empty() && command_batch_sent_.empty() &&
         commands_.empty();
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

}  // namespace pubsub

}  // namespace ray
