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

namespace ray {

namespace pubsub {

namespace pub_internal {

template <typename MessageID>
void SubscriptionIndex<MessageID>::AddEntry(const std::string &message_id_binary,
                                            const SubscriberID &subscriber_id) {
  const auto message_id = ParseBinary(message_id_binary);
  auto &subscribing_message_ids = subscribers_to_message_id_[subscriber_id];
  RAY_CHECK(subscribing_message_ids.emplace(message_id).second);
  auto &subscriber_map = message_id_to_subscribers_[message_id];
  RAY_CHECK(subscriber_map.emplace(subscriber_id).second);
}

template <typename MessageID>
absl::optional<std::reference_wrapper<const absl::flat_hash_set<SubscriberID>>>
SubscriptionIndex<MessageID>::GetSubscriberIdsByMessageId(
    const std::string &message_id_binary) const {
  const auto message_id = ParseBinary(message_id_binary);
  auto it = message_id_to_subscribers_.find(message_id);
  if (it == message_id_to_subscribers_.end()) {
    return absl::nullopt;
  }
  return absl::optional<std::reference_wrapper<const absl::flat_hash_set<SubscriberID>>>{
      std::ref(it->second)};
}

template <typename MessageID>
bool SubscriptionIndex<MessageID>::HasMessageId(
    const std::string &message_id_binary) const {
  const auto message_id = ParseBinary(message_id_binary);
  return message_id_to_subscribers_.count(message_id);
}

template <typename MessageID>
bool SubscriptionIndex<MessageID>::HasSubscriber(
    const SubscriberID &subscriber_id) const {
  return subscribers_to_message_id_.count(subscriber_id);
}

template <typename MessageID>
bool SubscriptionIndex<MessageID>::EraseSubscriber(const SubscriberID &subscriber_id) {
  auto subscribing_message_it = subscribers_to_message_id_.find(subscriber_id);
  if (subscribing_message_it == subscribers_to_message_id_.end()) {
    return false;
  }

  auto &subscribing_messages = subscribing_message_it->second;
  for (const auto &message_id : subscribing_messages) {
    // Erase the subscriber from the object map.
    auto subscribers_it = message_id_to_subscribers_.find(message_id);
    if (subscribers_it == message_id_to_subscribers_.end()) {
      continue;
    }
    auto &subscribers = subscribers_it->second;
    subscribers.erase(subscriber_id);
    if (subscribers.size() == 0) {
      message_id_to_subscribers_.erase(subscribers_it);
    }
  }
  subscribers_to_message_id_.erase(subscribing_message_it);
  return true;
}

template <typename MessageID>
bool SubscriptionIndex<MessageID>::EraseEntry(const std::string &message_id_binary,
                                              const SubscriberID &subscriber_id) {
  // Erase from subscribers_to_objects_;
  const auto message_id = ParseBinary(message_id_binary);
  auto subscribers_to_message_it = subscribers_to_message_id_.find(subscriber_id);
  if (subscribers_to_message_it == subscribers_to_message_id_.end()) {
    return false;
  }
  auto &objects = subscribers_to_message_it->second;
  auto object_it = objects.find(message_id);
  if (object_it == objects.end()) {
    RAY_CHECK(message_id_to_subscribers_.count(message_id) == 0);
    return false;
  }
  objects.erase(object_it);
  if (objects.size() == 0) {
    subscribers_to_message_id_.erase(subscribers_to_message_it);
  }

  // Erase from objects_to_subscribers_.
  auto message_id_to_subscriber_it = message_id_to_subscribers_.find(message_id);
  // If code reaches this line, that means the object id was in the index.
  RAY_CHECK(message_id_to_subscriber_it != message_id_to_subscribers_.end());
  auto &subscribers = message_id_to_subscriber_it->second;
  auto subscriber_it = subscribers.find(subscriber_id);
  // If code reaches this line, that means the subscriber id was in the index.
  RAY_CHECK(subscriber_it != subscribers.end());
  subscribers.erase(subscriber_it);
  if (subscribers.size() == 0) {
    message_id_to_subscribers_.erase(message_id_to_subscriber_it);
  }
  return true;
}

template <typename MessageID>
bool SubscriptionIndex<MessageID>::CheckNoLeaks() const {
  return message_id_to_subscribers_.size() == 0 && subscribers_to_message_id_.size() == 0;
}

bool Subscriber::ConnectToSubscriber(rpc::PubsubLongPollingReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  if (!long_polling_connection_) {
    RAY_CHECK(reply != nullptr);
    RAY_CHECK(send_reply_callback != nullptr);
    long_polling_connection_ =
        absl::make_unique<LongPollConnection>(reply, std::move(send_reply_callback));
    last_connection_update_time_ms_ = get_time_ms_();
    return true;
  }
  return false;
}

void Subscriber::QueueMessage(std::unique_ptr<rpc::PubMessage> pub_message,
                              bool try_publish) {
  if (mailbox_.size() == 0) {
    mailbox_.push_back(absl::make_unique<rpc::PubsubLongPollingReply>());
  }

  // Update the long polling reply.
  auto *next_long_polling_reply = mailbox_.back().get();
  auto *new_pub_message = next_long_polling_reply->add_pub_messages();
  new_pub_message->Swap(pub_message.get());

  // If the reply is too big, add a new entry.
  if (next_long_polling_reply->pub_messages_size() >= publish_batch_size_) {
    mailbox_.push_back(absl::make_unique<rpc::PubsubLongPollingReply>());
  }

  if (try_publish) {
    PublishIfPossible();
  }
}

bool Subscriber::PublishIfPossible(bool force) {
  if (!long_polling_connection_) {
    return false;
  }

  if (force || mailbox_.size() > 0) {
    // If force pubilsh is invoked, mailbox could be empty. Fill this up in that case.
    if (mailbox_.size() == 0) {
      mailbox_.push_back(absl::make_unique<rpc::PubsubLongPollingReply>());
    }

    // Reply to the long polling subscriber. Swap the reply here to avoid extra copy.
    long_polling_connection_->reply->Swap(mailbox_.front().get());
    long_polling_connection_->send_reply_callback(Status::OK(), nullptr, nullptr);

    // Clean up & update metadata.
    long_polling_connection_.reset(nullptr);
    mailbox_.pop_front();
    last_connection_update_time_ms_ = get_time_ms_();
    return true;
  }
  return false;
}

bool Subscriber::CheckNoLeaks() const {
  return !long_polling_connection_ && mailbox_.size() == 0;
}

bool Subscriber::IsDisconnected() const {
  return !long_polling_connection_ &&
         get_time_ms_() - last_connection_update_time_ms_ >= connection_timeout_ms_;
}

bool Subscriber::IsActiveConnectionTimedOut() const {
  return long_polling_connection_ &&
         get_time_ms_() - last_connection_update_time_ms_ >= connection_timeout_ms_;
}

// We need to define this in order for the compiler to find the definition.
template class pub_internal::SubscriptionIndex<ObjectID>;

}  // namespace pub_internal

void Publisher::ConnectToSubscriber(const SubscriberID &subscriber_id,
                                    rpc::PubsubLongPollingReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Long polling connection initiated by " << subscriber_id;
  RAY_CHECK(send_reply_callback != nullptr);

  absl::MutexLock lock(&mutex_);
  auto it = subscribers_.find(subscriber_id);
  if (it == subscribers_.end()) {
    it = subscribers_
             .emplace(subscriber_id,
                      std::make_shared<pub_internal::Subscriber>(
                          get_time_ms_, subscriber_timeout_ms_, publish_batch_size_))
             .first;
  }
  auto &subscriber = it->second;

  // Since the long polling connection is synchronous between the client and
  // coordinator, when it connects, the connection shouldn't have existed.
  RAY_CHECK(subscriber->ConnectToSubscriber(reply, std::move(send_reply_callback)));
  subscriber->PublishIfPossible();
}

void Publisher::RegisterSubscription(const rpc::ChannelType channel_type,
                                     const SubscriberID &subscriber_id,
                                     const std::string &message_id_binary) {
  absl::MutexLock lock(&mutex_);
  if (subscribers_.count(subscriber_id) == 0) {
    subscribers_.emplace(subscriber_id,
                         std::make_shared<pub_internal::Subscriber>(
                             get_time_ms_, subscriber_timeout_ms_, publish_batch_size_));
  }
  subscription_index_map_[channel_type].AddEntry(message_id_binary, subscriber_id);
}

void Publisher::Publish(const rpc::ChannelType channel_type,
                        std::unique_ptr<rpc::PubMessage> pub_message,
                        const std::string &message_id_binary) {
  absl::MutexLock lock(&mutex_);
  // TODO(sang): Currently messages are lost if publish happens
  // before there's any subscriber for the object.
  auto maybe_subscribers =
      subscription_index_map_[channel_type].GetSubscriberIdsByMessageId(
          message_id_binary);
  if (!maybe_subscribers.has_value()) {
    return;
  }

  for (const auto &subscriber_id : maybe_subscribers.value().get()) {
    auto it = subscribers_.find(subscriber_id);
    RAY_CHECK(it != subscribers_.end());
    auto &subscriber = it->second;
    subscriber->QueueMessage(std::move(pub_message));
  }
}

bool Publisher::UnregisterSubscription(const rpc::ChannelType channel_type,
                                       const SubscriberID &subscriber_id,
                                       const std::string &message_id_binary) {
  absl::MutexLock lock(&mutex_);
  return subscription_index_map_[channel_type].EraseEntry(message_id_binary,
                                                          subscriber_id);
}

bool Publisher::UnregisterSubscriber(const SubscriberID &subscriber_id) {
  absl::MutexLock lock(&mutex_);
  return UnregisterSubscriberInternal(subscriber_id);
}

bool Publisher::UnregisterSubscriberInternal(const SubscriberID &subscriber_id) {
  int erased = 0;
  for (auto &index : subscription_index_map_) {
    if (index.second.EraseSubscriber(subscriber_id)) {
      erased += 1;
    }
  }

  auto it = subscribers_.find(subscriber_id);
  if (it == subscribers_.end()) {
    return erased;
  }
  auto &subscriber = it->second;
  // Remove the long polling connection because otherwise, there's memory leak.
  subscriber->PublishIfPossible(/*force=*/true);
  subscribers_.erase(it);
  return erased;
}

void Publisher::CheckDeadSubscribers() {
  absl::MutexLock lock(&mutex_);
  for (const auto &it : subscribers_) {
    const auto &subscriber = it.second;

    auto disconnected = subscriber->IsDisconnected();
    auto active_connection_timed_out = subscriber->IsActiveConnectionTimedOut();
    RAY_CHECK(!(disconnected && active_connection_timed_out));

    if (disconnected) {
      const auto &subscriber_id = it.first;
      UnregisterSubscriberInternal(subscriber_id);
    } else if (active_connection_timed_out) {
      // Refresh the long polling connection. The subscriber will send it again.
      subscriber->PublishIfPossible(/*force*/ true);
    }
  }
}

bool Publisher::CheckNoLeaks() const {
  absl::MutexLock lock(&mutex_);
  for (const auto &subscriber : subscribers_) {
    if (!subscriber.second->CheckNoLeaks()) {
      return false;
    }
  }

  for (const auto &index : subscription_index_map_) {
    if (!index.second.CheckNoLeaks()) {
      return false;
    }
  }
  return true;
}

}  // namespace pubsub

}  // namespace ray
