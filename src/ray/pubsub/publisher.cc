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

bool SubscriptionIndex::AddEntry(const std::string &key_id,
                                 const SubscriberID &subscriber_id) {
  if (key_id.empty()) {
    return subscribers_to_all_.insert(subscriber_id).second;
  }

  auto &subscribing_key_ids = subscribers_to_key_id_[subscriber_id];
  bool key_added = subscribing_key_ids.emplace(key_id).second;
  auto &subscriber_map = key_id_to_subscribers_[key_id];
  auto subscriber_added = subscriber_map.emplace(subscriber_id).second;

  RAY_CHECK(key_added == subscriber_added);
  return key_added;
}

std::vector<SubscriberID> SubscriptionIndex::GetSubscriberIdsByKeyId(
    const std::string &key_id) const {
  std::vector<SubscriberID> subscribers;
  if (!subscribers_to_all_.empty()) {
    subscribers.insert(
        subscribers.end(), subscribers_to_all_.begin(), subscribers_to_all_.end());
  }
  auto it = key_id_to_subscribers_.find(key_id);
  if (it != key_id_to_subscribers_.end()) {
    auto &ids = it->second;
    subscribers.insert(subscribers.end(), ids.begin(), ids.end());
  }
  return subscribers;
}

bool SubscriptionIndex::EraseSubscriber(const SubscriberID &subscriber_id) {
  // Erase subscriber of all keys.
  if (subscribers_to_all_.erase(subscriber_id) > 0) {
    return true;
  }

  auto subscribing_key_it = subscribers_to_key_id_.find(subscriber_id);
  if (subscribing_key_it == subscribers_to_key_id_.end()) {
    return false;
  }

  // Erase subscriber of individual keys.
  const auto &subscribing_keys = subscribing_key_it->second;
  for (const auto &key_id : subscribing_keys) {
    // Erase the subscriber from the object map.
    auto subscribers_it = key_id_to_subscribers_.find(key_id);
    if (subscribers_it == key_id_to_subscribers_.end()) {
      continue;
    }
    auto &subscribers = subscribers_it->second;
    subscribers.erase(subscriber_id);
    if (subscribers.size() == 0) {
      key_id_to_subscribers_.erase(subscribers_it);
    }
  }
  subscribers_to_key_id_.erase(subscribing_key_it);
  return true;
}

bool SubscriptionIndex::EraseEntry(const std::string &key_id,
                                   const SubscriberID &subscriber_id) {
  // Erase the subscriber of all keys.
  if (key_id.empty()) {
    return subscribers_to_all_.erase(subscriber_id) > 0;
  }

  // Erase keys from the subscriber of individual keys.
  auto subscribers_to_message_it = subscribers_to_key_id_.find(subscriber_id);
  if (subscribers_to_message_it == subscribers_to_key_id_.end()) {
    return false;
  }
  auto &objects = subscribers_to_message_it->second;
  auto object_it = objects.find(key_id);
  if (object_it == objects.end()) {
    auto it = key_id_to_subscribers_.find(key_id);
    if (it != key_id_to_subscribers_.end()) {
      RAY_CHECK(it->second.count(subscriber_id) == 0);
    }
    return false;
  }
  objects.erase(object_it);
  if (objects.size() == 0) {
    subscribers_to_key_id_.erase(subscribers_to_message_it);
  }

  // Erase subscribers from keys (reverse index).
  auto key_id_to_subscriber_it = key_id_to_subscribers_.find(key_id);
  // If code reaches this line, that means the object id was in the index.
  RAY_CHECK(key_id_to_subscriber_it != key_id_to_subscribers_.end());
  auto &subscribers = key_id_to_subscriber_it->second;
  auto subscriber_it = subscribers.find(subscriber_id);
  // If code reaches this line, that means the subscriber id was in the index.
  RAY_CHECK(subscriber_it != subscribers.end());
  subscribers.erase(subscriber_it);
  if (subscribers.size() == 0) {
    key_id_to_subscribers_.erase(key_id_to_subscriber_it);
  }
  return true;
}

bool SubscriptionIndex::HasKeyId(const std::string &key_id) const {
  return key_id_to_subscribers_.contains(key_id);
}

bool SubscriptionIndex::HasSubscriber(const SubscriberID &subscriber_id) const {
  if (subscribers_to_all_.contains(subscriber_id)) {
    return true;
  }
  return subscribers_to_key_id_.contains(subscriber_id);
}

bool SubscriptionIndex::CheckNoLeaks() const {
  return key_id_to_subscribers_.size() == 0 && subscribers_to_key_id_.size() == 0;
}

void SubscriberState::ConnectToSubscriber(const rpc::PubsubLongPollingRequest &request,
                                          rpc::PubsubLongPollingReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  if (long_polling_connection_) {
    // Because of the new long polling request, flush the current polling request with an
    // empty reply.
    PublishIfPossible(/*force_noop=*/true);
  }
  RAY_CHECK(!long_polling_connection_);
  RAY_CHECK(reply != nullptr);
  RAY_CHECK(send_reply_callback != nullptr);
  long_polling_connection_ =
      std::make_unique<LongPollConnection>(reply, std::move(send_reply_callback));
  last_connection_update_time_ms_ = get_time_ms_();
  PublishIfPossible();
}

void SubscriberState::QueueMessage(const rpc::PubMessage &pub_message, bool try_publish) {
  if (mailbox_.empty() || mailbox_.back()->pub_messages_size() >= publish_batch_size_) {
    mailbox_.push(std::make_unique<rpc::PubsubLongPollingReply>());
  }

  // Update the long polling reply.
  auto *next_long_polling_reply = mailbox_.back().get();
  auto *new_pub_message = next_long_polling_reply->add_pub_messages();
  new_pub_message->CopyFrom(pub_message);

  if (try_publish) {
    PublishIfPossible();
  }
}

bool SubscriberState::PublishIfPossible(bool force_noop) {
  if (!long_polling_connection_) {
    return false;
  }
  if (!force_noop && mailbox_.empty()) {
    return false;
  }

  // No message should have been added to the reply.
  RAY_CHECK(long_polling_connection_->reply->pub_messages().empty());
  if (!force_noop) {
    // Reply to the long polling subscriber. Swap the reply here to avoid extra copy.
    long_polling_connection_->reply->Swap(mailbox_.front().get());
    mailbox_.pop();
  }
  long_polling_connection_->send_reply_callback(Status::OK(), nullptr, nullptr);

  // Clean up & update metadata.
  long_polling_connection_.reset();
  last_connection_update_time_ms_ = get_time_ms_();
  return true;
}

bool SubscriberState::CheckNoLeaks() const {
  // If all message in the mailbox has been replied, consider there is no leak.
  return !long_polling_connection_ && mailbox_.empty();
}

bool SubscriberState::ConnectionExists() const {
  return long_polling_connection_ != nullptr;
}

bool SubscriberState::IsActive() const {
  return get_time_ms_() - last_connection_update_time_ms_ < connection_timeout_ms_;
}

}  // namespace pub_internal

void Publisher::ConnectToSubscriber(const rpc::PubsubLongPollingRequest &request,
                                    rpc::PubsubLongPollingReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(reply != nullptr);
  RAY_CHECK(send_reply_callback != nullptr);

  const auto subscriber_id = SubscriberID::FromBinary(request.subscriber_id());
  RAY_LOG(DEBUG) << "Long polling connection initiated by " << subscriber_id.Hex();
  absl::MutexLock lock(&mutex_);
  auto it = subscribers_.find(subscriber_id);
  if (it == subscribers_.end()) {
    it = subscribers_
             .emplace(
                 subscriber_id,
                 std::make_shared<pub_internal::SubscriberState>(subscriber_id,
                                                                 get_time_ms_,
                                                                 subscriber_timeout_ms_,
                                                                 publish_batch_size_))
             .first;
  }
  auto &subscriber = it->second;

  // May flush the current long poll with an empty message, if a poll request exists.
  subscriber->ConnectToSubscriber(request, reply, std::move(send_reply_callback));
}

bool Publisher::RegisterSubscription(const rpc::ChannelType channel_type,
                                     const SubscriberID &subscriber_id,
                                     const std::optional<std::string> &key_id) {
  absl::MutexLock lock(&mutex_);
  if (!subscribers_.contains(subscriber_id)) {
    subscribers_.emplace(
        subscriber_id,
        std::make_shared<pub_internal::SubscriberState>(
            subscriber_id, get_time_ms_, subscriber_timeout_ms_, publish_batch_size_));
  }
  auto subscription_index_it = subscription_index_map_.find(channel_type);
  RAY_CHECK(subscription_index_it != subscription_index_map_.end());
  return subscription_index_it->second.AddEntry(key_id.value_or(""), subscriber_id);
}

void Publisher::Publish(const rpc::PubMessage &pub_message) {
  const auto channel_type = pub_message.channel_type();
  absl::MutexLock lock(&mutex_);
  auto subscription_index_it = subscription_index_map_.find(channel_type);
  RAY_CHECK(subscription_index_it != subscription_index_map_.end());
  // TODO(sang): Currently messages are lost if publish happens
  // before there's any subscriber for the object.
  const auto subscribers =
      subscription_index_it->second.GetSubscriberIdsByKeyId(pub_message.key_id());
  if (subscribers.empty()) {
    return;
  }

  cum_pub_message_cnt_[channel_type]++;

  for (const auto &subscriber_id : subscribers) {
    auto it = subscribers_.find(subscriber_id);
    RAY_CHECK(it != subscribers_.end());
    auto &subscriber = it->second;
    subscriber->QueueMessage(pub_message);
  }
}

void Publisher::PublishFailure(const rpc::ChannelType channel_type,
                               const std::string &key_id) {
  rpc::PubMessage pub_message;
  pub_message.set_key_id(key_id);
  pub_message.set_channel_type(channel_type);
  pub_message.mutable_failure_message();
  Publish(pub_message);
}

bool Publisher::UnregisterSubscription(const rpc::ChannelType channel_type,
                                       const SubscriberID &subscriber_id,
                                       const std::optional<std::string> &key_id) {
  absl::MutexLock lock(&mutex_);
  auto subscription_index_it = subscription_index_map_.find(channel_type);
  RAY_CHECK(subscription_index_it != subscription_index_map_.end());
  return subscription_index_it->second.EraseEntry(key_id.value_or(""), subscriber_id);
}

bool Publisher::UnregisterSubscriber(const SubscriberID &subscriber_id) {
  absl::MutexLock lock(&mutex_);
  return UnregisterSubscriberInternal(subscriber_id);
}

void Publisher::UnregisterAll() {
  absl::MutexLock lock(&mutex_);
  // Save the subscriber IDs to be removed, because UnregisterSubscriberInternal()
  // erases from subscribers_.
  std::vector<SubscriberID> ids;
  for (const auto &[id, subscriber] : subscribers_) {
    ids.push_back(id);
  }
  for (const auto &id : ids) {
    UnregisterSubscriberInternal(id);
  }
}

int Publisher::UnregisterSubscriberInternal(const SubscriberID &subscriber_id) {
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
  // Flush the long polling connection because otherwise the reply could be leaked.
  subscriber->PublishIfPossible(/*force_noop=*/true);
  subscribers_.erase(it);
  return erased;
}

void Publisher::CheckDeadSubscribers() {
  absl::MutexLock lock(&mutex_);
  std::vector<SubscriberID> dead_subscribers;

  for (const auto &it : subscribers_) {
    const auto &subscriber = it.second;
    if (subscriber->IsActive()) {
      continue;
    }
    // Subscriber has no activity for a while. It is considered timed out now.
    if (subscriber->ConnectionExists()) {
      // Release the long polling connection with a noop. The subscriber will poll again
      // if it is still alive. This also refreshes the last active time.
      subscriber->PublishIfPossible(/*force_noop*/ true);
    } else {
      // Subscriber state can be deleted since it does not have any active connection.
      dead_subscribers.push_back(it.first);
    }
  }

  for (const auto &subscriber_id : dead_subscribers) {
    UnregisterSubscriberInternal(subscriber_id);
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

std::string Publisher::DebugString() const {
  absl::MutexLock lock(&mutex_);
  std::stringstream result;
  result << "Publisher:";
  for (const auto &it : cum_pub_message_cnt_) {
    auto channel_type = it.first;
    const google::protobuf::EnumDescriptor *descriptor = rpc::ChannelType_descriptor();
    const auto &channel_name = descriptor->FindValueByNumber(channel_type)->name();
    result << "\n" << channel_name;
    result << "\n- cumulative published messages: " << it.second;
  }
  return result.str();
}

}  // namespace pubsub

}  // namespace ray
