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

#include "ray/common/ray_config.h"

namespace ray {

namespace pubsub {

namespace pub_internal {

bool BasicEntityState::Publish(const rpc::PubMessage &pub_message) {
  if (subscribers_.empty()) {
    return false;
  }
  const auto msg = std::make_shared<rpc::PubMessage>(pub_message);
  for (auto &[id, subscriber] : subscribers_) {
    subscriber->QueueMessage(msg);
  }
  return true;
}

bool CappedEntityState::Publish(const rpc::PubMessage &pub_message) {
  if (subscribers_.empty()) {
    return false;
  }

  const int64_t message_size = pub_message.ByteSizeLong();

  while (!pending_messages_.empty()) {
    // NOTE: if atomic ref counting becomes too expensive, it should be possible
    // to implement inflight message tracking across subscribers with non-atomic
    // ref-counting or with a LRU-like data structure tracking the range of buffered
    // messages for each subscriber.
    auto front_msg = pending_messages_.front().lock();
    if (front_msg == nullptr) {
      // The message has no other reference.
    } else if (total_size_ + message_size >
               RayConfig::instance().publisher_entity_buffer_max_bytes()) {
      RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 10000)
          << "Pub/sub message is dropped to stay under the maximum configured buffer "
             "size="
          << absl::StrCat(RayConfig::instance().publisher_entity_buffer_max_bytes(),
                          "B, ")
          << absl::StrCat("incoming msg size=",
                          message_size,
                          "B, current buffer size=",
                          total_size_,
                          "B")
          << ". Dropping the oldest message:\n"
          << front_msg->DebugString();
      // Clear the oldest message first, because presumably newer messages are more
      // useful. Clearing the shared message should be ok, since Publisher is single
      // threaded. NOTE: calling Clear() does not release memory from the underlying
      // protobuf message object.
      *front_msg = rpc::PubMessage();
    } else {
      // No message to drop.
      break;
    }

    pending_messages_.pop();
    total_size_ -= message_sizes_.front();
    message_sizes_.pop();
  }

  const auto msg = std::make_shared<rpc::PubMessage>(pub_message);
  pending_messages_.push(msg);
  total_size_ += message_size;
  message_sizes_.push(message_size);

  for (auto &[id, subscriber] : subscribers_) {
    subscriber->QueueMessage(msg);
  }
  return true;
}

bool EntityState::AddSubscriber(SubscriberState *subscriber) {
  return subscribers_.emplace(subscriber->id(), subscriber).second;
}

bool EntityState::RemoveSubscriber(const SubscriberID &id) {
  return subscribers_.erase(id) > 0;
}

const absl::flat_hash_map<SubscriberID, SubscriberState *> &EntityState::Subscribers()
    const {
  return subscribers_;
}

SubscriptionIndex::SubscriptionIndex(rpc::ChannelType channel_type)
    : channel_type_(channel_type), subscribers_to_all_(CreateEntityState()) {}

bool SubscriptionIndex::Publish(const rpc::PubMessage &pub_message) {
  const bool publish_to_all = subscribers_to_all_->Publish(pub_message);
  bool publish_to_entity = false;
  auto it = entities_.find(pub_message.key_id());
  if (it != entities_.end()) {
    publish_to_entity = it->second->Publish(pub_message);
  }
  return publish_to_all || publish_to_entity;
}

bool SubscriptionIndex::AddEntry(const std::string &key_id, SubscriberState *subscriber) {
  if (key_id.empty()) {
    return subscribers_to_all_->AddSubscriber(subscriber);
  }

  auto &subscribing_key_ids = subscribers_to_key_id_[subscriber->id()];
  const bool key_added = subscribing_key_ids.emplace(key_id).second;

  auto sub_it = entities_.find(key_id);
  if (sub_it == entities_.end()) {
    sub_it = entities_.emplace(key_id, CreateEntityState()).first;
  }
  const bool subscriber_added = sub_it->second->AddSubscriber(subscriber);

  RAY_CHECK(key_added == subscriber_added);
  return key_added;
}

std::vector<SubscriberID> SubscriptionIndex::GetSubscriberIdsByKeyId(
    const std::string &key_id) const {
  std::vector<SubscriberID> subscribers;
  if (!subscribers_to_all_->Subscribers().empty()) {
    for (const auto &[sub_id, sub] : subscribers_to_all_->Subscribers()) {
      subscribers.push_back(sub_id);
    }
  }
  auto it = entities_.find(key_id);
  if (it != entities_.end()) {
    for (const auto &[sub_id, sub] : it->second->Subscribers()) {
      subscribers.push_back(sub_id);
    }
  }
  return subscribers;
}

bool SubscriptionIndex::EraseSubscriber(const SubscriberID &subscriber_id) {
  // Erase subscriber of all keys.
  if (subscribers_to_all_->RemoveSubscriber(subscriber_id)) {
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
    auto entity_it = entities_.find(key_id);
    if (entity_it == entities_.end()) {
      continue;
    }
    auto &entity = *entity_it->second;
    entity.RemoveSubscriber(subscriber_id);
    if (entity.Subscribers().empty()) {
      entities_.erase(entity_it);
    }
  }
  subscribers_to_key_id_.erase(subscribing_key_it);
  return true;
}

bool SubscriptionIndex::EraseEntry(const std::string &key_id,
                                   const SubscriberID &subscriber_id) {
  // Erase the subscriber of all keys.
  if (key_id.empty()) {
    return subscribers_to_all_->RemoveSubscriber(subscriber_id);
  }

  // Erase keys from the subscriber of individual keys.
  auto subscribers_to_message_it = subscribers_to_key_id_.find(subscriber_id);
  if (subscribers_to_message_it == subscribers_to_key_id_.end()) {
    return false;
  }
  auto &objects = subscribers_to_message_it->second;
  auto object_it = objects.find(key_id);
  if (object_it == objects.end()) {
    auto it = entities_.find(key_id);
    if (it != entities_.end()) {
      RAY_CHECK(!it->second->Subscribers().contains(subscriber_id));
    }
    return false;
  }
  objects.erase(object_it);
  if (objects.empty()) {
    subscribers_to_key_id_.erase(subscribers_to_message_it);
  }

  // Erase subscribers from keys (reverse index).
  auto entity_it = entities_.find(key_id);
  // If code reaches this line, that means the object id was in the index.
  RAY_CHECK(entity_it != entities_.end());
  auto &entity = *entity_it->second;
  // If code reaches this line, that means the subscriber id was in the index.
  RAY_CHECK(entity.RemoveSubscriber(subscriber_id));
  if (entity.Subscribers().empty()) {
    entities_.erase(entity_it);
  }
  return true;
}

bool SubscriptionIndex::HasKeyId(const std::string &key_id) const {
  return entities_.contains(key_id);
}

bool SubscriptionIndex::HasSubscriber(const SubscriberID &subscriber_id) const {
  if (subscribers_to_all_->Subscribers().contains(subscriber_id)) {
    return true;
  }
  return subscribers_to_key_id_.contains(subscriber_id);
}

bool SubscriptionIndex::CheckNoLeaks() const {
  return entities_.empty() && subscribers_to_key_id_.empty();
}

std::unique_ptr<EntityState> SubscriptionIndex::CreateEntityState() {
  switch (channel_type_) {
  case rpc::ChannelType::RAY_ERROR_INFO_CHANNEL:
  case rpc::ChannelType::RAY_LOG_CHANNEL: {
    return std::make_unique<CappedEntityState>();
  }
  default:
    return std::make_unique<BasicEntityState>();
  }
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

void SubscriberState::QueueMessage(const std::shared_ptr<rpc::PubMessage> &pub_message,
                                   bool try_publish) {
  mailbox_.push(pub_message);
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
    for (int i = 0; i < publish_batch_size_ && !mailbox_.empty(); ++i) {
      const rpc::PubMessage &msg = *mailbox_.front();
      // Avoid sending empty message to the subscriber. The message might have been
      // cleared because the subscribed entity's buffer was full.
      if (msg.inner_message_case() != rpc::PubMessage::INNER_MESSAGE_NOT_SET) {
        *long_polling_connection_->reply->add_pub_messages() = msg;
      }
      mailbox_.pop();
    }
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
                 std::make_unique<pub_internal::SubscriberState>(subscriber_id,
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
  auto it = subscribers_.find(subscriber_id);
  if (it == subscribers_.end()) {
    it = subscribers_
             .emplace(
                 subscriber_id,
                 std::make_unique<pub_internal::SubscriberState>(subscriber_id,
                                                                 get_time_ms_,
                                                                 subscriber_timeout_ms_,
                                                                 publish_batch_size_))
             .first;
  }
  pub_internal::SubscriberState *subscriber = it->second.get();
  auto subscription_index_it = subscription_index_map_.find(channel_type);
  RAY_CHECK(subscription_index_it != subscription_index_map_.end());
  return subscription_index_it->second.AddEntry(key_id.value_or(""), subscriber);
}

void Publisher::Publish(const rpc::PubMessage &pub_message) {
  const auto channel_type = pub_message.channel_type();
  absl::MutexLock lock(&mutex_);
  auto &subscription_index = subscription_index_map_.at(channel_type);
  // TODO(sang): Currently messages are lost if publish happens
  // before there's any subscriber for the object.
  subscription_index.Publish(pub_message);
  cum_pub_message_cnt_[channel_type]++;
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
