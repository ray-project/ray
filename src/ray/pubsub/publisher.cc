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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/ray_config.h"

namespace ray {

namespace pubsub {

namespace pub_internal {

bool EntityState::Publish(std::shared_ptr<rpc::PubMessage> msg, size_t msg_size) {
  if (subscribers_.empty()) {
    return false;
  }

  if (msg_size > max_message_size_bytes_) {
    RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 10000)
        << "Pub/sub message exceeds max individual message "
           "size="
        << absl::StrCat(max_message_size_bytes_, "B, ")
        << absl::StrCat("incoming msg size=", msg_size, "B")
        << ". Dropping this message:\n"
        << msg->DebugString();
    return false;
  }

  while (!pending_messages_.empty()) {
    // NOTE: if atomic ref counting becomes too expensive, it should be possible
    // to implement inflight message tracking across subscribers with non-atomic
    // ref-counting or with a LRU-like data structure tracking the range of buffered
    // messages for each subscriber.
    auto front_msg = pending_messages_.front().lock();
    if (front_msg == nullptr) {
      // The message has no other reference.
      // This means that it has been published to all subscribers.
    } else if (max_buffered_bytes_ > 0 &&
               total_size_ + msg_size > static_cast<size_t>(max_buffered_bytes_)) {
      RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 10000)
          << "Pub/sub message is dropped to stay under the maximum configured buffer "
             "size="
          << absl::StrCat(max_buffered_bytes_, "B, ")
          << absl::StrCat("incoming msg size=",
                          msg_size,
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

    // The first message in the queue has been published to all subscribers, or
    // it has been dropped due to memory cap. Subtract it from memory
    // accounting.
    pending_messages_.pop();
    total_size_ -= message_sizes_.front();
    message_sizes_.pop();
  }

  pending_messages_.push(msg);
  total_size_ += msg_size;
  message_sizes_.push(msg_size);

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
    : channel_type_(channel_type),
      subscribers_to_all_(CreateEntityState(channel_type_)) {}

int64_t SubscriptionIndex::GetNumBufferedBytes() const {
  // TODO(swang): Some messages may get published to both subscribers listening
  // for any message and subscribers listening for a specific key. The pubsub
  // channels share the same message buffer, so these messages will currently
  // be double-counted when adding up the total number of buffered bytes.
  int64_t num_bytes_buffered = subscribers_to_all_->GetNumBufferedBytes();
  for (const auto &[key_id, entity_state] : entities_) {
    num_bytes_buffered += entity_state->GetNumBufferedBytes();
  }
  return num_bytes_buffered;
}

bool SubscriptionIndex::Publish(std::shared_ptr<rpc::PubMessage> pub_message,
                                size_t msg_size) {
  const bool publish_to_all = subscribers_to_all_->Publish(pub_message, msg_size);
  bool publish_to_entity = false;
  auto it = entities_.find(pub_message->key_id());
  if (it != entities_.end()) {
    publish_to_entity = it->second->Publish(pub_message, msg_size);
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
    sub_it = entities_.emplace(key_id, CreateEntityState(channel_type_)).first;
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

std::unique_ptr<EntityState> SubscriptionIndex::CreateEntityState(
    rpc::ChannelType channel_type) {
  switch (channel_type) {
  case rpc::ChannelType::RAY_ERROR_INFO_CHANNEL:
  case rpc::ChannelType::RAY_LOG_CHANNEL:
  case rpc::ChannelType::RAY_NODE_RESOURCE_USAGE_CHANNEL:
    return std::make_unique<EntityState>(
        RayConfig::instance().max_grpc_message_size(),
        RayConfig::instance().publisher_entity_buffer_max_bytes());

  case rpc::ChannelType::WORKER_OBJECT_EVICTION:
  case rpc::ChannelType::WORKER_REF_REMOVED_CHANNEL:
  case rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL:
  case rpc::ChannelType::GCS_ACTOR_CHANNEL:
  case rpc::ChannelType::GCS_JOB_CHANNEL:
  case rpc::ChannelType::GCS_NODE_INFO_CHANNEL:
  case rpc::ChannelType::GCS_WORKER_DELTA_CHANNEL:
    return std::make_unique<EntityState>(RayConfig::instance().max_grpc_message_size(),
                                         /*max_buffered_bytes=*/-1);

  default:
    RAY_LOG(FATAL) << "Unexpected channel type: " << rpc::ChannelType_Name(channel_type);
    return nullptr;
  }
}

void SubscriberState::ConnectToSubscriber(const rpc::PubsubLongPollingRequest &request,
                                          rpc::PubsubLongPollingReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  int64_t max_processed_sequence_id = request.max_processed_sequence_id();
  if (request.publisher_id().empty() ||
      publisher_id_ != PublisherID::FromBinary(request.publisher_id())) {
    // in case the publisher_id mismatches, we should ignore the
    // max_processed_sequence_id.
    max_processed_sequence_id = 0;
  }

  // clean up messages that have already been processed.
  while (!mailbox_.empty() &&
         mailbox_.front()->sequence_id() <= max_processed_sequence_id) {
    mailbox_.pop_front();
  }

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
  RAY_LOG(DEBUG) << "enqueue: " << pub_message->sequence_id();
  mailbox_.push_back(pub_message);
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
  *long_polling_connection_->reply->mutable_publisher_id() = publisher_id_.Binary();
  int64_t num_total_bytes = 0;
  if (!force_noop) {
    for (auto it = mailbox_.begin(); it != mailbox_.end(); it++) {
      if (long_polling_connection_->reply->pub_messages().size() >= publish_batch_size_) {
        break;
      }

      const rpc::PubMessage &msg = **it;

      int64_t msg_size_bytes = msg.ByteSizeLong();
      if (num_total_bytes > 0 &&
          num_total_bytes + msg_size_bytes >
              static_cast<int64_t>(RayConfig::instance().max_grpc_message_size())) {
        // Adding this message to the batch would put us over the serialization
        // size threshold.
        break;
      }
      num_total_bytes += msg_size_bytes;

      // Avoid sending empty message to the subscriber. The message might have been
      // cleared because the subscribed entity's buffer was full.
      if (msg.inner_message_case() != rpc::PubMessage::INNER_MESSAGE_NOT_SET) {
        *long_polling_connection_->reply->add_pub_messages() = msg;
      }
    }
  }

  RAY_LOG(DEBUG) << "sending reply back"
                 << long_polling_connection_->reply->DebugString();
  long_polling_connection_->send_reply_callback(Status::OK(), nullptr, nullptr);

  // Clean up & update metadata.
  long_polling_connection_.reset();
  // Clean up & update metadata.
  last_connection_update_time_ms_ = get_time_ms_();
  return true;
}

bool SubscriberState::CheckNoLeaks() const {
  // If all message in the mailbox has been replied, consider there is no leak.
  return mailbox_.empty();
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
  RAY_LOG(DEBUG) << "Long polling connection initiated by " << subscriber_id.Hex()
                 << ", publisher_id " << publisher_id_.Hex();
  absl::MutexLock lock(&mutex_);
  auto it = subscribers_.find(subscriber_id);
  if (it == subscribers_.end()) {
    it = subscribers_
             .emplace(
                 subscriber_id,
                 std::make_unique<pub_internal::SubscriberState>(subscriber_id,
                                                                 get_time_ms_,
                                                                 subscriber_timeout_ms_,
                                                                 publish_batch_size_,
                                                                 publisher_id_))
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
                                                                 publish_batch_size_,
                                                                 publisher_id_))
             .first;
  }
  pub_internal::SubscriberState *subscriber = it->second.get();
  auto subscription_index_it = subscription_index_map_.find(channel_type);
  RAY_CHECK(subscription_index_it != subscription_index_map_.end());
  return subscription_index_it->second.AddEntry(key_id.value_or(""), subscriber);
}

void Publisher::Publish(rpc::PubMessage pub_message) {
  RAY_CHECK_EQ(pub_message.sequence_id(), 0) << "sequence_id should not be set;";
  const auto channel_type = pub_message.channel_type();
  absl::MutexLock lock(&mutex_);
  auto &subscription_index = subscription_index_map_.at(channel_type);
  // TODO(sang): Currently messages are lost if publish happens
  // before there's any subscriber for the object.
  pub_message.set_sequence_id(++next_sequence_id_);

  const size_t msg_size = pub_message.ByteSizeLong();
  cum_pub_message_cnt_[channel_type]++;
  cum_pub_message_bytes_cnt_[channel_type] += msg_size;

  subscription_index.Publish(std::make_shared<rpc::PubMessage>(std::move(pub_message)),
                             msg_size);
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
  RAY_LOG(DEBUG) << "Unregistering subscriber " << subscriber_id.Hex();
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

    auto bytes_count_it = cum_pub_message_bytes_cnt_.find(channel_type);
    if (bytes_count_it != cum_pub_message_bytes_cnt_.end()) {
      result << "\n- cumulative published bytes: " << bytes_count_it->second;
    }

    auto index_it = subscription_index_map_.find(channel_type);
    if (index_it != subscription_index_map_.end()) {
      result << "\n- current buffered bytes: " << index_it->second.GetNumBufferedBytes();
    }
  }
  return result.str();
}

}  // namespace pubsub

}  // namespace ray
