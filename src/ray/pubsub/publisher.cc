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

void SubscriptionIndex::AddEntry(const ObjectID &object_id,
                                 const SubscriberID &subscriber_id) {
  auto &subscribing_objects = subscribers_to_objects_[subscriber_id];
  RAY_CHECK(subscribing_objects.emplace(object_id).second);
  auto &subscriber_map = objects_to_subscribers_[object_id];
  RAY_CHECK(subscriber_map.emplace(subscriber_id).second);
}

absl::optional<std::reference_wrapper<const absl::flat_hash_set<SubscriberID>>>
SubscriptionIndex::GetSubscriberIdsByObjectId(const ObjectID &object_id) const {
  auto it = objects_to_subscribers_.find(object_id);
  if (it == objects_to_subscribers_.end()) {
    return absl::nullopt;
  }
  return absl::optional<std::reference_wrapper<const absl::flat_hash_set<SubscriberID>>>{
      std::ref(it->second)};
}

bool SubscriptionIndex::HasObjectId(const ObjectID &object_id) const {
  return objects_to_subscribers_.count(object_id);
}

bool SubscriptionIndex::HasSubscriber(const SubscriberID &subscriber_id) const {
  return subscribers_to_objects_.count(subscriber_id);
}

bool SubscriptionIndex::EraseSubscriber(const SubscriberID &subscriber_id) {
  auto subscribing_objects_it = subscribers_to_objects_.find(subscriber_id);
  if (subscribing_objects_it == subscribers_to_objects_.end()) {
    return false;
  }

  auto &subscribing_objects = subscribing_objects_it->second;
  for (const auto &object_id : subscribing_objects) {
    // Erase the subscriber from the object map.
    auto subscribers_it = objects_to_subscribers_.find(object_id);
    if (subscribers_it == objects_to_subscribers_.end()) {
      continue;
    }
    auto &subscribers = subscribers_it->second;
    subscribers.erase(subscriber_id);
    if (subscribers.size() == 0) {
      objects_to_subscribers_.erase(subscribers_it);
    }
  }
  subscribers_to_objects_.erase(subscribing_objects_it);
  return true;
}

bool SubscriptionIndex::EraseEntry(const ObjectID &object_id,
                                   const SubscriberID &subscriber_id) {
  // Erase from subscribers_to_objects_;
  auto subscribers_to_objects_it = subscribers_to_objects_.find(subscriber_id);
  if (subscribers_to_objects_it == subscribers_to_objects_.end()) {
    return false;
  }
  auto &objects = subscribers_to_objects_it->second;
  auto object_it = objects.find(object_id);
  if (object_it == objects.end()) {
    RAY_CHECK(objects_to_subscribers_.count(object_id) == 0);
    return false;
  }
  objects.erase(object_it);
  if (objects.size() == 0) {
    subscribers_to_objects_.erase(subscribers_to_objects_it);
  }

  // Erase from objects_to_subscribers_.
  auto objects_to_subscribers_it = objects_to_subscribers_.find(object_id);
  // If code reaches this line, that means the object id was in the index.
  RAY_CHECK(objects_to_subscribers_it != objects_to_subscribers_.end());
  auto &subscribers = objects_to_subscribers_it->second;
  auto subscriber_it = subscribers.find(subscriber_id);
  // If code reaches this line, that means the subscriber id was in the index.
  RAY_CHECK(subscriber_it != subscribers.end());
  subscribers.erase(subscriber_it);
  if (subscribers.size() == 0) {
    objects_to_subscribers_.erase(objects_to_subscribers_it);
  }
  return true;
}

bool SubscriptionIndex::AssertNoLeak() const {
  return objects_to_subscribers_.size() == 0 && subscribers_to_objects_.size() == 0;
}

bool Subscriber::ConnectToSubscriber(
    LongPollConnectCallback long_polling_reply_callback) {
  if (long_polling_reply_callback_ == nullptr) {
    long_polling_reply_callback_ = long_polling_reply_callback;
    last_connection_update_time_ms_ = get_time_ms_();
    return true;
  }
  return false;
}

void Subscriber::QueueMessage(const ObjectID &object_id, bool try_publish) {
  mailbox_.push_back(object_id);
  if (try_publish) {
    PublishIfPossible();
  }
}

bool Subscriber::PublishIfPossible(bool force) {
  if (long_polling_reply_callback_ == nullptr) {
    return false;
  }

  if (force || mailbox_.size() > 0) {
    std::vector<ObjectID> mails_to_post;
    while (mailbox_.size() > 0 && mails_to_post.size() < publish_batch_size_) {
      // It ensures that mails are posted in the FIFO order.
      mails_to_post.push_back(mailbox_.front());
      mailbox_.pop_front();
    }
    long_polling_reply_callback_(mails_to_post);
    long_polling_reply_callback_ = nullptr;
    last_connection_update_time_ms_ = get_time_ms_();
    return true;
  }
  return false;
}

bool Subscriber::AssertNoLeak() const {
  return long_polling_reply_callback_ == nullptr && mailbox_.size() == 0;
}

bool Subscriber::IsDisconnected() const {
  return long_polling_reply_callback_ == nullptr &&
         get_time_ms_() - last_connection_update_time_ms_ >= connection_timeout_ms_;
}

bool Subscriber::IsActiveConnectionTimedOut() const {
  return long_polling_reply_callback_ != nullptr &&
         get_time_ms_() - last_connection_update_time_ms_ >= connection_timeout_ms_;
}

}  // namespace pub_internal

void Publisher::ConnectToSubscriber(const SubscriberID &subscriber_id,
                                    LongPollConnectCallback long_poll_connect_callback) {
  RAY_LOG(DEBUG) << "Long polling connection initiated by " << subscriber_id;
  RAY_CHECK(long_poll_connect_callback != nullptr);

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
  RAY_CHECK(subscriber->ConnectToSubscriber(std::move(long_poll_connect_callback)));
  subscriber->PublishIfPossible();
}

void Publisher::RegisterSubscription(const SubscriberID &subscriber_id,
                                     const ObjectID &object_id) {
  RAY_LOG(DEBUG) << "object id " << object_id << " is subscribed by " << subscriber_id;

  absl::MutexLock lock(&mutex_);
  if (subscribers_.count(subscriber_id) == 0) {
    subscribers_.emplace(subscriber_id,
                         std::make_shared<pub_internal::Subscriber>(
                             get_time_ms_, subscriber_timeout_ms_, publish_batch_size_));
  }
  subscription_index_.AddEntry(object_id, subscriber_id);
}

void Publisher::Publish(const ObjectID &object_id) {
  absl::MutexLock lock(&mutex_);
  // TODO(sang): Currently messages are lost if publish happens
  // before there's any subscriber for the object.
  auto maybe_subscribers = subscription_index_.GetSubscriberIdsByObjectId(object_id);
  if (!maybe_subscribers.has_value()) {
    return;
  }

  for (const auto &subscriber_id : maybe_subscribers.value().get()) {
    auto it = subscribers_.find(subscriber_id);
    RAY_CHECK(it != subscribers_.end());
    auto &subscriber = it->second;
    subscriber->QueueMessage(object_id);
  }
}

bool Publisher::UnregisterSubscriber(const SubscriberID &subscriber_id) {
  absl::MutexLock lock(&mutex_);
  return UnregisterSubscriberInternal(subscriber_id);
}

bool Publisher::UnregisterSubscription(const SubscriberID &subscriber_id,
                                       const ObjectID &object_id) {
  absl::MutexLock lock(&mutex_);
  return subscription_index_.EraseEntry(object_id, subscriber_id);
}

bool Publisher::UnregisterSubscriberInternal(const SubscriberID &subscriber_id) {
  int erased = subscription_index_.EraseSubscriber(subscriber_id);
  // Publish messages before removing the entry. Otherwise, it can have memory leak.
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

bool Publisher::AssertNoLeak() const {
  absl::MutexLock lock(&mutex_);
  for (const auto &subscriber : subscribers_) {
    if (!subscriber.second->AssertNoLeak()) {
      return false;
    }
  }
  return subscription_index_.AssertNoLeak();
}

}  // namespace pubsub

}  // namespace ray
