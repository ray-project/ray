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

#include "ray/core_worker/pubsub/pubsub_coordinator.h"

namespace ray {

void SubscriptionIndex::AddEntry(const ObjectID &object_id, const NodeID &subscriber_id) {
  auto &subscribing_objects = subscribers_to_objects_[subscriber_id];
  RAY_CHECK(subscribing_objects.emplace(object_id).second);
  auto &subscriber_map = objects_to_subscribers_[object_id];
  RAY_CHECK(subscriber_map.emplace(subscriber_id).second);
}

const absl::flat_hash_set<NodeID> &SubscriptionIndex::GetSubscriberIdsByObjectId(
    const ObjectID &object_id) {
  auto it = objects_to_subscribers_.find(object_id);
  RAY_CHECK(it != objects_to_subscribers_.end());
  return it->second;
}

bool SubscriptionIndex::IsObjectIdExist(const ObjectID &object_id) const {
  return objects_to_subscribers_.count(object_id) > 0;
}

bool SubscriptionIndex::IsSubscriberExist(const NodeID &subscriber_id) const {
  return subscribers_to_objects_.count(subscriber_id) > 0;
}

int SubscriptionIndex::EraseSubscriber(const NodeID &subscriber_id) {
  auto subscribing_objects_it = subscribers_to_objects_.find(subscriber_id);
  if (subscribing_objects_it == subscribers_to_objects_.end()) {
    return 0;
  }

  auto &subscribing_objects = subscribing_objects_it->second;
  for (auto object_id_it = subscribing_objects.begin();
       object_id_it != subscribing_objects.end();) {
    auto current = object_id_it++;
    const auto &object_id = *current;
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
  return 1;
}

int SubscriptionIndex::EraseEntry(const ObjectID &object_id,
                                  const NodeID &subscriber_id) {
  // Erase from subscribers_to_objects_;
  auto subscribers_to_objects_it = subscribers_to_objects_.find(subscriber_id);
  if (subscribers_to_objects_it == subscribers_to_objects_.end()) {
    return 0;
  }
  auto &objects = subscribers_to_objects_it->second;
  auto object_it = objects.find(object_id);
  if (object_it == objects.end()) {
    RAY_CHECK(objects_to_subscribers_.count(object_id) == 0);
    return 0;
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
  return 1;
}

bool Subscriber::Connect(LongPollConnectCallback long_polling_reply_callback) {
  if (long_polling_reply_callback_ == nullptr) {
    long_polling_reply_callback_ = long_polling_reply_callback;
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
    long_polling_reply_callback_(mailbox_);
    long_polling_reply_callback_ = nullptr;
    mailbox_.clear();
    return true;
  }
  return false;
}

void PubsubCoordinator::Connect(const NodeID &subscriber_node_id,
                                LongPollConnectCallback long_poll_connect_callback) {
  absl::MutexLock lock(&mutex_);
  RAY_LOG(DEBUG) << "Long polling connection is initiated by " << subscriber_node_id;
  RAY_CHECK(long_poll_connect_callback != nullptr);

  if (is_node_dead_(subscriber_node_id)) {
    std::vector<ObjectID> empty_ids;
    // Reply to the request so that it is properly GC'ed.
    long_poll_connect_callback(empty_ids);
    return;
  }

  if (subscribers_.count(subscriber_node_id) == 0) {
    subscribers_.emplace(subscriber_node_id, std::make_shared<Subscriber>());
  }
  auto subscriber = subscribers_[subscriber_node_id];

  // Since the long polling connection is synchronous between the client and coordinator,
  // when it connects, the connection shouldn't have existed.
  RAY_CHECK(subscriber->Connect(std::move(long_poll_connect_callback)));
  subscriber->PublishIfPossible();
}

void PubsubCoordinator::RegisterSubscription(const NodeID &subscriber_node_id,
                                             const ObjectID &object_id) {
  absl::MutexLock lock(&mutex_);
  RAY_LOG(DEBUG) << "object id " << object_id << " is subscribed by "
                 << subscriber_node_id;
  if (is_node_dead_(subscriber_node_id)) {
    return;
  }

  if (subscribers_.count(subscriber_node_id) == 0) {
    subscribers_.emplace(subscriber_node_id, std::make_shared<Subscriber>());
  }
  subscription_index_.AddEntry(object_id, subscriber_node_id);
}

void PubsubCoordinator::Publish(const ObjectID &object_id) {
  absl::MutexLock lock(&mutex_);
  for (const auto &subscriber_id :
       subscription_index_.GetSubscriberIdsByObjectId(object_id)) {
    auto it = subscribers_.find(subscriber_id);
    RAY_CHECK(it != subscribers_.end());
    auto subscriber = it->second;
    subscriber->QueueMessage(object_id);
  }
}

int PubsubCoordinator::UnregisterSubscriber(const NodeID &subscriber_node_id) {
  absl::MutexLock lock(&mutex_);
  int erased = subscription_index_.EraseSubscriber(subscriber_node_id);
  // Publish messages before removing the entry. Otherwise, it can have memory leak.
  auto it = subscribers_.find(subscriber_node_id);
  if (it == subscribers_.end()) {
    return erased;
  }
  auto subscriber = it->second;
  subscriber->PublishIfPossible(/*force=*/true);
  subscribers_.erase(it);
  return erased;
}

int PubsubCoordinator::UnregisterSubscription(const NodeID &subscriber_node_id,
                                              const ObjectID &object_id) {
  absl::MutexLock lock(&mutex_);
  return subscription_index_.EraseEntry(object_id, subscriber_node_id);
}

}  // namespace ray
