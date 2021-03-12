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

absl::flat_hash_set<NodeID> &SubscriptionIndex::GetSubscriberIdsByObjectId(
    const ObjectID &object_id) {
  RAY_CHECK(objects_to_subscribers_.count(object_id) > 0);
  return objects_to_subscribers_[object_id];
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
  auto objects_it = subscribers_to_objects_.find(subscriber_id);
  if (objects_it == subscribers_to_objects_.end()) {
    return 0;
  }

  objects_it->second.erase(object_id);
  auto subscribers_it = objects_to_subscribers_.find(object_id);
  RAY_CHECK(subscribers_it != objects_to_subscribers_.end());
  objects_to_subscribers_.erase(subscribers_it);
  return 1;
}

bool Subscriber::Connect(LongPollConnectCallback &long_polling_reply_callback) {
  if (!long_polling_reply_callback_) {
    long_polling_reply_callback_ = long_polling_reply_callback;
    return true;
  }
  return false;
}

void Subscriber::QueueMessage(const ObjectID &object_id) {
  mailbox_.push_back(object_id);
  PublishIfPossible();
}

bool Subscriber::PublishIfPossible() {
  if (long_polling_reply_callback_ && mailbox_.size() > 0) {
    long_polling_reply_callback_(mailbox_);
    long_polling_reply_callback_ = nullptr;
    mailbox_.clear();
    return true;
  }
  return false;
}

void PubsubCoordinator::Connect(const NodeID &subscriber_node_id,
                                LongPollConnectCallback long_poll_connect_callback) {
  RAY_LOG(DEBUG) << "Long polling connection is initiated by " << subscriber_node_id;
  RAY_CHECK(long_poll_connect_callback != nullptr);

  if (is_node_dead_(subscriber_node_id)) {
    return;
  }

  if (subscribers_.count(subscriber_node_id) == 0) {
    subscribers_.emplace(subscriber_node_id, std::make_shared<Subscriber>());
  }
  auto subscriber = subscribers_[subscriber_node_id];

  // Since the long polling connection is synchronous between the client and coordinator,
  // when it connects, the connection shouldn't have existed.
  RAY_CHECK(subscriber->Connect(long_poll_connect_callback));
  subscriber->PublishIfPossible();
}

void PubsubCoordinator::RegisterSubscription(const NodeID &subscriber_node_id,
                                             const ObjectID &object_id) {
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
  for (const auto &subscriber_id :
       subscription_index_.GetSubscriberIdsByObjectId(object_id)) {
    RAY_CHECK(subscribers_.count(subscriber_id) > 0);
    auto subscriber = subscribers_[subscriber_id];
    subscriber->QueueMessage(object_id);
  }
}

void PubsubCoordinator::UnregisterSubscriber(const NodeID &subscriber_node_id) {
  subscription_index_.EraseSubscriber(subscriber_node_id);
  // Publish messages before removing the entry. Otherwise, it can have memory leak.
  auto it = subscribers_.find(subscriber_node_id);
  if (it == subscribers_.end()) {
    return;
  }
  auto subscriber = subscribers_[subscriber_node_id];
  subscriber->PublishIfPossible();
  subscribers_.erase(it);
}

void PubsubCoordinator::UnregisterSubscription(const NodeID &subscriber_node_id,
                                               const ObjectID &object_id) {
  subscription_index_.EraseEntry(object_id, subscriber_node_id);
}

}  // namespace ray
