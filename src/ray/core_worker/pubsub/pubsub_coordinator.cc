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

void PubsubCoordinator::Connect(const NodeID &subscriber_node_id,
                                LongPollConnectCallback long_poll_connect_callback) {
  RAY_LOG(DEBUG) << "Long polling connection is initiated by " << subscriber_node_id;
  if (connection_pool_.count(subscriber_node_id) == 0) {
    RAY_CHECK(
        connection_pool_.emplace(subscriber_node_id, long_poll_connect_callback).second);
  }
}

void PubsubCoordinator::RegisterSubscriber(const NodeID &subscriber_node_id,
                                           const ObjectID &object_id) {
  RAY_LOG(DEBUG) << "object id " << object_id << " is subscribed by "
                 << subscriber_node_id;
  auto &subscribers = objects_to_subscribers_[object_id];
  subscribers.emplace(subscriber_node_id);
}

void PubsubCoordinator::Publish(const ObjectID &object_id,
                                bool publish_message_if_possible) {
  objects_to_publish_.emplace(object_id);
  if (publish_message_if_possible) {
    PublishAllMessages();
  }
}

void PubsubCoordinator::PublishAllMessages() {
  absl::flat_hash_map<NodeID, std::vector<ObjectID>> subscriber_to_objects_to_free;
  // We are not iterating the loop if we are waiting for any connection.
  // Without this, we will keep iterating the same loop if some of connections are not
  // initialized yet. This can cause problems because core worker can own millions of
  // objects with the object spilling.
  bool waiting_for_connection = false;
  for (auto it = objects_to_publish_.begin(); it != objects_to_publish_.end();) {
    // First, get all subscribers ids of the object to free.
    auto current_object_id_it = it++;
    const auto &object_id = *current_object_id_it;
    const auto &subscribers_it = objects_to_subscribers_.find(object_id);
    RAY_CHECK(subscribers_it != objects_to_subscribers_.end());
    absl::flat_hash_set<NodeID> &node_ids = subscribers_it->second;

    // Choose which subscribers are going to be notified.
    for (auto node_it = node_ids.begin(); node_it != node_ids.end();) {
      auto current_node_it = node_it++;
      const auto &node_id = *current_node_it;
      if (is_node_dead_(node_id)) {
        // If the subscriber node is dead, remove the connection.
        connection_pool_.erase(node_id);
        node_ids.erase(current_node_it);
      } else if (connection_pool_.find(node_id) == connection_pool_.end()) {
        ;  // If the connection is not initiated yet, do nothing.
        waiting_for_connection = true;
        break;
      } else {
        // Otherwise, update the objects to free so that it can reply to the subscribers.
        RAY_LOG(DEBUG) << "object id " << object_id
                       << " free information will be published to " << node_id;
        auto &objects_to_free = subscriber_to_objects_to_free[node_id];
        objects_to_free.push_back(object_id);
        node_ids.erase(current_node_it);
      }
    }
    if (waiting_for_connection) {
      break;
    }

    // Clean up metadata.
    if (node_ids.size() == 0) {
      objects_to_publish_.erase(current_object_id_it);
      objects_to_subscribers_.erase(subscribers_it);
    } else {
      current_object_id_it++;
    }
  }

  // Publish the result.
  for (auto it : subscriber_to_objects_to_free) {
    PublishObjects(it.first, it.second);
  }
}

void PubsubCoordinator::PublishObjects(const NodeID node_id,
                                       std::vector<ObjectID> &object_ids) {
  // We don't care if the node is dead or not here. If the node is dead, the reply will
  // just fail.
  const auto &connection_it = connection_pool_.find(node_id);
  RAY_CHECK(connection_it != connection_pool_.end());
  const auto &long_poll_connect_callback = connection_it->second;
  long_poll_connect_callback(object_ids);
  // We always erase the connection after publishing entries. The client should reinitiate
  // the connection if needed.
  connection_pool_.erase(connection_it);
}

}  // namespace ray
