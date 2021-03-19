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

#pragma once

#include <gtest/gtest_prod.h>
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

using LongPollConnectCallback = std::function<void(std::vector<ObjectID> &)>;

/// Index for Object ids and Node ids of subscribers.
class SubscriptionIndex {
 public:
  explicit SubscriptionIndex() {}
  ~SubscriptionIndex() = default;

  /// Add a new entry to the index.
  /// NOTE: If the entry already exists, it raises assert failure.
  void AddEntry(const ObjectID &object_id, const NodeID &subscriber_id);

  /// Return the set of subscriber ids that are subscribing to the given object ids.
  const absl::flat_hash_set<NodeID> &GetSubscriberIdsByObjectId(
      const ObjectID &object_id);

  /// Erase the subscriber from the index. Returns the number of erased subscribers.
  /// NOTE: It cannot erase subscribers that were never added.
  bool EraseSubscriber(const NodeID &subscriber_id);

  /// Erase the object id and subscriber id from the index. Return the number of erased
  /// entries.
  /// NOTE: It cannot erase subscribers that were never added.
  bool EraseEntry(const ObjectID &object_id, const NodeID &subscriber_id);

 private:
  FRIEND_TEST(PubsubCoordinatorTest, TestSubscriptionIndexErase);
  FRIEND_TEST(PubsubCoordinatorTest, TestSubscriptionIndexEraseSubscriber);

  /// Test only. Returns true if object id or subscriber id exists in the index.
  bool HasObjectId(const ObjectID &object_id) const;
  bool HasSubscriber(const NodeID &subscriber_id) const;

  /// Mapping from objects -> subscribers.
  absl::flat_hash_map<ObjectID, absl::flat_hash_set<NodeID>> objects_to_subscribers_;
  // Mapping from subscribers -> objects. Reverse index of objects_to_subscribers_.
  absl::flat_hash_map<NodeID, absl::flat_hash_set<ObjectID>> subscribers_to_objects_;
};

/// Abstraction to each subscriber for the coordinator.
class Subscriber {
 public:
  explicit Subscriber() {}
  ~Subscriber() = default;

  /// Connect to the subscriber. Currently, it means we cache the long polling request to
  /// memory. Once the bidirectional gRPC streaming is enabled, we should replace it.
  ///
  /// \param long_polling_reply_callback reply callback to the long polling request.
  /// \return True if connection is new. False if there were already connections cached.
  bool Connect(LongPollConnectCallback long_polling_reply_callback);

  /// Queue the object id to publish to the subscriber.
  ///
  /// \param object_id Object id to publish to the subscriber.
  /// \param try_publish If true, it try publishing the object id if there is a
  /// connection. False is used only for testing.
  void QueueMessage(const ObjectID &object_id, bool try_publish = true);

  /// Publish all queued messages if possible.
  ///
  /// \param force If true, we publish to the subscriber although there's no queued
  /// message.
  /// \return True if it publishes. False otherwise.
  bool PublishIfPossible(bool force = false);

 private:
  /// Cached long polling reply callback.
  LongPollConnectCallback long_polling_reply_callback_ = nullptr;
  /// Queued messages to publish.
  std::vector<ObjectID> mailbox_;
};

/// Pubsub server.
class PubsubCoordinator {
 public:
  /// Pubsub coordinator constructor.
  ///
  /// \param is_node_dead A callback that returns true if the given node id is dead.
  explicit PubsubCoordinator(std::function<bool(const NodeID &)> is_node_dead)
      : is_node_dead_(is_node_dead) {}

  ~PubsubCoordinator() = default;

  ///
  // TODO(sang): Currently, we need to pass the callback for connection because we are
  // using long polling internally. This should be changed once the bidirectional grpc
  // streaming is supported.
  void Connect(const NodeID &subscriber_node_id,
               LongPollConnectCallback long_poll_connect_callback);

  /// Register the subscription.
  ///
  /// \param subscriber_node_id The node id of the subscriber.
  /// \param object_id The object id that the subscriber is subscribing to.
  void RegisterSubscription(const NodeID &subscriber_node_id, const ObjectID &object_id);

  /// Publish the given object id to subscribers.
  ///
  /// \param object_id The object id to publish to subscribers.
  void Publish(const ObjectID &object_id);

  /// Remove the subscriber. Once the subscriber is removed, messages won't be published
  /// to it anymore.
  /// TODO(sang): Currently, clients don't send a RPC to unregister themselves.
  ///
  /// \param subscriber_node_id The node id of the subscriber to unsubscribe.
  /// \return True if erased. False otherwise.
  bool UnregisterSubscriber(const NodeID &subscriber_node_id);

  /// Unregister subscription. It means the given object id won't be published to the
  /// subscriber anymore.
  ///
  /// \param subscriber_node_id The node id of the subscriber.
  /// \param object_id The object id of the subscriber.
  /// \return True if erased. False otherwise.
  bool UnregisterSubscription(const NodeID &subscriber_node_id, const ObjectID &object_id);

 private:
  /// Protects below fields. Since the coordinator runs in a core worker, it should be
  /// thread safe.
  mutable absl::Mutex mutex_;

  /// Callback that returns true if the given node is dead.
  std::function<bool(const NodeID &)> is_node_dead_;

  /// Mapping of node id -> subscribers.
  absl::flat_hash_map<NodeID, std::shared_ptr<Subscriber>> subscribers_
      GUARDED_BY(mutex_);

  /// Index that stores the mapping of objects <-> subscribers.
  SubscriptionIndex subscription_index_ GUARDED_BY(mutex_);
};

}  // namespace ray
