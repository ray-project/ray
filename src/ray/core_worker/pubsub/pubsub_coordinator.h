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

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

using LongPollConnectCallback = std::function<void(std::vector<ObjectID> &)>;

class SubscriptionIndex {
 public:
  explicit SubscriptionIndex() {}
  ~SubscriptionIndex() = default;

  void AddEntry(const ObjectID &object_id, const NodeID &subscriber_id);
  const absl::flat_hash_set<NodeID> &GetSubscriberIdsByObjectId(
      const ObjectID &object_id);
  int EraseSubscriber(const NodeID &subscriber_id);
  int EraseEntry(const ObjectID &object_id, const NodeID &subscriber_id);
  // Test only.
  bool IsObjectIdExist(const ObjectID &object_id) const;
  bool IsSubscriberExist(const NodeID &subscriber_id) const;

 private:
  absl::flat_hash_map<ObjectID, absl::flat_hash_set<NodeID>> objects_to_subscribers_;
  // Reverse index of objects_to_subscribers_.
  absl::flat_hash_map<NodeID, absl::flat_hash_set<ObjectID>> subscribers_to_objects_;
};

class Subscriber {
 public:
  explicit Subscriber() {}
  ~Subscriber() = default;

  bool Connect(LongPollConnectCallback long_polling_reply_callback);
  void QueueMessage(const ObjectID &object_id, bool try_publish = true);
  bool PublishIfPossible(bool force = false);

 private:
  LongPollConnectCallback long_polling_reply_callback_ = nullptr;
  std::vector<ObjectID> mailbox_;
};

class PubsubCoordinator {
 public:
  explicit PubsubCoordinator(std::function<bool(const NodeID &)> is_node_dead)
      : is_node_dead_(is_node_dead) {}
  ~PubsubCoordinator() = default;

  // TODO(sang): Currently, we need to pass the callback for connection because we are
  // using long polling internally. This should be changed once the bidirectional grpc
  // streaming is supported.
  void Connect(const NodeID &subscriber_node_id,
               LongPollConnectCallback long_poll_connect_callback);
  void RegisterSubscription(const NodeID &subscriber_node_id, const ObjectID &object_id);
  void Publish(const ObjectID &object_id);
  int UnregisterSubscriber(const NodeID &subscriber_node_id);
  int UnregisterSubscription(const NodeID &subscriber_node_id, const ObjectID &object_id);

 private:
  ///
  /// Private attributes
  ///

  /// Protects below fields.
  mutable absl::Mutex mutex_;
  std::function<bool(const NodeID &)> is_node_dead_;

  absl::flat_hash_map<NodeID, std::shared_ptr<Subscriber>> subscribers_
      GUARDED_BY(mutex_);
  SubscriptionIndex subscription_index_ GUARDED_BY(mutex_);
};

}  // namespace ray
