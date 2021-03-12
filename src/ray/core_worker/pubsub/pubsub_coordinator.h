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

class PubsubCoordinator {
 public:
  explicit PubsubCoordinator(std::function<bool(const NodeID &)> is_node_dead)
      : is_node_dead_(is_node_dead) {}
  ~PubsubCoordinator() = default;

  // TODO(sang): Currently, we need to pass the callback for connection because we are
  // using long polling internally. This should be changed once the Grpc streaming is
  // supported.
  void Connect(const NodeID &subscriber_node_id,
               LongPollConnectCallback long_poll_connect_callback);
  void RegisterSubscriber(const NodeID &subscriber_node_id, const ObjectID &object_id);
  void Publish(const ObjectID &object_id, bool publish_message_if_possible = true);

 protected:
  void PublishAllMessages();

 private:
  void PublishObjects(const NodeID node_id, std::vector<ObjectID> &object_ids);

  /// Protects below fields.
  mutable absl::Mutex mu_;
  std::function<bool(const NodeID &)> is_node_dead_;

  absl::flat_hash_map<NodeID, LongPollConnectCallback> connection_pool_;
  /// Object ID -> subscriber IDs.
  absl::flat_hash_map<ObjectID, absl::flat_hash_set<NodeID>> objects_to_subscribers_;
  absl::flat_hash_set<ObjectID> objects_to_publish_;
};

}  // namespace ray
