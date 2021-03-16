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

#include "ray/common/id.h"
#include "ray/rpc/worker/core_worker_client_pool.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

using SubscriptionCallback = std::function<void(const ObjectID &)>;
using SubscriptionFailureCallback = std::function<void(const ObjectID &)>;

/// Interface for unit tests.
class PubsubClientInterface {
 public:
  virtual void SubcribeObject(
      const rpc::Address &owner_address, const ObjectID &object_id,
      SubscriptionCallback subscription_callback,
      SubscriptionFailureCallback subscription_failure_callback) = 0;

  /// NOTE(sang): Calling this method inside subscription_failure_callback is not allowed.
  virtual bool UnsubscribeObject(const rpc::Address &owner_address,
                                 const ObjectID &object_id) = 0;
  virtual ~PubsubClientInterface() {}
};

class PubsubClient : public PubsubClientInterface {
 public:
  explicit PubsubClient(const NodeID self_node_id, const std::string self_node_address,
                        const int self_node_port,
                        rpc::CoreWorkerClientPool &owner_client_pool)
      : self_node_id_(self_node_id),
        self_node_address_(self_node_address),
        self_node_port_(self_node_port),
        owner_client_pool_(owner_client_pool) {}
  ~PubsubClient() = default;

  void SubcribeObject(const rpc::Address &owner_address, const ObjectID &object_id,
                      SubscriptionCallback subscription_callback,
                      SubscriptionFailureCallback subscription_failure_callback) override;

  /// NOTE(sang): Calling this method inside subscription_failure_callback is not allowed.
  bool UnsubscribeObject(const rpc::Address &owner_address,
                         const ObjectID &object_id) override;

 private:
  /// TODO(sang): Use a channel abstraction instead of owner address once OBOD uses the
  /// pubsub.
  struct SubscriptionInfo {
    SubscriptionInfo(const rpc::Address &pubsub_server_address)
        : pubsub_server_address_(pubsub_server_address) {}

    const rpc::Address pubsub_server_address_;
    // Object ID -> subscription_callback
    absl::flat_hash_map<const ObjectID,
                        std::pair<SubscriptionCallback, SubscriptionFailureCallback>>
        subscription_callback_map_;
  };

  /// Note(sang): Currently, we assume that only registered objects are published. We may
  /// want to loose the restriction once OBOD is supported by this function.
  void MakeLongPollingPubsubConnection(const rpc::Address &owner_address,
                                       const rpc::Address &subscriber_address);

  void HandleLongPollingResponse(const rpc::Address &owner_address,
                                 const rpc::Address &subscriber_address, Status &status,
                                 const rpc::PubsubLongPollingReply &reply);
  void HandleCoordinatorFailure(const rpc::Address &owner_address);

  inline std::pair<bool, SubscriptionCallback> GetSubscriptionCallback(
      const rpc::Address &owner_address, const ObjectID &object_id) {
    const auto owner_worker_id = WorkerID::FromBinary(owner_address.worker_id());
    auto subscription_it = subscription_map_.find(owner_worker_id);
    if (subscription_it == subscription_map_.end()) {
      return std::make_pair(false, nullptr);
    }
    auto callback_it = subscription_it->second.subscription_callback_map_.find(object_id);
    bool exist = callback_it != subscription_it->second.subscription_callback_map_.end();
    if (!exist) {
      return std::make_pair(false, nullptr);
    }
    auto &subscription_callback = callback_it->second.first;
    return std::make_pair(true, subscription_callback);
  }

  inline std::pair<bool, SubscriptionCallback> GetFailureCallback(
      const rpc::Address &owner_address, const ObjectID &object_id) {
    const auto owner_worker_id = WorkerID::FromBinary(owner_address.worker_id());
    auto subscription_it = subscription_map_.find(owner_worker_id);
    if (subscription_it == subscription_map_.end()) {
      return std::make_pair(false, nullptr);
    }
    auto callback_it = subscription_it->second.subscription_callback_map_.find(object_id);
    bool exist = callback_it != subscription_it->second.subscription_callback_map_.end();
    if (!exist) {
      return std::make_pair(false, nullptr);
    }
    auto &subscription_callback = callback_it->second.second;
    return std::make_pair(true, subscription_callback);
  }

  /// Self node's address information.
  const NodeID self_node_id_;
  const std::string self_node_address_;
  const int self_node_port_;

  /// Mapping of the owner ID -> subscription info.
  absl::flat_hash_map<WorkerID, SubscriptionInfo> subscription_map_;

  /// Cache of gRPC clients to owners.
  rpc::CoreWorkerClientPool &owner_client_pool_;
};

}  // namespace ray
