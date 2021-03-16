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

/// Interface for pubsub client.
class PubsubClientInterface {
 public:
  /// Subscribe the object.
  ///
  /// \param owner_address Address of the owner to subscribe the object.
  /// \param object_id The object id to subscribe from the owner.
  /// \param subscription_callback A callback that is invoked whenever the given object
  /// information is published. \param subscription_failure_callback A callback that is
  /// invoked whenever the owner is dead (or failed).
  virtual void SubcribeObject(
      const rpc::Address &owner_address, const ObjectID &object_id,
      SubscriptionCallback subscription_callback,
      SubscriptionFailureCallback subscription_failure_callback) = 0;

  /// Unsubscribe the object.
  /// NOTE: Calling this method inside subscription_failure_callback is not allowed.
  /// NOTE: Currently, this method doesn't send a RPC to the pubsub server. It is because
  /// the client is currently used for WaitForObjectFree, and the coordinator will
  /// automatically unregister the subscriber after publishing the object. But if we use
  /// this method for OBOD, we should send an explicit RPC to unregister the subscriber
  /// from the server.
  ///
  /// \param owner_address The owner address that it will unsubscribe to.
  /// \param object_id The object id to unsubscribe.
  virtual bool UnsubscribeObject(const rpc::Address &owner_address,
                                 const ObjectID &object_id) = 0;

  virtual ~PubsubClientInterface() {}
};

/// The pubsub client implementation.
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

  bool UnsubscribeObject(const rpc::Address &owner_address,
                         const ObjectID &object_id) override;

 private:
  /// Encapsulates the subscription information such as subscribed object ids ->
  /// callbacks.
  /// TODO(sang): Use a channel abstraction instead of owner address once OBOD uses the
  /// pubsub.
  struct SubscriptionInfo {
    SubscriptionInfo(const rpc::Address &pubsub_server_address)
        : pubsub_server_address_(pubsub_server_address) {}

    /// Address of the pubsub server.
    const rpc::Address pubsub_server_address_;
    // Object ID -> subscription_callback
    absl::flat_hash_map<const ObjectID,
                        std::pair<SubscriptionCallback, SubscriptionFailureCallback>>
        subscription_callback_map_;
  };

  /// Create a long polling connection to the owner for receiving the published messages.
  /// TODO(sang): Currently, we assume that unregistered objects will never be published
  /// from the pubsub server. We may want to loose the restriction once OBOD is supported
  /// by this function. \param owner_address The address of the owner that publishes
  /// objects. \param subscriber_address The address of the subscriber.
  void MakeLongPollingPubsubConnection(const rpc::Address &owner_address,
                                       const rpc::Address &subscriber_address);

  /// Private method to handle long polling responses. Long polling responses contain the
  /// published messages.
  void HandleLongPollingResponse(const rpc::Address &owner_address,
                                 const rpc::Address &subscriber_address, Status &status,
                                 const rpc::PubsubLongPollingReply &reply);

  /// Private method to handle pubsub server failures.
  void HandleCoordinatorFailure(const rpc::Address &owner_address);

  /// Returns a subscription callback; The first entry indicates if the subscription
  /// callback exists. The second entry is the callback itself.
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

  /// Returns a owner failure callback; The first entry indicates if the subscription
  /// callback exists. The second entry is the callback itself.
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
