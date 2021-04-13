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

namespace pubsub {

using SubscriberID = UniqueID;
using PublisherID = UniqueID;
using SubscriptionCallback = std::function<void(const ObjectID &)>;
using SubscriptionFailureCallback = std::function<void(const ObjectID &)>;

/// Interface for the pubsub client.
class SubscriberInterface {
 public:
  /// Subscribe to the object.
  ///
  /// \param publisher_address Address of the publisher to subscribe the object.
  /// \param object_id The object id to subscribe from the publisher.
  /// \param subscription_callback A callback that is invoked whenever the given object
  /// information is published.
  /// \param subscription_failure_callback A callback that is
  /// invoked whenever the publisher is dead (or failed).
  virtual void SubcribeObject(
      const rpc::Address &publisher_address, const ObjectID &object_id,
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
  /// TODO(sang): Once it starts sending RPCs to unsubscribe, we should start handling
  /// message ordering.
  /// \param publisher_address The publisher address that it will unsubscribe to.
  /// \param object_id The object id to unsubscribe.
  virtual bool UnsubscribeObject(const rpc::Address &publisher_address,
                                 const ObjectID &object_id) = 0;

  /// Testing only. Return true if there's no metadata remained in the private attribute.
  virtual bool AssertNoLeak() const = 0;

  virtual ~SubscriberInterface() {}
};

/// The pubsub client implementation.
///
/// Protocol details:
///
/// - Publisher keeps refreshing the long polling connection every subscriber_timeout_ms.
/// - Subscriber always try making reconnection as long as there are subscribed entries.
/// - If long polling request is failed (if non-OK status is returned from the RPC),
/// consider the publisher is dead.
///
class Subscriber : public SubscriberInterface {
 public:
  explicit Subscriber(const PublisherID subscriber_id,
                      const std::string subscriber_address, const int subscriber_port,
                      rpc::CoreWorkerClientPool &publisher_client_pool)
      : subscriber_id_(subscriber_id),
        subscriber_address_(subscriber_address),
        subscriber_port_(subscriber_port),
        publisher_client_pool_(publisher_client_pool) {}
  ~Subscriber() = default;

  void SubcribeObject(const rpc::Address &publisher_address, const ObjectID &object_id,
                      SubscriptionCallback subscription_callback,
                      SubscriptionFailureCallback subscription_failure_callback) override;

  bool UnsubscribeObject(const rpc::Address &publisher_address,
                         const ObjectID &object_id) override;

  bool AssertNoLeak() const override;

 private:
  /// Encapsulates the subscription information such as subscribed object ids ->
  /// callbacks.
  /// TODO(sang): Use a channel abstraction instead of publisher address once OBOD uses
  /// the pubsub.
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

  /// Create a long polling connection to the publisher for receiving the published
  /// messages.
  ///
  /// TODO(sang): Currently, we assume that unregistered objects will never be published
  /// from the pubsub server. We may want to loose the restriction once OBOD is supported
  /// by this function.
  /// \param publisher_address The address of the publisher that publishes
  /// objects.
  /// \param subscriber_address The address of the subscriber.
  void MakeLongPollingPubsubConnection(const rpc::Address &publisher_address,
                                       const rpc::Address &subscriber_address);

  /// Private method to handle long polling responses. Long polling responses contain the
  /// published messages.
  void HandleLongPollingResponse(const rpc::Address &publisher_address,
                                 const rpc::Address &subscriber_address,
                                 const Status &status,
                                 const rpc::PubsubLongPollingReply &reply);

  /// Returns a subscription callback; Returns a nullopt if the object id is not
  /// subscribed.
  absl::optional<SubscriptionCallback> GetSubscriptionCallback(
      const rpc::Address &publisher_address, const ObjectID &object_id) const;

  /// Returns a publisher failure callback; Returns a nullopt if the object id is not
  /// subscribed.
  absl::optional<SubscriptionCallback> GetFailureCallback(
      const rpc::Address &publisher_address, const ObjectID &object_id) const;

  /// Self node's address information.
  const SubscriberID subscriber_id_;
  const std::string subscriber_address_;
  const int subscriber_port_;

  /// Mapping of the publisher ID -> subscription info.
  absl::flat_hash_map<PublisherID, SubscriptionInfo> subscription_map_;

  /// Cache of gRPC clients to publishers.
  rpc::CoreWorkerClientPool &publisher_client_pool_;
};

}  // namespace pubsub

}  // namespace ray
