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

#include <boost/any.hpp>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"

#include "ray/common/id.h"
#include "ray/rpc/worker/core_worker_client_pool.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/pubsub.pb.h"

namespace ray {

namespace pubsub {

using SubscriberID = UniqueID;
using PublisherID = UniqueID;
using SubscriptionCallback = std::function<void(const rpc::PubMessage &)>;
using SubscriptionFailureCallback = std::function<void()>;

///////////////////////////////////////////////////////////////////////////////
/// SubscriberChannel Abstraction
///////////////////////////////////////////////////////////////////////////////

template <typename MessageID>
struct SubscriptionInfo {
  SubscriptionInfo() {}

  // Message ID -> subscription_callback
  absl::flat_hash_map<const MessageID,
                      std::pair<SubscriptionCallback, SubscriptionFailureCallback>>
      subscription_callback_map;
};

class SubscribeChannelInterface {
 public:
  virtual ~SubscribeChannelInterface(){};

  virtual void Subcribe(const rpc::Address &publisher_address,
                        const std::string &message_id,
                        SubscriptionCallback subscription_callback,
                        SubscriptionFailureCallback subscription_failure_callback) = 0;

  virtual bool Unsubscribe(const rpc::Address &publisher_address,
                           const std::string &message_id) = 0;

  virtual bool AssertNoLeak() const = 0;

  virtual void HandleLongPollingResponse(const rpc::Address &publisher_address,
                                         const rpc::Address &subscriber_address,
                                         const rpc::PubsubLongPollingReply &reply) = 0;

  virtual void HandleLongPollingFailureResponse(
      const rpc::Address &publisher_address, const rpc::Address &subscriber_address) = 0;
  virtual bool SubscriptionExists(const PublisherID &publisher_id) = 0;
  virtual const rpc::ChannelType GetChannelType() const = 0;
};

template <typename MessageID>
class SubscriberChannel : public SubscribeChannelInterface {
 public:
  SubscriberChannel() {}
  ~SubscriberChannel() = default;

  void Subcribe(const rpc::Address &publisher_address, const std::string &message_id,
                SubscriptionCallback subscription_callback,
                SubscriptionFailureCallback subscription_failure_callback) override;

  bool Unsubscribe(const rpc::Address &publisher_address,
                   const std::string &message_id) override;

  bool AssertNoLeak() const override;

  void HandleLongPollingResponse(const rpc::Address &publisher_address,
                                 const rpc::Address &subscriber_address,
                                 const rpc::PubsubLongPollingReply &reply) override;

  void HandleLongPollingFailureResponse(const rpc::Address &publisher_address,
                                        const rpc::Address &subscriber_address) override;

  bool SubscriptionExists(const PublisherID &publisher_id) override;

  const rpc::ChannelType GetChannelType() const override { return channel_type_; }

  virtual MessageID ParseMessageID(const rpc::PubMessage &msg) = 0;

 protected:
  rpc::ChannelType channel_type_;

  /// Returns a subscription callback; Returns a nullopt if the object id is not
  /// subscribed.
  absl::optional<SubscriptionCallback> GetSubscriptionCallback(
      const rpc::Address &publisher_address, const MessageID &message_id) const;

  /// Returns a publisher failure callback; Returns a nullopt if the object id is not
  /// subscribed.
  absl::optional<SubscriptionFailureCallback> GetFailureCallback(
      const rpc::Address &publisher_address, const MessageID &message_id) const;

  /// Mapping of the publisher ID -> subscription info.
  absl::flat_hash_map<PublisherID, SubscriptionInfo<MessageID>> subscription_map_;
};

class WaitForObjectEvictionChannel : public SubscriberChannel<ObjectID> {
 public:
  WaitForObjectEvictionChannel() : SubscriberChannel() {
    channel_type_ = rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION;
  }
  ~WaitForObjectEvictionChannel() = default;

  ObjectID ParseMessageID(const rpc::PubMessage &msg) override {
    return ObjectID::FromBinary(msg.message_id());
  }
};

class WaitForRefRemovedChannel : public SubscriberChannel<ObjectID> {
 public:
  WaitForRefRemovedChannel() : SubscriberChannel() {
    channel_type_ = rpc::ChannelType::WAIT_FOR_REF_REMOVED_CHANNEL;
  }
  ~WaitForRefRemovedChannel() = default;

  ObjectID ParseMessageID(const rpc::PubMessage &msg) override {
    return ObjectID::FromBinary(msg.message_id());
  }
};

///////////////////////////////////////////////////////////////////////////////
/// Subscriber Abstraction
///////////////////////////////////////////////////////////////////////////////

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
  virtual void Subcribe(const rpc::ChannelType channel_type,
                        const rpc::Address &publisher_address,
                        const std::string &message_id_binary,
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
  virtual bool Unsubscribe(const rpc::ChannelType channel_type,
                           const rpc::Address &publisher_address,
                           const std::string &message_id_binary) = 0;

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
  explicit Subscriber(const SubscriberID subscriber_id,
                      const std::string subscriber_address, const int subscriber_port,
                      rpc::CoreWorkerClientPool &publisher_client_pool)
      : subscriber_id_(subscriber_id),
        subscriber_address_(subscriber_address),
        subscriber_port_(subscriber_port),
        publisher_client_pool_(publisher_client_pool),
        wait_for_object_eviction_channel_(
            std::make_shared<WaitForObjectEvictionChannel>()),
        wait_for_ref_removed_channel_(std::make_shared<WaitForRefRemovedChannel>()),
        channels_({{rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION,
                    wait_for_object_eviction_channel_},
                   {rpc::ChannelType::WAIT_FOR_REF_REMOVED_CHANNEL,
                    wait_for_ref_removed_channel_}}) {}

  ~Subscriber() = default;

  void Subcribe(const rpc::ChannelType channel_type,
                const rpc::Address &publisher_address,
                const std::string &message_id_binary,
                SubscriptionCallback subscription_callback,
                SubscriptionFailureCallback subscription_failure_callback) override;

  bool Unsubscribe(const rpc::ChannelType channel_type,
                   const rpc::Address &publisher_address,
                   const std::string &message_id_binary) override;

  std::shared_ptr<SubscribeChannelInterface> Channel(
      const rpc::ChannelType channel_type) const;

  bool AssertNoLeak() const override;

 private:
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

  bool SubscriptionExists(const PublisherID &publisher_id);

  /// Self node's address information.
  const SubscriberID subscriber_id_;
  const std::string subscriber_address_;
  const int subscriber_port_;

  /// Cache of gRPC clients to publishers.
  rpc::CoreWorkerClientPool &publisher_client_pool_;

  absl::flat_hash_set<PublisherID> publishers_connected_;

  std::shared_ptr<WaitForObjectEvictionChannel> wait_for_object_eviction_channel_;

  std::shared_ptr<WaitForRefRemovedChannel> wait_for_ref_removed_channel_;

  absl::flat_hash_map<rpc::ChannelType, std::shared_ptr<SubscribeChannelInterface>>
      channels_;
};

}  // namespace pubsub

}  // namespace ray
