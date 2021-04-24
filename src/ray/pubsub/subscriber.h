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

/// Subscription info stores metadata that is needed for subscription.
template <typename MessageID>
struct SubscriptionInfo {
  SubscriptionInfo() {}

  // Message ID -> subscription_callback
  absl::flat_hash_map<const MessageID,
                      std::pair<SubscriptionCallback, SubscriptionFailureCallback>>
      subscription_callback_map;
};

/// Subscriber channel is an abstraction for each channel.
/// Through the channel interface, components can subscribe data that belongs to each
/// channel. NOTE: channel is not supposed to be exposed.
class SubscribeChannelInterface {
 public:
  virtual ~SubscribeChannelInterface(){};

  /// Subscribe to the object.
  ///
  /// \param publisher_address Address of the publisher to subscribe the object.
  /// \param message id The message id to subscribe from the publisher.
  /// \param subscription_callback A callback that is invoked whenever the given object
  /// information is published.
  /// \param subscription_failure_callback A callback that is
  /// invoked whenever the publisher is dead (or failed).
  virtual void Subscribe(const rpc::Address &publisher_address,
                         const std::string &message_id,
                         SubscriptionCallback subscription_callback,
                         SubscriptionFailureCallback subscription_failure_callback) = 0;

  /// Unsubscribe the object.
  /// NOTE: Calling this method inside subscription_failure_callback is not allowed.
  ///
  /// \param publisher_address The publisher address that it will unsubscribe to.
  /// \param message_id The message id to unsubscribe.
  /// \return True if the publisher is unsubscribed.
  virtual bool Unsubscribe(const rpc::Address &publisher_address,
                           const std::string &message_id) = 0;

  /// Handle the published message from the publisher_address.
  ///
  /// \param publisher_address The address of the publisher.
  /// \param pub_message The message to handle from the publisher.
  virtual void HandlePublishedMessage(const rpc::Address &publisher_address,
                                      const rpc::PubMessage &pub_message) = 0;

  /// Handle the failure of the given publisher.
  ///
  /// \param publisher_address The address of the publisher.
  /// \param pub_message The message to handle from the publisher.
  virtual void HandlePublisherFailure(const rpc::Address &publisher_address) = 0;

  /// Return true if the subscription exists for a given publisher id.
  virtual bool SubscriptionExists(const PublisherID &publisher_id) = 0;

  /// Return the channel type of this subscribe channel.
  virtual const rpc::ChannelType GetChannelType() const = 0;

  /// Return true if there's no metadata leak.
  virtual bool CheckNoLeaks() const = 0;
};

template <typename MessageID>
class SubscriberChannel : public SubscribeChannelInterface {
 public:
  SubscriberChannel() {}
  ~SubscriberChannel() = default;

  void Subscribe(const rpc::Address &publisher_address, const std::string &message_id,
                 SubscriptionCallback subscription_callback,
                 SubscriptionFailureCallback subscription_failure_callback) override;

  bool Unsubscribe(const rpc::Address &publisher_address,
                   const std::string &message_id) override;

  bool CheckNoLeaks() const override;

  void HandlePublishedMessage(const rpc::Address &publisher_address,
                              const rpc::PubMessage &pub_message) override;

  void HandlePublisherFailure(const rpc::Address &publisher_address) override;

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

/// The below defines the list of channel implementation.

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
  /// \param channel_type The channel to subscribe to.
  /// \param publisher_address Address of the publisher to subscribe the object.
  /// \param message_id_binary The message id to subscribe from the publisher.
  /// \param subscription_callback A callback that is invoked whenever the given object
  /// information is published.
  /// \param subscription_failure_callback A callback that is
  /// invoked whenever the publisher is dead (or failed).
  virtual void Subscribe(const rpc::ChannelType channel_type,
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
  /// \param channel_type The channel to unsubscribe to.
  /// \param publisher_address The publisher address that it will unsubscribe to.
  /// \param message_id_binary The message id to unsubscribe.
  virtual bool Unsubscribe(const rpc::ChannelType channel_type,
                           const rpc::Address &publisher_address,
                           const std::string &message_id_binary) = 0;

  /// Testing only. Return true if there's no metadata remained in the private attribute.
  virtual bool CheckNoLeaks() const = 0;

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
/// How to extend new channels.
///
/// - Modify pubsub.proto to add a new channel and pub_message.
/// - Create a new channel implementation. Look WaitForObjectEvictionChannel as an
/// example.
/// - Define the newly created channel implementation in subscriber.cc file, so that
/// compiler can see them.
/// - Update channels_ field in the constructor.
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
        /// This is used to define new channel_type -> Channel abstraction.
        channels_({{rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION,
                    wait_for_object_eviction_channel_},
                   {rpc::ChannelType::WAIT_FOR_REF_REMOVED_CHANNEL,
                    wait_for_ref_removed_channel_}}) {}

  ~Subscriber() = default;

  void Subscribe(const rpc::ChannelType channel_type,
                 const rpc::Address &publisher_address,
                 const std::string &message_id_binary,
                 SubscriptionCallback subscription_callback,
                 SubscriptionFailureCallback subscription_failure_callback) override;

  bool Unsubscribe(const rpc::ChannelType channel_type,
                   const rpc::Address &publisher_address,
                   const std::string &message_id_binary) override;

  /// Return the Channel of the given channel type.
  std::shared_ptr<SubscribeChannelInterface> Channel(
      const rpc::ChannelType channel_type) const;

  bool CheckNoLeaks() const override;

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

  /// Return true if the given publisher id has subscription to any of channel.
  bool SubscriptionExists(const PublisherID &publisher_id);

  /// Self node's address information.
  const SubscriberID subscriber_id_;
  const std::string subscriber_address_;
  const int subscriber_port_;

  /// Cache of gRPC clients to publishers.
  rpc::CoreWorkerClientPool &publisher_client_pool_;

  /// A set to cache the connected publisher ids. "Connected" means the long polling
  /// request is in flight.
  absl::flat_hash_set<PublisherID> publishers_connected_;

  /// WaitForObjectEviction channel.
  std::shared_ptr<WaitForObjectEvictionChannel> wait_for_object_eviction_channel_;

  /// WaitForRefRemoved channel.
  std::shared_ptr<WaitForRefRemovedChannel> wait_for_ref_removed_channel_;

  /// Mapping of channel type to channels.
  absl::flat_hash_map<rpc::ChannelType, std::shared_ptr<SubscribeChannelInterface>>
      channels_;
};

}  // namespace pubsub

}  // namespace ray
