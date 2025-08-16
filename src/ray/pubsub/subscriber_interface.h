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

#include <grpcpp/grpcpp.h>

#include <memory>
#include <string>
#include <utility>

#include "ray/common/id.h"
#include "ray/rpc/client_call.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/pubsub.pb.h"

namespace ray {

namespace pubsub {

using SubscriberID = UniqueID;
using PublisherID = UniqueID;
using SubscribeDoneCallback = std::function<void(const Status &)>;
using SubscriptionItemCallback = std::function<void(rpc::PubMessage &&)>;
using SubscriptionFailureCallback =
    std::function<void(const std::string &, const Status &)>;

/// Interface for the pubsub client.
class SubscriberInterface {
 public:
  /// There are two modes of subscriptions. Each channel can only be subscribed in one
  /// mode, i.e.
  /// - Calling Subscribe() to subscribe to one or more entities in a channel
  /// - Calling SubscribeChannel() once to subscribe to all entities in a channel
  /// It is an error to call both Subscribe() and SubscribeChannel() on the same channel
  /// type. This restriction can be relaxed later, if there is a use case.

  /// Subscribe to entity key_id in channel channel_type.
  /// NOTE(sang): All the callbacks could be executed in a different thread from a caller.
  /// For example, Subscriber executes callbacks on a passed io_service.
  ///
  /// \param sub_message The subscription message.
  /// \param channel_type The channel to subscribe to.
  /// \param publisher_address Address of the publisher to subscribe the object.
  /// \param key_id The entity id to subscribe from the publisher.
  /// \param subscription_callback A callback that is invoked whenever the given entity
  /// information is received by the subscriber.
  /// \param subscription_failure_callback A callback that is invoked whenever the
  /// connection to publisher is broken (e.g. the publisher fails).
  /// \return True if inserted, false if the key already exists and this becomes a no-op.
  [[nodiscard]] virtual bool Subscribe(
      std::unique_ptr<rpc::SubMessage> sub_message,
      rpc::ChannelType channel_type,
      const rpc::Address &publisher_address,
      const std::string &key_id,
      SubscribeDoneCallback subscribe_done_callback,
      SubscriptionItemCallback subscription_callback,
      SubscriptionFailureCallback subscription_failure_callback) = 0;

  /// Subscribe to all entities in channel channel_type.
  ///
  /// \param sub_message The subscription message.
  /// \param channel_type The channel to subscribe to.
  /// \param publisher_address Address of the publisher to subscribe the object.
  /// \param subscription_callback A callback that is invoked whenever an entity
  /// information is received by the subscriber.
  /// \param subscription_failure_callback A callback that is invoked whenever the
  /// connection to publisher is broken (e.g. the publisher fails).
  /// \return True if inserted, false if the channel is already subscribed and this
  /// becomes a no-op.
  [[nodiscard]] virtual bool SubscribeChannel(
      std::unique_ptr<rpc::SubMessage> sub_message,
      rpc::ChannelType channel_type,
      const rpc::Address &publisher_address,
      SubscribeDoneCallback subscribe_done_callback,
      SubscriptionItemCallback subscription_callback,
      SubscriptionFailureCallback subscription_failure_callback) = 0;

  /// Unsubscribe the entity if the entity has been subscribed with Subscribe().
  /// NOTE: Calling this method inside subscription_failure_callback is not allowed.
  ///
  /// \param channel_type The channel to unsubscribe from.
  /// \param publisher_address The publisher address that it will unsubscribe from.
  /// \param key_id The entity id to unsubscribe.
  /// \return Returns whether the entity key_id has been subscribed before.
  virtual bool Unsubscribe(const rpc::ChannelType channel_type,
                           const rpc::Address &publisher_address,
                           const std::string &key_id) = 0;

  /// Unsubscribe from the channel_type. Must be paired with SubscribeChannel().
  /// NOTE: Calling this method inside subscription_failure_callback is not allowed.
  ///
  /// \param channel_type The channel to unsubscribe from.
  /// \param publisher_address The publisher address that it will unsubscribe from.
  /// \return Returns whether the entity key_id has been subscribed before.
  virtual bool UnsubscribeChannel(const rpc::ChannelType channel_type,
                                  const rpc::Address &publisher_address) = 0;

  /// Test only.
  /// Checks if the entity key_id is being subscribed to specifically.
  /// Does not consider if SubscribeChannel() has been called on the channel.
  ///
  /// \param publisher_address The publisher address to check.
  /// \param key_id The entity id to check.
  [[nodiscard]] virtual bool IsSubscribed(const rpc::ChannelType channel_type,
                                          const rpc::Address &publisher_address,
                                          const std::string &key_id) const = 0;

  virtual std::string DebugString() const = 0;

  virtual ~SubscriberInterface() {}
};

/// The grpc client that the subscriber needs.
class SubscriberClientInterface {
 public:
  /// Send a long polling request to a core worker for pubsub operations.
  virtual void PubsubLongPolling(
      const rpc::PubsubLongPollingRequest &request,
      const rpc::ClientCallback<rpc::PubsubLongPollingReply> &callback) = 0;

  /// Send a pubsub command batch request to a core worker for pubsub operations.
  virtual void PubsubCommandBatch(
      const rpc::PubsubCommandBatchRequest &request,
      const rpc::ClientCallback<rpc::PubsubCommandBatchReply> &callback) = 0;

  virtual std::string DebugString() const = 0;

  virtual ~SubscriberClientInterface() = default;
};

}  // namespace pubsub

}  // namespace ray
