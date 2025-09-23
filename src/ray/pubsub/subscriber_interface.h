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
#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/pubsub.pb.h"

namespace ray {

namespace pubsub {

using SubscribeDoneCallback = std::function<void(const Status &)>;
using SubscriptionItemCallback = std::function<void(rpc::PubMessage &&)>;
using SubscriptionFailureCallback =
    std::function<void(const std::string &, const Status &)>;

/// Interface for a subscriber to one or more pubsub channels from a publisher.
class SubscriberInterface {
 public:
  /// There are two modes of subscriptions. Each channel can only be subscribed in one
  /// mode, i.e.
  /// - Calling Subscribe() to subscribe to one or more entities in a channel.
  /// - Calling Subscribe() once to subscribe to all entities in a channel.
  /// NOTE: It is an error to call both Subscribe() to all entities and then only
  /// subscribe to one entity on the same channel type.

  /// Subscribe to entity key_id in channel channel_type.
  /// NOTE(sang): All the callbacks could be executed in a different thread from a caller.
  /// For example, Subscriber executes callbacks on a passed io_service.
  ///
  /// \param sub_message The subscription message.
  /// \param channel_type The channel to subscribe to.
  /// \param publisher_address Address of the publisher to subscribe the object.
  /// \param key_id The entity id to subscribe from the publisher. Subscribes to all
  /// entities if nullopt.
  /// \param subscription_callback A callback that is invoked whenever the given entity
  /// information is received by the subscriber.
  /// \param subscription_failure_callback A callback that is invoked whenever the
  /// connection to publisher is broken (e.g. the publisher fails).
  virtual void Subscribe(std::unique_ptr<rpc::SubMessage> sub_message,
                         rpc::ChannelType channel_type,
                         const rpc::Address &publisher_address,
                         const std::optional<std::string> &key_id,
                         SubscribeDoneCallback subscribe_done_callback,
                         SubscriptionItemCallback subscription_callback,
                         SubscriptionFailureCallback subscription_failure_callback) = 0;

  /// Unsubscribe the entity if the entity has been subscribed with Subscribe().
  /// NOTE: Calling this method inside subscription_failure_callback is not allowed.
  ///
  /// \param channel_type The channel to unsubscribe from.
  /// \param publisher_address The publisher address that it will unsubscribe from.
  /// \param key_id The entity id to unsubscribe. Unsubscribes from all entities if
  /// nullopt.
  /// \return Returns whether the entity key_id has been subscribed before.
  virtual bool Unsubscribe(rpc::ChannelType channel_type,
                           const rpc::Address &publisher_address,
                           const std::optional<std::string> &key_id) = 0;

  /// Test only.
  /// Checks if the entity key_id is being subscribed to specifically.
  /// Does not consider if the subscriber is subscribed to all entities in a channel.
  ///
  /// \param publisher_address The publisher address to check.
  /// \param key_id The entity id to check.
  virtual bool IsSubscribed(rpc::ChannelType channel_type,
                            const rpc::Address &publisher_address,
                            const std::string &key_id) const = 0;

  virtual std::string DebugString() const = 0;

  virtual ~SubscriberInterface() = default;
};

/// Interface for the client used by a subscriber.
class SubscriberClientInterface {
 public:
  /// Send a long polling request to a publisher.
  virtual void PubsubLongPolling(
      rpc::PubsubLongPollingRequest &&request,
      const rpc::ClientCallback<rpc::PubsubLongPollingReply> &callback) = 0;

  /// Send a pubsub command batch to a publisher.
  virtual void PubsubCommandBatch(
      rpc::PubsubCommandBatchRequest &&request,
      const rpc::ClientCallback<rpc::PubsubCommandBatchReply> &callback) = 0;

  virtual ~SubscriberClientInterface() = default;
};

}  // namespace pubsub

}  // namespace ray
