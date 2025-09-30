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

#include <string>

#include "ray/common/id.h"
#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/pubsub.pb.h"

namespace ray {
namespace pubsub {

/// Publisher interface. Note that message ids are passed as a string to avoid templated
/// definition which doesn't go well with virtual methods.
class PublisherInterface {
 public:
  virtual ~PublisherInterface() = default;

  /// Handle a long poll request from `subscriber_id`.
  virtual void ConnectToSubscriber(
      const rpc::PubsubLongPollingRequest &request,
      std::string *publisher_id,
      google::protobuf::RepeatedPtrField<rpc::PubMessage> *pub_messages,
      rpc::SendReplyCallback send_reply_callback) = 0;

  /// Register the subscription.
  ///
  /// \param channel_type The type of the channel.
  /// \param subscriber_id The ID of the subscriber.
  /// \param key_id The key_id that the subscriber is subscribing to. std::nullopt if
  /// subscribing to all.
  virtual void RegisterSubscription(const rpc::ChannelType channel_type,
                                    const UniqueID &subscriber_id,
                                    const std::optional<std::string> &key_id) = 0;

  /// Publish the given object id to subscribers.
  ///
  /// \param pub_message The message to publish.
  /// Required to contain channel_type and key_id fields.
  virtual void Publish(rpc::PubMessage pub_message) = 0;

  /// Publish to the subscriber that the given key id is not available anymore.
  /// It will invoke the failure callback on the subscriber side.
  ///
  /// \param channel_type The type of the channel.
  /// \param key_id The message id to publish.
  virtual void PublishFailure(const rpc::ChannelType channel_type,
                              const std::string &key_id) = 0;

  /// Unregister subscription. It means the given object id won't be published to the
  /// subscriber anymore.
  ///
  /// \param channel_type The type of the channel.
  /// \param subscriber_id The ID of the subscriber.
  /// \param key_id The key_id of the subscriber. std::nullopt if subscribing to all.
  virtual void UnregisterSubscription(const rpc::ChannelType channel_type,
                                      const UniqueID &subscriber_id,
                                      const std::optional<std::string> &key_id) = 0;

  /// Unregister subscriber. No messages on any channels will be published to it anymore.
  ///
  /// \param subscriber_id The ID of the subscriber.
  virtual void UnregisterSubscriber(const UniqueID &subscriber_id) = 0;

  virtual std::string DebugString() const = 0;
};

}  // namespace pubsub
}  // namespace ray
