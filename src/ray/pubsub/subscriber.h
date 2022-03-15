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
#include <gtest/gtest_prod.h>

#include <boost/any.hpp>
#include <queue>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/rpc/client_call.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/pubsub.pb.h"

namespace ray {

namespace pubsub {

using SubscriberID = UniqueID;
using PublisherID = UniqueID;
using SubscribeDoneCallback = std::function<void(const Status &)>;
using SubscriptionItemCallback = std::function<void(const rpc::PubMessage &)>;
using SubscriptionFailureCallback =
    std::function<void(const std::string &, const Status &)>;

///////////////////////////////////////////////////////////////////////////////
/// SubscriberChannel Abstraction
///////////////////////////////////////////////////////////////////////////////

/// Subscription info stores metadata that is needed for subscriptions.
struct SubscriptionInfo {
  SubscriptionInfo(SubscriptionItemCallback i_cb, SubscriptionFailureCallback f_cb)
      : item_cb(std::move(i_cb)), failure_cb(std::move(f_cb)) {}

  // Callback that runs on each received rpc::PubMessage.
  SubscriptionItemCallback item_cb;

  // Callback that runs after a polling request fails. The input is the failure status.
  SubscriptionFailureCallback failure_cb;
};

/// All subscription info for the publisher.
struct Subscriptions {
  // Subscriptions for all entities.
  std::unique_ptr<SubscriptionInfo> all_entities_subscription;

  // Subscriptions for each entity.
  absl::flat_hash_map<std::string, SubscriptionInfo> per_entity_subscription;
};

/// Subscriber channel is an abstraction for each channel.
/// Through the channel interface, components can subscribe data that belongs to each
/// channel. NOTE: channel is not supposed to be exposed.
class SubscriberChannel {
 public:
  SubscriberChannel(rpc::ChannelType type, instrumented_io_context *callback_service)
      : channel_type_(type), callback_service_(callback_service) {}
  ~SubscriberChannel() = default;

  /// Subscribe to the object.
  ///
  /// \param publisher_address Address of the publisher to subscribe the object.
  /// \param message id The message id to subscribe from the publisher.
  /// \param subscription_callback A callback that is invoked whenever the given object
  /// information is published.
  /// \param subscription_failure_callback A callback that is
  /// invoked whenever the publisher is dead (or failed).
  bool Subscribe(const rpc::Address &publisher_address,
                 const std::optional<std::string> &key_id,
                 SubscriptionItemCallback subscription_item_callback,
                 SubscriptionFailureCallback subscription_failure_callback);

  /// Unsubscribe the object.
  /// NOTE: Calling this method inside subscription_failure_callback is not allowed.
  ///
  /// \param publisher_address The publisher address that it will unsubscribe to.
  /// \param key_id The entity id to unsubscribe.
  /// \return True if the publisher is unsubscribed.
  bool Unsubscribe(const rpc::Address &publisher_address,
                   const std::optional<std::string> &key_id);

  /// Test only.
  /// Checks if the entity key_id is being subscribed to specifically.
  /// Does not consider if all entities in the channel are subscribed.
  ///
  /// \param publisher_address The publisher address to check.
  /// \param key_id The entity id to check.
  bool IsSubscribed(const rpc::Address &publisher_address,
                    const std::string &key_id) const;

  /// Return true if there's no metadata leak.
  bool CheckNoLeaks() const;

  /// Run a success callback for the given pub message.
  /// Note that this will ensure that the callback is running on a designated IO service.
  ///
  /// \param publisher_address The address of the publisher.
  /// \param pub_message The message to handle from the publisher.
  void HandlePublishedMessage(const rpc::Address &publisher_address,
                              const rpc::PubMessage &pub_message) const;

  /// Handle the RPC failure of the given publisher.
  /// Note that this will ensure that the callback is running on a designated IO service.
  ///
  /// \param publisher_address The address of the publisher.
  /// \param status The failure status.
  void HandlePublisherFailure(const rpc::Address &publisher_address,
                              const Status &status);

  /// Handle the failure of the given key_id.
  ///
  /// \param publisher_address The address of the publisher.
  /// \param key_id The specific key id that fails.
  void HandlePublisherFailure(const rpc::Address &publisher_address,
                              const std::string &key_id);

  /// Return true if the subscription exists for a given publisher id.
  bool SubscriptionExists(const PublisherID &publisher_id) {
    return subscription_map_.contains(publisher_id);
  }

  /// Return the channel type of this subscribe channel.
  const rpc::ChannelType GetChannelType() const { return channel_type_; }

  /// Return the statistics of the specific channel.
  std::string DebugString() const;

 protected:
  /// Invoke the publisher failure callback to the designated IO service for the given key
  /// id. \return Return true if the given key id needs to be unsubscribed. False
  /// otherwise.
  bool HandlePublisherFailureInternal(const rpc::Address &publisher_address,
                                      const std::string &key_id,
                                      const Status &status);

  /// Returns a subscription callback; Returns a nullopt if the object id is not
  /// subscribed.
  absl::optional<SubscriptionItemCallback> GetSubscriptionItemCallback(
      const rpc::Address &publisher_address, const std::string &key_id) const {
    const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
    auto subscription_it = subscription_map_.find(publisher_id);
    if (subscription_it == subscription_map_.end()) {
      return absl::nullopt;
    }
    if (subscription_it->second.all_entities_subscription != nullptr) {
      return subscription_it->second.all_entities_subscription->item_cb;
    }
    auto callback_it = subscription_it->second.per_entity_subscription.find(key_id);
    if (callback_it == subscription_it->second.per_entity_subscription.end()) {
      return absl::nullopt;
    }
    return callback_it->second.item_cb;
  }

  /// Returns a publisher failure callback; Returns a nullopt if the object id is not
  /// subscribed.
  absl::optional<SubscriptionFailureCallback> GetFailureCallback(
      const rpc::Address &publisher_address, const std::string &key_id) const {
    const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
    auto subscription_it = subscription_map_.find(publisher_id);
    if (subscription_it == subscription_map_.end()) {
      return absl::nullopt;
    }
    if (subscription_it->second.all_entities_subscription != nullptr) {
      return subscription_it->second.all_entities_subscription->failure_cb;
    }
    auto callback_it = subscription_it->second.per_entity_subscription.find(key_id);
    if (callback_it == subscription_it->second.per_entity_subscription.end()) {
      return absl::nullopt;
    }
    return callback_it->second.failure_cb;
  }

  const rpc::ChannelType channel_type_;

  /// Mapping of the publisher ID -> subscription info for the publisher.
  absl::flat_hash_map<PublisherID, Subscriptions> subscription_map_;

  /// An event loop to execute RPC callbacks. This should be equivalent to the client
  /// pool's io service.
  instrumented_io_context *callback_service_;

  ///
  /// Statistics attributes.
  ///
  uint64_t cum_subscribe_requests_ = 0;
  uint64_t cum_unsubscribe_requests_ = 0;
  mutable uint64_t cum_published_messages_ = 0;
  mutable uint64_t cum_processed_messages_ = 0;
};

///////////////////////////////////////////////////////////////////////////////
/// Subscriber Abstraction
///////////////////////////////////////////////////////////////////////////////

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

  /// Return the statistics string for the subscriber.
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

  virtual ~SubscriberClientInterface() = default;
};

/// The pubsub client implementation. The class is thread-safe.
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
/// - Update channels_ field in the constructor.
///
class Subscriber : public SubscriberInterface {
 public:
  Subscriber(
      const SubscriberID subscriber_id,
      const std::vector<rpc::ChannelType> &channels,
      const int64_t max_command_batch_size,
      std::function<std::shared_ptr<SubscriberClientInterface>(const rpc::Address &)>
          get_client,
      instrumented_io_context *callback_service)
      : subscriber_id_(subscriber_id),
        max_command_batch_size_(max_command_batch_size),
        get_client_(get_client) {
    for (auto type : channels) {
      channels_.emplace(type,
                        std::make_unique<SubscriberChannel>(type, callback_service));
    }
  }

  ~Subscriber();

  bool Subscribe(std::unique_ptr<rpc::SubMessage> sub_message,
                 const rpc::ChannelType channel_type,
                 const rpc::Address &publisher_address,
                 const std::string &key_id,
                 SubscribeDoneCallback subscribe_done_callback,
                 SubscriptionItemCallback subscription_callback,
                 SubscriptionFailureCallback subscription_failure_callback) override;

  bool SubscribeChannel(
      std::unique_ptr<rpc::SubMessage> sub_message,
      rpc::ChannelType channel_type,
      const rpc::Address &publisher_address,
      SubscribeDoneCallback subscribe_done_callback,
      SubscriptionItemCallback subscription_callback,
      SubscriptionFailureCallback subscription_failure_callback) override;

  bool Unsubscribe(const rpc::ChannelType channel_type,
                   const rpc::Address &publisher_address,
                   const std::string &key_id) override;

  bool UnsubscribeChannel(const rpc::ChannelType channel_type,
                          const rpc::Address &publisher_address) override;

  bool IsSubscribed(const rpc::ChannelType channel_type,
                    const rpc::Address &publisher_address,
                    const std::string &key_id) const override;

  /// Return the Channel of the given channel type. Subscriber keeps ownership.
  SubscriberChannel *Channel(const rpc::ChannelType channel_type) const
      EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    const auto it = channels_.find(channel_type);
    if (it == channels_.end()) {
      return nullptr;
    }
    return it->second.get();
  }

  std::string DebugString() const override;

 private:
  ///
  /// Testing fields
  ///

  FRIEND_TEST(IntegrationTest, SubscribersToOneIDAndAllIDs);
  FRIEND_TEST(SubscriberTest, TestBasicSubscription);
  FRIEND_TEST(SubscriberTest, TestSingleLongPollingWithMultipleSubscriptions);
  FRIEND_TEST(SubscriberTest, TestMultiLongPollingWithTheSameSubscription);
  FRIEND_TEST(SubscriberTest, TestCallbackNotInvokedForNonSubscribedObject);
  FRIEND_TEST(SubscriberTest, TestIgnoreBatchAfterUnsubscription);
  FRIEND_TEST(SubscriberTest, TestIgnoreBatchAfterUnsubscribeFromAll);
  FRIEND_TEST(SubscriberTest, TestLongPollingFailure);
  FRIEND_TEST(SubscriberTest, TestUnsubscribeInSubscriptionCallback);
  FRIEND_TEST(SubscriberTest, TestCommandsCleanedUponPublishFailure);
  // Testing only. Check if there are leaks.
  bool CheckNoLeaks() const LOCKS_EXCLUDED(mutex_);

  ///
  /// Private fields
  ///

  bool SubscribeInternal(std::unique_ptr<rpc::SubMessage> sub_message,
                         const rpc::ChannelType channel_type,
                         const rpc::Address &publisher_address,
                         const std::optional<std::string> &key_id,
                         SubscribeDoneCallback subscribe_done_callback,
                         SubscriptionItemCallback subscription_callback,
                         SubscriptionFailureCallback subscription_failure_callback);

  /// Create a long polling connection to the publisher for receiving the published
  /// messages.
  /// NOTE(sang): Note that the subscriber needs to "ensure" that the long polling
  /// requests are always in flight as long as the publisher is subscribed.
  /// The publisher failure should be only detected by this RPC.
  ///
  /// \param publisher_address The address of the publisher that publishes
  /// objects.
  /// \param subscriber_address The address of the subscriber.
  void MakeLongPollingPubsubConnection(const rpc::Address &publisher_address)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Private method to handle long polling responses. Long polling responses contain the
  /// published messages.
  void HandleLongPollingResponse(const rpc::Address &publisher_address,
                                 const Status &status,
                                 const rpc::PubsubLongPollingReply &reply)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Make a long polling connection if it never made the one with this publisher for
  /// pubsub operations.
  void MakeLongPollingConnectionIfNotConnected(const rpc::Address &publisher_address)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Send a command batch to the publisher. To ensure the FIFO order with unary GRPC
  /// requests (which don't guarantee ordering), the subscriber module only allows to have
  /// 1-flight GRPC request per the publisher. Since we batch all commands into a single
  /// request, it should have higher throughput than sending 1 RPC per command
  /// concurrently.
  /// This RPC should be independent from the long polling RPC to receive published
  /// messages.
  void SendCommandBatchIfPossible(const rpc::Address &publisher_address)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Return true if the given publisher id has subscription to any of channel.
  bool SubscriptionExists(const PublisherID &publisher_id)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    return std::any_of(channels_.begin(), channels_.end(), [publisher_id](const auto &p) {
      return p.second->SubscriptionExists(publisher_id);
    });
  }

  /// Self node's identifying information.
  const SubscriberID subscriber_id_;

  /// The command batch size for the subscriber.
  const int64_t max_command_batch_size_;

  /// Gets an rpc client for connecting to the publisher.
  const std::function<std::shared_ptr<SubscriberClientInterface>(const rpc::Address &)>
      get_client_;

  /// Protects below fields. Since the coordinator runs in a core worker, it should be
  /// thread safe.
  mutable absl::Mutex mutex_;

  /// Commands queue. Commands are reported in FIFO order to the publisher. This
  /// guarantees the ordering of commands because they are delivered only by a single RPC
  /// (long polling request).
  struct CommandItem {
    rpc::Command cmd;
    SubscribeDoneCallback done_cb;
  };
  using CommandQueue = std::queue<std::unique_ptr<CommandItem>>;
  absl::flat_hash_map<PublisherID, CommandQueue> commands_ GUARDED_BY(mutex_);

  /// A set to cache the connected publisher ids. "Connected" means the long polling
  /// request is in flight.
  absl::flat_hash_set<PublisherID> publishers_connected_ GUARDED_BY(mutex_);

  /// A set to keep track of in-flight command batch requests
  absl::flat_hash_set<PublisherID> command_batch_sent_ GUARDED_BY(mutex_);

  /// Mapping of channel type to channels.
  absl::flat_hash_map<rpc::ChannelType, std::unique_ptr<SubscriberChannel>> channels_
      GUARDED_BY(mutex_);
};

}  // namespace pubsub

}  // namespace ray
