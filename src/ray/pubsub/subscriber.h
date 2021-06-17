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
#include <boost/any.hpp>
#include <queue>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"

#include "ray/common/asio/instrumented_io_context.h"
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
template <typename KeyIdType>
struct SubscriptionInfo {
  SubscriptionInfo() {}

  // Message ID -> subscription_callback
  absl::flat_hash_map<const KeyIdType,
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
                         const std::string &key_id_binary,
                         SubscriptionCallback subscription_callback,
                         SubscriptionFailureCallback subscription_failure_callback) = 0;

  /// Unsubscribe the object.
  /// NOTE: Calling this method inside subscription_failure_callback is not allowed.
  ///
  /// \param publisher_address The publisher address that it will unsubscribe to.
  /// \param key_id The message id to unsubscribe.
  /// \return True if the publisher is unsubscribed.
  virtual bool Unsubscribe(const rpc::Address &publisher_address,
                           const std::string &key_id_binary) = 0;

  /// Get a subscription callback that corresponds to a pub message.
  ///
  /// \param publisher_address The address of the publisher.
  /// \param pub_message The message to handle from the publisher.
  /// \return the subscription callback registered. nullptr otherwise.
  virtual SubscriptionCallback GetCallbackForPubMessage(
      const rpc::Address &publisher_address,
      const rpc::PubMessage &pub_message) const = 0;

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

template <typename KeyIdType>
class SubscriberChannel : public SubscribeChannelInterface {
 public:
  SubscriberChannel() {}
  ~SubscriberChannel() = default;

  void Subscribe(const rpc::Address &publisher_address, const std::string &key_id,
                 SubscriptionCallback subscription_callback,
                 SubscriptionFailureCallback subscription_failure_callback) override;

  bool Unsubscribe(const rpc::Address &publisher_address,
                   const std::string &key_id) override;

  bool CheckNoLeaks() const override;

  SubscriptionCallback GetCallbackForPubMessage(
      const rpc::Address &publisher_address,
      const rpc::PubMessage &pub_message) const override;

  void HandlePublisherFailure(const rpc::Address &publisher_address) override;

  bool SubscriptionExists(const PublisherID &publisher_id) override {
    return subscription_map_.count(publisher_id);
  }

  const rpc::ChannelType GetChannelType() const override { return channel_type_; }

 protected:
  rpc::ChannelType channel_type_;

  /// Returns a subscription callback; Returns a nullopt if the object id is not
  /// subscribed.
  absl::optional<SubscriptionCallback> GetSubscriptionCallback(
      const rpc::Address &publisher_address, const KeyIdType &key_id) const {
    const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
    auto subscription_it = subscription_map_.find(publisher_id);
    if (subscription_it == subscription_map_.end()) {
      return absl::nullopt;
    }
    auto callback_it = subscription_it->second.subscription_callback_map.find(key_id);
    bool exist = callback_it != subscription_it->second.subscription_callback_map.end();
    if (!exist) {
      return absl::nullopt;
    }
    return absl::optional<SubscriptionCallback>{callback_it->second.first};
  }

  /// Returns a publisher failure callback; Returns a nullopt if the object id is not
  /// subscribed.
  absl::optional<SubscriptionFailureCallback> GetFailureCallback(
      const rpc::Address &publisher_address, const KeyIdType &key_id) const {
    const auto publisher_id = PublisherID::FromBinary(publisher_address.worker_id());
    auto subscription_it = subscription_map_.find(publisher_id);
    if (subscription_it == subscription_map_.end()) {
      return absl::nullopt;
    }
    auto callback_it = subscription_it->second.subscription_callback_map.find(key_id);
    bool exist = callback_it != subscription_it->second.subscription_callback_map.end();
    if (!exist) {
      return absl::nullopt;
    }
    return absl::optional<SubscriptionFailureCallback>{callback_it->second.second};
  }

  /// Mapping of the publisher ID -> subscription info.
  absl::flat_hash_map<PublisherID, SubscriptionInfo<KeyIdType>> subscription_map_;
};

/// The below defines the list of channel implementation.

class WaitForObjectEvictionChannel : public SubscriberChannel<ObjectID> {
 public:
  WaitForObjectEvictionChannel() : SubscriberChannel() {
    channel_type_ = rpc::ChannelType::WORKER_OBJECT_EVICTION;
  }
  ~WaitForObjectEvictionChannel() = default;
};

class WaitForRefRemovedChannel : public SubscriberChannel<ObjectID> {
 public:
  WaitForRefRemovedChannel() : SubscriberChannel() {
    channel_type_ = rpc::ChannelType::WORKER_REF_REMOVED_CHANNEL;
  }
  ~WaitForRefRemovedChannel() = default;
};

///////////////////////////////////////////////////////////////////////////////
/// Subscriber Abstraction
///////////////////////////////////////////////////////////////////////////////

/// Interface for the pubsub client.
class SubscriberInterface {
 public:
  /// Subscribe to the object.
  /// NOTE(sang): All the callbacks could be executed in a different thread from a caller.
  /// For example, Subscriber executes callbacks on a passed io_service.
  ///
  /// \param sub_message The subscription message.
  /// \param channel_type The channel to subscribe to.
  /// \param publisher_address Address of the publisher to subscribe the object.
  /// \param key_id_binary The message id to subscribe from the publisher.
  /// \param subscription_callback A callback that is invoked whenever the given object
  /// information is published.
  /// \param subscription_failure_callback A callback that is
  /// invoked whenever the publisher is dead (or failed).
  virtual void Subscribe(std::unique_ptr<rpc::SubMessage> sub_message,
                         const rpc::ChannelType channel_type,
                         const rpc::Address &publisher_address,
                         const std::string &key_id_binary,
                         SubscriptionCallback subscription_callback,
                         SubscriptionFailureCallback subscription_failure_callback) = 0;

  /// Unsubscribe the object.
  /// NOTE: Calling this method inside subscription_failure_callback is not allowed.
  ///
  /// \param channel_type The channel to unsubscribe to.
  /// \param publisher_address The publisher address that it will unsubscribe to.
  /// \param key_id_binary The message id to unsubscribe.
  virtual bool Unsubscribe(const rpc::ChannelType channel_type,
                           const rpc::Address &publisher_address,
                           const std::string &key_id_binary) = 0;

  virtual ~SubscriberInterface() {}
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
                      const int64_t max_command_batch_size,
                      rpc::CoreWorkerClientPool &publisher_client_pool,
                      instrumented_io_context *callback_service)
      : callback_service_(callback_service),
        subscriber_id_(subscriber_id),
        subscriber_address_(subscriber_address),
        subscriber_port_(subscriber_port),
        max_command_batch_size_(max_command_batch_size),
        publisher_client_pool_(publisher_client_pool),
        wait_for_object_eviction_channel_(
            std::make_shared<WaitForObjectEvictionChannel>()),
        wait_for_ref_removed_channel_(std::make_shared<WaitForRefRemovedChannel>()),
        /// This is used to define new channel_type -> Channel abstraction.
        channels_({{rpc::ChannelType::WORKER_OBJECT_EVICTION,
                    wait_for_object_eviction_channel_},
                   {rpc::ChannelType::WORKER_REF_REMOVED_CHANNEL,
                    wait_for_ref_removed_channel_}}) {}

  ~Subscriber() = default;

  void Subscribe(std::unique_ptr<rpc::SubMessage> sub_message,
                 const rpc::ChannelType channel_type,
                 const rpc::Address &publisher_address, const std::string &key_id_binary,
                 SubscriptionCallback subscription_callback,
                 SubscriptionFailureCallback subscription_failure_callback) override;

  bool Unsubscribe(const rpc::ChannelType channel_type,
                   const rpc::Address &publisher_address,
                   const std::string &key_id_binary) override;

  /// Return the Channel of the given channel type.
  std::shared_ptr<SubscribeChannelInterface> Channel(
      const rpc::ChannelType channel_type) const EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    const auto it = channels_.find(channel_type);
    RAY_CHECK(it != channels_.end()) << "Unknown channel: " << channel_type;
    return it->second;
  }

 private:
  ///
  /// Testing fields
  ///

  FRIEND_TEST(SubscriberTest, TestBasicSubscription);
  FRIEND_TEST(SubscriberTest, TestSingleLongPollingWithMultipleSubscriptions);
  FRIEND_TEST(SubscriberTest, TestMultiLongPollingWithTheSameSubscription);
  FRIEND_TEST(SubscriberTest, TestCallbackNotInvokedForNonSubscribedObject);
  FRIEND_TEST(SubscriberTest, TestIgnoreBatchAfterUnsubscription);
  FRIEND_TEST(SubscriberTest, TestLongPollingFailure);
  FRIEND_TEST(SubscriberTest, TestUnsubscribeInSubscriptionCallback);
  FRIEND_TEST(SubscriberTest, TestCommandsCleanedUponPublishFailure);
  // Testing only. Check if there are leaks.
  bool CheckNoLeaks() const;

  ///
  /// Private fields
  ///

  /// Create a long polling connection to the publisher for receiving the published
  /// messages.
  /// NOTE(sang): Note that the subscriber needs to "ensure" that the long polling
  /// requests are always in flight as long as the publisher is subscribed.
  /// The publisher failure should be only detected by this RPC.
  ///
  /// \param publisher_address The address of the publisher that publishes
  /// objects.
  /// \param subscriber_address The address of the subscriber.
  void MakeLongPollingPubsubConnection(const rpc::Address &publisher_address,
                                       const rpc::Address &subscriber_address)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Private method to handle long polling responses. Long polling responses contain the
  /// published messages.
  void HandleLongPollingResponse(const rpc::Address &publisher_address,
                                 const rpc::Address &subscriber_address,
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

  /// An event loop to execute RPC callbacks. This should be equivalent to the client
  /// pool's io service.
  instrumented_io_context *callback_service_;

  /// Self node's address information.
  const SubscriberID subscriber_id_;
  const std::string subscriber_address_;
  const int subscriber_port_;

  /// The command batch size for the subscriber.
  const int64_t max_command_batch_size_;

  /// Protects below fields. Since the coordinator runs in a core worker, it should be
  /// thread safe.
  mutable absl::Mutex mutex_;

  /// Commands queue. Commands are reported in FIFO order to the publisher. This
  /// guarantees the ordering of commands because they are delivered only by a single RPC
  /// (long polling request).
  using CommandQueue = std::queue<std::unique_ptr<rpc::Command>>;
  absl::flat_hash_map<PublisherID, CommandQueue> commands_ GUARDED_BY(mutex_);

  /// Cache of gRPC clients to publishers.
  rpc::CoreWorkerClientPool &publisher_client_pool_;

  /// A set to cache the connected publisher ids. "Connected" means the long polling
  /// request is in flight.
  absl::flat_hash_set<PublisherID> publishers_connected_ GUARDED_BY(mutex_);

  /// A set to keep track of in-flight command batch requests
  absl::flat_hash_set<PublisherID> command_batch_sent_ GUARDED_BY(mutex_);

  /// WaitForObjectEviction channel.
  std::shared_ptr<WaitForObjectEvictionChannel> wait_for_object_eviction_channel_
      GUARDED_BY(mutex_);

  /// WaitForRefRemoved channel.
  std::shared_ptr<WaitForRefRemovedChannel> wait_for_ref_removed_channel_
      GUARDED_BY(mutex_);

  /// Mapping of channel type to channels.
  absl::flat_hash_map<rpc::ChannelType, std::shared_ptr<SubscribeChannelInterface>>
      channels_ GUARDED_BY(mutex_);
};

}  // namespace pubsub

}  // namespace ray
