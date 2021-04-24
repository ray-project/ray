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
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"

#include "ray/common/asio/periodical_runner.h"
#include "ray/common/id.h"
#include "ray/rpc/worker/core_worker_client_pool.h"
#include "ray/rpc/worker/core_worker_server.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/pubsub.pb.h"

namespace ray {

namespace pubsub {

using SubscriberID = UniqueID;

namespace pub_internal {

/// Index for object ids and node ids of subscribers.
template <typename MessageID>
class SubscriptionIndex {
 public:
  explicit SubscriptionIndex() {}
  ~SubscriptionIndex() = default;

  /// Add a new entry to the index.
  /// NOTE: If the entry already exists, it raises assert failure.
  void AddEntry(const std::string &message_id_binary, const SubscriberID &subscriber_id);

  /// Return the set of subscriber ids that are subscribing to the given object ids.
  absl::optional<std::reference_wrapper<const absl::flat_hash_set<SubscriberID>>>
  GetSubscriberIdsByMessageId(const std::string &message_id_binary) const;

  /// Erase the subscriber from the index. Returns the number of erased subscribers.
  /// NOTE: It cannot erase subscribers that were never added.
  bool EraseSubscriber(const SubscriberID &subscriber_id);

  /// Erase the object id and subscriber id from the index. Return the number of erased
  /// entries.
  /// NOTE: It cannot erase subscribers that were never added.
  bool EraseEntry(const std::string &message_id_binary,
                  const SubscriberID &subscriber_id);

  /// Return true if the object id exists in the index.
  bool HasMessageId(const std::string &message_id_binary) const;

  /// Returns true if object id or subscriber id exists in the index.
  bool HasSubscriber(const SubscriberID &subscriber_id) const;

  /// Testing only. Return true if there's no metadata remained in the private attribute.
  bool CheckNoLeaks() const;

 private:
  const MessageID ParseBinary(const std::string &message_id_binary) const {
    return MessageID::FromBinary(message_id_binary);
  }

  /// Mapping from message id -> subscribers.
  absl::flat_hash_map<MessageID, absl::flat_hash_set<SubscriberID>>
      message_id_to_subscribers_;
  // Mapping from subscribers -> message ids. Reverse index of message_id_to_subscribers_.
  absl::flat_hash_map<SubscriberID, absl::flat_hash_set<MessageID>>
      subscribers_to_message_id_;
};

struct LongPollConnection {
  LongPollConnection(rpc::PubsubLongPollingReply *reply,
                     rpc::SendReplyCallback send_reply_callback)
      : reply(reply), send_reply_callback(send_reply_callback) {}

  rpc::PubsubLongPollingReply *reply;
  rpc::SendReplyCallback send_reply_callback;
};

/// Abstraction to each subscriber.
class Subscriber {
 public:
  explicit Subscriber(const std::function<double()> &get_time_ms,
                      uint64_t connection_timeout_ms, const int publish_batch_size)
      : get_time_ms_(get_time_ms),
        connection_timeout_ms_(connection_timeout_ms),
        publish_batch_size_(publish_batch_size),
        last_connection_update_time_ms_(get_time_ms()) {}

  ~Subscriber() = default;

  /// Connect to the subscriber. Currently, it means we cache the long polling request to
  /// memory. Once the bidirectional gRPC streaming is enabled, we should replace it.
  ///
  /// \param reply pubsub long polling reply.
  /// \param send_reply_callback A callback to reply to the long polling subscriber.
  /// \return True if connection is new. False if there were already connections cached.
  bool ConnectToSubscriber(rpc::PubsubLongPollingReply *reply,
                           rpc::SendReplyCallback send_reply_callback);

  /// Queue the object id to publish to the subscriber.
  ///
  /// \param pub_message A message to publish.
  /// \param try_publish If true, it try publishing the object id if there is a
  /// connection. False is used only for testing.
  void QueueMessage(std::unique_ptr<rpc::PubMessage> pub_message,
                    bool try_publish = true);

  /// Publish all queued messages if possible.
  ///
  /// \param force If true, we publish to the subscriber although there's no queued
  /// message.
  /// \return True if it publishes. False otherwise.
  bool PublishIfPossible(bool force = false);

  /// Testing only. Return true if there's no metadata remained in the private attribute.
  bool CheckNoLeaks() const;

  /// Return true if the subscriber is disconnected (if the subscriber is dead).
  /// The subscriber is considered to be dead if there was no long polling connection for
  /// the timeout.
  bool IsDisconnected() const;

  /// Return true if there was no new long polling connection for a long time.
  bool IsActiveConnectionTimedOut() const;

 private:
  /// Cached long polling reply callback.
  /// It is cached whenever new long polling is coming from the subscriber.
  /// It becomes a nullptr whenever the long polling request is replied.
  std::unique_ptr<LongPollConnection> long_polling_connection_;
  /// Queued messages to publish.
  std::list<std::unique_ptr<rpc::PubsubLongPollingReply>> mailbox_;
  /// Callback to get the current time.
  const std::function<double()> get_time_ms_;
  /// The time in which the connection is considered as timed out.
  uint64_t connection_timeout_ms_;
  /// The maximum number of objects to publish for each publish calls.
  const int publish_batch_size_;
  /// The last time long polling was connected in milliseconds.
  double last_connection_update_time_ms_;
};

}  // namespace pub_internal

/// Publisher interface. Note that message ids are passed as a string to avoid templated
/// definition which doesn't go well with virutal methods.
class PublisherInterface {
 public:
  virtual ~PublisherInterface() {}

  /// Register the subscription.
  ///
  /// \param channel_type The type of the channel.
  /// \param subscriber_id The node id of the subscriber.
  /// \param message_id_binary The message_id that the subscriber is subscribing to.
  virtual void RegisterSubscription(const rpc::ChannelType channel_type,
                                    const SubscriberID &subscriber_id,
                                    const std::string &message_id_binary) = 0;

  /// Publish the given object id to subscribers.
  ///
  /// \param channel_type The type of the channel.
  /// \param pub_message The message to publish.
  /// \param message_id_binary The message id to publish.
  virtual void Publish(const rpc::ChannelType channel_type,
                       std::unique_ptr<rpc::PubMessage> pub_message,
                       const std::string &message_id_binary) = 0;

  /// Unregister subscription. It means the given object id won't be published to the
  /// subscriber anymore.
  ///
  /// \param channel_type The type of the channel.
  /// \param subscriber_id The node id of the subscriber.
  /// \param message_id_binary The message_id of the subscriber.
  /// \return True if erased. False otherwise.
  virtual bool UnregisterSubscription(const rpc::ChannelType channel_type,
                                      const SubscriberID &subscriber_id,
                                      const std::string &message_id_binary) = 0;
};

/// Protocol detail
///
/// - Subscriber always send a long polling connection as long as there are subscribed
/// entries from the publisher.
/// - Publisher caches the long polling request and reply whenever there are published
/// messages.
/// - Publishes messages are batched in order to avoid gRPC message limit.
/// - Look at CheckDeadSubscribers for failure handling mechanism.
///
/// How to add new publisher channel?
///
/// - Update pubsub.proto.
/// - Add a new channel type -> index to subscription_index_map_.
/// - If your SubscriptionIndex requires different id types, make sure to add template
/// definition to the publisher.cc file.
///
class Publisher : public PublisherInterface {
 public:
  /// Pubsub coordinator constructor.
  ///
  /// \param periodical_runner Periodic runner. Used to periodically run
  /// CheckDeadSubscribers.
  /// \param get_time_ms A callback to get the current time in
  /// milliseconds.
  /// \param subscriber_timeout_ms The subscriber timeout in milliseconds.
  /// Check out CheckDeadSubscribers for more details.
  /// \param publish_batch_size The batch size of published messages.
  explicit Publisher(PeriodicalRunner *periodical_runner,
                     const std::function<double()> get_time_ms,
                     const uint64_t subscriber_timeout_ms, const int publish_batch_size)
      : periodical_runner_(periodical_runner),
        get_time_ms_(get_time_ms),
        subscriber_timeout_ms_(subscriber_timeout_ms),
        publish_batch_size_(publish_batch_size) {
    periodical_runner_->RunFnPeriodically([this] { CheckDeadSubscribers(); },
                                          subscriber_timeout_ms);
    // Insert index map for each channel.
    subscription_index_map_.emplace(rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION,
                                    pub_internal::SubscriptionIndex<ObjectID>());
    subscription_index_map_.emplace(rpc::ChannelType::WAIT_FOR_REF_REMOVED_CHANNEL,
                                    pub_internal::SubscriptionIndex<ObjectID>());
  }

  ~Publisher() = default;

  /// Cache the subscriber's long polling request's information.
  ///
  /// TODO(sang): Currently, we need to pass the callback for connection because we are
  /// using long polling internally. This should be changed once the bidirectional grpc
  /// streaming is supported.
  void ConnectToSubscriber(const SubscriberID &subscriber_id,
                           rpc::PubsubLongPollingReply *reply,
                           rpc::SendReplyCallback send_reply_callback);

  /// Register the subscription.
  ///
  /// \param channel_type The type of the channel.
  /// \param subscriber_id The node id of the subscriber.
  /// \param message_id_binary The message_id that the subscriber is subscribing to.
  void RegisterSubscription(const rpc::ChannelType channel_type,
                            const SubscriberID &subscriber_id,
                            const std::string &message_id_binary) override;

  /// Publish the given object id to subscribers.
  ///
  /// \param channel_type The type of the channel.
  /// \param pub_message The message to publish.
  /// \param message_id_binary The message id to publish.
  void Publish(const rpc::ChannelType channel_type,
               std::unique_ptr<rpc::PubMessage> pub_message,
               const std::string &message_id_binary) override;

  /// Unregister subscription. It means the given object id won't be published to the
  /// subscriber anymore.
  ///
  /// \param channel_type The type of the channel.
  /// \param subscriber_id The node id of the subscriber.
  /// \param message_id_binary The message_id of the subscriber.
  /// \return True if erased. False otherwise.
  bool UnregisterSubscription(const rpc::ChannelType channel_type,
                              const SubscriberID &subscriber_id,
                              const std::string &message_id_binary) override;

  /// Remove the subscriber. Once the subscriber is removed, messages won't be published
  /// to it anymore.
  /// TODO(sang): Currently, clients don't send a RPC to unregister themselves.
  ///
  /// \param subscriber_id The node id of the subscriber to unsubscribe.
  /// \return True if erased. False otherwise.
  bool UnregisterSubscriber(const SubscriberID &subscriber_id);

  /// Check all subscribers, detect which subscribers are dead or its connection is timed
  /// out, and clean up their metadata. This uses the goal-oriented logic to clean up all
  /// metadata that can happen by subscriber failures. It is how it works;
  ///
  /// - If there's no new long polling connection for the timeout, it refreshes the long
  /// polling connection.
  /// - If the subscriber is dead, there will be no new long polling connection coming in
  /// again. Otherwise, the long polling connection will be reestablished by the
  /// subscriber.
  /// - If there's no long polling connection for another timeout, it treats the
  /// subscriber as dead.
  ///
  /// TODO(sang): Currently, it iterates all subscribers periodically. This can be
  /// inefficient in some scenarios.
  /// For example, think about we have a driver with 100K subscribers (it can
  /// happen due to reference counting). We might want to optimize this by
  /// having a timer per subscriber.
  void CheckDeadSubscribers();

  /// Testing only. Return true if there's no metadata remained in the private attribute.
  bool CheckNoLeaks() const;

 private:
  bool UnregisterSubscriberInternal(const SubscriberID &subscriber_id)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Periodic runner to invoke CheckDeadSubscribers.
  PeriodicalRunner *periodical_runner_;

  /// Callback to get the current time.
  const std::function<double()> get_time_ms_;

  /// The timeout where subscriber is considered as dead.
  const uint64_t subscriber_timeout_ms_;

  /// Protects below fields. Since the coordinator runs in a core worker, it should be
  /// thread safe.
  mutable absl::Mutex mutex_;

  /// Mapping of node id -> subscribers.
  absl::flat_hash_map<SubscriberID, std::shared_ptr<pub_internal::Subscriber>>
      subscribers_ GUARDED_BY(mutex_);

  /// Index that stores the mapping of messages <-> subscribers.
  absl::flat_hash_map<rpc::ChannelType, pub_internal::SubscriptionIndex<ObjectID>>
      subscription_index_map_ GUARDED_BY(mutex_);

  /// The maximum number of objects to publish for each publish calls.
  const int publish_batch_size_;
};

}  // namespace pubsub

}  // namespace ray
