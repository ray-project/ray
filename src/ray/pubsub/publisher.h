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

#include <functional>
#include <queue>
#include <string>
#include <string_view>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/id.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/pubsub.pb.h"

namespace ray {

namespace pubsub {

using SubscriberID = UniqueID;

namespace pub_internal {

class SubscriberState;

/// State for an entity / topic in a pub/sub channel.
class EntityState {
 public:
  /// Publishes the message to subscribers of the entity.
  /// Returns true if there are subscribers, returns false otherwise.
  bool Publish(const rpc::PubMessage &pub_message);

  /// Manages the set of subscribers of this entity.
  bool AddSubscriber(SubscriberState *subscriber);
  bool RemoveSubscriber(const SubscriberID &id);

  /// Gets the current set of subscribers, keyed by subscriber IDs.
  const absl::flat_hash_map<SubscriberID, SubscriberState *> &Subscribers() const;

 private:
  // Tracks inflight messages. The messages have shared ownership by
  // individual subscribers, and get deleted after no subscriber has
  // the message in buffer.
  std::queue<std::weak_ptr<rpc::PubMessage>> pending_messages_;
  // Size of each inflight message.
  std::queue<int64_t> message_sizes_;
  // Total size of inflight messages.
  int64_t total_size_ = 0;

  // Subscribers of this entity.
  // The underlying SubscriberState is owned by Publisher.
  absl::flat_hash_map<SubscriberID, SubscriberState *> subscribers_;
};

/// Per-channel two-way index for subscribers and the keys they subscribe to.
/// Also supports subscribers to all keys in the channel.
class SubscriptionIndex {
 public:
  SubscriptionIndex() = default;
  ~SubscriptionIndex() = default;

  /// Publishes the message to relevant subscribers.
  /// Returns true if there are subscribers listening on the entity key of the message,
  /// returns false otherwise.
  bool Publish(const rpc::PubMessage &pub_message);

  /// Adds a new subscriber and the key it subscribes to.
  /// When `key_id` is empty, the subscriber subscribes to all keys.
  /// NOTE: The method is idempotent. If it adds a duplicated entry, it will be no-op.
  bool AddEntry(const std::string &key_id, SubscriberState *subscriber);

  /// Erases the subscriber from this index.
  /// Returns whether the subscriber exists before the call.
  bool EraseSubscriber(const SubscriberID &subscriber_id);

  /// Erases the subscriber from the particular key.
  /// When `key_id` is empty, the subscriber subscribes to all keys.
  bool EraseEntry(const std::string &key_id, const SubscriberID &subscriber_id);

  /// Test only.
  /// Returns true if the entity id exists in the index.
  /// Only checks entities that are explicitly subscribed.
  bool HasKeyId(const std::string &key_id) const;

  /// Test only.
  /// Returns true if the subscriber id exists in the index, including both per-entity
  /// and all-entity subscribers.
  bool HasSubscriber(const SubscriberID &subscriber_id) const;

  /// Returns a vector of subscriber ids that are subscribing to the given object ids.
  /// Test only.
  std::vector<SubscriberID> GetSubscriberIdsByKeyId(const std::string &key_id) const;

  /// Returns true if there's no metadata remained in the private attribute.
  bool CheckNoLeaks() const;

 private:
  // Collection of subscribers that subscribe to all entities of the channel.
  EntityState subscribers_to_all_;
  // Mapping from subscribed entity id -> entity state.
  absl::flat_hash_map<std::string, EntityState> entities_;
  // Mapping from subscriber IDs -> subscribed key ids.
  // Reverse index of key_id_to_subscribers_.
  absl::flat_hash_map<SubscriberID, absl::flat_hash_set<std::string>>
      subscribers_to_key_id_;
};

struct LongPollConnection {
  LongPollConnection(rpc::PubsubLongPollingReply *reply,
                     rpc::SendReplyCallback send_reply_callback)
      : reply(reply), send_reply_callback(send_reply_callback) {}

  rpc::PubsubLongPollingReply *reply;
  rpc::SendReplyCallback send_reply_callback;
};

/// Keeps the state of each connected subscriber.
class SubscriberState {
 public:
  SubscriberState(SubscriberID subscriber_id,
                  std::function<double()> get_time_ms,
                  uint64_t connection_timeout_ms,
                  const int publish_batch_size)
      : subscriber_id_(subscriber_id),
        get_time_ms_(std::move(get_time_ms)),
        connection_timeout_ms_(connection_timeout_ms),
        publish_batch_size_(publish_batch_size),
        last_connection_update_time_ms_(get_time_ms_()) {}

  ~SubscriberState() = default;

  /// Connect to the subscriber. Currently, it means we cache the long polling request to
  /// memory. Once the bidirectional gRPC streaming is enabled, we should replace it.
  ///
  /// \param reply pubsub long polling reply.
  /// \param send_reply_callback A callback to reply to the long polling subscriber.
  void ConnectToSubscriber(const rpc::PubsubLongPollingRequest &request,
                           rpc::PubsubLongPollingReply *reply,
                           rpc::SendReplyCallback send_reply_callback);

  /// Queue the pubsub message to publish to the subscriber.
  ///
  /// \param pub_message A message to publish.
  /// \param try_publish If true, try publishing the object id if there is a connection.
  ///     Currently only set to false in tests.
  void QueueMessage(const std::shared_ptr<rpc::PubMessage> &pub_message,
                    bool try_publish = true);

  /// Publish all queued messages if possible.
  ///
  /// \param force_noop If true, reply to the subscriber with an empty message, regardless
  /// of whethere there is any queued message. This is for cases where the current poll
  /// might have been cancelled, or the subscriber might be dead.
  /// \return True if it publishes. False otherwise.
  bool PublishIfPossible(bool force_noop = false);

  /// Testing only. Return true if there's no metadata remained in the private attribute.
  bool CheckNoLeaks() const;

  /// Returns true if there is a long polling connection.
  bool ConnectionExists() const;

  /// Returns true if there are recent activities (requests or replies) between the
  /// subscriber and publisher.
  bool IsActive() const;

  /// Returns the ID of this subscriber.
  const SubscriberID &id() const { return subscriber_id_; }

 private:
  /// Subscriber ID, for logging and debugging.
  const SubscriberID subscriber_id_;
  /// Inflight long polling reply callback, for replying to the subscriber.
  std::unique_ptr<LongPollConnection> long_polling_connection_;
  /// Queued messages to publish.
  std::queue<std::shared_ptr<rpc::PubMessage>> mailbox_;
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
/// definition which doesn't go well with virtual methods.
class PublisherInterface {
 public:
  virtual ~PublisherInterface() = default;

  /// Register the subscription.
  ///
  /// \param channel_type The type of the channel.
  /// \param subscriber_id The node id of the subscriber.
  /// \param key_id The key_id that the subscriber is subscribing to. std::nullopt if
  /// subscribing to all.
  /// \return True if registration is new. False otherwise.
  virtual bool RegisterSubscription(const rpc::ChannelType channel_type,
                                    const SubscriberID &subscriber_id,
                                    const std::optional<std::string> &key_id) = 0;

  /// Publish the given object id to subscribers.
  ///
  /// \param pub_message The message to publish.
  /// Required to contain channel_type and key_id fields.
  virtual void Publish(const rpc::PubMessage &pub_message) = 0;

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
  /// \param subscriber_id The node id of the subscriber.
  /// \param key_id The key_id of the subscriber. std::nullopt if subscribing to all.
  /// \return True if erased. False otherwise.
  virtual bool UnregisterSubscription(const rpc::ChannelType channel_type,
                                      const SubscriberID &subscriber_id,
                                      const std::optional<std::string> &key_id) = 0;
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
///
class Publisher : public PublisherInterface {
 public:
  /// Pubsub coordinator constructor.
  ///
  /// \param channels Channels where publishing and subscribing are accepted.
  /// \param periodical_runner Periodic runner. Used to periodically run
  /// CheckDeadSubscribers.
  /// \param get_time_ms A callback to get the current time in
  /// milliseconds.
  /// \param subscriber_timeout_ms The subscriber timeout in milliseconds.
  /// Check out CheckDeadSubscribers for more details.
  /// \param publish_batch_size The batch size of published messages.
  Publisher(const std::vector<rpc::ChannelType> &channels,
            PeriodicalRunner *const periodical_runner,
            std::function<double()> get_time_ms,
            const uint64_t subscriber_timeout_ms,
            const int publish_batch_size)
      : periodical_runner_(periodical_runner),
        get_time_ms_(std::move(get_time_ms)),
        subscriber_timeout_ms_(subscriber_timeout_ms),
        publish_batch_size_(publish_batch_size) {
    // Insert index map for each channel.
    for (auto type : channels) {
      subscription_index_map_.emplace(type, pub_internal::SubscriptionIndex());
    }

    periodical_runner_->RunFnPeriodically([this] { CheckDeadSubscribers(); },
                                          subscriber_timeout_ms);
  }

  ~Publisher() override = default;

  /// Handle a long poll request from `subscriber_id`.
  ///
  /// TODO(sang): Currently, we need to pass the callback for connection because we are
  /// using long polling internally. This should be changed once the bidirectional grpc
  /// streaming is supported.
  void ConnectToSubscriber(const rpc::PubsubLongPollingRequest &request,
                           rpc::PubsubLongPollingReply *reply,
                           rpc::SendReplyCallback send_reply_callback);

  /// Register the subscription.
  ///
  /// \param channel_type The type of the channel.
  /// \param subscriber_id The node id of the subscriber.
  /// \param key_id The key_id that the subscriber is subscribing to.
  /// \return True if the registration is new. False otherwise.
  bool RegisterSubscription(const rpc::ChannelType channel_type,
                            const SubscriberID &subscriber_id,
                            const std::optional<std::string> &key_id) override;

  /// Publish the given object id to subscribers.
  ///
  /// \param pub_message The message to publish.
  /// Required to contain channel_type and key_id fields.
  void Publish(const rpc::PubMessage &pub_message) override;

  /// Publish to the subscriber that the given key id is not available anymore.
  /// It will invoke the failure callback on the subscriber side.
  ///
  /// \param channel_type The type of the channel.
  /// \param key_id The message id to publish.
  void PublishFailure(const rpc::ChannelType channel_type,
                      const std::string &key_id) override;

  /// Unregister subscription. It means the given object id won't be published to the
  /// subscriber anymore.
  ///
  /// \param channel_type The type of the channel.
  /// \param subscriber_id The node id of the subscriber.
  /// \param key_id The key_id of the subscriber.
  /// \return True if erased. False otherwise.
  bool UnregisterSubscription(const rpc::ChannelType channel_type,
                              const SubscriberID &subscriber_id,
                              const std::optional<std::string> &key_id) override;

  /// Remove the subscriber. Once the subscriber is removed, messages won't be published
  /// to it anymore.
  /// TODO(sang): Currently, clients don't send a RPC to unregister themselves.
  ///
  /// \param subscriber_id The node id of the subscriber to unsubscribe.
  /// \return True if erased. False otherwise.
  bool UnregisterSubscriber(const SubscriberID &subscriber_id);

  /// Flushes all inflight pollings and unregisters all subscribers.
  void UnregisterAll();

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

  std::string DebugString() const;

 private:
  ///
  /// Testing fields
  ///

  FRIEND_TEST(PublisherTest, TestBasicSingleSubscriber);
  FRIEND_TEST(PublisherTest, TestNoConnectionWhenRegistered);
  FRIEND_TEST(PublisherTest, TestMultiObjectsFromSingleNode);
  FRIEND_TEST(PublisherTest, TestMultiObjectsFromMultiNodes);
  FRIEND_TEST(PublisherTest, TestMultiSubscribers);
  FRIEND_TEST(PublisherTest, TestBatch);
  FRIEND_TEST(PublisherTest, TestNodeFailureWhenConnectionExisted);
  FRIEND_TEST(PublisherTest, TestNodeFailureWhenConnectionDoesntExist);
  FRIEND_TEST(PublisherTest, TestUnregisterSubscription);
  FRIEND_TEST(PublisherTest, TestUnregisterSubscriber);
  FRIEND_TEST(PublisherTest, TestRegistrationIdempotency);
  friend class MockPublisher;
  Publisher() {}

  /// Testing only. Return true if there's no metadata remained in the private attribute.
  bool CheckNoLeaks() const;

  ///
  /// Private fields
  ///

  int UnregisterSubscriberInternal(const SubscriberID &subscriber_id)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Periodic runner to invoke CheckDeadSubscribers.
  PeriodicalRunner *periodical_runner_;

  /// Callback to get the current time.
  std::function<double()> get_time_ms_;

  /// The timeout where subscriber is considered as dead.
  uint64_t subscriber_timeout_ms_;

  /// Protects below fields. Since the coordinator runs in a core worker, it should be
  /// thread safe.
  mutable absl::Mutex mutex_;

  /// Mapping of node id -> subscribers.
  absl::flat_hash_map<SubscriberID, std::unique_ptr<pub_internal::SubscriberState>>
      subscribers_ GUARDED_BY(mutex_);

  /// Index that stores the mapping of messages <-> subscribers.
  absl::flat_hash_map<rpc::ChannelType, pub_internal::SubscriptionIndex>
      subscription_index_map_ GUARDED_BY(mutex_);

  /// The maximum number of objects to publish for each publish calls.
  int publish_batch_size_;

  absl::flat_hash_map<rpc::ChannelType, uint64_t> cum_pub_message_cnt_ GUARDED_BY(mutex_);
};

}  // namespace pubsub

}  // namespace ray
