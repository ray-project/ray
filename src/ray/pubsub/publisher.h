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

#include <deque>
#include <functional>
#include <memory>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/id.h"
#include "ray/pubsub/publisher_interface.h"
#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/pubsub.pb.h"

namespace ray {

namespace pubsub {

class SubscriberState;

/**
 * @brief State for an entity / topic in a pub/sub channel.
 */
class EntityState {
 public:
  /**
   * @brief Construct a new EntityState.
   *
   * @param max_message_size_bytes Maximum size of a single message.
   * @param max_buffered_bytes Maximum bytes to buffer. Set to -1 to disable buffering.
   */
  EntityState(int64_t max_message_size_bytes, int64_t max_buffered_bytes)
      : max_message_size_bytes_(max_message_size_bytes),
        max_buffered_bytes_(max_buffered_bytes) {}

  /**
   * @brief Publishes the message to subscribers of the entity.
   *
   * @param pub_message The message to publish to subscribers.
   * @param msg_size The size of the message in bytes.
   * @return true if there are subscribers, false otherwise.
   */
  bool Publish(const std::shared_ptr<rpc::PubMessage> &pub_message, size_t msg_size);

  /**
   * @brief Add a subscriber to this entity.
   *
   * @param subscriber subscriber state.
   */
  void AddSubscriber(SubscriberState *subscriber);

  /**
   * @brief Remove a subscriber from this entity.
   *
   * @param subscriber_id The ID of the subscriber to remove.
   */
  void RemoveSubscriber(const UniqueID &subscriber_id);

  /**
   * @brief Gets the current set of subscribers, keyed by subscriber IDs.
   *
   * @return Map of subscriber IDs to subscriber state.
   */
  const absl::flat_hash_map<UniqueID, SubscriberState *> &Subscribers() const;

  /**
   * @brief Gets the number of bytes currently buffered.
   *
   * @return The total size of buffered messages in bytes.
   */
  size_t GetNumBufferedBytes() const { return total_size_; }

 protected:
  // Subscribers of this entity.
  // The underlying SubscriberState is owned by Publisher.
  absl::flat_hash_map<UniqueID, SubscriberState *> subscribers_;

 private:
  // Tracks inflight messages. The messages have shared ownership by
  // individual subscribers, and get deleted after no subscriber has
  // the message in buffer. Also stores the size of the message so that we can keep track
  // of total_size_.
  std::queue<std::pair<std::weak_ptr<rpc::PubMessage>, size_t>> pending_messages_;

  // Protobuf messages fail to serialize if 2GB or larger. Cap published
  // message batches to this size to ensure that we can publish each message
  // batch. Individual messages larger than this limit will also be dropped.
  // TODO(swang): Pubsub clients should also ensure that they don't try to
  // publish messages larger than this.
  const size_t max_message_size_bytes_;

  // Set to -1 to disable buffering.
  const int64_t max_buffered_bytes_;

  // Total size of inflight messages.
  size_t total_size_ = 0;
};

/**
 * @brief Per-channel two-way index for subscribers and the keys they subscribe to.
 *
 * Also supports subscribers to all keys in the channel.
 */
class SubscriptionIndex {
 public:
  explicit SubscriptionIndex(rpc::ChannelType channel_type);

  /**
   * @brief Publishes the message to relevant subscribers.
   *
   * @param pub_message The message to publish.
   * @param msg_size The size of the message in bytes.
   * @return true if there are subscribers listening on the entity key of the message,
   *         false otherwise.
   */
  bool Publish(const std::shared_ptr<rpc::PubMessage> &pub_message, size_t msg_size);

  /**
   * @brief Adds a new subscriber and the key it subscribes to.
   *
   * @param key_id The key to subscribe to. When empty, the subscriber subscribes to all
   *               keys.
   * @param subscriber Pointer to the subscriber state.
   */
  void AddEntry(const std::string &key_id, SubscriberState *subscriber);

  /**
   * @brief Erases the subscriber from this index.
   *
   * @param subscriber_id The ID of the subscriber to erase.
   */
  void EraseSubscriber(const UniqueID &subscriber_id);

  /**
   * @brief Erases the subscriber from a particular key.
   *
   * @param key_id The key to unsubscribe from. When empty, unsubscribes from all keys.
   * @param subscriber_id The ID of the subscriber to erase.
   */
  void EraseEntry(const std::string &key_id, const UniqueID &subscriber_id);

  /**
   * @brief Returns true if the entity id exists in the index.
   *
   * Only checks entities that are explicitly subscribed.
   *
   * @note Test only.
   * @param key_id The key ID to check.
   * @return true if the key exists in the index, false otherwise.
   */
  bool HasKeyId(const std::string &key_id) const;

  /**
   * @brief Returns true if the subscriber id exists in the index.
   *
   * Includes both per-entity and all-entity subscribers.
   *
   * @note Test only.
   * @param subscriber_id The subscriber ID to check.
   * @return true if the subscriber exists, false otherwise.
   */
  bool HasSubscriber(const UniqueID &subscriber_id) const;

  /**
   * @brief Returns subscriber IDs that are subscribing to the given key.
   *
   * @note Test only.
   * @param key_id The key ID to query.
   * @return Vector of subscriber IDs subscribing to the key.
   */
  std::vector<UniqueID> GetSubscriberIdsByKeyId(const std::string &key_id) const;

  int64_t GetNumBufferedBytes() const;

  /**
   * @brief Checks if there's no metadata remaining in the private attributes.
   *
   * @return true if no metadata remains, false otherwise.
   */
  bool CheckNoLeaks() const;

 private:
  static std::unique_ptr<EntityState> CreateEntityState(rpc::ChannelType channel_type);

  // Type of channel this index is for.
  rpc::ChannelType channel_type_;
  // Collection of subscribers that subscribe to all entities of the channel.
  std::unique_ptr<EntityState> subscribers_to_all_;
  // Mapping from subscribed entity id -> entity state.
  absl::flat_hash_map<std::string, std::unique_ptr<EntityState>> entities_;
  // Mapping from subscriber IDs -> subscribed key ids.
  // Reverse index of key_id_to_subscribers_.
  absl::flat_hash_map<UniqueID, absl::flat_hash_set<std::string>> subscribers_to_key_id_;
};

/**
 * @brief Holds the state of a long poll connection from a subscriber.
 */
struct LongPollConnection {
  LongPollConnection(std::string *publisher_id,
                     google::protobuf::RepeatedPtrField<rpc::PubMessage> *pub_messages,
                     rpc::SendReplyCallback send_reply_callback)
      : publisher_id_(publisher_id),
        pub_messages_(pub_messages),
        send_reply_callback_(std::move(send_reply_callback)) {}

  std::string *publisher_id_;
  google::protobuf::RepeatedPtrField<rpc::PubMessage> *pub_messages_;
  rpc::SendReplyCallback send_reply_callback_;
};

/**
 * @brief Keeps the state of each connected subscriber.
 */
class SubscriberState {
 public:
  SubscriberState(UniqueID subscriber_id,
                  std::function<double()> get_time_ms,
                  uint64_t connection_timeout_ms,
                  int64_t publish_batch_size,
                  UniqueID publisher_id)
      : subscriber_id_(subscriber_id),
        get_time_ms_(std::move(get_time_ms)),
        connection_timeout_ms_(connection_timeout_ms),
        publish_batch_size_(publish_batch_size),
        last_connection_update_time_ms_(get_time_ms_()),
        publisher_id_binary_(publisher_id.Binary()) {}

  ~SubscriberState() {
    // Force a push to close the long-polling.
    // Otherwise, there will be a connection leak.
    PublishIfPossible(/*force_noop=*/true);
  }

  SubscriberState(const SubscriberState &) = delete;
  SubscriberState &operator=(const SubscriberState &) = delete;

  /**
   * @brief Connect to the subscriber.
   *
   * Currently, this caches the long polling request in memory.
   *
   * @param request The long polling request from the subscriber.
   * @param publisher_id[out] Pointer to store the publisher ID.
   * @param pub_messages[out] Pointer to store published messages.
   * @param send_reply_callback Callback to send the reply.
   */
  void ConnectToSubscriber(
      const rpc::PubsubLongPollingRequest &request,
      std::string *publisher_id,
      google::protobuf::RepeatedPtrField<rpc::PubMessage> *pub_messages,
      rpc::SendReplyCallback send_reply_callback);

  /**
   * @brief Queue a pubsub message to publish to the subscriber.
   *
   * @param pub_message The message to queue.
   */
  void QueueMessage(const std::shared_ptr<rpc::PubMessage> &pub_message);

  /**
   * @brief Publish all queued messages if possible.
   *
   * @param force_noop If true, reply to the subscriber with an empty message,
   *                   regardless of whether there is any queued message. This is for
   *                   cases where the current poll might have been cancelled, or the
   *                   subscriber might be dead.
   */
  void PublishIfPossible(bool force_noop);

  /**
   * @brief Checks if there's no metadata remaining in private attributes.
   *
   * @note Testing only.
   * @return true if no metadata remains (no leaks), false otherwise.
   */
  bool CheckNoLeaks() const;

  /**
   * @brief Checks if there is an active long polling connection.
   *
   * @return true if a connection exists, false otherwise.
   */
  bool ConnectionExists() const;

  /**
   * @brief Checks if there are recent activities between subscriber and publisher.
   *
   * @return true if there have been recent requests or replies, false otherwise.
   */
  bool IsActive() const;

  /**
   * @brief Returns the ID of this subscriber.
   *
   * @return Reference to the subscriber's unique ID.
   */
  const UniqueID &id() const { return subscriber_id_; }

 private:
  /// Subscriber ID, for logging and debugging.
  const UniqueID subscriber_id_;
  /// Inflight long polling reply callback, for replying to the subscriber.
  std::unique_ptr<LongPollConnection> long_polling_connection_;
  /// Queued messages to publish.
  std::deque<std::shared_ptr<rpc::PubMessage>> mailbox_;
  /// Callback to get the current time.
  const std::function<double()> get_time_ms_;
  /// The time in which the connection is considered as timed out.
  uint64_t connection_timeout_ms_;
  /// The maximum number of objects to publish for each publish calls.
  const int64_t publish_batch_size_;
  /// The last time long polling was connected in milliseconds.
  double last_connection_update_time_ms_;
  std::string publisher_id_binary_;
};

/**
 * @brief Publisher implementation for the pub/sub system.
 *
 * ## Protocol Details
 *
 * - Subscriber always sends a long polling connection as long as there are subscribed
 *   entries from the publisher.
 * - Publisher caches the long polling request and replies whenever there are published
 *   messages.
 * - Published messages are batched in order to avoid gRPC message limits.
 * - See CheckDeadSubscribers() for the failure handling mechanism.
 *
 * ## How to Add a New Publisher Channel
 *
 * 1. Update pubsub.proto.
 * 2. Add a new channel type -> index to subscription_index_map_.
 */
class Publisher : public PublisherInterface {
 public:
  /**
   * @brief Construct a new Publisher.
   *
   * @param channels Channels where publishing and subscribing are accepted.
   * @param periodical_runner Periodic runner used to periodically run
   *                          CheckDeadSubscribers.
   * @param get_time_ms Callback to get the current time in milliseconds.
   * @param subscriber_timeout_ms The subscriber timeout in milliseconds.
   *                              See CheckDeadSubscribers() for more details.
   * @param publish_batch_size The batch size of published messages.
   * @param publisher_id Optional publisher ID. Defaults to a random NodeID.
   */
  Publisher(const std::vector<rpc::ChannelType> &channels,
            PeriodicalRunnerInterface &periodical_runner,
            std::function<double()> get_time_ms,
            const uint64_t subscriber_timeout_ms,
            int64_t publish_batch_size,
            UniqueID publisher_id = NodeID::FromRandom())
      : periodical_runner_(&periodical_runner),
        get_time_ms_(std::move(get_time_ms)),
        subscriber_timeout_ms_(subscriber_timeout_ms),
        publish_batch_size_(publish_batch_size),
        publisher_id_(publisher_id) {
    // Insert index map for each channel.
    for (auto type : channels) {
      subscription_index_map_.emplace(type, SubscriptionIndex(type));
      possible_channel_types += rpc::ChannelType_Name(type) + ", ";
    }

    periodical_runner_->RunFnPeriodically([this] { CheckDeadSubscribers(); },
                                          subscriber_timeout_ms,
                                          "Publisher.CheckDeadSubscribers");
  }

  void ConnectToSubscriber(
      const rpc::PubsubLongPollingRequest &request,
      std::string *publisher_id,
      google::protobuf::RepeatedPtrField<rpc::PubMessage> *pub_messages,
      rpc::SendReplyCallback send_reply_callback) override;

  StatusSet<StatusT::InvalidArgument> RegisterSubscription(
      const rpc::ChannelType channel_type,
      const UniqueID &subscriber_id,
      const std::optional<std::string> &key_id) override;

  void Publish(rpc::PubMessage pub_message) override;

  void PublishFailure(const rpc::ChannelType channel_type,
                      const std::string &key_id) override;

  void UnregisterSubscription(const rpc::ChannelType channel_type,
                              const UniqueID &subscriber_id,
                              const std::optional<std::string> &key_id) override;

  void UnregisterSubscriber(const UniqueID &subscriber_id) override;

  std::string DebugString() const override;

  /**
   * @brief Check all subscribers for dead or timed-out connections.
   *
   * Detects which subscribers are dead or have timed-out connections, and cleans up
   * their metadata. This uses goal-oriented logic to clean up all metadata that can
   * result from subscriber failures. The process works as follows:
   *
   * 1. If there's no new long polling connection for the timeout period, it refreshes
   *    the long polling connection.
   * 2. If the subscriber is dead, there will be no new long polling connection coming
   *    in again. Otherwise, the connection will be reestablished by the subscriber.
   * 3. If there's no long polling connection for another timeout period, the subscriber
   *    is treated as dead.
   *
   * TODO(sang): Currently iterates all subscribers periodically, which can be inefficient
   *       with many subscribers (e.g., a driver with 100K subscribers due to reference
   *       counting). Consider optimizing with a per-subscriber timer.
   */
  void CheckDeadSubscribers();

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
  friend class FakePublisher;

  /// Testing only.
  Publisher() : publish_batch_size_(-1) {}

  /// Testing only. Return true if there's no metadata remained in the private attribute.
  bool CheckNoLeaks() const;

  ///
  /// Private fields
  ///

  void UnregisterSubscriberInternal(const UniqueID &subscriber_id)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Periodic runner to invoke CheckDeadSubscribers.
  // The pointer must outlive the Publisher.
  // Nonnull in production, may be nullptr in tests.
  PeriodicalRunnerInterface *periodical_runner_;

  /// Callback to get the current time.
  std::function<double()> get_time_ms_;

  /// The timeout where subscriber is considered as dead.
  uint64_t subscriber_timeout_ms_;

  /// Protects below fields. Since the coordinator runs in a core worker, it should be
  /// thread safe.
  mutable absl::Mutex mutex_;

  /// Mapping of node id -> subscribers.
  absl::flat_hash_map<UniqueID, std::unique_ptr<SubscriberState>> subscribers_
      ABSL_GUARDED_BY(mutex_);

  /// Index that stores the mapping of messages <-> subscribers.
  absl::flat_hash_map<rpc::ChannelType, SubscriptionIndex> subscription_index_map_
      ABSL_GUARDED_BY(mutex_);

  /// The maximum number of objects to publish for each publish calls.
  const int64_t publish_batch_size_;

  absl::flat_hash_map<rpc::ChannelType, uint64_t> cum_pub_message_cnt_
      ABSL_GUARDED_BY(mutex_);

  absl::flat_hash_map<rpc::ChannelType, uint64_t> cum_pub_message_bytes_cnt_
      ABSL_GUARDED_BY(mutex_);

  /// The monotonically increasing sequence_id for this publisher.
  /// The publisher will add this sequence_id to every message to be published.
  /// The sequence_id is used for handling failures: the publisher will not delete
  /// a message from the sending queue until the subscriber has acknowledge
  /// it has processed beyond the message's sequence_id.
  ///
  /// Note:
  ///  - a valide sequence_id starts from 1.
  ///  - the subscriber doesn't expect the sequences it receives are contiguous.
  ///    this is due the fact a subscriber can only subscribe a subset
  ///    of a channel.
  int64_t next_sequence_id_ ABSL_GUARDED_BY(mutex_) = 0;

  const UniqueID publisher_id_;

  std::string possible_channel_types;
};

}  // namespace pubsub

}  // namespace ray
