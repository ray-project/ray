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

/// State for an entity / topic in a pub/sub channel.
class EntityState {
 public:
  /// \param max_buffered_bytes set to -1 to disable buffering.
  EntityState(int64_t max_message_size_bytes, int64_t max_buffered_bytes)
      : max_message_size_bytes_(max_message_size_bytes),
        max_buffered_bytes_(max_buffered_bytes) {}

  /// Publishes the message to subscribers of the entity.
  /// Returns true if there are subscribers, returns false otherwise.
  bool Publish(const std::shared_ptr<rpc::PubMessage> &pub_message, size_t msg_size);

  /// Manages the set of subscribers of this entity.
  void AddSubscriber(SubscriberState *subscriber);
  void RemoveSubscriber(const UniqueID &subscriber_id);

  /// Gets the current set of subscribers, keyed by subscriber IDs.
  const absl::flat_hash_map<UniqueID, SubscriberState *> &Subscribers() const;

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

/// Per-channel two-way index for subscribers and the keys they subscribe to.
/// Also supports subscribers to all keys in the channel.
class SubscriptionIndex {
 public:
  explicit SubscriptionIndex(rpc::ChannelType channel_type);

  /// Publishes the message to relevant subscribers.
  /// Returns true if there are subscribers listening on the entity key of the message,
  /// returns false otherwise.
  bool Publish(const std::shared_ptr<rpc::PubMessage> &pub_message, size_t msg_size);

  /// Adds a new subscriber and the key it subscribes to.
  /// When `key_id` is empty, the subscriber subscribes to all keys.
  void AddEntry(const std::string &key_id, SubscriberState *subscriber);

  /// Erases the subscriber from this index.
  /// Returns whether the subscriber exists before the call.
  void EraseSubscriber(const UniqueID &subscriber_id);

  /// Erases the subscriber from the particular key.
  /// When `key_id` is empty, the subscriber subscribes to all keys.
  void EraseEntry(const std::string &key_id, const UniqueID &subscriber_id);

  /// Test only.
  /// Returns true if the entity id exists in the index.
  /// Only checks entities that are explicitly subscribed.
  bool HasKeyId(const std::string &key_id) const;

  /// Test only.
  /// Returns true if the subscriber id exists in the index, including both per-entity
  /// and all-entity subscribers.
  bool HasSubscriber(const UniqueID &subscriber_id) const;

  /// Returns a vector of subscriber ids that are subscribing to the given object ids.
  /// Test only.
  std::vector<UniqueID> GetSubscriberIdsByKeyId(const std::string &key_id) const;

  int64_t GetNumBufferedBytes() const;

  /// Returns true if there's no metadata remained in the private attribute.
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

/// Keeps the state of each connected subscriber.
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

  /// Connect to the subscriber. Currently, it means we cache the long polling request to
  /// memory.
  void ConnectToSubscriber(
      const rpc::PubsubLongPollingRequest &request,
      std::string *publisher_id,
      google::protobuf::RepeatedPtrField<rpc::PubMessage> *pub_messages,
      rpc::SendReplyCallback send_reply_callback);

  /// Queue the pubsub message to publish to the subscriber.
  void QueueMessage(const std::shared_ptr<rpc::PubMessage> &pub_message);

  /// Publish all queued messages if possible.
  ///
  /// \param force_noop If true, reply to the subscriber with an empty message, regardless
  /// of whethere there is any queued message. This is for cases where the current poll
  /// might have been cancelled, or the subscriber might be dead.
  void PublishIfPossible(bool force_noop);

  /// Testing only. Return true if there's no metadata remained in the private attribute.
  bool CheckNoLeaks() const;

  /// Returns true if there is a long polling connection.
  bool ConnectionExists() const;

  /// Returns true if there are recent activities (requests or replies) between the
  /// subscriber and publisher.
  bool IsActive() const;

  /// Returns the ID of this subscriber.
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

  void RegisterSubscription(const rpc::ChannelType channel_type,
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
};

}  // namespace pubsub

}  // namespace ray
