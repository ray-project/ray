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

#include <optional>
#include <string>
#include <string_view>

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/redis_context.h"
#include "ray/pubsub/publisher.h"
#include "ray/pubsub/subscriber.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

/// Channel name constants for Redis pubsub.
/// Will be removed after migrating to GCS pubsub.
inline constexpr std::string_view JOB_CHANNEL = "JOB";
inline constexpr std::string_view NODE_CHANNEL = "NODE";
inline constexpr std::string_view NODE_RESOURCE_CHANNEL = "NODE_RESOURCE";
inline constexpr std::string_view ACTOR_CHANNEL = "ACTOR";
inline constexpr std::string_view WORKER_CHANNEL = "WORKER";
inline constexpr std::string_view TASK_LEASE_CHANNEL = "TASK_LEASE";
inline constexpr std::string_view RESOURCES_BATCH_CHANNEL = "RESOURCES_BATCH";
inline constexpr std::string_view ERROR_INFO_CHANNEL = "ERROR_INFO";

/// \class GcsPubSub
///
/// GcsPubSub supports publishing, subscription and unsubscribing of data.
/// This class is thread safe.
class GcsPubSub {
 public:
  /// The callback is called when a subscription message is received.
  using Callback = std::function<void(const std::string &id, const std::string &data)>;

  explicit GcsPubSub(const std::shared_ptr<RedisClient> &redis_client)
      : redis_client_(redis_client), total_commands_queued_(0) {}

  virtual ~GcsPubSub() = default;

  /// Posts a message to the given channel.
  ///
  /// \param channel The channel to publish to redis.
  /// \param id The id of message to be published to redis.
  /// \param data The data of message to be published to redis.
  /// \param done Callback that will be called when the message is published to redis.
  /// \return Status
  virtual Status Publish(std::string_view channel,
                         const std::string &id,
                         const std::string &data,
                         const StatusCallback &done);

  /// Subscribe to messages with the specified ID under the specified channel.
  ///
  /// \param channel The channel to subscribe from redis.
  /// \param id The id of message to be subscribed from redis.
  /// \param subscribe Callback that will be called when a subscription message is
  /// received.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  Status Subscribe(std::string_view channel,
                   const std::string &id,
                   const Callback &subscribe,
                   const StatusCallback &done);

  /// Subscribe to messages with the specified channel.
  ///
  /// \param channel The channel to subscribe from redis.
  /// \param subscribe Callback that will be called when a subscription message is
  /// received.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  Status SubscribeAll(std::string_view channel,
                      const Callback &subscribe,
                      const StatusCallback &done);

  /// Unsubscribe to messages with the specified ID under the specified channel.
  ///
  /// \param channel The channel to unsubscribe from redis.
  /// \param id The id of message to be unsubscribed from redis.
  /// \return Status
  Status Unsubscribe(std::string_view channel, const std::string &id);

  /// Check if the specified ID under the specified channel is unsubscribed.
  ///
  /// \param channel The channel to unsubscribe from redis.
  /// \param id The id of message to be unsubscribed from redis.
  /// \return Whether the specified ID under the specified channel is unsubscribed.
  bool IsUnsubscribed(std::string_view channel, const std::string &id);

  std::string DebugString() const;

 protected:
  GcsPubSub() : GcsPubSub(nullptr) {}

 private:
  /// Represents a caller's command to subscribe or unsubscribe to a given
  /// channel.
  struct Command {
    /// SUBSCRIBE constructor.
    Command(const Callback &subscribe_callback,
            const StatusCallback &done_callback,
            bool is_sub_or_unsub_all)
        : is_subscribe(true),
          subscribe_callback(subscribe_callback),
          done_callback(done_callback),
          is_sub_or_unsub_all(is_sub_or_unsub_all) {}
    /// UNSUBSCRIBE constructor.
    Command() : is_subscribe(false), is_sub_or_unsub_all(false) {}
    /// True if this is a SUBSCRIBE command and false if UNSUBSCRIBE.
    const bool is_subscribe;
    /// Callback that is called whenever a new pubsub message is received from
    /// Redis. This should only be set if is_subscribe is true.
    const Callback subscribe_callback;
    /// Callback that is called once we have successfully subscribed to a
    /// channel. This should only be set if is_subscribe is true.
    const StatusCallback done_callback;
    /// True if this is a SUBSCRIBE all or UNSUBSCRIBE all command else false.
    const bool is_sub_or_unsub_all;
  };

  struct Channel {
    Channel() {}
    /// Queue of subscribe/unsubscribe commands to this channel. The queue
    /// asserts that subscribe and unsubscribe commands alternate, i.e. there
    /// cannot be more than one subscribe/unsubscribe command in a row. A
    /// subscribe command can execute if the callback index below is not set,
    /// i.e. this is the first subscribe command or the last unsubscribe
    /// command's reply has been received. An unsubscribe command can execute
    /// if the callback index is set, i.e. the last subscribe command's reply
    /// has been received.
    std::deque<Command> command_queue;
    /// The current Redis callback index stored in the RedisContext for this
    /// channel. This callback index is used to identify any pubsub
    /// notifications meant for this channel. The callback index is set once we
    /// have received a reply from Redis that we have subscribed. The callback
    /// index is set back to -1 if we receive a reply from Redis that we have
    /// unsubscribed.
    int64_t callback_index = -1;
    /// Whether we are pending a reply from Redis. We cannot send another
    /// command from the queue until this has been reset to false.
    bool pending_reply = false;
  };

  /// Execute the first queued command for the given channel, if possible.  A
  /// subscribe command can execute if the channel's callback index is not set.
  /// An unsubscribe command can execute if the channel's callback index is
  /// set.
  Status ExecuteCommandIfPossible(const std::string &channel_key,
                                  GcsPubSub::Channel &channel)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status SubscribeInternal(std::string_view channel_name,
                           const Callback &subscribe,
                           const StatusCallback &done,
                           const std::optional<std::string_view> &id);

  std::string GenChannelPattern(std::string_view channel,
                                const std::optional<std::string_view> &id);

  std::shared_ptr<RedisClient> redis_client_;

  /// Mutex to protect the subscribe_callback_index_ field.
  mutable absl::Mutex mutex_;

  absl::flat_hash_map<std::string, Channel> channels_ GUARDED_BY(mutex_);

  size_t total_commands_queued_ GUARDED_BY(mutex_);
};

/// \class GcsPublisher
///
/// Supports publishing per-entity data and errors from GCS. Thread safe.
class GcsPublisher {
 public:
  /// Initializes GcsPublisher with both Redis and GCS based publishers.
  /// Publish*() member functions below would be incrementally converted to use the GCS
  /// based publisher, if available.
  GcsPublisher(const std::shared_ptr<RedisClient> &redis_client,
               std::unique_ptr<pubsub::Publisher> publisher)
      : pubsub_(std::make_unique<GcsPubSub>(redis_client)),
        publisher_(std::move(publisher)) {}

  /// Test only.
  /// Initializes GcsPublisher with GcsPubSub, usually a mock.
  /// TODO: remove this constructor and inject mock / fake from the other constructor.
  explicit GcsPublisher(std::unique_ptr<GcsPubSub> pubsub) : pubsub_(std::move(pubsub)) {}

  /// Returns the underlying pubsub::Publisher. Caller does not take ownership.
  /// Returns nullptr when RayConfig::instance().gcs_grpc_based_pubsub() is false.
  pubsub::Publisher *GetPublisher() const { return publisher_.get(); }

  /// Each publishing method below publishes to a different "channel".
  /// ID is the entity which the message is associated with, e.g. ActorID for Actor data.
  /// Subscribers receive typed messages for the ID that they subscribe to.
  ///
  /// The full stream of NodeResource and Error channels are needed by its subscribers.
  /// But for other channels, subscribers should only need the latest data.
  ///
  /// TODO: Verify GCS pubsub satisfies the streaming semantics.
  /// TODO: Implement optimization for channels where only latest data per ID is useful.

  /// Uses Redis pubsub.
  Status PublishActor(const ActorID &id,
                      const rpc::ActorTableData &message,
                      const StatusCallback &done);

  /// Uses Redis pubsub.
  Status PublishJob(const JobID &id,
                    const rpc::JobTableData &message,
                    const StatusCallback &done);

  /// Uses Redis pubsub.
  Status PublishNodeInfo(const NodeID &id,
                         const rpc::GcsNodeInfo &message,
                         const StatusCallback &done);

  /// Uses Redis pubsub.
  Status PublishNodeResource(const NodeID &id,
                             const rpc::NodeResourceChange &message,
                             const StatusCallback &done);

  /// Actually rpc::WorkerDeltaData is not a delta message.
  /// Uses Redis pubsub.
  Status PublishWorkerFailure(const WorkerID &id,
                              const rpc::WorkerDeltaData &message,
                              const StatusCallback &done);

  /// Uses Redis pubsub.
  Status PublishError(const std::string &id,
                      const rpc::ErrorTableData &message,
                      const StatusCallback &done);

  /// TODO: remove once it is converted to GRPC-based push broadcasting.
  /// Uses Redis pubsub.
  Status PublishResourceBatch(const rpc::ResourceUsageBatchData &message,
                              const StatusCallback &done);

  /// Prints debugging info for the publisher.
  std::string DebugString() const;

 private:
  const std::unique_ptr<GcsPubSub> pubsub_;
  const std::unique_ptr<pubsub::Publisher> publisher_;
};

/// \class GcsSubscriber
///
/// Supports subscribing to an entity or a channel from GCS. Thread safe.
class GcsSubscriber {
 public:
  /// Initializes GcsSubscriber with both Redis and GCS based GcsSubscribers.
  // TODO: Support restarted GCS publisher, at the same or a different address.
  GcsSubscriber(const std::shared_ptr<RedisClient> &redis_client,
                const rpc::Address &gcs_address,
                std::unique_ptr<pubsub::Subscriber> subscriber)
      : gcs_address_(gcs_address), subscriber_(std::move(subscriber)) {
    if (redis_client) {
      pubsub_ = std::make_unique<GcsPubSub>(redis_client);
    } else {
      RAY_CHECK(::RayConfig::instance().gcs_grpc_based_pubsub())
          << "gRPC based pubsub has to be enabled";
    }
  }

  /// Subscribe*() member functions below would be incrementally converted to use the GCS
  /// based subscriber, if available.
  /// The `subscribe` callbacks must not be empty. The `done` callbacks can optionally be
  /// empty.

  /// Uses GCS pubsub when created with `subscriber`.
  Status SubscribeActor(const ActorID &id,
                        const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
                        const StatusCallback &done);
  Status UnsubscribeActor(const ActorID &id);
  bool IsActorUnsubscribed(const ActorID &id);

  /// Uses Redis pubsub.
  Status SubscribeAllJobs(const SubscribeCallback<JobID, rpc::JobTableData> &subscribe,
                          const StatusCallback &done);

  /// Uses Redis pubsub.
  Status SubscribeAllNodeInfo(const ItemCallback<rpc::GcsNodeInfo> &subscribe,
                              const StatusCallback &done);

  /// Uses Redis pubsub.
  Status SubscribeAllNodeResources(const ItemCallback<rpc::NodeResourceChange> &subscribe,
                                   const StatusCallback &done);

  /// Uses Redis pubsub.
  Status SubscribeAllWorkerFailures(const ItemCallback<rpc::WorkerDeltaData> &subscribe,
                                    const StatusCallback &done);

  /// TODO: remove once it is converted to GRPC-based push broadcasting.
  /// Uses Redis pubsub.
  Status SubscribeResourcesBatch(
      const ItemCallback<rpc::ResourceUsageBatchData> &subscribe,
      const StatusCallback &done);

  /// Prints debugging info for the subscriber.
  std::string DebugString() const;

 private:
  std::unique_ptr<GcsPubSub> pubsub_;
  const rpc::Address gcs_address_;
  const std::unique_ptr<pubsub::SubscriberInterface> subscriber_;
};

}  // namespace gcs
}  // namespace ray
