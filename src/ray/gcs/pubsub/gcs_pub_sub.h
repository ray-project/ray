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

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/redis_context.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

#define JOB_CHANNEL "JOB"
#define NODE_CHANNEL "NODE"
#define NODE_RESOURCE_CHANNEL "NODE_RESOURCE"
#define ACTOR_CHANNEL "ACTOR"
#define WORKER_CHANNEL "WORKER"
#define OBJECT_CHANNEL "OBJECT"
#define TASK_CHANNEL "TASK"
#define TASK_LEASE_CHANNEL "TASK_LEASE"
#define RESOURCES_BATCH_CHANNEL "RESOURCES_BATCH"
#define ERROR_INFO_CHANNEL "ERROR_INFO"

/// \class GcsPubSub
///
/// GcsPubSub supports publishing, subscription and unsubscribing of data.
/// This class is thread safe.
class GcsPubSub {
 public:
  /// The callback is called when a subscription message is received.
  using Callback = std::function<void(const std::string &id, const std::string &data)>;

  explicit GcsPubSub(std::shared_ptr<RedisClient> redis_client)
      : redis_client_(redis_client) {}

  virtual ~GcsPubSub() = default;

  /// Posts a message to the given channel.
  ///
  /// \param channel The channel to publish to redis.
  /// \param id The id of message to be published to redis.
  /// \param data The data of message to be published to redis.
  /// \param done Callback that will be called when the message is published to redis.
  /// \return Status
  virtual Status Publish(const std::string &channel, const std::string &id,
                         const std::string &data, const StatusCallback &done);

  /// Subscribe to messages with the specified ID under the specified channel.
  ///
  /// \param channel The channel to subscribe from redis.
  /// \param id The id of message to be subscribed from redis.
  /// \param subscribe Callback that will be called when a subscription message is
  /// received.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  Status Subscribe(const std::string &channel, const std::string &id,
                   const Callback &subscribe, const StatusCallback &done);

  /// Subscribe to messages with the specified channel.
  ///
  /// \param channel The channel to subscribe from redis.
  /// \param subscribe Callback that will be called when a subscription message is
  /// received.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  Status SubscribeAll(const std::string &channel, const Callback &subscribe,
                      const StatusCallback &done);

  /// Unsubscribe to messages with the specified ID under the specified channel.
  ///
  /// \param channel The channel to unsubscribe from redis.
  /// \param id The id of message to be unsubscribed from redis.
  /// \return Status
  Status Unsubscribe(const std::string &channel, const std::string &id);

  /// Check if the specified ID under the specified channel is unsubscribed.
  ///
  /// \param channel The channel to unsubscribe from redis.
  /// \param id The id of message to be unsubscribed from redis.
  /// \return Whether the specified ID under the specified channel is unsubscribed.
  bool IsUnsubscribed(const std::string &channel, const std::string &id);

  std::string DebugString() const;

 private:
  /// Represents a caller's command to subscribe or unsubscribe to a given
  /// channel.
  struct Command {
    /// SUBSCRIBE constructor.
    Command(const Callback &subscribe_callback, const StatusCallback &done_callback,
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

  Status SubscribeInternal(const std::string &channel_name, const Callback &subscribe,
                           const StatusCallback &done, bool is_sub_or_unsub_all,
                           const boost::optional<std::string> &id = boost::none);

  std::string GenChannelPattern(const std::string &channel,
                                const boost::optional<std::string> &id);

  std::shared_ptr<RedisClient> redis_client_;

  /// Mutex to protect the subscribe_callback_index_ field.
  mutable absl::Mutex mutex_;

  absl::flat_hash_map<std::string, Channel> channels_ GUARDED_BY(mutex_);

  size_t total_commands_queued_ GUARDED_BY(mutex_);
};

}  // namespace gcs
}  // namespace ray
