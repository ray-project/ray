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

#ifndef RAY_GCS_GCS_PUB_SUB_H_
#define RAY_GCS_GCS_PUB_SUB_H_

#include "absl/synchronization/mutex.h"

#include "ray/gcs/callback.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/redis_context.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

#define JOB_CHANNEL "JOB"
#define WORKER_FAILURE_CHANNEL "WORKER_FAILURE"

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
  Status Publish(const std::string &channel, const std::string &id,
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

 private:
  Status SubscribeInternal(const std::string &channel, const Callback &subscribe,
                           const StatusCallback &done,
                           const boost::optional<std::string> &id = boost::none);

  std::string GenChannelPattern(const std::string &channel,
                                const boost::optional<std::string> &id);

  std::shared_ptr<RedisClient> redis_client_;

  /// Mutex to protect the subscribe_callback_index_ field.
  absl::Mutex mutex_;

  std::unordered_map<std::string, int64_t> subscribe_callback_index_ GUARDED_BY(mutex_);
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_GCS_PUB_SUB_H_
