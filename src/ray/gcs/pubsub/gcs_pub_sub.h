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

#include "ray/common/id.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/redis_context.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

using rpc::TablePubsub;

/// \class GcsPubSub
///
/// GcsPubSub supports publishing, subscription and unsubscribing of data.
/// This class is thread safe.
class GcsPubSub {
 public:
  /// The callback is called when a subscription message is received.
  template <typename ID, typename Data>
  using Callback = std::function<void(const ID &id, const Data &data)>;

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
  template <typename ID, typename Data>
  Status Publish(const TablePubsub &channel, const ID &id, const Data &data,
                 const StatusCallback &done) {
    rpc::PubSubMessage message;
    message.set_id(id.Binary());
    std::string data_str;
    data.SerializeToString(&data_str);
    message.set_data(data_str);

    auto on_done = [done](std::shared_ptr<CallbackReply> reply) {
      if (done) {
        done(Status::OK());
      }
    };

    return redis_client_->GetPrimaryContext()->PublishAsync(
        GenChannelPattern(channel, boost::optional<ID>(id)), message.SerializeAsString(),
        on_done);
  }

  /// Subscribe to messages with the specified ID under the specified channel.
  ///
  /// \param channel The channel to subscribe from redis.
  /// \param id The id of message to be subscribed from redis.
  /// \param subscribe Callback that will be called when a subscription message is
  /// received.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  template <typename ID, typename Data>
  Status Subscribe(const TablePubsub &channel, const ID &id,
                   const Callback<ID, Data> &subscribe, const StatusCallback &done) {
    return SubscribeInternal(channel, subscribe, done, boost::optional<ID>(id));
  }

  /// Subscribe to messages with the specified channel.
  ///
  /// \param channel The channel to subscribe from redis.
  /// \param subscribe Callback that will be called when a subscription message is
  /// received.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  template <typename ID, typename Data>
  Status SubscribeAll(const TablePubsub &channel, const Callback<ID, Data> &subscribe,
                      const StatusCallback &done) {
    return SubscribeInternal(channel, subscribe, done);
  }

  /// Unsubscribe to messages with the specified ID under the specified channel.
  ///
  /// \param channel The channel to unsubscribe from redis.
  /// \param id The id of message to be unsubscribed from redis.
  /// \return Status
  template <typename ID>
  Status Unsubscribe(const TablePubsub &channel, const ID &id) {
    return redis_client_->GetPrimaryContext()->PUnsubscribeAsync(
        GenChannelPattern(channel, boost::optional<ID>(id)));
  }

 private:
  template <typename ID, typename Data>
  Status SubscribeInternal(const TablePubsub &channel,
                           const Callback<ID, Data> &subscribe,
                           const StatusCallback &done,
                           const boost::optional<ID> &id = boost::none) {
    std::string pattern = GenChannelPattern(channel, id);
    auto callback = [this, pattern, subscribe](std::shared_ptr<CallbackReply> reply) {
      if (!reply->IsNil()) {
        if (reply->GetMessageType() == "punsubscribe") {
          absl::MutexLock lock(&mutex_);
          ray::gcs::RedisCallbackManager::instance().remove(
              subscribe_callback_index_[pattern]);
          subscribe_callback_index_.erase(pattern);
        } else {
          const auto reply_data = reply->ReadAsPubsubData();
          if (!reply_data.empty()) {
            rpc::PubSubMessage message;
            message.ParseFromString(reply_data);
            Data data;
            data.ParseFromString(message.data());
            subscribe(ID::FromBinary(message.id()), data);
          }
        }
      }
    };

    int64_t out_callback_index;
    auto status = redis_client_->GetPrimaryContext()->PSubscribeAsync(
        pattern, callback, &out_callback_index);
    if (id) {
      absl::MutexLock lock(&mutex_);
      subscribe_callback_index_[pattern] = out_callback_index;
    }
    if (done) {
      done(status);
    }
    return status;
  }

  template <typename ID>
  std::string GenChannelPattern(const TablePubsub &channel,
                                const boost::optional<ID> &id) {
    std::stringstream pattern;
    pattern << channel << ":";
    if (id) {
      pattern << id->Binary();
    } else {
      pattern << "*";
    }
    return pattern.str();
  }

  std::shared_ptr<RedisClient> redis_client_;

  /// Mutex to protect the subscribe_callback_index_ field.
  absl::Mutex mutex_;

  std::unordered_map<std::string, int64_t> subscribe_callback_index_ GUARDED_BY(mutex_);
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_GCS_TABLE_PUB_SUB_H_
