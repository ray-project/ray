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

#ifndef RAY_GCS_GCS_TABLE_PUB_SUB_H_
#define RAY_GCS_GCS_TABLE_PUB_SUB_H_

#include "ray/common/id.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/redis_client.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

using rpc::ActorTableData;
using rpc::GcsNodeInfo;
using rpc::HeartbeatBatchTableData;
using rpc::HeartbeatTableData;
using rpc::JobTableData;
using rpc::ObjectChange;
using rpc::ResourceChange;
using rpc::TablePubsub;
using rpc::TaskLeaseData;
using rpc::TaskTableData;
using rpc::WorkerFailureData;

/// \class GcsTablePubSub
///
/// GcsTablePubSub supports publishing, subscription and unsubscribing of GCS table data.
/// This class is not meant to be used directly. All gcs table pub sub classes should
/// derive from this class and override the pub_sub_channel_ member with a unique value
/// for that table.
template <typename ID, typename Data>
class GcsTablePubSub {
 public:
  /// The callback is called when a subscription message is received.
  using Callback = std::function<void(const ID &id, const Data &data)>;

  explicit GcsTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : pub_sub_channel_(TablePubsub::NO_PUBLISH), redis_client_(redis_client) {}

  virtual ~GcsTablePubSub() = default;

  /// Posts a message to the given channel.
  ///
  /// \param id The id of message to be published to redis.
  /// \param data The data of message to be published to redis.
  /// \param done Callback that will be called when the message is published to redis.
  /// \return Status
  Status Publish(const ID &id, const Data &data, const StatusCallback &done);

  /// Subscribe to messages with the specified ID under the specified channel.
  ///
  /// \param id The id of message to be subscribed from redis.
  /// \param subscribe Callback that will be called when a subscription message is
  /// received. \param done Callback that will be called when subscription is complete.
  /// \return Status
  Status Subscribe(const ID &id, const Callback &subscribe, const StatusCallback &done);

  /// Subscribe to messages with the specified channel.
  ///
  /// \param subscribe Callback that will be called when a subscription message is
  /// received. \param done Callback that will be called when subscription is complete.
  /// \return Status
  Status SubscribeAll(const Callback &subscribe, const StatusCallback &done);

  /// Unsubscribe to messages with the specified ID under the specified channel.
  ///
  /// \param id The id of message to be unsubscribed from redis.
  /// \param done Callback that will be called when unsubscription is complete.
  /// \return Status
  Status Unsubscribe(const ID &id, const StatusCallback &done);

 protected:
  TablePubsub pub_sub_channel_;

 private:
  Status Subscribe(const boost::optional<ID> &id, const Callback &subscribe,
                   const StatusCallback &done);

  std::string GenChannelPattern(const boost::optional<ID> &id = boost::none);

  std::shared_ptr<RedisClient> redis_client_;
  std::unordered_map<std::string, int64_t> subscribe_callback_index_;
  std::unordered_map<std::string, StatusCallback> unsubscribe_callbacks_;
};

class GcsJobTablePubSub : public GcsTablePubSub<JobID, JobTableData> {
 public:
  explicit GcsJobTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTablePubSub(redis_client) {
    pub_sub_channel_ = TablePubsub::JOB_PUBSUB;
  }
};

class GcsActorTablePubSub : public GcsTablePubSub<ActorID, ActorTableData> {
 public:
  explicit GcsActorTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTablePubSub(redis_client) {
    pub_sub_channel_ = TablePubsub::ACTOR_PUBSUB;
  }
};

class GcsTaskTablePubSub : public GcsTablePubSub<TaskID, TaskTableData> {
 public:
  explicit GcsTaskTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTablePubSub(redis_client) {
    pub_sub_channel_ = TablePubsub::TASK_PUBSUB;
  }
};

class GcsTaskLeaseTablePubSub : public GcsTablePubSub<TaskID, TaskLeaseData> {
 public:
  explicit GcsTaskLeaseTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTablePubSub(redis_client) {
    pub_sub_channel_ = TablePubsub::TASK_LEASE_PUBSUB;
  }
};

class GcsObjectTablePubSub : public GcsTablePubSub<ObjectID, ObjectChange> {
 public:
  explicit GcsObjectTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTablePubSub(redis_client) {
    pub_sub_channel_ = TablePubsub::OBJECT_PUBSUB;
  }
};

class GcsNodeTablePubSub : public GcsTablePubSub<ClientID, GcsNodeInfo> {
 public:
  explicit GcsNodeTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTablePubSub(redis_client) {
    pub_sub_channel_ = TablePubsub::CLIENT_PUBSUB;
  }
};

class GcsNodeResourceTablePubSub : public GcsTablePubSub<ClientID, ResourceChange> {
 public:
  explicit GcsNodeResourceTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTablePubSub(redis_client) {
    pub_sub_channel_ = TablePubsub::NODE_RESOURCE_PUBSUB;
  }
};

class GcsHeartbeatTablePubSub : public GcsTablePubSub<ClientID, HeartbeatTableData> {
 public:
  explicit GcsHeartbeatTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTablePubSub(redis_client) {
    pub_sub_channel_ = TablePubsub::HEARTBEAT_PUBSUB;
  }
};

class GcsHeartbeatBatchTablePubSub
    : public GcsTablePubSub<ClientID, HeartbeatBatchTableData> {
 public:
  explicit GcsHeartbeatBatchTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTablePubSub(redis_client) {
    pub_sub_channel_ = TablePubsub::HEARTBEAT_BATCH_PUBSUB;
  }
};

class GcsWorkerFailureTablePubSub : public GcsTablePubSub<WorkerID, WorkerFailureData> {
 public:
  explicit GcsWorkerFailureTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTablePubSub(redis_client) {
    pub_sub_channel_ = TablePubsub::WORKER_FAILURE_PUBSUB;
  }
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_GCS_TABLE_PUB_SUB_H_
