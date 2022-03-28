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

#include <string>

#include "absl/container/flat_hash_map.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/redis_context.h"

namespace ray {
namespace gcs {

/// \class GcsPublisher
///
/// Interface for supporting publishing per-entity data and errors from GCS.
/// It's implemented based on either Redis or gRpc. Thread safe.
class GcsPublisher {
 public:
  /// Initializes GcsPublisher with either Redis or gRpc based publisher.
  GcsPublisher() {}

  virtual pubsub::Publisher *GetPublisher() const = 0;

  /// Each publishing method below publishes to a different "channel".
  /// ID is the entity which the message is associated with, e.g. ActorID for Actor data.
  /// Subscribers receive typed messages for the ID that they subscribe to.
  ///
  /// The full stream of NodeResource and Error channels are needed by its subscribers.
  /// But for other channels, subscribers should only need the latest data.

  virtual Status PublishActor(const ActorID &id,
                              const rpc::ActorTableData &message,
                              const StatusCallback &done) = 0;

  virtual Status PublishJob(const JobID &id,
                            const rpc::JobTableData &message,
                            const StatusCallback &done) = 0;

  virtual Status PublishNodeInfo(const NodeID &id,
                                 const rpc::GcsNodeInfo &message,
                                 const StatusCallback &done) = 0;

  virtual Status PublishNodeResource(const NodeID &id,
                                     const rpc::NodeResourceChange &message,
                                     const StatusCallback &done) = 0;

  /// Actually rpc::WorkerDeltaData is not a delta message.
  virtual Status PublishWorkerFailure(const WorkerID &id,
                                      const rpc::WorkerDeltaData &message,
                                      const StatusCallback &done) = 0;

  virtual Status PublishError(const std::string &id,
                              const rpc::ErrorTableData &message,
                              const StatusCallback &done) = 0;

  /// Prints debugging info for the publisher.
  virtual std::string DebugString() const = 0;
};

/// \class GrpcBasedGcsPublisher
///
/// Gcs publisher based on gRpc.
class GrpcBasedGcsPublisher : public GcsPublisher {
 public:
  GrpcBasedGcsPublisher(const std::vector<rpc::ChannelType> &channels,
                        PeriodicalRunner *const periodical_runner)
      : publisher_(std::make_unique<pubsub::Publisher>(
            /*channels=*/channels,
            /*periodical_runner=*/periodical_runner,
            /*get_time_ms=*/[]() { return absl::GetCurrentTimeNanos() / 1e6; },
            /*subscriber_timeout_ms=*/RayConfig::instance().subscriber_timeout_ms(),
            /*publish_batch_size_=*/RayConfig::instance().publish_batch_size())) {}

  /// Returns the underlying pubsub::Publisher. Caller does not take ownership.
  pubsub::Publisher *GetPublisher() const { return publisher_.get(); }

  /// TODO: Verify GCS pubsub satisfies the streaming semantics.
  /// TODO: Implement optimization for channels where only latest data per ID is useful.

  Status PublishActor(const ActorID &id,
                      const rpc::ActorTableData &message,
                      const StatusCallback &done) override;

  Status PublishJob(const JobID &id,
                    const rpc::JobTableData &message,
                    const StatusCallback &done) override;

  Status PublishNodeInfo(const NodeID &id,
                         const rpc::GcsNodeInfo &message,
                         const StatusCallback &done) override;

  Status PublishNodeResource(const NodeID &id,
                             const rpc::NodeResourceChange &message,
                             const StatusCallback &done) override;

  Status PublishWorkerFailure(const WorkerID &id,
                              const rpc::WorkerDeltaData &message,
                              const StatusCallback &done) override;

  Status PublishError(const std::string &id,
                      const rpc::ErrorTableData &message,
                      const StatusCallback &done) override;

  std::string DebugString() const override;

 private:
  const std::unique_ptr<pubsub::Publisher> publisher_;
};

/// \class RedisBasedGcsPublisher
///
/// Gcs publisher based on Redis.
class RedisBasedGcsPublisher : public GcsPublisher {
 public:
  RedisBasedGcsPublisher(const std::shared_ptr<RedisClient> &redis_client)
      : pubsub_(std::make_unique<GcsPubSub>(redis_client)) {}

  /// Test only.
  /// Initializes GcsPublisher with GcsPubSub, usually a mock.
  /// TODO: remove this constructor and inject mock / fake from the other constructor.
  explicit RedisBasedGcsPublisher(std::unique_ptr<GcsPubSub> pubsub)
      : pubsub_(std::move(pubsub)) {}

  /// Returns nullptr when RayConfig::instance().gcs_grpc_based_pubsub() is false.
  pubsub::Publisher *GetPublisher() const { return nullptr; }

  Status PublishActor(const ActorID &id,
                      const rpc::ActorTableData &message,
                      const StatusCallback &done) override;

  Status PublishJob(const JobID &id,
                    const rpc::JobTableData &message,
                    const StatusCallback &done) override;

  Status PublishNodeInfo(const NodeID &id,
                         const rpc::GcsNodeInfo &message,
                         const StatusCallback &done) override;

  Status PublishNodeResource(const NodeID &id,
                             const rpc::NodeResourceChange &message,
                             const StatusCallback &done) override;

  Status PublishWorkerFailure(const WorkerID &id,
                              const rpc::WorkerDeltaData &message,
                              const StatusCallback &done) override;

  Status PublishError(const std::string &id,
                      const rpc::ErrorTableData &message,
                      const StatusCallback &done) override;

  std::string DebugString() const override;

 private:
  std::unique_ptr<GcsPubSub> pubsub_;
};

}  // namespace gcs
}  // namespace ray