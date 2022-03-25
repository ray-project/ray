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

/// \class GcsPublisher
///
/// Supports publishing per-entity data and errors from GCS. Thread safe.
class GrpcBasedGcsPublisher : class GcsPublisher{
 public:
  /// Initializes GcsPublisher with both Redis and GCS based publishers.
  /// Publish*() member functions below would be incrementally converted to use the GCS
  /// based publisher, if available.
  GrpcBasedGcsPublisher(std::unique_ptr<pubsub::Publisher> publisher)
      : publisher_(std::move(publisher))

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
                      const StatusCallback &done) override;

  /// Uses Redis pubsub.
  Status PublishJob(const JobID &id,
                    const rpc::JobTableData &message,
                    const StatusCallback &done) override;

  /// Uses Redis pubsub.
  Status PublishNodeInfo(const NodeID &id,
                         const rpc::GcsNodeInfo &message,
                         const StatusCallback &done) override;

  /// Uses Redis pubsub.
  Status PublishNodeResource(const NodeID &id,
                             const rpc::NodeResourceChange &message,
                             const StatusCallback &done) override;

  /// Actually rpc::WorkerDeltaData is not a delta message.
  /// Uses Redis pubsub.
  Status PublishWorkerFailure(const WorkerID &id,
                              const rpc::WorkerDeltaData &message,
                              const StatusCallback &done) override;

  /// Uses Redis pubsub.
  Status PublishError(const std::string &id,
                      const rpc::ErrorTableData &message,
                      const StatusCallback &done) override;

  /// Prints debugging info for the publisher.
  std::string DebugString() const override;

 private:
  const std::unique_ptr<pubsub::Publisher> publisher_;
};

/// \class GcsPublisher
///
/// Supports publishing per-entity data and errors from GCS. Thread safe.
class RedisBasedGcsPublisher : class GcsPublisher{
 public:
  /// Initializes GcsPublisher with both Redis and GCS based publishers.
  /// Publish*() member functions below would be incrementally converted to use the GCS
  /// based publisher, if available.
  RedisBasedGcsPublisher(const std::shared_ptr<RedisClient> &redis_client)
      : pubsub_(std::make_unique<GcsPubSub>(redis_client)) {}

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
                      const StatusCallback &done) override;

  /// Uses Redis pubsub.
  Status PublishJob(const JobID &id,
                    const rpc::JobTableData &message,
                    const StatusCallback &done) override;

  /// Uses Redis pubsub.
  Status PublishNodeInfo(const NodeID &id,
                         const rpc::GcsNodeInfo &message,
                         const StatusCallback &done) override;

  /// Uses Redis pubsub.
  Status PublishNodeResource(const NodeID &id,
                             const rpc::NodeResourceChange &message,
                             const StatusCallback &done) override;

  /// Actually rpc::WorkerDeltaData is not a delta message.
  /// Uses Redis pubsub.
  Status PublishWorkerFailure(const WorkerID &id,
                              const rpc::WorkerDeltaData &message,
                              const StatusCallback &done) override;

  /// Uses Redis pubsub.
  Status PublishError(const std::string &id,
                      const rpc::ErrorTableData &message,
                      const StatusCallback &done) override;

  /// Prints debugging info for the publisher.
  std::string DebugString() const override;

 private:
  const std::unique_ptr<pubsub::Publisher> publisher_;
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
  virtual Status PublishActor(const ActorID &id,
                      const rpc::ActorTableData &message,
                      const StatusCallback &done) = 0;

  /// Uses Redis pubsub.
  virtual Status PublishJob(const JobID &id,
                    const rpc::JobTableData &message,
                    const StatusCallback &done) = 0;

  /// Uses Redis pubsub.
  virtual Status PublishNodeInfo(const NodeID &id,
                         const rpc::GcsNodeInfo &message,
                         const StatusCallback &done) = 0;

  /// Uses Redis pubsub.
  virtual Status PublishNodeResource(const NodeID &id,
                             const rpc::NodeResourceChange &message,
                             const StatusCallback &done) = 0;

  /// Actually rpc::WorkerDeltaData is not a delta message.
  /// Uses Redis pubsub.
  virtual Status PublishWorkerFailure(const WorkerID &id,
                              const rpc::WorkerDeltaData &message,
                              const StatusCallback &done) = 0;

  /// Uses Redis pubsub.
  virtual Status PublishError(const std::string &id,
                      const rpc::ErrorTableData &message,
                      const StatusCallback &done) = 0;

  /// Prints debugging info for the publisher.
  virtual std::string DebugString() const = 0;

};