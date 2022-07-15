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
#include "ray/pubsub/publisher.h"
#include "ray/pubsub/subscriber.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

/// \class GcsPublisher
///
/// Supports publishing per-entity data and errors from GCS. Thread safe.
class GcsPublisher {
 public:
  /// Initializes GcsPublisher with GCS based publishers.
  /// Publish*() member functions below would be incrementally converted to use the GCS
  /// based publisher, if available.
  GcsPublisher(std::unique_ptr<pubsub::Publisher> publisher)
      : publisher_(std::move(publisher)) {}

  /// Test only.
  /// TODO: remove this constructor and inject mock / fake from the other constructor.
  explicit GcsPublisher() {}

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

  Status PublishActor(const ActorID &id,
                      const rpc::ActorTableData &message,
                      const StatusCallback &done);

  Status PublishJob(const JobID &id,
                    const rpc::JobTableData &message,
                    const StatusCallback &done);

  Status PublishNodeInfo(const NodeID &id,
                         const rpc::GcsNodeInfo &message,
                         const StatusCallback &done);

  /// Actually rpc::WorkerDeltaData is not a delta message.
  Status PublishWorkerFailure(const WorkerID &id,
                              const rpc::WorkerDeltaData &message,
                              const StatusCallback &done);

  Status PublishError(const std::string &id,
                      const rpc::ErrorTableData &message,
                      const StatusCallback &done);

  /// TODO: remove once it is converted to GRPC-based push broadcasting.
  Status PublishResourceBatch(const rpc::ResourceUsageBatchData &message,
                              const StatusCallback &done);

  /// Prints debugging info for the publisher.
  std::string DebugString() const;

 private:
  const std::unique_ptr<pubsub::Publisher> publisher_;
};

/// \class GcsSubscriber
///
/// Supports subscribing to an entity or a channel from GCS. Thread safe.
class GcsSubscriber {
 public:
  /// Initializes GcsSubscriber with GCS based GcsSubscribers.
  // TODO: Support restarted GCS publisher, at the same or a different address.
  GcsSubscriber(const rpc::Address &gcs_address,
                std::unique_ptr<pubsub::Subscriber> subscriber)
      : gcs_address_(gcs_address), subscriber_(std::move(subscriber)) {}

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

  Status SubscribeAllJobs(const SubscribeCallback<JobID, rpc::JobTableData> &subscribe,
                          const StatusCallback &done);

  Status SubscribeAllNodeInfo(const ItemCallback<rpc::GcsNodeInfo> &subscribe,
                              const StatusCallback &done);

  Status SubscribeAllWorkerFailures(const ItemCallback<rpc::WorkerDeltaData> &subscribe,
                                    const StatusCallback &done);

  /// Prints debugging info for the subscriber.
  std::string DebugString() const;

 private:
  const rpc::Address gcs_address_;
  const std::unique_ptr<pubsub::SubscriberInterface> subscriber_;
};

}  // namespace gcs
}  // namespace ray
