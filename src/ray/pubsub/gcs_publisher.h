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

#include <memory>
#include <string>
#include <utility>

#include "ray/pubsub/publisher_interface.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace pubsub {

/// \class GcsPublisher
///
/// Supports publishing per-entity data and errors from GCS. Thread safe.
class GcsPublisher {
 public:
  /// Initializes GcsPublisher with GCS based publishers.
  /// Publish*() member functions below would be incrementally converted to use the GCS
  /// based publisher, if available.
  explicit GcsPublisher(std::unique_ptr<pubsub::PublisherInterface> publisher)
      : publisher_(std::move(publisher)) {
    RAY_CHECK(publisher_);
  }

  /// Returns the underlying pubsub::Publisher. Caller does not take ownership.
  pubsub::PublisherInterface &GetPublisher() const { return *publisher_; }

  /// Each publishing method below publishes to a different "channel".
  /// ID is the entity which the message is associated with, e.g. ActorID for Actor data.
  /// Subscribers receive typed messages for the ID that they subscribe to.
  ///
  /// The full stream of NodeResource and Error channels are needed by its subscribers.
  /// But for other channels, subscribers should only need the latest data.
  ///
  /// TODO: Verify GCS pubsub satisfies the streaming semantics.
  /// TODO: Implement optimization for channels where only latest data per ID is useful.

  void PublishActor(const ActorID &id, rpc::ActorTableData message);

  void PublishJob(const JobID &id, rpc::JobTableData message);

  void PublishNodeInfo(const NodeID &id, rpc::GcsNodeInfo message);

  /// Actually rpc::WorkerDeltaData is not a delta message.
  void PublishWorkerFailure(const WorkerID &id, rpc::WorkerDeltaData message);

  void PublishError(std::string id, rpc::ErrorTableData message);

  /// Prints debugging info for the publisher.
  std::string DebugString() const;

 private:
  const std::unique_ptr<pubsub::PublisherInterface> publisher_;
};

}  // namespace pubsub
}  // namespace ray
