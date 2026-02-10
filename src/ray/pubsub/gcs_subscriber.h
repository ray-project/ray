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

#include "ray/pubsub/subscriber_interface.h"
#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace pubsub {

/// \class GcsSubscriber
///
/// Supports subscribing to an entity or a channel from GCS. Thread safe.
class GcsSubscriber {
 public:
  /// Initializes GcsSubscriber with GCS based GcsSubscribers.
  // TODO(mwtian): Support restarted GCS publisher, at the same or a different address.
  GcsSubscriber(rpc::Address gcs_address,
                std::unique_ptr<pubsub::SubscriberInterface> subscriber)
      : gcs_address_(std::move(gcs_address)), subscriber_(std::move(subscriber)) {}

  /// Subscribe*() member functions below would be incrementally converted to use the GCS
  /// based subscriber, if available.
  /// The `subscribe` callbacks must not be empty. The `done` callbacks can optionally be
  /// empty.

  /// Uses GCS pubsub when created with `subscriber`.
  void SubscribeActor(
      const ActorID &id,
      const rpc::SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
      const rpc::StatusCallback &done);
  void UnsubscribeActor(const ActorID &id);

  bool IsActorUnsubscribed(const ActorID &id);

  void SubscribeAllJobs(const rpc::SubscribeCallback<JobID, rpc::JobTableData> &subscribe,
                        const rpc::StatusCallback &done);

  void SubscribeAllNodeInfo(const rpc::ItemCallback<rpc::GcsNodeInfo> &subscribe,
                            const rpc::StatusCallback &done);

  void SubscribeAllNodeAddressAndLiveness(
      const rpc::ItemCallback<rpc::GcsNodeAddressAndLiveness> &subscribe,
      const rpc::StatusCallback &done);

  void SubscribeAllWorkerFailures(
      const rpc::ItemCallback<rpc::WorkerDeltaData> &subscribe,
      const rpc::StatusCallback &done);

  /// Prints debugging info for the subscriber.
  std::string DebugString() const;

 private:
  const rpc::Address gcs_address_;
  const std::unique_ptr<pubsub::SubscriberInterface> subscriber_;
};

}  // namespace pubsub
}  // namespace ray
