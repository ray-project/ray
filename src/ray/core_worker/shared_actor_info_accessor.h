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

#include "ray/gcs/gcs_client/gcs_client.h"

namespace ray {

// Multiple core worker instances need to share a single instance of this class, to avoid
// duplicated actor subscription.
class SharedActorInfoAccessor {
 public:
  SharedActorInfoAccessor(std::shared_ptr<gcs::GcsClient> gcs_client);

  Status AsyncSubscribe(const ActorID &actor_id, const WorkerID &worker_id,
                        const gcs::SubscribeCallback<ActorID, rpc::ActorTableData>
                            &actor_notification_callback,
                        const gcs::StatusCallback &done);

  void OnWorkerShutdown(const WorkerID &worker_id);

 private:
  std::shared_ptr<gcs::GcsClient> gcs_client_;

  absl::Mutex mutex_;

  /// This mutex is used to ensure there's no running callbacks when a CoreWorker instance
  /// is shutting down. With this mutex, we can avoid the case that the CoreWorker
  /// instance is already been destroyed while there is a running callback tries to access
  /// the destroyed CoreWorker instance or its fields/sub-fields.
  absl::Mutex worker_shutdown_mutex_;

  /// This map stores notification callbacks of subscribe actions from all CoreWorker
  /// instances. We use this map to invoke the notification callbacks of those workers
  /// which subscribes an actor when the actor state notification message arrives.
  std::unordered_map<
      ActorID,
      std::unordered_map<WorkerID, gcs::SubscribeCallback<ActorID, rpc::ActorTableData>>>
      id_to_notification_callbacks_ GUARDED_BY(mutex_);

  /// This map stores done callbacks of subscribe actions from all CoreWorker
  /// instances. We use this map to invoke the done callbacks of those workers
  /// which subscribes an actor when the underlying subscribe operatation finishes.
  std::unordered_map<ActorID, std::unordered_map<WorkerID, gcs::StatusCallback>>
      id_to_done_callbacks_ GUARDED_BY(mutex_);

  /// This map stores the cached subscribe status of all actors. Because the underlying
  /// subscribe operation is only performed once for each actor, we need to cache the
  /// status so we can invoke the done callback immediately for the subsequent subscribe
  /// operations from other workers.
  std::unordered_map<ActorID, Status> ids_gcs_done_callback_invoked_ GUARDED_BY(mutex_);

  /// This map stores the cached actor state notification messages of all subscribed
  /// actors. If the actor is already subscribed by a worker and the latest actor state
  /// notification has already arrived, and another worker is subscribing to the same
  /// actor, we need to invoke the notification callback from the latter worker with the
  /// cached message immediately.
  std::unordered_map<ActorID, rpc::ActorTableData> actor_infos_ GUARDED_BY(mutex_);
};

}  // namespace ray
