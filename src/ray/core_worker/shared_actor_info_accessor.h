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

#include "ray/gcs/gcs_client.h"

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

  std::unordered_map<
      ActorID,
      std::unordered_map<WorkerID, gcs::SubscribeCallback<ActorID, rpc::ActorTableData>>>
      id_to_notification_callbacks_ GUARDED_BY(mutex_);

  std::unordered_map<ActorID, std::unordered_map<WorkerID, gcs::StatusCallback>>
      id_to_done_callbacks_ GUARDED_BY(mutex_);

  std::unordered_map<ActorID, Status> ids_gcs_done_callback_invoked_ GUARDED_BY(mutex_);

  std::unordered_map<ActorID, rpc::ActorTableData> actor_infos_ GUARDED_BY(mutex_);
};

}  // namespace ray
