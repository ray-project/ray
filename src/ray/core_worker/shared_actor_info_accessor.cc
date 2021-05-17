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

#include "ray/core_worker/shared_actor_info_accessor.h"

namespace ray {

SharedActorInfoAccessor::SharedActorInfoAccessor(
    std::shared_ptr<gcs::GcsClient> gcs_client)
    : gcs_client_(gcs_client) {}

Status SharedActorInfoAccessor::AsyncSubscribe(
    const ActorID &actor_id, const WorkerID &worker_id,
    gcs::SubscribeCallback<ActorID, rpc::ActorTableData> actor_notification_callback) {
  absl::ReleasableMutexLock lock(&mutex_);
  auto &callbacks = id_to_callbacks_[actor_id];
  if (callbacks.empty()) {
    callbacks.emplace(worker_id, actor_notification_callback);
    lock.Release();
    auto gcs_callback = [this](const ActorID &actor_id,
                               const rpc::ActorTableData &actor_data) {
      std::unordered_map<WorkerID, gcs::SubscribeCallback<ActorID, rpc::ActorTableData>>
          callbacks;
      {
        absl::MutexLock lock(&mutex_);
        actor_infos_[actor_id] = actor_data;
        auto it = id_to_callbacks_.find(actor_id);
        if (it != id_to_callbacks_.end()) {
          // AsyncUnsubscribe maybe called in callbacks. Copy the callbacks to a local
          // variable and iterate it later to avoid dead lock.
          callbacks = it->second;
        }
      }
      for (auto &callback : callbacks) {
        callback.second(actor_id, actor_data);
      }
    };
    return gcs_client_->Actors().AsyncSubscribe(actor_id, gcs_callback, nullptr);
  } else {
    callbacks.emplace(worker_id, actor_notification_callback);
    // Invoke the callback with the last received actor update immediately, if available.
    auto it = actor_infos_.find(actor_id);
    if (it != actor_infos_.end()) {
      // Copy the actor info before releasing the lock.
      auto actor_info = it->second;
      lock.Release();
      actor_notification_callback(actor_id, actor_info);
    }
    return Status::OK();
  }
}

void SharedActorInfoAccessor::OnWorkerShutdown(const WorkerID &worker_id) {
  absl::ReleasableMutexLock lock(&mutex_);
  for (auto &entry : id_to_callbacks_) {
    entry.second.erase(worker_id);
  }
}

}  // namespace ray
