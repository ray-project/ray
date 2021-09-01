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
    const gcs::SubscribeCallback<ActorID, rpc::ActorTableData>
        &actor_notification_callback,
    const gcs::StatusCallback &done) {
  {
    absl::ReleasableMutexLock lock(&mutex_);
    auto done_it = ids_gcs_done_callback_invoked_.find(actor_id);
    if (done_it != ids_gcs_done_callback_invoked_.end()) {
      RAY_CHECK(id_to_done_callbacks_.find(actor_id) == id_to_done_callbacks_.end());
      // Invoke the done callback immediately
      lock.Release();
      done(done_it->second);
    } else {
      id_to_done_callbacks_[actor_id].emplace(worker_id, done);
    }
  }

  absl::ReleasableMutexLock lock(&mutex_);
  auto &notification_callbacks = id_to_notification_callbacks_[actor_id];
  if (notification_callbacks.empty()) {
    notification_callbacks.emplace(worker_id, actor_notification_callback);
    lock.Release();
    auto gcs_notification_callback = [this](const ActorID &actor_id,
                                            const rpc::ActorTableData &actor_data) {
      std::unordered_map<WorkerID, gcs::SubscribeCallback<ActorID, rpc::ActorTableData>>
          notification_callbacks;
      {
        absl::MutexLock lock(&mutex_);
        actor_infos_[actor_id] = actor_data;
        auto it = id_to_notification_callbacks_.find(actor_id);
        if (it != id_to_notification_callbacks_.end()) {
          // AsyncUnsubscribe maybe called in notification_callbacks. Copy the
          // notification_callbacks to a local variable and iterate it later to avoid dead
          // lock.
          notification_callbacks = it->second;
        }
      }
      for (auto &notification_callback : notification_callbacks) {
        notification_callback.second(actor_id, actor_data);
      }
    };
    auto gcs_done_callback = [this, actor_id](Status status) {
      std::unordered_map<WorkerID, gcs::StatusCallback> done_callbacks;
      {
        absl::MutexLock lock(&mutex_);
        RAY_CHECK(ids_gcs_done_callback_invoked_.emplace(actor_id, status).second);
        auto it = id_to_done_callbacks_.find(actor_id);
        if (it != id_to_done_callbacks_.end()) {
          done_callbacks = std::move(it->second);
        }
        id_to_done_callbacks_.erase(it);
      }
      for (auto &done_callback : done_callbacks) {
        done_callback.second(status);
      }
    };
    return gcs_client_->Actors().AsyncSubscribe(actor_id, gcs_notification_callback,
                                                gcs_done_callback);
  } else {
    notification_callbacks.emplace(worker_id, actor_notification_callback);
    // Invoke the notification callback with the last received actor update immediately,
    // if available.
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
  for (auto &entry : id_to_notification_callbacks_) {
    entry.second.erase(worker_id);
  }
}

}  // namespace ray
