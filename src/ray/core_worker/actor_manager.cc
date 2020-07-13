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

#include "ray/core_worker/actor_manager.h"

#include "ray/gcs/pb_util.h"
#include "ray/gcs/redis_accessor.h"

namespace ray {

ActorID ActorManager::RegisterActorHandle(std::unique_ptr<ActorHandle> actor_handle,
                                          const ObjectID &outer_object_id,
                                          const TaskID &caller_id,
                                          const std::string &call_site,
                                          const rpc::Address &caller_address) {
  const ActorID actor_id = actor_handle->GetActorID();
  const rpc::Address owner_address = actor_handle->GetOwnerAddress();
  const auto actor_creation_return_id = ObjectID::ForActorHandle(actor_id);

  RAY_UNUSED(AddActorHandle(std::move(actor_handle),
                            /*is_owner_handle=*/false, caller_id, call_site,
                            caller_address, actor_id, actor_creation_return_id));
  ObjectID actor_handle_id = ObjectID::ForActorHandle(actor_id);
  reference_counter_->AddBorrowedObject(actor_handle_id, outer_object_id, owner_address);
  return actor_id;
}

const std::unique_ptr<ActorHandle> &ActorManager::GetActorHandle(
    const ActorID &actor_id) {
  absl::MutexLock lock(&mutex_);
  return GetActorHandleInternal(actor_id);
}

std::unique_ptr<ActorHandle> &ActorManager::GetActorHandleInternal(
    const ActorID &actor_id) {
  auto it = actor_handles_.find(actor_id);
  RAY_CHECK(it != actor_handles_.end())
      << "Cannot find an actor handle of id, " << actor_id
      << ". This method should be called only when you ensure actor handles exists.";
  return it->second;
}

bool ActorManager::CheckActorHandleExists(const ActorID &actor_id) {
  absl::MutexLock lock(&mutex_);
  return actor_handles_.find(actor_id) != actor_handles_.end();
}

bool ActorManager::AddNewActorHandle(std::unique_ptr<ActorHandle> actor_handle,
                                     const TaskID &caller_id,
                                     const std::string &call_site,
                                     const rpc::Address &caller_address,
                                     bool is_detached) {
  const auto &actor_id = actor_handle->GetActorID();
  const auto actor_creation_return_id = ObjectID::ForActorHandle(actor_id);
  // Detached actor doesn't need ref counting.
  if (!is_detached) {
    reference_counter_->AddOwnedObject(actor_creation_return_id,
                                       /*inner_ids=*/{}, caller_address, call_site,
                                       /*object_size*/ -1,
                                       /*is_reconstructable=*/true);
  }

  return AddActorHandle(std::move(actor_handle),
                        /*is_owner_handle=*/!is_detached, caller_id, call_site,
                        caller_address, actor_id, actor_creation_return_id);
}

bool ActorManager::AddActorHandle(std::unique_ptr<ActorHandle> actor_handle,
                                  bool is_owner_handle, const TaskID &caller_id,
                                  const std::string &call_site,
                                  const rpc::Address &caller_address,
                                  const ActorID &actor_id,
                                  const ObjectID &actor_creation_return_id) {
  reference_counter_->AddLocalReference(actor_creation_return_id, call_site);
  direct_actor_submitter_->AddActorQueueIfNotExists(actor_id);
  bool inserted;
  {
    absl::MutexLock lock(&mutex_);
    inserted = actor_handles_.emplace(actor_id, std::move(actor_handle)).second;
    // For new actor handles, their location is not resolved at GCS yet.
    // We should register them here until we get notification from GCS that they are
    // successfully registered.
    if (inserted) actors_pending_location_resolution_.insert(actor_id);
  }

  if (inserted) {
    // Register a callback to handle actor notifications.
    auto actor_notification_callback =
        std::bind(&ActorManager::HandleActorStateNotification, this,
                  std::placeholders::_1, std::placeholders::_2);

    RAY_CHECK_OK(gcs_client_->Actors().AsyncSubscribe(
        actor_id, actor_notification_callback, nullptr));

    if (!RayConfig::instance().gcs_actor_service_enabled()) {
      RAY_CHECK(reference_counter_->SetDeleteCallback(
          actor_creation_return_id,
          [this, actor_id, is_owner_handle](const ObjectID &object_id) {
            if (is_owner_handle) {
              // If we own the actor and the actor handle is no longer in scope,
              // terminate the actor. We do not do this if the GCS service is
              // enabled since then the GCS will terminate the actor for us.
              // TODO(sang): Remove this block once gcs_actor_service is enabled by
              // default.
              RAY_LOG(INFO) << "Owner's handle and creation ID " << object_id
                            << " has gone out of scope, sending message to actor "
                            << actor_id << " to do a clean exit.";
              RAY_CHECK(CheckActorHandleExists(actor_id));
              direct_actor_submitter_->KillActor(actor_id,
                                                 /*force_kill=*/false,
                                                 /*no_restart=*/false);

              // TODO(swang): Erase the actor handle once all refs to the actor
              // have gone out of scope. We cannot erase it here in case the
              // language frontend receives another ref to the same actor. In this
              // case, we must remember the last task counter that we sent to the
              // actor.
              // TODO(ekl) we can't unsubscribe to actor notifications here due to
              // https://github.com/ray-project/ray/pull/6885
            }
          }));
    }
  }

  return inserted;
}

void ActorManager::WaitForActorOutOfScope(
    const ActorID &actor_id,
    std::function<void(const ActorID &)> actor_out_of_scope_callback) {
  absl::MutexLock lock(&mutex_);
  auto it = actor_handles_.find(actor_id);
  if (it == actor_handles_.end()) {
    actor_out_of_scope_callback(actor_id);
  } else {
    // GCS actor manager will wait until the actor has been created before polling the
    // owner. This should avoid any asynchronous problems.
    auto callback = [actor_id, actor_out_of_scope_callback](const ObjectID &object_id) {
      actor_out_of_scope_callback(actor_id);
    };

    // Returns true if the object was present and the callback was added. It might have
    // already been evicted by the time we get this request, in which case we should
    // respond immediately so the gcs server can destroy the actor.
    const auto actor_creation_return_id = ObjectID::ForActorHandle(actor_id);
    if (!reference_counter_->SetDeleteCallback(actor_creation_return_id, callback)) {
      RAY_LOG(DEBUG) << "ActorID reference already gone for " << actor_id;
      actor_out_of_scope_callback(actor_id);
    }
  }
}

void ActorManager::HandleActorStateNotification(const ActorID &actor_id,
                                                const gcs::ActorTableData &actor_data) {
  // If it recieves notification from GCS, it means the actor location has been resolved.
  MarkPendingActorLocationResolved(actor_id);

  if (actor_data.state() == gcs::ActorTableData::PENDING) {
    // The actor is being created and not yet ready, just ignore!
  } else if (actor_data.state() == gcs::ActorTableData::RESTARTING) {
    direct_actor_submitter_->DisconnectActor(actor_id, false);
  } else if (actor_data.state() == gcs::ActorTableData::DEAD) {
    direct_actor_submitter_->DisconnectActor(actor_id, true);
    // We cannot erase the actor handle here because clients can still
    // submit tasks to dead actors. This also means we defer unsubscription,
    // otherwise we crash when bulk unsubscribing all actor handles.
  } else {
    direct_actor_submitter_->ConnectActor(actor_id, actor_data.address());
  }

  const auto &actor_state = gcs::ActorTableData::ActorState_Name(actor_data.state());
  RAY_LOG(INFO) << "received notification on actor, state: " << actor_state
                << ", actor_id: " << actor_id
                << ", ip address: " << actor_data.address().ip_address()
                << ", port: " << actor_data.address().port() << ", worker_id: "
                << WorkerID::FromBinary(actor_data.address().worker_id())
                << ", raylet_id: "
                << ClientID::FromBinary(actor_data.address().raylet_id());
}

std::vector<ObjectID> ActorManager::GetActorHandleIDsFromHandles() {
  absl::MutexLock lock(&mutex_);
  std::vector<ObjectID> actor_handle_ids;
  for (const auto &handle : actor_handles_) {
    auto actor_id = handle.first;
    auto actor_handle_id = ObjectID::ForActorHandle(actor_id);
    actor_handle_ids.push_back(actor_handle_id);
  }
  return actor_handle_ids;
}

void ActorManager::MarkPendingLocationActorsFailed() {
  absl::MutexLock lock(&mutex_);
  for (auto it = actors_pending_location_resolution_.begin();
       it != actors_pending_location_resolution_.end();) {
    auto current = it++;
    const auto &actor_id = *current;

    // This means that we didn't receive notification from GCS that this actor is
    // registered yet. Check if the owner is still alive and mark the actor as dead if the
    // owner is dead. Detail: https://github.com/ray-project/ray/pull/8679/files
    DisconnectPendingLocationActorIfNeeded(actor_id);
  }
}

void ActorManager::MarkPendingActorLocationResolved(const ActorID &actor_id) {
  absl::MutexLock lock(&mutex_);
  auto it = actors_pending_location_resolution_.find(actor_id);
  if (it == actors_pending_location_resolution_.end()) {
    return;
  }
  actors_pending_location_resolution_.erase(it);
}

bool ActorManager::IsActorLocationPending(const ActorID &actor_id) {
  absl::MutexLock lock(&mutex_);
  auto it = actors_pending_location_resolution_.find(actor_id);
  return it != actors_pending_location_resolution_.end();
}

void ActorManager::DisconnectPendingLocationActorIfNeeded(const ActorID &actor_id) {
  const std::unique_ptr<ActorHandle> &actor_handle = GetActorHandleInternal(actor_id);
  const ClientID &node_id =
      ClientID::FromBinary(actor_handle->GetOwnerAddress().raylet_id());
  const WorkerID &worker_id =
      WorkerID::FromBinary(actor_handle->GetOwnerAddress().worker_id());

  RAY_CHECK_OK(gcs_client_->Workers().AsyncGet(
      worker_id, [this, actor_id, node_id](
                     Status status, const boost::optional<rpc::WorkerTableData> &result) {
        if (!status.ok() || !IsActorLocationPending(actor_id)) {
          return;
        }

        bool worker_or_node_failed = false;
        // Check whether or not worker has failed.
        if (result && !result->is_alive()) {
          worker_or_node_failed = true;
        } else {
          // Check node failure. We should do this because worker failure event is not
          // reported when nodes fail.
          const auto &optional_node_info = gcs_client_->Nodes().Get(node_id);
          RAY_CHECK(optional_node_info)
              << "Node information for an actor_id, " << actor_id << " is not found.";
          if (optional_node_info->state() ==
              rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_DEAD) {
            worker_or_node_failed = true;
          }
        }

        if (worker_or_node_failed) {
          // We should make sure one more time that an actor is not registered to GCS
          // because the actor could've been registered at this point.
          RAY_CHECK_OK(gcs_client_->Actors().AsyncGet(
              actor_id,
              [this, actor_id](Status status,
                               const boost::optional<gcs::ActorTableData> &result) {
                if (!IsActorLocationPending(actor_id)) return;
                if (status.ok() && !result) {
                  direct_actor_submitter_->DisconnectActor(actor_id, /*dead*/ true);
                  MarkPendingActorLocationResolved(actor_id);
                }
              }));
        }
      }));
}

}  // namespace ray
