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

std::shared_ptr<ActorHandle> ActorManager::GetActorHandle(const ActorID &actor_id) {
  absl::MutexLock lock(&mutex_);
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
  }
  if (inserted) {
    // Register a callback to handle actor notifications.
    auto actor_notification_callback =
        std::bind(&ActorManager::HandleActorStateNotification, this,
                  std::placeholders::_1, std::placeholders::_2);
    RAY_CHECK_OK(gcs_client_->Actors().AsyncSubscribe(
        actor_id, actor_notification_callback, nullptr));
  } else {
    RAY_LOG(ERROR) << "Actor handle already exists " << actor_id.Hex();
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
                                                const rpc::ActorTableData &actor_data) {
  const auto &actor_state = rpc::ActorTableData::ActorState_Name(actor_data.state());
  RAY_LOG(INFO) << "received notification on actor, state: " << actor_state
                << ", actor_id: " << actor_id
                << ", ip address: " << actor_data.address().ip_address()
                << ", port: " << actor_data.address().port() << ", worker_id: "
                << WorkerID::FromBinary(actor_data.address().worker_id())
                << ", raylet_id: " << NodeID::FromBinary(actor_data.address().raylet_id())
                << ", num_restarts: " << actor_data.num_restarts();
  if (actor_data.state() == rpc::ActorTableData::RESTARTING) {
    direct_actor_submitter_->DisconnectActor(actor_id, actor_data.num_restarts(), false);
  } else if (actor_data.state() == rpc::ActorTableData::DEAD) {
    direct_actor_submitter_->DisconnectActor(actor_id, actor_data.num_restarts(), true);
    // We cannot erase the actor handle here because clients can still
    // submit tasks to dead actors. This also means we defer unsubscription,
    // otherwise we crash when bulk unsubscribing all actor handles.
  } else if (actor_data.state() == rpc::ActorTableData::ALIVE) {
    direct_actor_submitter_->ConnectActor(actor_id, actor_data.address(),
                                          actor_data.num_restarts());
  } else {
    // The actor is being created and not yet ready, just ignore!
  }
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

}  // namespace ray
