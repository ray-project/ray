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

ActorID ActorManager::DeserializeAndRegisterActorHandle(
    const std::string &serialized, const ObjectID &outer_object_id,
    const TaskID &caller_id, const std::string &call_site,
    const rpc::Address &caller_address) {
  std::shared_ptr<ActorHandle> actor_handle(new ActorHandle(serialized));
  const auto actor_id = actor_handle->GetActorID();
  const auto owner_id = actor_handle->GetOwnerId();
  const auto owner_address = actor_handle->GetOwnerAddress();

  RAY_UNUSED(AddActorHandle(std::move(actor_handle), /*is_owner_handle=*/false, caller_id,
                            call_site, caller_address));

  ObjectID actor_handle_id = ObjectID::ForActorHandle(actor_id);
  reference_counter_->AddBorrowedObject(actor_handle_id, outer_object_id, owner_id,
                                        owner_address);

  return actor_id;
}

Status ActorManager::SerializeActorHandle(const ActorID &actor_id, std::string *output,
                                          ObjectID *actor_handle_id) const {
  ActorHandle *actor_handle = nullptr;
  auto status = GetActorHandle(actor_id, &actor_handle);
  if (status.ok()) {
    actor_handle->Serialize(output);
    *actor_handle_id = ObjectID::ForActorHandle(actor_id);
  }
  return status;
}

Status ActorManager::GetActorHandle(const ActorID &actor_id,
                                    ActorHandle **actor_handle) const {
  absl::MutexLock lock(&mutex_);
  auto it = actor_handles_.find(actor_id);
  if (it == actor_handles_.end()) {
    return Status::Invalid("Handle for actor does not exist");
  }
  *actor_handle = it->second.get();
  return Status::OK();
}

Status ActorManager::GetNamedActorHandle(const std::string &name,
                                         ActorHandle **actor_handle,
                                         const TaskID &caller_id,
                                         const std::string &call_site,
                                         const rpc::Address &caller_address) {
  RAY_CHECK(RayConfig::instance().gcs_service_enabled());
  RAY_CHECK(RayConfig::instance().gcs_actor_service_enabled());
  RAY_CHECK(!name.empty());

  // This call needs to be blocking because we can't return until the actor
  // handle is created, which requires the response from the RPC. This is
  // implemented using a condition variable that's captured in the RPC
  // callback. There should be no risk of deadlock because we don't hold any
  // locks during the call and the RPCs run on a separate thread.
  ActorID actor_id;
  std::shared_ptr<bool> ready = std::make_shared<bool>(false);
  std::shared_ptr<std::mutex> m = std::make_shared<std::mutex>();
  std::shared_ptr<std::condition_variable> cv =
      std::make_shared<std::condition_variable>();
  std::unique_lock<std::mutex> lk(*m);
  RAY_CHECK_OK(gcs_client_->Actors().AsyncGetByName(
      name, [this, &actor_id, name, ready, m, cv, caller_id, call_site, caller_address](
                Status status, const boost::optional<gcs::ActorTableData> &result) {
        if (status.ok() && result) {
          auto actor_handle = std::shared_ptr<ActorHandle>(new ActorHandle(*result));
          actor_id = actor_handle->GetActorID();
          AddActorHandle(std::move(actor_handle), /*is_owner_handle=*/false, caller_id,
                         call_site, caller_address);
        } else {
          RAY_LOG(INFO) << "Failed to look up actor with name: " << name;
          // Use a NIL actor ID to signal that the actor wasn't found.
          actor_id = ActorID::Nil();
        }

        // Notify the main thread that the RPC has finished.
        {
          std::unique_lock<std::mutex> lk(*m);
          *ready = true;
        }
        cv->notify_one();
      }));

  // Block until the RPC completes. Set a timeout to avoid hangs if the
  // GCS service crashes.
  cv->wait_for(lk, std::chrono::seconds(5), [ready] { return *ready; });
  if (!*ready) {
    return Status::TimedOut("Timed out trying to get named actor.");
  }

  Status status;
  if (actor_id.IsNil()) {
    std::stringstream stream;
    stream << "Failed to look up actor with name '" << name
           << "'. It is either you look up the named actor you didn't create or the named"
              "actor hasn't been created because named actor creation is asynchronous.";
    status = Status::NotFound(stream.str());
  } else {
    status = GetActorHandle(actor_id, actor_handle);
  }
  return status;
}

bool ActorManager::AddActorHandle(std::shared_ptr<ActorHandle> actor_handle,
                                  bool is_owner_handle, const TaskID &caller_id,
                                  const std::string &call_site,
                                  const rpc::Address &caller_address) {
  const auto &actor_id = actor_handle->GetActorID();
  const auto actor_creation_return_id = ObjectID::ForActorHandle(actor_id);
  if (is_owner_handle) {
    reference_counter_->AddOwnedObject(actor_creation_return_id,
                                       /*inner_ids=*/{}, caller_id, caller_address,
                                       call_site, -1,
                                       /*is_reconstructable=*/true);
  }

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

    RAY_CHECK(reference_counter_->SetDeleteCallback(
        actor_creation_return_id,
        [this, actor_id, is_owner_handle](const ObjectID &object_id) {
          if (is_owner_handle) {
            // If we own the actor and the actor handle is no longer in scope,
            // terminate the actor. We do not do this if the GCS service is
            // enabled since then the GCS will terminate the actor for us.
            if (!(RayConfig::instance().gcs_service_enabled() &&
                  RayConfig::instance().gcs_actor_service_enabled())) {
              RAY_LOG(INFO) << "Owner's handle and creation ID " << object_id
                            << " has gone out of scope, sending message to actor "
                            << actor_id << " to do a clean exit.";
              RAY_CHECK_OK(
                  KillActor(actor_id, /*force_kill=*/false, /*no_restart=*/false));
            }
          }

          absl::MutexLock lock(&mutex_);
          // TODO(swang): Erase the actor handle once all refs to the actor
          // have gone out of scope. We cannot erase it here in case the
          // language frontend receives another ref to the same actor. In this
          // case, we must remember the last task counter that we sent to the
          // actor.
          // TODO(ekl) we can't unsubscribe to actor notifications here due to
          // https://github.com/ray-project/ray/pull/6885
          auto callback = actor_out_of_scope_callbacks_.extract(actor_id);
          if (callback) {
            callback.mapped()(actor_id);
          }
        }));
  }

  return inserted;
}

void ActorManager::AddActorOutOfScopeCallback(
    const ActorID &actor_id,
    std::function<void(const ActorID &)> actor_out_of_scope_callbacks) {
  absl::MutexLock lock(&mutex_);
  auto it = actor_handles_.find(actor_id);
  if (it == actor_handles_.end()) {
    actor_out_of_scope_callbacks(actor_id);
  } else {
    RAY_CHECK(actor_out_of_scope_callbacks_
                  .emplace(actor_id, std::move(actor_out_of_scope_callbacks))
                  .second);
  }
}

Status ActorManager::KillActor(const ActorID &actor_id, bool force_kill,
                               bool no_restart) {
  ActorHandle *actor_handle = nullptr;
  RAY_RETURN_NOT_OK(GetActorHandle(actor_id, &actor_handle));
  direct_actor_submitter_->KillActor(actor_id, force_kill, no_restart);
  return Status::OK();
}

void ActorManager::HandleActorStateNotification(const ActorID &actor_id,
                                                const gcs::ActorTableData &actor_data) {
  ActorHandle *actor_handle = nullptr;
  RAY_CHECK_OK(GetActorHandle(actor_id, &actor_handle));
  // If any notification comes in, that means this actor is persisted to GCS.
  actor_handle->SetIsPersistedToGCSFlag();

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

void ActorManager::ResolveActorsLocationNotPersistedToGCS() {
  absl::MutexLock lock(&mutex_);
  for (auto it = actors_pending_location_resolution_.begin();
       it != actors_pending_location_resolution_.end();) {
    auto current = it++;
    const auto &actor_id = *current;
    ActorHandle *actor_handle = nullptr;
    RAY_CHECK_OK(GetActorHandle(actor_id, &actor_handle));
    const ClientID &node_id =
        ClientID::FromBinary(actor_handle->GetOwnerAddress().raylet_id());

    if (actor_handle->IsPersistedToGCS()) {
      // if an actor is already persisted to GCS, GCS will be responsible for lifecycle of
      // actors.
      actors_pending_location_resolution_.erase(current);
    } else {
      // https://github.com/ray-project/ray/pull/8679/files
      // Run a protocol to resolve actors location that haven't been registered to GCS.
      RAY_CHECK_OK(gcs_client_->Workers().AsyncGetWorkerFailure(
          WorkerID::FromBinary(actor_handle->GetOwnerAddress().worker_id()),
          [this, actor_id, node_id](
              Status status, const boost::optional<gcs::WorkerFailureData> &result) {
            bool worker_or_node_failed = false;

            // If a worker failure events is found.
            if (status.ok() && result) {
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
              // Detached actors are not fate sharing with an owner.
              // We should make sure one more time that an actor is not registered to GCS
              // before resolving actor's location.
              RAY_CHECK_OK(gcs_client_->Actors().AsyncGet(
                  actor_id,
                  [this, actor_id](Status status,
                                   const boost::optional<gcs::ActorTableData> &result) {
                    // If actor information is not found, this means an actor is safe to
                    // disconnect.
                    if (status.ok() && !result) {
                      ActorHandle *actor_handle = nullptr;
                      RAY_CHECK_OK(GetActorHandle(actor_id, &actor_handle));
                      if (!actor_handle->IsPersistedToGCS()) {
                        direct_actor_submitter_->DisconnectActor(actor_id, /*dead*/ true);
                        absl::MutexLock lock(&mutex_);
                        actors_pending_location_resolution_.erase(actor_id);
                      }
                    }
                  }));
            }
          }));
    }
  }
}

}  // namespace ray
