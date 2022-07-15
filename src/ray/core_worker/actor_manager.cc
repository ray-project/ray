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
namespace core {

ActorID ActorManager::RegisterActorHandle(std::unique_ptr<ActorHandle> actor_handle,
                                          const ObjectID &outer_object_id,
                                          const std::string &call_site,
                                          const rpc::Address &caller_address,
                                          bool is_self) {
  const ActorID actor_id = actor_handle->GetActorID();
  const rpc::Address owner_address = actor_handle->GetOwnerAddress();
  const auto actor_creation_return_id = ObjectID::ForActorHandle(actor_id);

  // Note we need set `cached_actor_name` to empty string as we only cache named actors
  // when getting them from GCS.
  RAY_UNUSED(AddActorHandle(std::move(actor_handle),
                            call_site,
                            caller_address,
                            actor_id,
                            actor_creation_return_id,
                            is_self));
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

std::pair<std::shared_ptr<const ActorHandle>, Status> ActorManager::GetNamedActorHandle(
    const std::string &name,
    const std::string &ray_namespace,
    const std::string &call_site,
    const rpc::Address &caller_address) {
  ActorID actor_id = GetCachedNamedActorID(GenerateCachedActorName(ray_namespace, name));
  if (actor_id.IsNil()) {
    // This call needs to be blocking because we can't return until the actor
    // handle is created, which requires the response from the RPC. This is
    // implemented using a promise that's captured in the RPC callback.
    // There should be no risk of deadlock because we don't hold any
    // locks during the call and the RPCs run on a separate thread.
    rpc::ActorTableData actor_table_data;
    rpc::TaskSpec task_spec;
    const auto status = gcs_client_->Actors().SyncGetByName(
        name, ray_namespace, actor_table_data, task_spec);
    if (status.ok()) {
      auto actor_handle = std::make_unique<ActorHandle>(actor_table_data, task_spec);
      actor_id = actor_handle->GetActorID();
      AddNewActorHandle(std::move(actor_handle),
                        call_site,
                        caller_address,
                        /*is_detached*/ true);
    } else {
      // Use a NIL actor ID to signal that the actor wasn't found.
      RAY_LOG(DEBUG) << "Failed to look up actor with name: " << name;
      actor_id = ActorID::Nil();
    }

    if (status.IsTimedOut()) {
      std::ostringstream stream;
      stream << "There was timeout in getting the actor handle, "
                "probably because the GCS server is dead or under high load .";
      std::string error_str = stream.str();
      RAY_LOG(ERROR) << error_str;
      return std::make_pair(nullptr, Status::TimedOut(error_str));
    }
  } else {
    // When the named actor is already cached, the reference of actor_creation_return_id
    // must be increased, so we call AddActorHandle here to ensure that.
    std::string serialized_actor_handle;
    auto actor_handle = GetActorHandle(actor_id);
    actor_handle->Serialize(&serialized_actor_handle);

    AddActorHandle(std::make_unique<ActorHandle>(serialized_actor_handle),
                   call_site,
                   caller_address,
                   actor_id,
                   ObjectID::ForActorHandle(actor_id));
  }

  if (actor_id.IsNil()) {
    std::ostringstream stream;
    stream << "Failed to look up actor with name '" << name << "'. This could "
           << "because 1. You are trying to look up a named actor you "
           << "didn't create. 2. The named actor died. "
           << "3. You did not use a namespace matching the namespace of the "
           << "actor.";
    auto error_msg = stream.str();
    RAY_LOG(WARNING) << error_msg;
    return std::make_pair(nullptr, Status::NotFound(error_msg));
  }

  return std::make_pair(GetActorHandle(actor_id), Status::OK());
}

bool ActorManager::CheckActorHandleExists(const ActorID &actor_id) {
  absl::MutexLock lock(&mutex_);
  return actor_handles_.find(actor_id) != actor_handles_.end();
}

bool ActorManager::AddNewActorHandle(std::unique_ptr<ActorHandle> actor_handle,
                                     const std::string &call_site,
                                     const rpc::Address &caller_address,
                                     bool is_detached) {
  const auto &actor_id = actor_handle->GetActorID();
  const auto actor_creation_return_id = ObjectID::ForActorHandle(actor_id);
  // Detached actor doesn't need ref counting.
  if (!is_detached) {
    // We don't need to add an initial local ref here because it will get added
    // in AddActorHandle.
    reference_counter_->AddOwnedObject(actor_creation_return_id,
                                       /*inner_ids=*/{},
                                       caller_address,
                                       call_site,
                                       /*object_size*/ -1,
                                       /*is_reconstructable=*/true,
                                       /*add_local_ref=*/false);
  }

  return AddActorHandle(std::move(actor_handle),
                        call_site,
                        caller_address,
                        actor_id,
                        actor_creation_return_id);
}

bool ActorManager::AddActorHandle(std::unique_ptr<ActorHandle> actor_handle,
                                  const std::string &call_site,
                                  const rpc::Address &caller_address,
                                  const ActorID &actor_id,
                                  const ObjectID &actor_creation_return_id,
                                  bool is_self) {
  reference_counter_->AddLocalReference(actor_creation_return_id, call_site);
  direct_actor_submitter_->AddActorQueueIfNotExists(
      actor_id,
      actor_handle->MaxPendingCalls(),
      actor_handle->ExecuteOutOfOrder(),
      /*fail_if_actor_unreachable=*/actor_handle->MaxTaskRetries() == 0);
  bool inserted;
  {
    absl::MutexLock lock(&mutex_);
    inserted = actor_handles_.emplace(actor_id, std::move(actor_handle)).second;
  }

  if (is_self) {
    // Current actor doesn't need to subscribe its state from GCS.
    // num_restarts is used for dropping out-of-order pub messages. Since we won't
    // subscribe any messages, we can set any value bigger than -1(we use 0 here).
    direct_actor_submitter_->ConnectActor(actor_id, caller_address, /*num_restarts=*/0);
  }

  return inserted;
}

void ActorManager::OnActorKilled(const ActorID &actor_id) {
  MarkActorKilledOrOutOfScope(GetActorHandle(actor_id));
}

void ActorManager::WaitForActorOutOfScope(
    const ActorID &actor_id,
    std::function<void(const ActorID &)> actor_out_of_scope_callback) {
  absl::MutexLock lock(&mutex_);
  auto it = actor_handles_.find(actor_id);
  if (it == actor_handles_.end()) {
    actor_out_of_scope_callback(actor_id);
  } else {
    auto actor_handle = it->second;
    // GCS actor manager will wait until the actor has been created before polling the
    // owner. This should avoid any asynchronous problems.
    auto callback = [this, actor_id, actor_handle, actor_out_of_scope_callback](
                        const ObjectID &object_id) {
      MarkActorKilledOrOutOfScope(actor_handle);
      actor_out_of_scope_callback(actor_id);
    };

    // Returns true if the object was present and the callback was added. It might have
    // already been evicted by the time we get this request, in which case we should
    // respond immediately so the gcs server can destroy the actor.
    const auto actor_creation_return_id = ObjectID::ForActorHandle(actor_id);
    if (!reference_counter_->SetDeleteCallback(actor_creation_return_id, callback)) {
      RAY_LOG(DEBUG) << "ActorID reference already gone for " << actor_id;
      MarkActorKilledOrOutOfScope(actor_handle);
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
                << ", num_restarts: " << actor_data.num_restarts()
                << ", death context type="
                << gcs::GetActorDeathCauseString(actor_data.death_cause());
  if (actor_data.state() == rpc::ActorTableData::RESTARTING) {
    direct_actor_submitter_->DisconnectActor(actor_id,
                                             actor_data.num_restarts(),
                                             /*is_dead=*/false,
                                             actor_data.death_cause());
  } else if (actor_data.state() == rpc::ActorTableData::DEAD) {
    OnActorKilled(actor_id);
    direct_actor_submitter_->DisconnectActor(actor_id,
                                             actor_data.num_restarts(),
                                             /*is_dead=*/true,
                                             actor_data.death_cause());
    // We cannot erase the actor handle here because clients can still
    // submit tasks to dead actors. This also means we defer unsubscription,
    // otherwise we crash when bulk unsubscribing all actor handles.
  } else if (actor_data.state() == rpc::ActorTableData::ALIVE) {
    direct_actor_submitter_->ConnectActor(
        actor_id, actor_data.address(), actor_data.num_restarts());
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

ActorID ActorManager::GetCachedNamedActorID(const std::string &actor_name) {
  {
    absl::MutexLock lock(&cache_mutex_);
    auto it = cached_actor_name_to_ids_.find(actor_name);
    if (it != cached_actor_name_to_ids_.end()) {
      absl::MutexLock lock(&mutex_);
      auto handle_it = actor_handles_.find(it->second);
      RAY_CHECK(handle_it != actor_handles_.end());
      return it->second;
    }
  }
  return ActorID::Nil();
}

void ActorManager::SubscribeActorState(const ActorID &actor_id) {
  {
    // Make sure this method only be executed once.
    absl::MutexLock lock(&cache_mutex_);
    if (!subscribed_actors_.emplace(actor_id, /*valid*/ true).second) {
      return;
    }
  }

  auto actor_handle = GetActorHandle(actor_id);
  RAY_CHECK(actor_handle != nullptr);

  std::string cached_actor_name;
  if (!actor_handle->GetName().empty()) {
    cached_actor_name =
        GenerateCachedActorName(actor_handle->GetNamespace(), actor_handle->GetName());
  }

  // Register a callback to handle actor notifications.
  auto actor_notification_callback =
      std::bind(&ActorManager::HandleActorStateNotification,
                this,
                std::placeholders::_1,
                std::placeholders::_2);
  RAY_CHECK_OK(gcs_client_->Actors().AsyncSubscribe(
      actor_id,
      actor_notification_callback,
      [this, actor_id, cached_actor_name](Status status) {
        if (status.ok() && !cached_actor_name.empty()) {
          // It should be ignored if the finished callback of the subscription comes
          // after the actor died, otherwise the stale named actor will be cached.
          // NOTE: We can not guarantee the order of arrival of `on_done` callback and
          // subscribe callback for the time being.
          absl::MutexLock lock(&cache_mutex_);
          auto iter = subscribed_actors_.find(actor_id);
          if (iter != subscribed_actors_.end() && iter->second) {
            cached_actor_name_to_ids_.emplace(cached_actor_name, actor_id);
          }
        }
      }));
}

void ActorManager::MarkActorKilledOrOutOfScope(
    std::shared_ptr<ActorHandle> actor_handle) {
  RAY_CHECK(actor_handle != nullptr);
  const auto &actor_id = actor_handle->GetActorID();
  const auto &actor_name = actor_handle->GetName();
  const auto &ray_namespace = actor_handle->GetNamespace();

  absl::MutexLock lock(&cache_mutex_);

  auto iter = subscribed_actors_.find(actor_id);
  if (iter != subscribed_actors_.end()) {
    iter->second = false;
  }

  /// Invalidate named actor cache.
  if (!actor_name.empty()) {
    RAY_LOG(DEBUG) << "Actor name cache is invalidated for the actor of name "
                   << actor_name << " namespace " << ray_namespace << " id " << actor_id;
    cached_actor_name_to_ids_.erase(GenerateCachedActorName(ray_namespace, actor_name));
  }
}

bool ActorManager::IsActorKilledOrOutOfScope(const ActorID &actor_id) const {
  absl::MutexLock lock(&cache_mutex_);
  auto iter = subscribed_actors_.find(actor_id);
  return iter == subscribed_actors_.end() || !iter->second;
}

}  // namespace core
}  // namespace ray
