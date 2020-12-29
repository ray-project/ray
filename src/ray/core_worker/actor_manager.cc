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

const std::unique_ptr<ActorHandle> &ActorManager::GetActorHandle(
    const ActorID &actor_id) {
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
    inserted = actor_handles_.insert_or_assign(actor_id, std::move(actor_handle)).second;
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

std::pair<const ActorHandle *, Status> ActorManager::GetNamedActorHandle(
    const std::string &name, const TaskID &caller_id, const std::string &call_site,
    const rpc::Address &rpc_address) {
  ActorID found_actor_id_in_cache = ActorID::Nil();
  {
    absl::MutexLock lock(&named_actor_cache_mutex_);
    auto it = actor_name_to_ids_cache_.find(name);
    if (it != actor_name_to_ids_cache_.end()) {
      found_actor_id_in_cache = it->second;
    }
  }

  if (!found_actor_id_in_cache.IsNil()) {
    // We found the actor in local named actor cache, no need to fetch it from Gcs.
    return std::make_pair(GetActorHandle(found_actor_id_in_cache).get(), Status::OK());
  }

  // We didn't find the actor in named actor cache locally,
  // fetch it from Gcs synchronously.
  auto result = SyncFetchNamedActorFromGcs(name, caller_id, call_site, rpc_address);
  if (result.second.ok()) {
    // Fetched the named actor from Gcs, let's cache it on local.
    absl::MutexLock lock(&named_actor_cache_mutex_);
    RAY_IGNORE_EXPR(actor_name_to_ids_cache_.emplace(name, result.first->GetActorID()));
  }
  if (result.second.ok()) {
    RAY_LOG(DEBUG) << "Success to cache the named actor on local with actor_name=" << name
                   << " and actor_id=" << result.first;
  }
  return result;
}

std::pair<const ActorHandle *, Status> ActorManager::SyncFetchNamedActorFromGcs(
    const std::string &name, const TaskID &caller_id, const std::string &call_site,
    const rpc::Address &rpc_address) {
  // This call needs to be blocking because we can't return until the actor
  // handle is created, which requires the response from the RPC. This is
  // implemented using a promise that's captured in the RPC callback.
  // There should be no risk of deadlock because we don't hold any
  // locks during the call and the RPCs run on a separate thread.
  ActorID actor_id;
  std::shared_ptr<std::promise<void>> ready_promise =
      std::make_shared<std::promise<void>>(std::promise<void>());
  RAY_CHECK_OK(gcs_client_->Actors().AsyncGetByName(
      name, [this, &actor_id, name, ready_promise, caller_id, call_site, rpc_address](
                Status status, const boost::optional<rpc::ActorTableData> &result) {
        if (status.ok() && result) {
          auto actor_handle = std::unique_ptr<ActorHandle>(new ActorHandle(*result));
          actor_id = actor_handle->GetActorID();
          AddNewActorHandleForNamedActor(name, std::move(actor_handle), caller_id,
                                         call_site, rpc_address);
        } else {
          // Use a NIL actor ID to signal that the actor wasn't found.
          RAY_LOG(DEBUG) << "Failed to look up actor with name: " << name;
          actor_id = ActorID::Nil();
        }
        ready_promise->set_value();
      }));
  // Block until the RPC completes. Set a timeout to avoid hangs if the
  // GCS service crashes.
  if (ready_promise->get_future().wait_for(std::chrono::seconds(
          RayConfig::instance().gcs_server_request_timeout_seconds())) !=
      std::future_status::ready) {
    std::ostringstream stream;
    stream << "There was timeout in getting the actor handle. It is probably "
              "because GCS server is dead or there's a high load there.";
    return std::make_pair(nullptr, Status::TimedOut(stream.str()));
  }

  if (actor_id.IsNil()) {
    std::ostringstream stream;
    stream << "Failed to look up actor with name '" << name << "'. You are "
           << "either trying to look up a named actor you didn't create, "
           << "the named actor died, or the actor hasn't been created "
           << "because named actor creation is asynchronous.";
    return std::make_pair(nullptr, Status::NotFound(stream.str()));
  }

  return std::make_pair(GetActorHandle(actor_id).get(), Status::OK());
}

void ActorManager::AddNewActorHandleForNamedActor(
    const std::string &name, std::unique_ptr<ActorHandle> actor_handle,
    const TaskID &caller_id, const std::string &call_site,
    const rpc::Address &caller_address) {
  const auto &actor_id = actor_handle->GetActorID();
  const auto actor_creation_return_id = ObjectID::ForActorHandle(actor_id);

  AddActorHandle(std::move(actor_handle), /*is_owner_handle=*/false, caller_id, call_site,
                 caller_address, actor_id, actor_creation_return_id);

  reference_counter_->SetDeleteCallback(
      actor_creation_return_id, [name, this](const ObjectID &actor_handle_id) {
        absl::MutexLock lock(&named_actor_cache_mutex_);
        auto num_erased = actor_name_to_ids_cache_.erase(name);
        RAY_CHECK(num_erased == 1) << "Failed to erase the actor handle from cache.";
      });
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
