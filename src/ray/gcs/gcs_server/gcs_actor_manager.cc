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

#include "ray/gcs/gcs_server/gcs_actor_manager.h"

#include <utility>

#include "ray/common/ray_config.h"
#include "ray/stats/stats.h"

namespace ray {
namespace gcs {

NodeID GcsActor::GetNodeID() const {
  const auto &raylet_id_binary = actor_table_data_.address().raylet_id();
  if (raylet_id_binary.empty()) {
    return NodeID::Nil();
  }
  return NodeID::FromBinary(raylet_id_binary);
}

void GcsActor::UpdateAddress(const rpc::Address &address) {
  actor_table_data_.mutable_address()->CopyFrom(address);
}

const rpc::Address &GcsActor::GetAddress() const { return actor_table_data_.address(); }

WorkerID GcsActor::GetWorkerID() const {
  const auto &address = actor_table_data_.address();
  if (address.worker_id().empty()) {
    return WorkerID::Nil();
  }
  return WorkerID::FromBinary(address.worker_id());
}

WorkerID GcsActor::GetOwnerID() const {
  return WorkerID::FromBinary(GetOwnerAddress().worker_id());
}

NodeID GcsActor::GetOwnerNodeID() const {
  return NodeID::FromBinary(GetOwnerAddress().raylet_id());
}

const rpc::Address &GcsActor::GetOwnerAddress() const {
  return actor_table_data_.owner_address();
}

void GcsActor::UpdateState(rpc::ActorTableData::ActorState state) {
  actor_table_data_.set_state(state);
}

rpc::ActorTableData::ActorState GcsActor::GetState() const {
  return actor_table_data_.state();
}

ActorID GcsActor::GetActorID() const {
  return ActorID::FromBinary(actor_table_data_.actor_id());
}

bool GcsActor::IsDetached() const { return actor_table_data_.is_detached(); }

std::string GcsActor::GetName() const { return actor_table_data_.name(); }

std::string GcsActor::GetRayNamespace() const {
  return actor_table_data_.ray_namespace();
}

TaskSpecification GcsActor::GetCreationTaskSpecification() const {
  const auto &task_spec = actor_table_data_.task_spec();
  return TaskSpecification(task_spec);
}

const rpc::ActorTableData &GcsActor::GetActorTableData() const {
  return actor_table_data_;
}

rpc::ActorTableData *GcsActor::GetMutableActorTableData() { return &actor_table_data_; }

/////////////////////////////////////////////////////////////////////////////////////////
GcsActorManager::GcsActorManager(
    std::shared_ptr<GcsActorSchedulerInterface> scheduler,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
    std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub, RuntimeEnvManager &runtime_env_manager,
    std::function<void(const ActorID &)> destroy_owned_placement_group_if_needed,
    std::function<std::string(const JobID &)> get_ray_namespace,
    const rpc::ClientFactoryFn &worker_client_factory)
    : gcs_actor_scheduler_(std::move(scheduler)),
      gcs_table_storage_(std::move(gcs_table_storage)),
      gcs_pub_sub_(std::move(gcs_pub_sub)),
      worker_client_factory_(worker_client_factory),
      destroy_owned_placement_group_if_needed_(destroy_owned_placement_group_if_needed),
      get_ray_namespace_(get_ray_namespace),
      runtime_env_manager_(runtime_env_manager) {
  RAY_CHECK(worker_client_factory_);
  RAY_CHECK(destroy_owned_placement_group_if_needed_);
}

void GcsActorManager::HandleRegisterActor(const rpc::RegisterActorRequest &request,
                                          rpc::RegisterActorReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(request.task_spec().type() == TaskType::ACTOR_CREATION_TASK);
  auto actor_id =
      ActorID::FromBinary(request.task_spec().actor_creation_task_spec().actor_id());

  RAY_LOG(INFO) << "Registering actor, job id = " << actor_id.JobId()
                << ", actor id = " << actor_id;
  Status status =
      RegisterActor(request, [reply, send_reply_callback,
                              actor_id](const std::shared_ptr<gcs::GcsActor> &actor) {
        RAY_LOG(INFO) << "Registered actor, job id = " << actor_id.JobId()
                      << ", actor id = " << actor_id;
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      });
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to register actor: " << status.ToString()
                   << ", job id = " << actor_id.JobId() << ", actor id = " << actor_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }
  ++counts_[CountType::REGISTER_ACTOR_REQUEST];
}

void GcsActorManager::HandleCreateActor(const rpc::CreateActorRequest &request,
                                        rpc::CreateActorReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(request.task_spec().type() == TaskType::ACTOR_CREATION_TASK);
  auto actor_id =
      ActorID::FromBinary(request.task_spec().actor_creation_task_spec().actor_id());

  RAY_LOG(INFO) << "Creating actor, job id = " << actor_id.JobId()
                << ", actor id = " << actor_id;
  Status status = CreateActor(request, [reply, send_reply_callback, actor_id](
                                           const std::shared_ptr<gcs::GcsActor> &actor) {
    RAY_LOG(INFO) << "Finished creating actor, job id = " << actor_id.JobId()
                  << ", actor id = " << actor_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  });
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to create actor, job id = " << actor_id.JobId()
                   << ", actor id = " << actor_id << ", status: " << status.ToString();
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }
  ++counts_[CountType::CREATE_ACTOR_REQUEST];
}

void GcsActorManager::HandleGetActorInfo(const rpc::GetActorInfoRequest &request,
                                         rpc::GetActorInfoReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_id());
  RAY_LOG(DEBUG) << "Getting actor info"
                 << ", job id = " << actor_id.JobId() << ", actor id = " << actor_id;

  const auto &registered_actor_iter = registered_actors_.find(actor_id);
  if (registered_actor_iter != registered_actors_.end()) {
    reply->mutable_actor_table_data()->CopyFrom(
        registered_actor_iter->second->GetActorTableData());
  } else {
    const auto &destroyed_actor_iter = destroyed_actors_.find(actor_id);
    if (destroyed_actor_iter != destroyed_actors_.end()) {
      reply->mutable_actor_table_data()->CopyFrom(
          destroyed_actor_iter->second->GetActorTableData());
    }
  }

  RAY_LOG(DEBUG) << "Finished getting actor info, job id = " << actor_id.JobId()
                 << ", actor id = " << actor_id;
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_ACTOR_INFO_REQUEST];
}

void GcsActorManager::HandleGetAllActorInfo(const rpc::GetAllActorInfoRequest &request,
                                            rpc::GetAllActorInfoReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all actor info.";

  for (const auto &iter : registered_actors_) {
    reply->add_actor_table_data()->CopyFrom(iter.second->GetActorTableData());
  }
  for (const auto &iter : destroyed_actors_) {
    reply->add_actor_table_data()->CopyFrom(iter.second->GetActorTableData());
  }
  RAY_LOG(DEBUG) << "Finished getting all actor info.";
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_ALL_ACTOR_INFO_REQUEST];
}

void GcsActorManager::HandleGetNamedActorInfo(
    const rpc::GetNamedActorInfoRequest &request, rpc::GetNamedActorInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const std::string &name = request.name();
  const std::string &ray_namespace = request.ray_namespace();
  RAY_LOG(DEBUG) << "Getting actor info, name = " << name;

  // Try to look up the actor ID for the named actor.
  ActorID actor_id = GetActorIDByName(name, ray_namespace);

  Status status = Status::OK();
  if (actor_id.IsNil()) {
    // The named actor was not found.
    std::stringstream stream;
    stream << "Actor with name '" << name << "' was not found.";
    RAY_LOG(WARNING) << stream.str();
    status = Status::NotFound(stream.str());
  } else {
    const auto &iter = registered_actors_.find(actor_id);
    RAY_CHECK(iter != registered_actors_.end());
    reply->mutable_actor_table_data()->CopyFrom(iter->second->GetActorTableData());
    RAY_LOG(DEBUG) << "Finished getting actor info, job id = " << actor_id.JobId()
                   << ", actor id = " << actor_id;
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  ++counts_[CountType::GET_NAMED_ACTOR_INFO_REQUEST];
}

void GcsActorManager::HandleKillActorViaGcs(const rpc::KillActorViaGcsRequest &request,
                                            rpc::KillActorViaGcsReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  const auto &actor_id = ActorID::FromBinary(request.actor_id());
  bool force_kill = request.force_kill();
  bool no_restart = request.no_restart();
  if (no_restart) {
    DestroyActor(actor_id);
  } else {
    KillActor(actor_id, force_kill, no_restart);
  }

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  RAY_LOG(DEBUG) << "Finished killing actor, job id = " << actor_id.JobId()
                 << ", actor id = " << actor_id << ", force_kill = " << force_kill
                 << ", no_restart = " << no_restart;
  ++counts_[CountType::KILL_ACTOR_REQUEST];
}

Status GcsActorManager::RegisterActor(const ray::rpc::RegisterActorRequest &request,
                                      RegisterActorCallback success_callback) {
  // NOTE: After the abnormal recovery of the network between GCS client and GCS server or
  // the GCS server is restarted, it is required to continue to register actor
  // successfully.
  RAY_CHECK(success_callback);
  const auto &actor_creation_task_spec = request.task_spec().actor_creation_task_spec();
  auto actor_id = ActorID::FromBinary(actor_creation_task_spec.actor_id());

  auto iter = registered_actors_.find(actor_id);
  if (iter != registered_actors_.end()) {
    auto pending_register_iter = actor_to_register_callbacks_.find(actor_id);
    if (pending_register_iter != actor_to_register_callbacks_.end()) {
      // 1. The GCS client sends the `RegisterActor` request to the GCS server.
      // 2. The GCS client receives some network errors.
      // 3. The GCS client resends the `RegisterActor` request to the GCS server.
      pending_register_iter->second.emplace_back(std::move(success_callback));
    } else {
      // 1. The GCS client sends the `RegisterActor` request to the GCS server.
      // 2. The GCS server flushes the actor to the storage and restarts before replying
      // to the GCS client.
      // 3. The GCS client resends the `RegisterActor` request to the GCS server.
      success_callback(iter->second);
    }
    return Status::OK();
  }

  const auto job_id = JobID::FromBinary(request.task_spec().job_id());
  auto actor =
      std::make_shared<GcsActor>(request.task_spec(), get_ray_namespace_(job_id));
  if (!actor->GetName().empty()) {
    auto &actors_in_namespace = named_actors_[actor->GetRayNamespace()];
    auto it = actors_in_namespace.find(actor->GetName());
    if (it == actors_in_namespace.end()) {
      actors_in_namespace.emplace(actor->GetName(), actor->GetActorID());
    } else {
      std::stringstream stream;
      stream << "Actor with name '" << actor->GetName() << "' already exists.";
      return Status::Invalid(stream.str());
    }
  }

  actor_to_register_callbacks_[actor_id].emplace_back(std::move(success_callback));
  registered_actors_.emplace(actor->GetActorID(), actor);

  const auto &owner_address = actor->GetOwnerAddress();
  auto node_id = NodeID::FromBinary(owner_address.raylet_id());
  auto worker_id = WorkerID::FromBinary(owner_address.worker_id());
  RAY_CHECK(unresolved_actors_[node_id][worker_id].emplace(actor->GetActorID()).second);

  if (!actor->IsDetached()) {
    // This actor is owned. Send a long polling request to the actor's
    // owner to determine when the actor should be removed.
    PollOwnerForActorOutOfScope(actor);
  } else {
    // If it's a detached actor, we need to register the runtime env it used to GC
    auto job_id = JobID::FromBinary(request.task_spec().job_id());
    const auto &uris = runtime_env_manager_.GetReferences(job_id.Hex());
    auto actor_id_hex = actor->GetActorID().Hex();
    for (const auto &uri : uris) {
      runtime_env_manager_.AddURIReference(actor_id_hex, uri);
    }
  }

  // The backend storage is supposed to be reliable, so the status must be ok.
  RAY_CHECK_OK(gcs_table_storage_->ActorTable().Put(
      actor->GetActorID(), *actor->GetMutableActorTableData(),
      [this, actor](const Status &status) {
        // The backend storage is supposed to be reliable, so the status must be ok.
        RAY_CHECK_OK(status);
        // If a creator dies before this callback is called, the actor could have been
        // already destroyed. It is okay not to invoke a callback because we don't need
        // to reply to the creator as it is already dead.
        auto registered_actor_it = registered_actors_.find(actor->GetActorID());
        if (registered_actor_it == registered_actors_.end()) {
          // NOTE(sang): This logic assumes that the ordering of backend call is
          // guaranteed. It is currently true because we use a single TCP socket to call
          // the default Redis backend. If ordering is not guaranteed, we should overwrite
          // the actor state to DEAD to avoid race condition.
          return;
        }
        RAY_CHECK_OK(gcs_pub_sub_->Publish(ACTOR_CHANNEL, actor->GetActorID().Hex(),
                                           actor->GetActorTableData().SerializeAsString(),
                                           nullptr));
        // Invoke all callbacks for all registration requests of this actor (duplicated
        // requests are included) and remove all of them from
        // actor_to_register_callbacks_.
        // Reply to the owner to indicate that the actor has been registered.
        auto iter = actor_to_register_callbacks_.find(actor->GetActorID());
        RAY_CHECK(iter != actor_to_register_callbacks_.end() && !iter->second.empty());
        auto callbacks = std::move(iter->second);
        actor_to_register_callbacks_.erase(iter);
        for (auto &callback : callbacks) {
          callback(actor);
        }
      }));
  return Status::OK();
}

Status GcsActorManager::CreateActor(const ray::rpc::CreateActorRequest &request,
                                    CreateActorCallback callback) {
  // NOTE: After the abnormal recovery of the network between GCS client and GCS server or
  // the GCS server is restarted, it is required to continue to create actor
  // successfully.
  RAY_CHECK(callback);
  const auto &actor_creation_task_spec = request.task_spec().actor_creation_task_spec();
  auto actor_id = ActorID::FromBinary(actor_creation_task_spec.actor_id());

  auto iter = registered_actors_.find(actor_id);
  if (iter == registered_actors_.end()) {
    RAY_LOG(DEBUG) << "Actor " << actor_id
                   << " may be already destroyed, job id = " << actor_id.JobId();
    return Status::Invalid("Actor may be already destroyed.");
  }

  if (iter->second->GetState() == rpc::ActorTableData::ALIVE) {
    // In case of temporary network failures, workers will re-send multiple duplicate
    // requests to GCS server.
    // In this case, we can just reply.
    callback(iter->second);
    return Status::OK();
  }

  auto actor_creation_iter = actor_to_create_callbacks_.find(actor_id);
  if (actor_creation_iter != actor_to_create_callbacks_.end()) {
    // It is a duplicate message, just mark the callback as pending and invoke it after
    // the actor has been successfully created.
    actor_creation_iter->second.emplace_back(std::move(callback));
    return Status::OK();
  }
  // Mark the callback as pending and invoke it after the actor has been successfully
  // created.
  actor_to_create_callbacks_[actor_id].emplace_back(std::move(callback));

  // If GCS restarts while processing `CreateActor` request, GCS client will resend the
  // `CreateActor` request.
  // After GCS restarts, the state of the actor may not be `DEPENDENCIES_UNREADY`.
  if (iter->second->GetState() != rpc::ActorTableData::DEPENDENCIES_UNREADY) {
    RAY_LOG(INFO) << "Actor " << actor_id
                  << " is already in the process of creation. Skip it directly, job id = "
                  << actor_id.JobId();
    return Status::OK();
  }

  // Remove the actor from the unresolved actor map.
  const auto job_id = JobID::FromBinary(request.task_spec().job_id());
  auto actor =
      std::make_shared<GcsActor>(request.task_spec(), get_ray_namespace_(job_id));
  actor->GetMutableActorTableData()->set_state(rpc::ActorTableData::PENDING_CREATION);
  RemoveUnresolvedActor(actor);

  // Update the registered actor as its creation task specification may have changed due
  // to resolved dependencies.
  registered_actors_[actor_id] = actor;

  // Schedule the actor.
  gcs_actor_scheduler_->Schedule(actor);
  return Status::OK();
}

ActorID GcsActorManager::GetActorIDByName(const std::string &name,
                                          const std::string &ray_namespace) const {
  ActorID actor_id = ActorID::Nil();
  auto namespace_it = named_actors_.find(ray_namespace);
  if (namespace_it != named_actors_.end()) {
    auto it = namespace_it->second.find(name);
    if (it != namespace_it->second.end()) {
      actor_id = it->second;
    }
  }
  return actor_id;
}

void GcsActorManager::PollOwnerForActorOutOfScope(
    const std::shared_ptr<GcsActor> &actor) {
  const auto &actor_id = actor->GetActorID();
  const auto &owner_node_id = actor->GetOwnerNodeID();
  const auto &owner_id = actor->GetOwnerID();
  auto &workers = owners_[owner_node_id];
  auto it = workers.find(owner_id);
  if (it == workers.end()) {
    RAY_LOG(DEBUG) << "Adding owner " << owner_id << " of actor " << actor_id
                   << ", job id = " << actor_id.JobId();
    std::shared_ptr<rpc::CoreWorkerClientInterface> client =
        worker_client_factory_(actor->GetOwnerAddress());
    it = workers.emplace(owner_id, Owner(std::move(client))).first;
  }
  it->second.children_actor_ids.insert(actor_id);

  rpc::WaitForActorOutOfScopeRequest wait_request;
  wait_request.set_intended_worker_id(owner_id.Binary());
  wait_request.set_actor_id(actor_id.Binary());
  it->second.client->WaitForActorOutOfScope(
      wait_request, [this, owner_node_id, owner_id, actor_id](
                        Status status, const rpc::WaitForActorOutOfScopeReply &reply) {
        if (!status.ok()) {
          RAY_LOG(INFO) << "Worker " << owner_id
                        << " failed, destroying actor child, job id = "
                        << actor_id.JobId();
        } else {
          RAY_LOG(INFO) << "Actor " << actor_id
                        << " is out of scope, destroying actor child, job id = "
                        << actor_id.JobId();
        }

        auto node_it = owners_.find(owner_node_id);
        if (node_it != owners_.end() && node_it->second.count(owner_id)) {
          // Only destroy the actor if its owner is still alive. The actor may
          // have already been destroyed if the owner died.
          DestroyActor(actor_id);
        }
      });
}

void GcsActorManager::DestroyActor(const ActorID &actor_id) {
  RAY_LOG(INFO) << "Destroying actor, actor id = " << actor_id
                << ", job id = " << actor_id.JobId();
  actor_to_register_callbacks_.erase(actor_id);
  actor_to_create_callbacks_.erase(actor_id);
  auto it = registered_actors_.find(actor_id);
  if (it == registered_actors_.end()) {
    RAY_LOG(INFO) << "Tried to destroy actor that does not exist " << actor_id;
    return;
  }
  const auto &task_id = it->second->GetCreationTaskSpecification().TaskId();
  it->second->GetMutableActorTableData()->mutable_task_spec()->Clear();
  it->second->GetMutableActorTableData()->set_timestamp(current_sys_time_ms());
  AddDestroyedActorToCache(it->second);
  const auto actor = std::move(it->second);

  registered_actors_.erase(it);

  // Clean up the client to the actor's owner, if necessary.
  if (!actor->IsDetached()) {
    RemoveActorFromOwner(actor);
  } else {
    runtime_env_manager_.RemoveURIReference(actor->GetActorID().Hex());
  }

  // Remove actor from `named_actors_` if its name is not empty.
  if (!actor->GetName().empty()) {
    auto namespace_it = named_actors_.find(actor->GetRayNamespace());
    if (namespace_it != named_actors_.end()) {
      auto it = namespace_it->second.find(actor->GetName());
      if (it != namespace_it->second.end()) {
        namespace_it->second.erase(it);
      }
      // If we just removed the last actor in the namespace, remove the map.
      if (namespace_it->second.empty()) {
        named_actors_.erase(namespace_it);
      }
    }
  }

  // The actor is already dead, most likely due to process or node failure.
  if (actor->GetState() == rpc::ActorTableData::DEAD) {
    return;
  }

  if (actor->GetState() == rpc::ActorTableData::DEPENDENCIES_UNREADY) {
    // The actor creation task still has unresolved dependencies. Remove from the
    // unresolved actors map.
    RemoveUnresolvedActor(actor);
  } else {
    // The actor is still alive or pending creation. Clean up all remaining
    // state.
    const auto &node_id = actor->GetNodeID();
    const auto &worker_id = actor->GetWorkerID();
    auto node_it = created_actors_.find(node_id);
    if (node_it != created_actors_.end() && node_it->second.count(worker_id)) {
      // The actor has already been created. Destroy the process by force-killing
      // it.
      NotifyCoreWorkerToKillActor(actor);
      RAY_CHECK(node_it->second.erase(actor->GetWorkerID()));
      if (node_it->second.empty()) {
        created_actors_.erase(node_it);
      }
    } else {
      CancelActorInScheduling(actor, task_id);
    }
  }

  // Update the actor to DEAD in case any callers are still alive. This can
  // happen if the owner of the actor dies while there are still callers.
  // TODO(swang): We can skip this step and delete the actor table entry
  // entirely if the callers check directly whether the owner is still alive.
  auto mutable_actor_table_data = actor->GetMutableActorTableData();
  mutable_actor_table_data->set_state(rpc::ActorTableData::DEAD);
  auto actor_table_data =
      std::make_shared<rpc::ActorTableData>(*mutable_actor_table_data);
  // The backend storage is reliable in the future, so the status must be ok.
  RAY_CHECK_OK(gcs_table_storage_->ActorTable().Put(
      actor->GetActorID(), *actor_table_data,
      [this, actor_id, actor_table_data](Status status) {
        RAY_CHECK_OK(gcs_pub_sub_->Publish(
            ACTOR_CHANNEL, actor_id.Hex(),
            GenActorDataOnlyWithStates(*actor_table_data)->SerializeAsString(), nullptr));
        // Destroy placement group owned by this actor.
        destroy_owned_placement_group_if_needed_(actor_id);
      }));
}

absl::flat_hash_set<ActorID> GcsActorManager::GetUnresolvedActorsByOwnerNode(
    const NodeID &node_id) const {
  absl::flat_hash_set<ActorID> actor_ids;
  auto iter = unresolved_actors_.find(node_id);
  if (iter != unresolved_actors_.end()) {
    for (auto &entry : iter->second) {
      actor_ids.insert(entry.second.begin(), entry.second.end());
    }
  }
  return actor_ids;
}

absl::flat_hash_set<ActorID> GcsActorManager::GetUnresolvedActorsByOwnerWorker(
    const NodeID &node_id, const WorkerID &worker_id) const {
  absl::flat_hash_set<ActorID> actor_ids;
  auto iter = unresolved_actors_.find(node_id);
  if (iter != unresolved_actors_.end()) {
    auto it = iter->second.find(worker_id);
    if (it != iter->second.end()) {
      actor_ids.insert(it->second.begin(), it->second.end());
    }
  }
  return actor_ids;
}

void GcsActorManager::CollectStats() const {
  stats::PendingActors.Record(pending_actors_.size());
}

void GcsActorManager::OnWorkerDead(const ray::NodeID &node_id,
                                   const ray::WorkerID &worker_id) {
  OnWorkerDead(node_id, worker_id, rpc::WorkerExitType::SYSTEM_ERROR_EXIT);
}

void GcsActorManager::OnWorkerDead(
    const ray::NodeID &node_id, const ray::WorkerID &worker_id,
    const rpc::WorkerExitType disconnect_type,
    const std::shared_ptr<rpc::RayException> &creation_task_exception) {
  RAY_LOG(INFO) << "Worker " << worker_id << " on node " << node_id
                << " exited, type=" << rpc::WorkerExitType_Name(disconnect_type)
                << ", has creation_task_exception = "
                << (creation_task_exception != nullptr);
  if (creation_task_exception != nullptr) {
    RAY_LOG(INFO) << "Formatted creation task exception: "
                  << creation_task_exception->formatted_exception_string();
  }

  bool need_reconstruct = disconnect_type != rpc::WorkerExitType::INTENDED_EXIT &&
                          disconnect_type != rpc::WorkerExitType::CREATION_TASK_ERROR;
  // Destroy all actors that are owned by this worker.
  const auto it = owners_.find(node_id);
  if (it != owners_.end() && it->second.count(worker_id)) {
    auto owner = it->second.find(worker_id);
    // Make a copy of the children actor IDs since we will delete from the
    // list.
    const auto children_ids = owner->second.children_actor_ids;
    for (const auto &child_id : children_ids) {
      DestroyActor(child_id);
    }
  }

  // The creator worker of these actors died before resolving their dependencies. In this
  // case, these actors will never be created successfully. So we need to destroy them,
  // to prevent actor tasks hang forever.
  auto unresolved_actors = GetUnresolvedActorsByOwnerWorker(node_id, worker_id);
  for (auto &actor_id : unresolved_actors) {
    if (registered_actors_.count(actor_id)) {
      DestroyActor(actor_id);
    }
  }

  // Find if actor is already created or in the creation process (lease request is
  // granted)
  ActorID actor_id;
  auto iter = created_actors_.find(node_id);
  if (iter != created_actors_.end() && iter->second.count(worker_id)) {
    actor_id = iter->second[worker_id];
    iter->second.erase(worker_id);
    if (iter->second.empty()) {
      created_actors_.erase(iter);
    }
  } else {
    actor_id = gcs_actor_scheduler_->CancelOnWorker(node_id, worker_id);
    if (actor_id.IsNil()) {
      return;
    }
  }

  // Otherwise, try to reconstruct the actor that was already created or in the creation
  // process.
  ReconstructActor(actor_id, /*need_reschedule=*/need_reconstruct,
                   creation_task_exception);
}

void GcsActorManager::OnNodeDead(const NodeID &node_id) {
  RAY_LOG(INFO) << "Node " << node_id << " failed, reconstructing actors.";
  const auto it = owners_.find(node_id);
  if (it != owners_.end()) {
    std::vector<ActorID> children_ids;
    // Make a copy of all the actor IDs owned by workers on the dead node.
    for (const auto &owner : it->second) {
      for (const auto &child_id : owner.second.children_actor_ids) {
        children_ids.push_back(child_id);
      }
    }
    for (const auto &child_id : children_ids) {
      DestroyActor(child_id);
    }
  }

  // Cancel the scheduling of all related actors.
  auto scheduling_actor_ids = gcs_actor_scheduler_->CancelOnNode(node_id);
  for (auto &actor_id : scheduling_actor_ids) {
    // Reconstruct the canceled actor.
    ReconstructActor(actor_id);
  }

  // Find all actors that were created on this node.
  auto iter = created_actors_.find(node_id);
  if (iter != created_actors_.end()) {
    auto created_actors = std::move(iter->second);
    // Remove all created actors from node_to_created_actors_.
    created_actors_.erase(iter);
    for (auto &entry : created_actors) {
      // Reconstruct the removed actor.
      ReconstructActor(entry.second);
    }
  }

  // The creator node of these actors died before resolving their dependencies. In this
  // case, these actors will never be created successfully. So we need to destroy them,
  // to prevent actor tasks hang forever.
  auto unresolved_actors = GetUnresolvedActorsByOwnerNode(node_id);
  for (auto &actor_id : unresolved_actors) {
    if (registered_actors_.count(actor_id)) {
      DestroyActor(actor_id);
    }
  }
}

void GcsActorManager::ReconstructActor(const ActorID &actor_id) {
  ReconstructActor(actor_id, /*need_reschedule=*/true);
}

void GcsActorManager::ReconstructActor(
    const ActorID &actor_id, bool need_reschedule,
    const std::shared_ptr<rpc::RayException> &creation_task_exception) {
  // If the owner and this actor is dead at the same time, the actor
  // could've been destroyed and dereigstered before reconstruction.
  auto iter = registered_actors_.find(actor_id);
  if (iter == registered_actors_.end()) {
    RAY_LOG(DEBUG) << "Actor is destroyed before reconstruction, actor id = " << actor_id
                   << ", job id = " << actor_id.JobId();
    return;
  }
  auto &actor = iter->second;
  auto node_id = actor->GetNodeID();
  auto worker_id = actor->GetWorkerID();
  auto mutable_actor_table_data = actor->GetMutableActorTableData();
  // If the need_reschedule is set to false, then set the `remaining_restarts` to 0
  // so that the actor will never be rescheduled.
  int64_t max_restarts = mutable_actor_table_data->max_restarts();
  uint64_t num_restarts = mutable_actor_table_data->num_restarts();
  int64_t remaining_restarts;
  // Destroy placement group owned by this actor.
  destroy_owned_placement_group_if_needed_(actor_id);
  if (!need_reschedule) {
    remaining_restarts = 0;
  } else if (max_restarts == -1) {
    remaining_restarts = -1;
  } else {
    int64_t remaining = max_restarts - num_restarts;
    remaining_restarts = std::max(remaining, static_cast<int64_t>(0));
  }
  RAY_LOG(INFO) << "Actor is failed " << actor_id << " on worker " << worker_id
                << " at node " << node_id << ", need_reschedule = " << need_reschedule
                << ", remaining_restarts = " << remaining_restarts
                << ", job id = " << actor_id.JobId();
  if (remaining_restarts != 0) {
    // num_restarts must be set before updating GCS, or num_restarts will be inconsistent
    // between memory cache and storage.
    mutable_actor_table_data->set_num_restarts(num_restarts + 1);
    mutable_actor_table_data->set_state(rpc::ActorTableData::RESTARTING);
    // Make sure to reset the address before flushing to GCS. Otherwise,
    // GCS will mistakenly consider this lease request succeeds when restarting.
    actor->UpdateAddress(rpc::Address());
    mutable_actor_table_data->clear_resource_mapping();
    // The backend storage is reliable in the future, so the status must be ok.
    RAY_CHECK_OK(gcs_table_storage_->ActorTable().Put(
        actor_id, *mutable_actor_table_data,
        [this, actor_id, mutable_actor_table_data](Status status) {
          RAY_CHECK_OK(gcs_pub_sub_->Publish(
              ACTOR_CHANNEL, actor_id.Hex(),
              GenActorDataOnlyWithStates(*mutable_actor_table_data)->SerializeAsString(),
              nullptr));
        }));
    gcs_actor_scheduler_->Schedule(actor);
  } else {
    // Remove actor from `named_actors_` if its name is not empty.
    if (!actor->GetName().empty()) {
      auto namespace_it = named_actors_.find(actor->GetRayNamespace());
      if (namespace_it != named_actors_.end()) {
        auto it = namespace_it->second.find(actor->GetName());
        if (it != namespace_it->second.end()) {
          namespace_it->second.erase(it);
        }
        // If we just removed the last actor in the namespace, remove the map.
        if (namespace_it->second.empty()) {
          named_actors_.erase(namespace_it);
        }
      }
    }

    mutable_actor_table_data->set_state(rpc::ActorTableData::DEAD);
    if (creation_task_exception != nullptr) {
      mutable_actor_table_data->set_allocated_creation_task_exception(
          new rpc::RayException(*creation_task_exception));
    }
    mutable_actor_table_data->set_timestamp(current_sys_time_ms());
    // The backend storage is reliable in the future, so the status must be ok.
    RAY_CHECK_OK(gcs_table_storage_->ActorTable().Put(
        actor_id, *mutable_actor_table_data,
        [this, actor, actor_id, mutable_actor_table_data](Status status) {
          // If actor was an detached actor, make sure to destroy it.
          // We need to do this because detached actors are not destroyed
          // when its owners are dead because it doesn't have owners.
          if (actor->IsDetached()) {
            DestroyActor(actor_id);
          }
          RAY_CHECK_OK(gcs_pub_sub_->Publish(
              ACTOR_CHANNEL, actor_id.Hex(),
              GenActorDataOnlyWithStates(*mutable_actor_table_data)->SerializeAsString(),
              nullptr));
        }));
    // The actor is dead, but we should not remove the entry from the
    // registered actors yet. If the actor is owned, we will destroy the actor
    // once the owner fails or notifies us that the actor's handle has gone out
    // of scope.
  }
}

void GcsActorManager::OnActorCreationFailed(std::shared_ptr<GcsActor> actor) {
  // We will attempt to schedule this actor once an eligible node is
  // registered.
  pending_actors_.emplace_back(std::move(actor));
}

void GcsActorManager::OnActorCreationSuccess(const std::shared_ptr<GcsActor> &actor) {
  auto actor_id = actor->GetActorID();
  RAY_LOG(INFO) << "Actor created successfully, actor id = " << actor_id
                << ", job id = " << actor_id.JobId();
  // NOTE: If an actor is deleted immediately after the user creates the actor, reference
  // counter may return a reply to the request of WaitForActorOutOfScope to GCS server,
  // and GCS server will destroy the actor. The actor creation is asynchronous, it may be
  // destroyed before the actor creation is completed.
  if (registered_actors_.count(actor_id) == 0) {
    return;
  }
  actor->UpdateState(rpc::ActorTableData::ALIVE);
  auto actor_table_data = actor->GetActorTableData();
  actor_table_data.set_timestamp(current_sys_time_ms());

  // We should register the entry to the in-memory index before flushing them to
  // GCS because otherwise, there could be timing problems due to asynchronous Put.
  auto worker_id = actor->GetWorkerID();
  auto node_id = actor->GetNodeID();
  RAY_CHECK(!worker_id.IsNil());
  RAY_CHECK(!node_id.IsNil());
  RAY_CHECK(created_actors_[node_id].emplace(worker_id, actor_id).second);
  // The backend storage is reliable in the future, so the status must be ok.
  RAY_CHECK_OK(gcs_table_storage_->ActorTable().Put(
      actor_id, actor_table_data,
      [this, actor_id, actor_table_data, actor](Status status) {
        RAY_CHECK_OK(gcs_pub_sub_->Publish(
            ACTOR_CHANNEL, actor_id.Hex(),
            GenActorDataOnlyWithStates(actor_table_data)->SerializeAsString(), nullptr));
        // Invoke all callbacks for all registration requests of this actor (duplicated
        // requests are included) and remove all of them from
        // actor_to_create_callbacks_.
        auto iter = actor_to_create_callbacks_.find(actor_id);
        if (iter != actor_to_create_callbacks_.end()) {
          for (auto &callback : iter->second) {
            callback(actor);
          }
          actor_to_create_callbacks_.erase(iter);
        }
      }));
}

void GcsActorManager::SchedulePendingActors() {
  if (pending_actors_.empty()) {
    return;
  }

  RAY_LOG(DEBUG) << "Scheduling actor creation tasks, size = " << pending_actors_.size();
  auto actors = std::move(pending_actors_);
  for (auto &actor : actors) {
    gcs_actor_scheduler_->Schedule(std::move(actor));
  }
}

void GcsActorManager::Initialize(const GcsInitData &gcs_init_data) {
  const auto &jobs = gcs_init_data.Jobs();
  std::unordered_map<NodeID, std::vector<WorkerID>> node_to_workers;
  for (const auto &entry : gcs_init_data.Actors()) {
    auto job_iter = jobs.find(entry.first.JobId());
    auto is_job_dead = (job_iter == jobs.end() || job_iter->second.is_dead());
    auto actor = std::make_shared<GcsActor>(entry.second);
    if (entry.second.state() != ray::rpc::ActorTableData::DEAD && !is_job_dead) {
      registered_actors_.emplace(entry.first, actor);

      if (!actor->GetName().empty()) {
        auto &actors_in_namespace = named_actors_[actor->GetRayNamespace()];
        actors_in_namespace.emplace(actor->GetName(), actor->GetActorID());
      }

      if (entry.second.state() == ray::rpc::ActorTableData::DEPENDENCIES_UNREADY) {
        const auto &owner = actor->GetOwnerAddress();
        const auto &owner_node = NodeID::FromBinary(owner.raylet_id());
        const auto &owner_worker = WorkerID::FromBinary(owner.worker_id());
        RAY_CHECK(unresolved_actors_[owner_node][owner_worker]
                      .emplace(actor->GetActorID())
                      .second);
      } else if (entry.second.state() == ray::rpc::ActorTableData::ALIVE) {
        created_actors_[actor->GetNodeID()].emplace(actor->GetWorkerID(),
                                                    actor->GetActorID());
      }

      if (!actor->IsDetached()) {
        // This actor is owned. Send a long polling request to the actor's
        // owner to determine when the actor should be removed.
        PollOwnerForActorOutOfScope(actor);
      }

      if (!actor->GetWorkerID().IsNil()) {
        RAY_CHECK(!actor->GetNodeID().IsNil());
        node_to_workers[actor->GetNodeID()].emplace_back(actor->GetWorkerID());
      }
    } else {
      destroyed_actors_.emplace(entry.first, actor);
      sorted_destroyed_actor_list_.emplace_back(entry.first,
                                                (int64_t)entry.second.timestamp());
    }
  }
  sorted_destroyed_actor_list_.sort([](const std::pair<ActorID, int64_t> &left,
                                       const std::pair<ActorID, int64_t> &right) {
    return left.second < right.second;
  });

  // Notify raylets to release unused workers.
  gcs_actor_scheduler_->ReleaseUnusedWorkers(node_to_workers);

  RAY_LOG(DEBUG) << "The number of registered actors is " << registered_actors_.size()
                 << ", and the number of created actors is " << created_actors_.size();
  for (auto &item : registered_actors_) {
    auto &actor = item.second;
    if (actor->GetState() == ray::rpc::ActorTableData::PENDING_CREATION ||
        actor->GetState() == ray::rpc::ActorTableData::RESTARTING) {
      // We should not reschedule actors in state of `ALIVE`.
      // We could not reschedule actors in state of `DEPENDENCIES_UNREADY` because the
      // dependencies of them may not have been resolved yet.
      RAY_LOG(INFO) << "Rescheduling a non-alive actor, actor id = "
                    << actor->GetActorID() << ", state = " << actor->GetState()
                    << ", job id = " << actor->GetActorID().JobId();
      gcs_actor_scheduler_->Reschedule(actor);
    }
  }
}

void GcsActorManager::OnJobFinished(const JobID &job_id) {
  auto on_done = [this,
                  job_id](const std::unordered_map<ActorID, ActorTableData> &result) {
    if (!result.empty()) {
      std::vector<ActorID> non_detached_actors;
      std::unordered_set<ActorID> non_detached_actors_set;
      for (auto &item : result) {
        if (!item.second.is_detached()) {
          non_detached_actors.push_back(item.first);
          non_detached_actors_set.insert(item.first);
        }
      }
      RAY_CHECK_OK(
          gcs_table_storage_->ActorTable().BatchDelete(non_detached_actors, nullptr));

      for (auto iter = destroyed_actors_.begin(); iter != destroyed_actors_.end();) {
        if (iter->first.JobId() == job_id && !iter->second->IsDetached()) {
          destroyed_actors_.erase(iter++);
        } else {
          iter++;
        }
      };
    }
  };

  // Only non-detached actors should be deleted. We get all actors of this job and to the
  // filtering.
  RAY_CHECK_OK(gcs_table_storage_->ActorTable().GetByJobId(job_id, on_done));
}

const absl::flat_hash_map<NodeID, absl::flat_hash_map<WorkerID, ActorID>>
    &GcsActorManager::GetCreatedActors() const {
  return created_actors_;
}

const absl::flat_hash_map<ActorID, std::shared_ptr<GcsActor>>
    &GcsActorManager::GetRegisteredActors() const {
  return registered_actors_;
}

const absl::flat_hash_map<ActorID, std::vector<RegisterActorCallback>>
    &GcsActorManager::GetActorRegisterCallbacks() const {
  return actor_to_register_callbacks_;
}

void GcsActorManager::RemoveUnresolvedActor(const std::shared_ptr<GcsActor> &actor) {
  const auto &owner_address = actor->GetOwnerAddress();
  auto node_id = NodeID::FromBinary(owner_address.raylet_id());
  auto worker_id = WorkerID::FromBinary(owner_address.worker_id());
  auto iter = unresolved_actors_.find(node_id);
  if (iter != unresolved_actors_.end()) {
    auto it = iter->second.find(worker_id);
    RAY_CHECK(it != iter->second.end());
    RAY_CHECK(it->second.erase(actor->GetActorID()) != 0);
    if (it->second.empty()) {
      iter->second.erase(it);
      if (iter->second.empty()) {
        unresolved_actors_.erase(iter);
      }
    }
  }
}

void GcsActorManager::RemoveActorFromOwner(const std::shared_ptr<GcsActor> &actor) {
  const auto &actor_id = actor->GetActorID();
  const auto &owner_id = actor->GetOwnerID();
  RAY_LOG(DEBUG) << "Erasing actor " << actor_id << " owned by " << owner_id
                 << ", job id = " << actor_id.JobId();

  const auto &owner_node_id = actor->GetOwnerNodeID();
  auto &node = owners_[owner_node_id];
  auto worker_it = node.find(owner_id);
  RAY_CHECK(worker_it != node.end());
  auto &owner = worker_it->second;
  RAY_CHECK(owner.children_actor_ids.erase(actor_id));
  if (owner.children_actor_ids.empty()) {
    node.erase(worker_it);
    if (node.empty()) {
      owners_.erase(owner_node_id);
    }
  }
}

void GcsActorManager::NotifyCoreWorkerToKillActor(const std::shared_ptr<GcsActor> &actor,
                                                  bool force_kill, bool no_restart) {
  auto actor_client = worker_client_factory_(actor->GetAddress());
  rpc::KillActorRequest request;
  request.set_intended_actor_id(actor->GetActorID().Binary());
  request.set_force_kill(force_kill);
  request.set_no_restart(no_restart);
  RAY_UNUSED(actor_client->KillActor(request, nullptr));
}

void GcsActorManager::KillActor(const ActorID &actor_id, bool force_kill,
                                bool no_restart) {
  RAY_LOG(DEBUG) << "Killing actor, job id = " << actor_id.JobId()
                 << ", actor id = " << actor_id << ", force_kill = " << force_kill;
  const auto &it = registered_actors_.find(actor_id);
  if (it == registered_actors_.end()) {
    RAY_LOG(INFO) << "Tried to kill actor that does not exist " << actor_id;
    return;
  }

  const auto &actor = it->second;
  if (actor->GetState() == rpc::ActorTableData::DEAD ||
      actor->GetState() == rpc::ActorTableData::DEPENDENCIES_UNREADY) {
    return;
  }

  // The actor is still alive or pending creation.
  const auto &node_id = actor->GetNodeID();
  const auto &worker_id = actor->GetWorkerID();
  auto node_it = created_actors_.find(node_id);
  if (node_it != created_actors_.end() && node_it->second.count(worker_id)) {
    // The actor has already been created. Destroy the process by force-killing
    // it.
    NotifyCoreWorkerToKillActor(actor, force_kill, no_restart);
  } else {
    const auto &task_id = actor->GetCreationTaskSpecification().TaskId();
    CancelActorInScheduling(actor, task_id);
    ReconstructActor(actor_id, /*need_reschedule=*/true);
  }
}

void GcsActorManager::AddDestroyedActorToCache(const std::shared_ptr<GcsActor> &actor) {
  if (destroyed_actors_.size() >=
      RayConfig::instance().maximum_gcs_destroyed_actor_cached_count()) {
    const auto &actor_id = sorted_destroyed_actor_list_.front().first;
    RAY_CHECK_OK(gcs_table_storage_->ActorTable().Delete(actor_id, nullptr));
    destroyed_actors_.erase(actor_id);
    sorted_destroyed_actor_list_.pop_front();
  }

  if (destroyed_actors_.emplace(actor->GetActorID(), actor).second) {
    sorted_destroyed_actor_list_.emplace_back(
        actor->GetActorID(), (int64_t)actor->GetActorTableData().timestamp());
  }
}

void GcsActorManager::CancelActorInScheduling(const std::shared_ptr<GcsActor> &actor,
                                              const TaskID &task_id) {
  const auto &actor_id = actor->GetActorID();
  const auto &node_id = actor->GetNodeID();
  // The actor has not been created yet. It is either being scheduled or is
  // pending scheduling.
  auto canceled_actor_id =
      gcs_actor_scheduler_->CancelOnWorker(actor->GetNodeID(), actor->GetWorkerID());
  if (!canceled_actor_id.IsNil()) {
    // The actor was being scheduled and has now been canceled.
    RAY_CHECK(canceled_actor_id == actor_id);
  } else {
    auto pending_it = std::find_if(pending_actors_.begin(), pending_actors_.end(),
                                   [actor_id](const std::shared_ptr<GcsActor> &actor) {
                                     return actor->GetActorID() == actor_id;
                                   });

    // The actor was pending scheduling. Remove it from the queue.
    if (pending_it != pending_actors_.end()) {
      pending_actors_.erase(pending_it);
    } else {
      // When actor creation request of this actor id is pending in raylet,
      // it doesn't responds, and the actor should be still in leasing state.
      // NOTE: We will cancel outstanding lease request by calling
      // `raylet_client->CancelWorkerLease`.
      gcs_actor_scheduler_->CancelOnLeasing(node_id, actor_id, task_id);
    }
  }
}

std::string GcsActorManager::DebugString() const {
  uint64_t num_named_actors = 0;
  for (const auto &pair : named_actors_) {
    num_named_actors += pair.second.size();
  }
  std::ostringstream stream;
  stream << "GcsActorManager: {RegisterActor request count: "
         << counts_[CountType::REGISTER_ACTOR_REQUEST]
         << ", CreateActor request count: " << counts_[CountType::CREATE_ACTOR_REQUEST]
         << ", GetActorInfo request count: " << counts_[CountType::GET_ACTOR_INFO_REQUEST]
         << ", GetNamedActorInfo request count: "
         << counts_[CountType::GET_NAMED_ACTOR_INFO_REQUEST]
         << ", KillActor request count: " << counts_[CountType::KILL_ACTOR_REQUEST]
         << ", Registered actors count: " << registered_actors_.size()
         << ", Destroyed actors count: " << destroyed_actors_.size()
         << ", Named actors count: " << num_named_actors
         << ", Unresolved actors count: " << unresolved_actors_.size()
         << ", Pending actors count: " << pending_actors_.size()
         << ", Created actors count: " << created_actors_.size() << "}";
  return stream.str();
}

}  // namespace gcs
}  // namespace ray
