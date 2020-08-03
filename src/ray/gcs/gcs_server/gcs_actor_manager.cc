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

namespace ray {
namespace gcs {

ClientID GcsActor::GetNodeID() const {
  const auto &raylet_id_binary = actor_table_data_.address().raylet_id();
  if (raylet_id_binary.empty()) {
    return ClientID::Nil();
  }
  return ClientID::FromBinary(raylet_id_binary);
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

ClientID GcsActor::GetOwnerNodeID() const {
  return ClientID::FromBinary(GetOwnerAddress().raylet_id());
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

TaskSpecification GcsActor::GetCreationTaskSpecification() const {
  const auto &task_spec = actor_table_data_.task_spec();
  return TaskSpecification(task_spec);
}

const rpc::ActorTableData &GcsActor::GetActorTableData() const {
  return actor_table_data_;
}

rpc::ActorTableData *GcsActor::GetMutableActorTableData() { return &actor_table_data_; }

/////////////////////////////////////////////////////////////////////////////////////////
GcsActorManager::GcsActorManager(std::shared_ptr<GcsActorSchedulerInterface> scheduler,
                                 std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
                                 std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
                                 const rpc::ClientFactoryFn &worker_client_factory)
    : gcs_actor_scheduler_(std::move(scheduler)),
      gcs_table_storage_(std::move(gcs_table_storage)),
      gcs_pub_sub_(std::move(gcs_pub_sub)),
      worker_client_factory_(worker_client_factory) {}

void GcsActorManager::HandleRegisterActor(const rpc::RegisterActorRequest &request,
                                          rpc::RegisterActorReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(request.task_spec().type() == TaskType::ACTOR_CREATION_TASK);
  auto actor_id =
      ActorID::FromBinary(request.task_spec().actor_creation_task_spec().actor_id());

  RAY_LOG(INFO) << "Registering actor, actor id = " << actor_id;
  Status status =
      RegisterActor(request, [reply, send_reply_callback,
                              actor_id](const std::shared_ptr<gcs::GcsActor> &actor) {
        RAY_LOG(INFO) << "Registered actor, actor id = " << actor_id;
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      });
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to register actor: " << status.ToString();
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }
}

void GcsActorManager::HandleCreateActor(const rpc::CreateActorRequest &request,
                                        rpc::CreateActorReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(request.task_spec().type() == TaskType::ACTOR_CREATION_TASK);
  auto actor_id =
      ActorID::FromBinary(request.task_spec().actor_creation_task_spec().actor_id());

  RAY_LOG(INFO) << "Creating actor, actor id = " << actor_id;
  Status status = CreateActor(request, [reply, send_reply_callback, actor_id](
                                           const std::shared_ptr<gcs::GcsActor> &actor) {
    RAY_LOG(INFO) << "Created actor, actor id = " << actor_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  });
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to create actor, actor id = " << actor_id
                   << "  status: " << status.ToString();
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }
}

void GcsActorManager::HandleGetActorInfo(const rpc::GetActorInfoRequest &request,
                                         rpc::GetActorInfoReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_id());
  RAY_LOG(DEBUG) << "Getting actor info"
                 << ", job id = " << actor_id.JobId() << ", actor id = " << actor_id;

  auto on_done = [actor_id, reply, send_reply_callback](
                     const Status &status,
                     const boost::optional<ActorTableData> &result) {
    if (result) {
      reply->mutable_actor_table_data()->CopyFrom(*result);
    }
    RAY_LOG(DEBUG) << "Finished getting actor info, job id = " << actor_id.JobId()
                   << ", actor id = " << actor_id << ", status = " << status;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };

  // Look up the actor_id in the GCS.
  Status status = gcs_table_storage_->ActorTable().Get(actor_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
}

void GcsActorManager::HandleGetAllActorInfo(const rpc::GetAllActorInfoRequest &request,
                                            rpc::GetAllActorInfoReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all actor info.";

  auto on_done = [reply, send_reply_callback](
                     const std::unordered_map<ActorID, ActorTableData> &result) {
    for (auto &it : result) {
      reply->add_actor_table_data()->CopyFrom(it.second);
    }
    RAY_LOG(DEBUG) << "Finished getting all actor info.";
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };

  Status status = gcs_table_storage_->ActorTable().GetAll(on_done);
  if (!status.ok()) {
    on_done(std::unordered_map<ActorID, ActorTableData>());
  }
}

void GcsActorManager::HandleGetNamedActorInfo(
    const rpc::GetNamedActorInfoRequest &request, rpc::GetNamedActorInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const std::string &name = request.name();
  RAY_LOG(DEBUG) << "Getting actor info"
                 << ", name = " << name;

  auto on_done = [name, reply, send_reply_callback](
                     const Status &status,
                     const boost::optional<ActorTableData> &result) {
    if (status.ok()) {
      if (result) {
        reply->mutable_actor_table_data()->CopyFrom(*result);
      }
    } else {
      RAY_LOG(ERROR) << "Failed to get actor info: " << status.ToString()
                     << ", name = " << name;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  // Try to look up the actor ID for the named actor.
  ActorID actor_id = GetActorIDByName(name);

  if (actor_id.IsNil()) {
    // The named actor was not found.
    std::stringstream stream;
    stream << "Actor with name '" << name << "' was not found.";
    on_done(Status::NotFound(stream.str()), boost::none);
  } else {
    // Look up the actor_id in the GCS.
    Status status = gcs_table_storage_->ActorTable().Get(actor_id, on_done);
    if (!status.ok()) {
      on_done(status, boost::none);
    }
    RAY_LOG(DEBUG) << "Finished getting actor info, job id = " << actor_id.JobId()
                   << ", actor id = " << actor_id;
  }
}
void GcsActorManager::HandleRegisterActorInfo(
    const rpc::RegisterActorInfoRequest &request, rpc::RegisterActorInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_table_data().actor_id());
  RAY_LOG(DEBUG) << "Registering actor info, job id = " << actor_id.JobId()
                 << ", actor id = " << actor_id;
  const auto &actor_table_data = request.actor_table_data();
  auto on_done = [this, actor_id, actor_table_data, reply,
                  send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to register actor info: " << status.ToString()
                     << ", job id = " << actor_id.JobId() << ", actor id = " << actor_id;
    } else {
      RAY_CHECK_OK(gcs_pub_sub_->Publish(ACTOR_CHANNEL, actor_id.Hex(),
                                         actor_table_data.SerializeAsString(), nullptr));
      RAY_LOG(DEBUG) << "Finished registering actor info, job id = " << actor_id.JobId()
                     << ", actor id = " << actor_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status =
      gcs_table_storage_->ActorTable().Put(actor_id, actor_table_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void GcsActorManager::HandleUpdateActorInfo(const rpc::UpdateActorInfoRequest &request,
                                            rpc::UpdateActorInfoReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_id());
  RAY_LOG(DEBUG) << "Updating actor info, job id = " << actor_id.JobId()
                 << ", actor id = " << actor_id;
  const auto &actor_table_data = request.actor_table_data();
  auto on_done = [this, actor_id, actor_table_data, reply,
                  send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to update actor info: " << status.ToString()
                     << ", job id = " << actor_id.JobId() << ", actor id = " << actor_id;
    } else {
      RAY_CHECK_OK(gcs_pub_sub_->Publish(ACTOR_CHANNEL, actor_id.Hex(),
                                         actor_table_data.SerializeAsString(), nullptr));
      RAY_LOG(DEBUG) << "Finished updating actor info, job id = " << actor_id.JobId()
                     << ", actor id = " << actor_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status =
      gcs_table_storage_->ActorTable().Put(actor_id, actor_table_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void GcsActorManager::HandleAddActorCheckpoint(
    const rpc::AddActorCheckpointRequest &request, rpc::AddActorCheckpointReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.checkpoint_data().actor_id());
  ActorCheckpointID checkpoint_id =
      ActorCheckpointID::FromBinary(request.checkpoint_data().checkpoint_id());
  RAY_LOG(DEBUG) << "Adding actor checkpoint, job id = " << actor_id.JobId()
                 << ", actor id = " << actor_id << ", checkpoint id = " << checkpoint_id;
  auto on_done = [this, actor_id, checkpoint_id, reply,
                  send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to add actor checkpoint: " << status.ToString()
                     << ", job id = " << actor_id.JobId() << ", actor id = " << actor_id
                     << ", checkpoint id = " << checkpoint_id;
    } else {
      auto on_get_done = [this, actor_id, checkpoint_id, reply, send_reply_callback](
                             const Status &status,
                             const boost::optional<ActorCheckpointIdData> &result) {
        ActorCheckpointIdData actor_checkpoint_id;
        if (result) {
          actor_checkpoint_id.CopyFrom(*result);
        } else {
          actor_checkpoint_id.set_actor_id(actor_id.Binary());
        }
        actor_checkpoint_id.add_checkpoint_ids(checkpoint_id.Binary());
        actor_checkpoint_id.add_timestamps(absl::GetCurrentTimeNanos() / 1000000);
        auto on_put_done = [actor_id, checkpoint_id, reply,
                            send_reply_callback](const Status &status) {
          RAY_LOG(DEBUG) << "Finished adding actor checkpoint, job id = "
                         << actor_id.JobId() << ", actor id = " << actor_id
                         << ", checkpoint id = " << checkpoint_id;
          GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
        };
        RAY_CHECK_OK(gcs_table_storage_->ActorCheckpointIdTable().Put(
            actor_id, actor_checkpoint_id, on_put_done));
      };
      RAY_CHECK_OK(
          gcs_table_storage_->ActorCheckpointIdTable().Get(actor_id, on_get_done));
    }
  };

  Status status = gcs_table_storage_->ActorCheckpointTable().Put(
      checkpoint_id, request.checkpoint_data(), on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void GcsActorManager::HandleGetActorCheckpoint(
    const rpc::GetActorCheckpointRequest &request, rpc::GetActorCheckpointReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_id());
  ActorCheckpointID checkpoint_id =
      ActorCheckpointID::FromBinary(request.checkpoint_id());
  RAY_LOG(DEBUG) << "Getting actor checkpoint, job id = " << actor_id.JobId()
                 << ", checkpoint id = " << checkpoint_id;
  auto on_done = [actor_id, checkpoint_id, reply, send_reply_callback](
                     const Status &status,
                     const boost::optional<ActorCheckpointData> &result) {
    if (status.ok()) {
      if (result) {
        reply->mutable_checkpoint_data()->CopyFrom(*result);
      }
      RAY_LOG(DEBUG) << "Finished getting actor checkpoint, job id = " << actor_id.JobId()
                     << ", checkpoint id = " << checkpoint_id;
    } else {
      RAY_LOG(ERROR) << "Failed to get actor checkpoint: " << status.ToString()
                     << ", job id = " << actor_id.JobId()
                     << ", checkpoint id = " << checkpoint_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_table_storage_->ActorCheckpointTable().Get(checkpoint_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
}

void GcsActorManager::HandleGetActorCheckpointID(
    const rpc::GetActorCheckpointIDRequest &request,
    rpc::GetActorCheckpointIDReply *reply, rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_id());
  RAY_LOG(DEBUG) << "Getting actor checkpoint id, job id = " << actor_id.JobId()
                 << ", actor id = " << actor_id;
  auto on_done = [actor_id, reply, send_reply_callback](
                     const Status &status,
                     const boost::optional<ActorCheckpointIdData> &result) {
    if (status.ok()) {
      if (result) {
        reply->mutable_checkpoint_id_data()->CopyFrom(*result);
      }
      RAY_LOG(DEBUG) << "Finished getting actor checkpoint id, job id = "
                     << actor_id.JobId() << ", actor id = " << actor_id;
    } else {
      RAY_LOG(ERROR) << "Failed to get actor checkpoint id: " << status.ToString()
                     << ", job id = " << actor_id.JobId() << ", actor id = " << actor_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_table_storage_->ActorCheckpointIdTable().Get(actor_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
}

Status GcsActorManager::RegisterActor(
    const ray::rpc::RegisterActorRequest &request,
    std::function<void(std::shared_ptr<GcsActor>)> callback) {
  RAY_CHECK(callback);
  const auto &actor_creation_task_spec = request.task_spec().actor_creation_task_spec();
  auto actor_id = ActorID::FromBinary(actor_creation_task_spec.actor_id());

  auto iter = registered_actors_.find(actor_id);
  if (iter != registered_actors_.end() &&
      iter->second->GetState() == rpc::ActorTableData::ALIVE) {
    // In case of temporary network failures, workers will re-send multiple duplicate
    // requests to GCS server.
    // In this case, we can just reply.
    callback(iter->second);
    return Status::OK();
  }

  auto pending_register_iter = actor_to_register_callbacks_.find(actor_id);
  if (pending_register_iter != actor_to_register_callbacks_.end()) {
    // It is a duplicate message, just mark the callback as pending and invoke it after
    // the actor has been flushed to the storage.
    pending_register_iter->second.emplace_back(std::move(callback));
    return Status::OK();
  }

  auto actor = std::make_shared<GcsActor>(request.task_spec());
  if (!actor->GetName().empty()) {
    auto it = named_actors_.find(actor->GetName());
    if (it == named_actors_.end()) {
      named_actors_.emplace(actor->GetName(), actor->GetActorID());
    } else {
      std::stringstream stream;
      stream << "Actor with name '" << actor->GetName() << "' already exists.";
      return Status::Invalid(stream.str());
    }
  }

  // Mark the callback as pending and invoke it after the actor has been successfully
  // flushed to the storage.
  actor_to_register_callbacks_[actor_id].emplace_back(std::move(callback));
  RAY_CHECK(registered_actors_.emplace(actor->GetActorID(), actor).second);

  const auto &owner_address = actor->GetOwnerAddress();
  auto node_id = ClientID::FromBinary(owner_address.raylet_id());
  auto worker_id = WorkerID::FromBinary(owner_address.worker_id());
  RAY_CHECK(unresolved_actors_[node_id][worker_id].emplace(actor->GetActorID()).second);

  if (!actor->IsDetached() && worker_client_factory_) {
    // This actor is owned. Send a long polling request to the actor's
    // owner to determine when the actor should be removed.
    PollOwnerForActorOutOfScope(actor);
  }

  // The backend storage is supposed to be reliable, so the status must be ok.
  RAY_CHECK_OK(gcs_table_storage_->ActorTable().Put(
      actor->GetActorID(), *actor->GetMutableActorTableData(),
      [this, actor](const Status &status) {
        // The backend storage is supposed to be reliable, so the status must be ok.
        RAY_CHECK_OK(status);
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
  RAY_CHECK(callback);
  const auto &actor_creation_task_spec = request.task_spec().actor_creation_task_spec();
  auto actor_id = ActorID::FromBinary(actor_creation_task_spec.actor_id());

  auto iter = registered_actors_.find(actor_id);
  if (iter != registered_actors_.end() &&
      iter->second->GetState() == rpc::ActorTableData::ALIVE) {
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

  // Remove the actor from the unresolved actor map.
  auto actor = std::make_shared<GcsActor>(request.task_spec());
  actor->GetMutableActorTableData()->set_state(rpc::ActorTableData::PENDING_CREATION);
  const auto &owner_address = actor->GetOwnerAddress();
  auto node_id = ClientID::FromBinary(owner_address.raylet_id());
  auto worker_id = WorkerID::FromBinary(owner_address.worker_id());
  auto it = unresolved_actors_.find(node_id);
  RAY_CHECK(it != unresolved_actors_.end());
  auto worker_to_actors_it = it->second.find(worker_id);
  RAY_CHECK(worker_to_actors_it != it->second.end());
  RAY_CHECK(worker_to_actors_it->second.erase(actor_id) != 0);
  if (worker_to_actors_it->second.empty()) {
    it->second.erase(worker_to_actors_it);
    if (it->second.empty()) {
      unresolved_actors_.erase(it);
    }
  }
  // Update the registered actor as its creation task specification may have changed due
  // to resolved dependencies.
  registered_actors_[actor_id] = actor;

  // Schedule the actor.
  gcs_actor_scheduler_->Schedule(actor);
  return Status::OK();
}

ActorID GcsActorManager::GetActorIDByName(const std::string &name) {
  ActorID actor_id = ActorID::Nil();
  auto it = named_actors_.find(name);
  if (it != named_actors_.end()) {
    actor_id = it->second;
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
    RAY_LOG(DEBUG) << "Adding owner " << owner_id << " of actor " << actor_id;
    std::shared_ptr<rpc::CoreWorkerClientInterface> client =
        worker_client_factory_(actor->GetOwnerAddress());
    it = workers.emplace(owner_id, Owner(std::move(client))).first;
  }
  it->second.children_actor_ids.insert(actor_id);

  rpc::WaitForActorOutOfScopeRequest wait_request;
  wait_request.set_intended_worker_id(owner_id.Binary());
  wait_request.set_actor_id(actor_id.Binary());
  RAY_CHECK_OK(it->second.client->WaitForActorOutOfScope(
      wait_request, [this, owner_node_id, owner_id, actor_id](
                        Status status, const rpc::WaitForActorOutOfScopeReply &reply) {
        if (!status.ok()) {
          RAY_LOG(INFO) << "Worker " << owner_id << " failed, destroying actor child";
        }

        auto node_it = owners_.find(owner_node_id);
        if (node_it != owners_.end() && node_it->second.count(owner_id)) {
          // Only destroy the actor if its owner is still alive. The actor may
          // have already been destroyed if the owner died.
          DestroyActor(actor_id);
        }
      }));
}

void GcsActorManager::DestroyActor(const ActorID &actor_id) {
  RAY_LOG(DEBUG) << "Destroying actor " << actor_id;
  actor_to_register_callbacks_.erase(actor_id);
  actor_to_create_callbacks_.erase(actor_id);
  auto it = registered_actors_.find(actor_id);
  RAY_CHECK(it != registered_actors_.end())
      << "Tried to destroy actor that does not exist " << actor_id;
  const auto actor = std::move(it->second);
  registered_actors_.erase(it);

  // Clean up the client to the actor's owner, if necessary.
  if (!actor->IsDetached()) {
    const auto &owner_node_id = actor->GetOwnerNodeID();
    const auto &owner_id = actor->GetOwnerID();
    RAY_LOG(DEBUG) << "Erasing actor " << actor_id << " owned by " << owner_id;

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

  // The actor is already dead, most likely due to process or node failure.
  if (actor->GetState() == rpc::ActorTableData::DEAD) {
    return;
  }

  if (actor->GetState() == rpc::ActorTableData::DEPENDENCIES_UNREADY) {
    // The actor creation task still has unresolved dependencies. Remove from the
    // unresolved actors map.
    const auto &owner_address = actor->GetOwnerAddress();
    auto node_id = ClientID::FromBinary(owner_address.raylet_id());
    auto worker_id = WorkerID::FromBinary(owner_address.worker_id());
    auto iter = unresolved_actors_.find(node_id);
    if (iter != unresolved_actors_.end()) {
      auto it = iter->second.find(worker_id);
      RAY_CHECK(it != iter->second.end());
      RAY_CHECK(it->second.erase(actor_id) != 0);
      if (it->second.empty()) {
        iter->second.erase(it);
        if (iter->second.empty()) {
          unresolved_actors_.erase(iter);
        }
      }
    }
  } else {
    // The actor is still alive or pending creation. Clean up all remaining
    // state.
    const auto &node_id = actor->GetNodeID();
    const auto &worker_id = actor->GetWorkerID();
    auto node_it = created_actors_.find(node_id);
    if (node_it != created_actors_.end() && node_it->second.count(worker_id)) {
      // The actor has already been created. Destroy the process by force-killing
      // it.
      auto actor_client = worker_client_factory_(actor->GetAddress());
      rpc::KillActorRequest request;
      request.set_intended_actor_id(actor_id.Binary());
      request.set_force_kill(true);
      request.set_no_restart(true);
      RAY_UNUSED(actor_client->KillActor(request, nullptr));

      RAY_CHECK(node_it->second.erase(actor->GetWorkerID()));
      if (node_it->second.empty()) {
        created_actors_.erase(node_it);
      }
    } else {
      // The actor has not been created yet. It is either being scheduled or is
      // pending scheduling.
      auto canceled_actor_id =
          gcs_actor_scheduler_->CancelOnWorker(actor->GetNodeID(), actor->GetWorkerID());
      if (!canceled_actor_id.IsNil()) {
        // The actor was being scheduled and has now been canceled.
        RAY_CHECK(canceled_actor_id == actor_id);
      } else {
        auto pending_it =
            std::find_if(pending_actors_.begin(), pending_actors_.end(),
                         [actor_id](const std::shared_ptr<GcsActor> &actor) {
                           return actor->GetActorID() == actor_id;
                         });

        // The actor was pending scheduling. Remove it from the queue.
        if (pending_it != pending_actors_.end()) {
          pending_actors_.erase(pending_it);
        } else {
          // When actor creation request of this actor id is pending in raylet,
          // it doesn't responds, and the actor should be still in leasing state.
          // NOTE: Raylet will cancel the lease request once it receives the
          // actor state notification. So this method doesn't have to cancel
          // outstanding lease request by calling raylet_client->CancelWorkerLease
          gcs_actor_scheduler_->CancelOnLeasing(node_id, actor_id);
        }
      }
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
        RAY_CHECK_OK(gcs_pub_sub_->Publish(ACTOR_CHANNEL, actor_id.Hex(),
                                           actor_table_data->SerializeAsString(),
                                           nullptr));
      }));
}

absl::flat_hash_set<ActorID> GcsActorManager::GetUnresolvedActorsByOwnerNode(
    const ClientID &node_id) const {
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
    const ClientID &node_id, const WorkerID &worker_id) const {
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

void GcsActorManager::OnWorkerDead(const ray::ClientID &node_id,
                                   const ray::WorkerID &worker_id,
                                   bool intentional_exit) {
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
  RAY_LOG(WARNING) << "Worker " << worker_id << " on node " << node_id
                   << " failed, restarting actor " << actor_id
                   << ", intentional exit: " << intentional_exit;
  ReconstructActor(actor_id, /*need_reschedule=*/!intentional_exit);
}

void GcsActorManager::OnNodeDead(const ClientID &node_id) {
  RAY_LOG(WARNING) << "Node " << node_id << " failed, reconstructing actors.";
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

void GcsActorManager::ReconstructActor(const ActorID &actor_id, bool need_reschedule) {
  auto &actor = registered_actors_[actor_id];
  // If the owner and this actor is dead at the same time, the actor
  // could've been destroyed and dereigstered before reconstruction.
  if (actor == nullptr) {
    RAY_LOG(INFO) << "Actor is destroyed before reconstruction, actor id = " << actor_id;
    return;
  }
  auto node_id = actor->GetNodeID();
  auto worker_id = actor->GetWorkerID();
  auto mutable_actor_table_data = actor->GetMutableActorTableData();
  // If the need_reschedule is set to false, then set the `remaining_restarts` to 0
  // so that the actor will never be rescheduled.
  int64_t max_restarts = mutable_actor_table_data->max_restarts();
  uint64_t num_restarts = mutable_actor_table_data->num_restarts();
  int64_t remaining_restarts;
  if (!need_reschedule) {
    remaining_restarts = 0;
  } else if (max_restarts == -1) {
    remaining_restarts = -1;
  } else {
    int64_t remaining = max_restarts - num_restarts;
    remaining_restarts = std::max(remaining, static_cast<int64_t>(0));
  }
  RAY_LOG(WARNING) << "Actor is failed " << actor_id << " on worker " << worker_id
                   << " at node " << node_id << ", need_reschedule = " << need_reschedule
                   << ", remaining_restarts = " << remaining_restarts;
  if (remaining_restarts != 0) {
    mutable_actor_table_data->set_state(rpc::ActorTableData::RESTARTING);
    const auto actor_table_data = actor->GetActorTableData();
    // Make sure to reset the address before flushing to GCS. Otherwise,
    // GCS will mistakenly consider this lease request succeeds when restarting.
    actor->UpdateAddress(rpc::Address());
    mutable_actor_table_data->clear_resource_mapping();
    // The backend storage is reliable in the future, so the status must be ok.
    RAY_CHECK_OK(gcs_table_storage_->ActorTable().Put(
        actor_id, *mutable_actor_table_data,
        [this, actor_id, actor_table_data](Status status) {
          RAY_CHECK_OK(gcs_pub_sub_->Publish(ACTOR_CHANNEL, actor_id.Hex(),
                                             actor_table_data.SerializeAsString(),
                                             nullptr));
        }));
    gcs_actor_scheduler_->Schedule(actor);
    mutable_actor_table_data->set_num_restarts(num_restarts + 1);
  } else {
    // Remove actor from `named_actors_` if its name is not empty.
    if (!actor->GetName().empty()) {
      auto it = named_actors_.find(actor->GetName());
      if (it != named_actors_.end()) {
        RAY_CHECK(it->second == actor->GetActorID());
        named_actors_.erase(it);
      }
    }
    mutable_actor_table_data->set_state(rpc::ActorTableData::DEAD);
    // The backend storage is reliable in the future, so the status must be ok.
    RAY_CHECK_OK(gcs_table_storage_->ActorTable().Put(
        actor_id, *mutable_actor_table_data,
        [this, actor, actor_id, mutable_actor_table_data](Status status) {
          // if actor was an detached actor, make sure to destroy it.
          // We need to do this because detached actors are not destroyed
          // when its owners are dead because it doesn't have owners.
          if (actor->IsDetached()) DestroyActor(actor_id);
          RAY_CHECK_OK(gcs_pub_sub_->Publish(
              ACTOR_CHANNEL, actor_id.Hex(),
              mutable_actor_table_data->SerializeAsString(), nullptr));
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
  RAY_LOG(DEBUG) << "Actor created successfully, actor id = " << actor_id;
  // NOTE: If an actor is deleted immediately after the user creates the actor, reference
  // counter may return a reply to the request of WaitForActorOutOfScope to GCS server,
  // and GCS server will destroy the actor. The actor creation is asynchronous, it may be
  // destroyed before the actor creation is completed.
  if (registered_actors_.count(actor_id) == 0) {
    RAY_LOG(WARNING) << "Actor is destroyed before the creation is completed, actor id = "
                     << actor_id;
    return;
  }
  actor->UpdateState(rpc::ActorTableData::ALIVE);
  auto actor_table_data = actor->GetActorTableData();
  // The backend storage is reliable in the future, so the status must be ok.
  RAY_CHECK_OK(gcs_table_storage_->ActorTable().Put(
      actor_id, actor_table_data,
      [this, actor_id, actor_table_data, actor](Status status) {
        RAY_CHECK_OK(gcs_pub_sub_->Publish(ACTOR_CHANNEL, actor_id.Hex(),
                                           actor_table_data.SerializeAsString(),
                                           nullptr));

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

        auto worker_id = actor->GetWorkerID();
        auto node_id = actor->GetNodeID();
        RAY_CHECK(!worker_id.IsNil());
        RAY_CHECK(!node_id.IsNil());
        RAY_CHECK(created_actors_[node_id].emplace(worker_id, actor_id).second);
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

void GcsActorManager::LoadInitialData(const EmptyCallback &done) {
  RAY_LOG(INFO) << "Loading initial data.";
  auto callback = [this,
                   done](const std::unordered_map<ActorID, ActorTableData> &result) {
    std::unordered_map<ClientID, std::vector<WorkerID>> node_to_workers;
    for (auto &item : result) {
      if (item.second.state() != ray::rpc::ActorTableData::DEAD) {
        auto actor = std::make_shared<GcsActor>(item.second);
        registered_actors_.emplace(item.first, actor);

        if (!actor->GetName().empty()) {
          named_actors_.emplace(actor->GetName(), actor->GetActorID());
        }

        if (item.second.state() == ray::rpc::ActorTableData::DEPENDENCIES_UNREADY) {
          const auto &owner = actor->GetOwnerAddress();
          const auto &owner_node = ClientID::FromBinary(owner.raylet_id());
          const auto &owner_worker = WorkerID::FromBinary(owner.worker_id());
          RAY_CHECK(unresolved_actors_[owner_node][owner_worker]
                        .emplace(actor->GetActorID())
                        .second);
          if (!actor->IsDetached() && worker_client_factory_) {
            // This actor is owned. Send a long polling request to the actor's
            // owner to determine when the actor should be removed.
            PollOwnerForActorOutOfScope(actor);
          }
        } else if (item.second.state() == ray::rpc::ActorTableData::ALIVE) {
          created_actors_[actor->GetNodeID()].emplace(actor->GetWorkerID(),
                                                      actor->GetActorID());
        }

        auto &workers = owners_[actor->GetNodeID()];
        auto it = workers.find(actor->GetWorkerID());
        if (it == workers.end()) {
          std::shared_ptr<rpc::CoreWorkerClientInterface> client =
              worker_client_factory_(actor->GetOwnerAddress());
          workers.emplace(actor->GetOwnerID(), Owner(std::move(client)));
        }

        if (!actor->GetWorkerID().IsNil()) {
          RAY_CHECK(!actor->GetNodeID().IsNil());
          node_to_workers[actor->GetNodeID()].emplace_back(actor->GetWorkerID());
        }
      }
    }

    // Notify raylets to release unused workers.
    if (RayConfig::instance().gcs_actor_service_enabled()) {
      gcs_actor_scheduler_->ReleaseUnusedWorkers(node_to_workers);
    }

    RAY_LOG(DEBUG) << "The number of registered actors is " << registered_actors_.size()
                   << ", and the number of created actors is " << created_actors_.size();
    for (auto &item : registered_actors_) {
      auto &actor = item.second;
      if (actor->GetState() == ray::rpc::ActorTableData::PENDING_CREATION ||
          actor->GetState() == ray::rpc::ActorTableData::RESTARTING) {
        // We should not reschedule actors in state of `ALIVE`.
        // We could not reschedule actors in state of `DEPENDENCIES_UNREADY` because the
        // dependencies of them may not have been resolved yet.
        RAY_LOG(DEBUG) << "Rescheduling a non-alive actor, actor id = "
                       << actor->GetActorID() << ", state = " << actor->GetState();
        gcs_actor_scheduler_->Reschedule(actor);
      }
    }

    RAY_LOG(INFO) << "Finished loading initial data.";
    done();
  };
  RAY_CHECK_OK(gcs_table_storage_->ActorTable().GetAll(callback));
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

      // Get checkpoint id first from checkpoint id table and delete all checkpoints
      // related to this job
      RAY_CHECK_OK(gcs_table_storage_->ActorCheckpointIdTable().GetByJobId(
          job_id, [this, non_detached_actors_set](
                      const std::unordered_map<ActorID, ActorCheckpointIdData> &result) {
            if (!result.empty()) {
              std::vector<ActorID> checkpoints;
              std::vector<ActorCheckpointID> checkpoint_ids;
              for (auto &item : result) {
                if (non_detached_actors_set.find(item.first) !=
                    non_detached_actors_set.end()) {
                  checkpoints.push_back(item.first);
                  for (auto &id : item.second.checkpoint_ids()) {
                    checkpoint_ids.push_back(ActorCheckpointID::FromBinary(id));
                  }
                }
              }

              RAY_CHECK_OK(gcs_table_storage_->ActorCheckpointIdTable().BatchDelete(
                  checkpoints, nullptr));
              RAY_CHECK_OK(gcs_table_storage_->ActorCheckpointTable().BatchDelete(
                  checkpoint_ids, nullptr));
            }
          }));
    }
  };

  // Only non-detached actors should be deleted. We get all actors of this job and to the
  // filtering.
  RAY_CHECK_OK(gcs_table_storage_->ActorTable().GetByJobId(job_id, on_done));
}

const absl::flat_hash_map<ClientID, absl::flat_hash_map<WorkerID, ActorID>>
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

}  // namespace gcs
}  // namespace ray
