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

#include "gcs_actor_manager.h"
#include <ray/common/ray_config.h>

#include <utility>

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

bool GcsActor::IsDetached() const { return actor_table_data_.is_detached(); }

WorkerID GcsActor::GetOwnerID() const {
  return WorkerID::FromBinary(GetOwnerAddress().worker_id());
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
                                 gcs::ActorInfoAccessor &actor_info_accessor,
                                 const rpc::ClientFactoryFn &worker_client_factory)
    : gcs_actor_scheduler_(std::move(scheduler)),
      actor_info_accessor_(actor_info_accessor),
      worker_client_factory_(worker_client_factory) {}

void GcsActorManager::RegisterActor(
    const ray::rpc::CreateActorRequest &request,
    std::function<void(std::shared_ptr<GcsActor>)> callback) {
  RAY_CHECK(callback);
  const auto &actor_creation_task_spec = request.task_spec().actor_creation_task_spec();
  auto actor_id = ActorID::FromBinary(actor_creation_task_spec.actor_id());

  auto iter = registered_actors_.find(actor_id);
  if (iter != registered_actors_.end() &&
      iter->second->GetState() == rpc::ActorTableData::ALIVE) {
    // When the network fails, Driver/Worker is not sure whether GcsServer has received
    // the request of actor creation task, so Driver/Worker will try again and again until
    // receiving the reply from GcsServer. If the actor has been created successfully then
    // just reply to the caller.
    callback(iter->second);
    return;
  }

  auto pending_register_iter = actor_to_register_callbacks_.find(actor_id);
  if (pending_register_iter != actor_to_register_callbacks_.end()) {
    // It is a duplicate message, just mark the callback as pending and invoke it after
    // the actor has been successfully created.
    pending_register_iter->second.emplace_back(std::move(callback));
    return;
  }

  // Mark the callback as pending and invoke it after the actor has been successfully
  // created.
  actor_to_register_callbacks_[actor_id].emplace_back(std::move(callback));

  auto actor = std::make_shared<GcsActor>(request);
  RAY_CHECK(registered_actors_.emplace(actor->GetActorID(), actor).second);

  if (!actor->IsDetached()) {
    // This actor is owned. Send a long polling request to the actor's
    // owner to determine when the actor should be removed.
    auto owner_id = actor->GetOwnerID();
    auto it = owner_clients_.find(owner_id);
    if (it == owner_clients_.end()) {
      std::shared_ptr<rpc::CoreWorkerClientInterface> client =
          worker_client_factory_(actor->GetOwnerAddress());
      it = owner_clients_
               .emplace(owner_id,
                        std::make_pair(std::move(client), absl::flat_hash_set<ActorID>()))
               .first;
    }
    it->second.second.insert(actor_id);

    rpc::WaitForActorOutOfScopeRequest wait_request;
    wait_request.set_intended_worker_id(owner_id.Binary());
    wait_request.set_actor_id(actor_id.Binary());
    RAY_CHECK_OK(it->second.first->WaitForActorOutOfScope(
        wait_request, [this, owner_id, actor_id](
                          Status status, const rpc::WaitForActorOutOfScopeReply &reply) {
          if (!status.ok()) {
            RAY_LOG(INFO) << "Worker " << owner_id << " failed, destroying actor child";
          }
          auto it = owner_clients_.find(owner_id);
          if (it != owner_clients_.end() && it->second.second.count(actor_id) > 0) {
            // Only destroy the actor if still alive. The actor may have already
            // been destroyed if the owner died.
            DestroyActor(actor_id);
          }
        }));
  }

  gcs_actor_scheduler_->Schedule(actor);
}

void GcsActorManager::DestroyActor(const ActorID &actor_id) {
  RAY_LOG(DEBUG) << "Destroying actor " << actor_id;
  actor_to_register_callbacks_.erase(actor_id);
  auto it = registered_actors_.find(actor_id);
  RAY_CHECK(it != registered_actors_.end())
      << "Tried to destroy actor that does not exist " << actor_id;

  const auto &actor = it->second;
  auto worker_it = worker_to_created_actor_.find(actor->GetWorkerID());
  auto canceled_actor_id =
      gcs_actor_scheduler_->CancelOnWorker(actor->GetNodeID(), actor->GetWorkerID());
  if (worker_it != worker_to_created_actor_.end()) {
    // The actor has already been created. Destroy the process by force-killing
    // it.
    auto actor_client = worker_client_factory_(actor->GetAddress());
    rpc::KillActorRequest request;
    request.set_intended_actor_id(actor_id.Binary());
    request.set_force_kill(true);
    request.set_no_reconstruction(true);
    actor_client->KillActor(request, nullptr);
    worker_to_created_actor_.erase(worker_it);

    auto node_it = node_to_created_actors_.find(actor->GetNodeID());
    RAY_CHECK(node_it != node_to_created_actors_.end());
    RAY_CHECK(node_it->second.erase(actor_id));
    if (node_it->second.empty()) {
      node_to_created_actors_.erase(node_it);
    }
  } else if (!canceled_actor_id.IsNil()) {
    // The actor was being scheduled and has now been canceled.
    RAY_CHECK(canceled_actor_id == actor_id);
  } else {
    // The actor was pending scheduling. Remove it from the queue.
    bool canceled = false;
    for (auto pending_it = pending_actors_.begin(); pending_it != pending_actors_.end();
         pending_it++) {
      if ((*pending_it)->GetActorID() == actor_id) {
        pending_actors_.erase(pending_it);
        canceled = true;
        break;
      }
    }
    RAY_CHECK(canceled);
  }

  // Clean up the client to the actor's owner, if necessary.
  if (!actor->IsDetached()) {
    auto owner_it = owner_clients_.find(actor->GetOwnerID());
    RAY_CHECK(owner_it != owner_clients_.end());
    RAY_CHECK(owner_it->second.second.erase(actor_id));
    if (owner_it->second.second.empty()) {
      owner_clients_.erase(owner_it);
    }
  }

  // Update the actor to DEAD in case any callers are still alive. This can
  // happen if the owner of the actor dies while there are still callers.
  auto mutable_actor_table_data = actor->GetMutableActorTableData();
  mutable_actor_table_data->set_state(rpc::ActorTableData::DEAD);
  auto actor_table_data =
      std::make_shared<rpc::ActorTableData>(*mutable_actor_table_data);
  // The backend storage is reliable in the future, so the status must be ok.
  RAY_CHECK_OK(
      actor_info_accessor_.AsyncUpdate(actor->GetActorID(), actor_table_data, nullptr));

  registered_actors_.erase(it);
}

void GcsActorManager::ReconstructActorOnWorker(const ray::ClientID &node_id,
                                               const ray::WorkerID &worker_id,
                                               bool need_reschedule) {
  const auto owner_it = owner_clients_.find(worker_id);
  if (owner_it != owner_clients_.end()) {
    const auto owned_actor_ids = owner_it->second.second;
    for (const auto &actor_id : owned_actor_ids) {
      DestroyActor(actor_id);
    }
  }

  std::shared_ptr<GcsActor> actor;
  // Find from worker_to_created_actor_.
  auto iter = worker_to_created_actor_.find(worker_id);
  if (iter != worker_to_created_actor_.end()) {
    actor = std::move(iter->second);
    // Remove the created actor from worker_to_created_actor_.
    worker_to_created_actor_.erase(iter);
    // remove the created actor from node_to_created_actors_.
    auto node_iter = node_to_created_actors_.find(node_id);
    RAY_CHECK(node_iter != node_to_created_actors_.end());
    RAY_CHECK(node_iter->second.erase(actor->GetActorID()) != 0);
    if (node_iter->second.empty()) {
      node_to_created_actors_.erase(node_iter);
    }
  } else {
    auto actor_id = gcs_actor_scheduler_->CancelOnWorker(node_id, worker_id);
    if (!actor_id.IsNil()) {
      auto iter = registered_actors_.find(actor_id);
      RAY_CHECK(iter != registered_actors_.end());
      actor = iter->second;
    }
  }

  if (actor != nullptr) {
    RAY_LOG(INFO) << "Worker " << worker_id << " on node " << node_id
                  << " failed, reconstructing actor " << actor->GetActorID();
    // Reconstruct the actor.
    ReconstructActor(actor, need_reschedule);
  }
}

void GcsActorManager::ReconstructActorsOnNode(const ClientID &node_id) {
  RAY_LOG(INFO) << "Node " << node_id << " failed, reconstructing actors";
  // Cancel the scheduling of all related actors.
  auto scheduling_actor_ids = gcs_actor_scheduler_->CancelOnNode(node_id);
  for (auto &actor_id : scheduling_actor_ids) {
    auto iter = registered_actors_.find(actor_id);
    if (iter != registered_actors_.end()) {
      // Reconstruct the canceled actor.
      ReconstructActor(iter->second);
    }
  }

  // Find all actors that were created on this node.
  auto iter = node_to_created_actors_.find(node_id);
  if (iter != node_to_created_actors_.end()) {
    auto created_actors = std::move(iter->second);
    // Remove all created actors from node_to_created_actors_.
    node_to_created_actors_.erase(iter);
    for (auto &entry : created_actors) {
      // Remove the actor from worker_to_created_actor_.
      RAY_CHECK(worker_to_created_actor_.erase(entry.second->GetWorkerID()) != 0);
      // Reconstruct the removed actor.
      ReconstructActor(std::move(entry.second));
    }
  }
}

void GcsActorManager::ReconstructActor(std::shared_ptr<GcsActor> actor,
                                       bool need_reschedule) {
  const auto owner_id = actor->GetOwnerID();
  auto owner_it = owner_clients_.find(owner_id);
  if (owner_it == owner_clients_.end()) {
    RAY_LOG(INFO) << "Owner " << owner_id << " of " << actor->GetActorID()
                  << " is already dead, canceling reconstruction";
    return;
  }

  RAY_CHECK(actor != nullptr);
  auto node_id = actor->GetNodeID();
  auto worker_id = actor->GetWorkerID();
  actor->UpdateAddress(rpc::Address());
  auto mutable_actor_table_data = actor->GetMutableActorTableData();
  // If the need_reschedule is set to false, then set the `remaining_reconstructions` to 0
  // so that the actor will never be rescheduled.
  auto remaining_reconstructions =
      need_reschedule ? mutable_actor_table_data->remaining_reconstructions() : 0;
  RAY_LOG(WARNING) << "Actor is failed " << actor->GetActorID() << " on worker "
                   << worker_id << " at node " << node_id
                   << ", need_reschedule = " << need_reschedule
                   << ", remaining_reconstructions = " << remaining_reconstructions;

  if (remaining_reconstructions > 0) {
    mutable_actor_table_data->set_remaining_reconstructions(--remaining_reconstructions);
    mutable_actor_table_data->set_state(rpc::ActorTableData::RECONSTRUCTING);
    auto actor_table_data =
        std::make_shared<rpc::ActorTableData>(*mutable_actor_table_data);
    // The backend storage is reliable in the future, so the status must be ok.
    RAY_CHECK_OK(actor_info_accessor_.AsyncUpdate(actor->GetActorID(), actor_table_data,
                                                  [this, actor](Status status) {
                                                    RAY_CHECK_OK(status);
                                                    gcs_actor_scheduler_->Schedule(actor);
                                                  }));
  } else {
    mutable_actor_table_data->set_state(rpc::ActorTableData::DEAD);
    auto actor_table_data =
        std::make_shared<rpc::ActorTableData>(*mutable_actor_table_data);
    // The backend storage is reliable in the future, so the status must be ok.
    RAY_CHECK_OK(
        actor_info_accessor_.AsyncUpdate(actor->GetActorID(), actor_table_data, nullptr));
  }
}

void GcsActorManager::OnActorCreationFailed(std::shared_ptr<GcsActor> actor) {
  // We will attempt to schedule this actor once an eligible node is
  // registered.
  pending_actors_.emplace_back(std::move(actor));
}

void GcsActorManager::OnActorCreationSuccess(std::shared_ptr<GcsActor> actor) {
  auto worker_id = actor->GetWorkerID();
  RAY_CHECK(!worker_id.IsNil());
  RAY_CHECK(worker_to_created_actor_.emplace(worker_id, actor).second);

  auto actor_id = actor->GetActorID();
  auto node_id = actor->GetNodeID();
  RAY_CHECK(!node_id.IsNil());
  RAY_CHECK(node_to_created_actors_[node_id].emplace(actor_id, actor).second);

  actor->UpdateState(rpc::ActorTableData::ALIVE);
  auto actor_table_data =
      std::make_shared<rpc::ActorTableData>(actor->GetActorTableData());
  // The backend storage is reliable in the future, so the status must be ok.
  RAY_CHECK_OK(actor_info_accessor_.AsyncUpdate(actor_id, actor_table_data, nullptr));

  // Invoke all callbacks for all registration requests of this actor (duplicated
  // requests are included) and remove all of them from actor_to_register_callbacks_.
  auto iter = actor_to_register_callbacks_.find(actor->GetActorID());
  if (iter != actor_to_register_callbacks_.end()) {
    for (auto &callback : iter->second) {
      callback(actor);
    }
    actor_to_register_callbacks_.erase(iter);
  }
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

}  // namespace gcs
}  // namespace ray
