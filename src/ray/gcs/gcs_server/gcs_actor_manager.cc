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
                                 gcs::ActorInfoAccessor &actor_info_accessor)
    : gcs_actor_scheduler_(std::move(scheduler)),
      actor_info_accessor_(actor_info_accessor) {}

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
  gcs_actor_scheduler_->Schedule(actor);
}

void GcsActorManager::ReconstructActorOnWorker(const ray::ClientID &node_id,
                                               const ray::WorkerID &worker_id,
                                               bool need_reschedule) {
  ActorID actor_id;
  // Find from worker_to_created_actor_.
  auto iter = created_actors_.find(node_id);
  if (iter != created_actors_.end() && iter->second.count(worker_id)) {
    actor_id = iter->second[worker_id];
    iter->second.erase(worker_id);
    if (iter->second.empty()) {
      created_actors_.erase(iter);
    }
  } else {
    actor_id = gcs_actor_scheduler_->CancelOnWorker(node_id, worker_id);
  }

  if (!actor_id.IsNil()) {
    RAY_LOG(INFO) << "Worker " << worker_id << " on node " << node_id
                  << " failed, reconstructing actor " << actor_id;
    // Reconstruct the actor.
    ReconstructActor(actor_id, need_reschedule);
  }
}

void GcsActorManager::ReconstructActorsOnNode(const ClientID &node_id) {
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
}

void GcsActorManager::ReconstructActor(const ActorID &actor_id, bool need_reschedule) {
  auto &actor = registered_actors_[actor_id];
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
    RAY_CHECK_OK(actor_info_accessor_.AsyncUpdate(
        actor->GetActorID(), actor_table_data,
        [this, actor_id](Status status) { RAY_CHECK_OK(status); }));
    gcs_actor_scheduler_->Schedule(actor);
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
  auto actor_id = actor->GetActorID();
  RAY_CHECK(registered_actors_.count(actor_id) > 0);
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

  auto worker_id = actor->GetWorkerID();
  auto node_id = actor->GetNodeID();
  RAY_CHECK(!worker_id.IsNil());
  RAY_CHECK(!node_id.IsNil());
  RAY_CHECK(created_actors_[node_id].emplace(worker_id, actor_id).second);
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
