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
GcsActorManager::GcsActorManager(boost::asio::io_context &io_context,
                                 gcs::ActorInfoAccessor &actor_info_accessor,
                                 gcs::GcsNodeManager &gcs_node_manager,
                                 LeaseClientFactoryFn lease_client_factory,
                                 rpc::ClientFactoryFn client_factory)
    : actor_info_accessor_(actor_info_accessor),
      gcs_actor_scheduler_(new gcs::GcsActorScheduler(
          io_context, actor_info_accessor, gcs_node_manager,
          [this](std::shared_ptr<GcsActor> actor) {
            // When there are no available nodes to schedule the actor the
            // gcs_actor_scheduler will treat it as failed and invoke this handler. In
            // this case, the actor should be appended to the `pending_actors_` and wait
            // for the registration of new node.
            pending_actors_.emplace(std::move(actor));
          },
          [this](std::shared_ptr<GcsActor> actor) {
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
            RAY_CHECK_OK(
                actor_info_accessor_.AsyncUpdate(actor_id, actor_table_data, nullptr));
          },
          std::move(lease_client_factory), std::move(client_factory))) {
  RAY_LOG(INFO) << "Initializing GcsActorManager.";
  gcs_node_manager.AddNodeAddedListener(
      [this](const std::shared_ptr<rpc::GcsNodeInfo> &) {
        // Because a new node has been added, we need to try to schedule the pending
        // actors.
        SchedulePendingActors();
      });

  gcs_node_manager.AddNodeRemovedListener([this](std::shared_ptr<rpc::GcsNodeInfo> node) {
    // All of the related actors should be reconstructed when a node is remove from gcs
    // node manager.
    ReconstructActorsOnNode(ClientID::FromBinary(node->node_id()));
  });
  RAY_LOG(INFO) << "Finished initialing GcsActorManager.";
}

void GcsActorManager::RegisterActor(
    const ray::rpc::CreateActorRequest &request,
    std::function<void(std::shared_ptr<GcsActor>)> callback) {
  RAY_CHECK(callback);
  const auto &actor_creation_task_spec = request.task_spec().actor_creation_task_spec();
  auto actor_id = ActorID::FromBinary(actor_creation_task_spec.actor_id());

  auto iter = registered_actors_.find(actor_id);
  if (iter != registered_actors_.end()) {
    // When the network fails, Driver/Worker is not sure whether GcsServer has received
    // the request of actor creation task, so Driver/Worker will try again and again until
    // receiving the reply from GcsServer. If the actor is already records on the GCS
    // Server side, the GCS Server will be responsible for creating or reconstructing the
    // actor regardless of whether the Driver/Worker sends the request to create the actor
    // again, so we just need fast reply OK to the Driver/Worker that the actor is already
    // recorded by GCS Server.
    callback(iter->second);
    return;
  }

  auto pending_register_iter = actor_to_register_callbacks_.find(actor_id);
  if (pending_register_iter != actor_to_register_callbacks_.end()) {
    // It is a duplicate message, just pending the callback and invoke it after the
    // related actor is flushed.
    pending_register_iter->second.emplace_back(std::move(callback));
    return;
  }

  // Pending the callback and invoke it after the related actor is flushed.
  actor_to_register_callbacks_[actor_id].emplace_back(std::move(callback));

  auto actor_table_data = std::make_shared<rpc::ActorTableData>();
  actor_table_data->set_actor_id(actor_id.Binary());
  actor_table_data->set_job_id(request.task_spec().job_id());
  actor_table_data->set_max_reconstructions(
      actor_creation_task_spec.max_actor_reconstructions());
  actor_table_data->set_remaining_reconstructions(
      actor_creation_task_spec.max_actor_reconstructions());

  auto dummy_object = TaskSpecification(request.task_spec()).ActorDummyObject().Binary();
  actor_table_data->set_actor_creation_dummy_object_id(dummy_object);

  actor_table_data->set_is_detached(actor_creation_task_spec.is_detached());
  actor_table_data->mutable_owner_address()->CopyFrom(
      request.task_spec().caller_address());

  actor_table_data->set_state(rpc::ActorTableData::PENDING);
  actor_table_data->mutable_task_spec()->CopyFrom(request.task_spec());

  actor_table_data->mutable_address()->set_raylet_id(ClientID::Nil().Binary());
  actor_table_data->mutable_address()->set_worker_id(WorkerID::Nil().Binary());

  auto actor = std::make_shared<GcsActor>(*actor_table_data);
  // The backend storage is reliable in the future, so the status must be ok.
  RAY_CHECK_OK(actor_info_accessor_.AsyncUpdate(
      actor_id, actor_table_data, [this, actor](Status status) {
        RAY_CHECK_OK(status);
        registered_actors_.emplace(actor->GetActorID(), actor);
        gcs_actor_scheduler_->Schedule(actor);
        // Invoke all callbacks for all registration requests of this actor (duplicated
        // requests are included) and remove all of them from
        // actor_to_register_callbacks_.
        auto iter = actor_to_register_callbacks_.find(actor->GetActorID());
        RAY_CHECK(iter != actor_to_register_callbacks_.end() && !iter->second.empty());
        for (auto &callback : iter->second) {
          callback(actor);
        }
        actor_to_register_callbacks_.erase(iter);
      }));
}

void GcsActorManager::ReconstructActorOnWorker(const ray::ClientID &node_id,
                                               const ray::WorkerID &worker_id,
                                               bool need_reschedule) {
  std::shared_ptr<GcsActor> actor;
  // Cancel the scheduling of the related actor.
  auto actor_id = gcs_actor_scheduler_->CancelOnWorker(node_id, worker_id);
  if (!actor_id.IsNil()) {
    auto iter = registered_actors_.find(actor_id);
    if (iter != registered_actors_.end()) {
      actor = iter->second;
    }
  } else {
    // Find from worker_to_created_actor_.
    auto iter = worker_to_created_actor_.find(worker_id);
    if (iter != worker_to_created_actor_.end()) {
      actor = std::move(iter->second);
      // Remove the created actor from worker_to_created_actor_.
      worker_to_created_actor_.erase(iter);
      // remove the created actor from node_to_created_actors_.
      auto node_iter = node_to_created_actors_.find(node_id);
      RAY_CHECK(node_iter != node_to_created_actors_.end());
      node_iter->second.erase(actor->GetActorID());
      if (node_iter->second.empty()) {
        node_to_created_actors_.erase(node_iter);
      }
    }
  }
  if (actor != nullptr) {
    // Reconstruct the actor.
    ReconstructActor(actor, need_reschedule);
  }
}

void GcsActorManager::ReconstructActorsOnNode(const ClientID &node_id) {
  // Cancel the scheduling of all related actors.
  auto scheduling_actor_ids = gcs_actor_scheduler_->CancelOnNode(node_id);
  for (auto &actor_id : scheduling_actor_ids) {
    auto iter = registered_actors_.find(actor_id);
    if (iter != registered_actors_.end()) {
      // Reconstruct the canceled actor.
      ReconstructActor(iter->second);
    }
  }

  // Find from node_to_created_actors_.
  auto iter = node_to_created_actors_.find(node_id);
  if (iter != node_to_created_actors_.end()) {
    auto created_actors = std::move(iter->second);
    // Remove all created actors from node_to_created_actors_.
    node_to_created_actors_.erase(iter);
    for (auto &entry : created_actors) {
      // Remove the actor from worker_to_created_actor_.
      worker_to_created_actor_.erase(entry.second->GetWorkerID());
      // Reconstruct the removed actor.
      ReconstructActor(entry.second);
    }
  }
}

void GcsActorManager::ReconstructActor(std::shared_ptr<GcsActor> actor,
                                       bool need_reschedule) {
  RAY_CHECK(actor != nullptr);
  auto node_id = actor->GetNodeID();
  auto worker_id = actor->GetWorkerID();
  RAY_LOG(WARNING) << "Actor is failed " << actor->GetActorID() << " on worker "
                   << worker_id << " at node " << node_id
                   << ", need_reschedule = " << need_reschedule;
  actor->UpdateAddress(rpc::Address());
  auto mutable_actor_table_data = actor->GetMutableActorTableData();
  // If the need_reschedule is set to false, then set the `remaining_reconstructions` to 0
  // so that the actor will never be rescheduled.
  auto remaining_reconstructions =
      need_reschedule ? mutable_actor_table_data->remaining_reconstructions() : 0;
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

void GcsActorManager::SchedulePendingActors() {
  if (pending_actors_.empty()) {
    return;
  }

  RAY_LOG(DEBUG) << "Scheduling actor creation tasks, size = " << pending_actors_.size();
  // TODO(ZhuSenlin): This loop maybe inefficient, it will be optimized later.
  auto actors = std::move(pending_actors_);
  while (!actors.empty()) {
    auto actor = actors.front();
    actors.pop();
    gcs_actor_scheduler_->Schedule(actor);
  }
}

}  // namespace gcs
}  // namespace ray
