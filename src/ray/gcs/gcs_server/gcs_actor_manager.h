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

#ifndef RAY_GCS_ACTOR_MANAGER_H
#define RAY_GCS_ACTOR_MANAGER_H

#include <ray/common/id.h>
#include <ray/common/task/task_execution_spec.h>
#include <ray/common/task/task_spec.h>
#include <ray/protobuf/gcs_service.pb.h>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "gcs_actor_scheduler.h"

namespace ray {
namespace gcs {

/// GcsActor just wraps `ActorTableData` and provides some convenient interfaces to access
/// the fields inside `ActorTableData`.
/// This class is not thread-safe.
class GcsActor {
 public:
  /// Create a GcsActor by actor_table_data.
  ///
  /// \param actor_table_data Data of the actor (see gcs.proto).
  explicit GcsActor(rpc::ActorTableData actor_table_data)
      : actor_table_data_(std::move(actor_table_data)) {}

  /// Create a GcsActor by CreateActorRequest.
  ///
  /// \param request Contains the actor creation task specification.
  explicit GcsActor(const ray::rpc::CreateActorRequest &request) {
    RAY_CHECK(request.task_spec().type() == TaskType::ACTOR_CREATION_TASK);
    const auto &actor_creation_task_spec = request.task_spec().actor_creation_task_spec();
    actor_table_data_.set_actor_id(actor_creation_task_spec.actor_id());
    actor_table_data_.set_job_id(request.task_spec().job_id());
    actor_table_data_.set_max_reconstructions(
        actor_creation_task_spec.max_actor_reconstructions());
    actor_table_data_.set_remaining_reconstructions(
        actor_creation_task_spec.max_actor_reconstructions());

    auto dummy_object =
        TaskSpecification(request.task_spec()).ActorDummyObject().Binary();
    actor_table_data_.set_actor_creation_dummy_object_id(dummy_object);

    actor_table_data_.set_is_detached(actor_creation_task_spec.is_detached());
    actor_table_data_.mutable_owner_address()->CopyFrom(
        request.task_spec().caller_address());

    actor_table_data_.set_state(rpc::ActorTableData::PENDING);
    actor_table_data_.mutable_task_spec()->CopyFrom(request.task_spec());

    actor_table_data_.mutable_address()->set_raylet_id(ClientID::Nil().Binary());
    actor_table_data_.mutable_address()->set_worker_id(WorkerID::Nil().Binary());
  }

  /// Get the node id on which this actor is created.
  ClientID GetNodeID() const;
  /// Get the id of the worker on which this actor is created.
  WorkerID GetWorkerID() const;

  /// Update the `Address` of this actor (see gcs.proto).
  void UpdateAddress(const rpc::Address &address);
  /// Get the `Address` of this actor.
  const rpc::Address &GetAddress() const;

  /// Update the state of this actor.
  void UpdateState(rpc::ActorTableData::ActorState state);
  /// Get the state of this gcs actor.
  rpc::ActorTableData::ActorState GetState() const;

  /// Get the id of this actor.
  ActorID GetActorID() const;
  /// Get the task specification of this actor.
  TaskSpecification GetCreationTaskSpecification() const;

  /// Get the immutable ActorTableData of this actor.
  const rpc::ActorTableData &GetActorTableData() const;
  /// Get the mutable ActorTableData of this actor.
  rpc::ActorTableData *GetMutableActorTableData();

 private:
  /// The actor meta data which contains the task specification as well as the state of
  /// the gcs actor and so on (see gcs.proto).
  rpc::ActorTableData actor_table_data_;
};

using RegisterActorCallback = std::function<void(std::shared_ptr<GcsActor>)>;
/// GcsActorManager is responsible for managing the lifecycle of all actors.
/// This class is not thread-safe.
class GcsActorManager {
 public:
  /// Create a GcsActorManager
  ///
  /// \param io_context The main event loop.
  /// \param actor_info_accessor Used to flush actor data to storage.
  /// \param gcs_node_manager The actor manager needs to listen to the node change events
  /// inside gcs_node_manager.
  /// \param lease_client_factory Factory to create remote lease client, it will be passed
  /// through to the constructor of gcs_actor_scheduler, the gcs_actor_scheduler will use
  /// default factory inside itself if it is not set.
  /// \param client_factory Factory to create remote core worker client, it will be passed
  /// through to the constructor of gcs_actor_scheduler, the gcs_actor_scheduler will use
  /// default factory inside itself if it is not set.
  explicit GcsActorManager(boost::asio::io_context &io_context,
                           gcs::ActorInfoAccessor &actor_info_accessor,
                           gcs::GcsNodeManager &gcs_node_manager,
                           LeaseClientFactoryFn lease_client_factory = nullptr,
                           rpc::ClientFactoryFn client_factory = nullptr);

  virtual ~GcsActorManager() = default;

  /// Register actor asynchronously.
  ///
  /// \param request Contains the meta info to create the actor.
  /// \param callback Will be invoked after the actor is created successfully or be
  /// invoked immediately if the actor is already registered to `registered_actors_` and
  /// its state is `ALIVE`.
  void RegisterActor(const rpc::CreateActorRequest &request,
                     RegisterActorCallback callback);

  /// Reconstruct all actors associated with the specified node id, including actors which
  /// are scheduled or have been created on this node. Triggered when the given node goes
  /// down.
  ///
  /// \param node_id The specified node id.
  void ReconstructActorsOnNode(const ClientID &node_id);

  /// Reconstruct actor associated with the specified node_id and worker_id.
  /// The actor may be pending or already created.
  ///
  /// \param node_id ID of the node where the worker is located
  /// \param worker_id  ID of the worker that the actor is creating/created on
  /// \param need_reschedule Whether to reschedule the actor creation task, sometimes
  /// users want to kill an actor intentionally and don't want it to be rescheduled
  /// again.
  void ReconstructActorOnWorker(const ClientID &node_id, const WorkerID &worker_id,
                                bool need_reschedule = true);

 protected:
  /// Schedule actors in the `pending_actors_` queue.
  /// This method is triggered when new nodes are registered or resources change.
  void SchedulePendingActors();

  /// Reconstruct the specified actor.
  ///
  /// \param actor The target actor to be reconstructed.
  /// \param need_reschedule Whether to reschedule the actor creation task, sometimes
  /// users want to kill an actor intentionally and don't want it to be reconstructed
  /// again.
  void ReconstructActor(std::shared_ptr<GcsActor> actor, bool need_reschedule = true);

  /// This method is a callback of gcs_actor_scheduler when actor is created successfully.
  /// It will update the state of actor as well as the worker_to_created_actor_ and
  /// node_to_created_actors_ and flush the actor data to the storage.
  void OnActorCreateSuccess(std::shared_ptr<GcsActor> actor);

 protected:
  /// Callbacks of actor registration requests that are not yet flushed.
  /// This map is used to filter duplicated messages from a Driver/Worker caused by some
  /// network problems.
  absl::flat_hash_map<ActorID, std::vector<RegisterActorCallback>>
      actor_to_register_callbacks_;
  /// All registered actors (pending actors are also included).
  absl::flat_hash_map<ActorID, std::shared_ptr<GcsActor>> registered_actors_;
  /// The pending actors which will not be scheduled until there's a resource change.
  std::vector<std::shared_ptr<GcsActor>> pending_actors_;
  /// Map contains the relationship of worker and created actor.
  absl::flat_hash_map<WorkerID, std::shared_ptr<GcsActor>> worker_to_created_actor_;
  /// Map contains the relationship of node and created actors.
  absl::flat_hash_map<ClientID, absl::flat_hash_map<ActorID, std::shared_ptr<GcsActor>>>
      node_to_created_actors_;
  /// The access info accessor.
  gcs::ActorInfoAccessor &actor_info_accessor_;
  /// The scheduler to schedule all registered actors.
  std::unique_ptr<gcs::GcsActorScheduler> gcs_actor_scheduler_;
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_ACTOR_MANAGER_H
