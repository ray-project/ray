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

#pragma once

#include <utility>

#include "absl/container/flat_hash_map.h"
#include "ray/common/id.h"
#include "ray/common/task/task_execution_spec.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs/gcs_server/gcs_actor_scheduler.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "src/ray/protobuf/gcs_service.pb.h"

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

  /// Create a GcsActor by TaskSpec.
  ///
  /// \param task_spec Contains the actor creation task specification.
  explicit GcsActor(const ray::rpc::TaskSpec &task_spec) {
    RAY_CHECK(task_spec.type() == TaskType::ACTOR_CREATION_TASK);
    const auto &actor_creation_task_spec = task_spec.actor_creation_task_spec();
    actor_table_data_.set_actor_id(actor_creation_task_spec.actor_id());
    actor_table_data_.set_job_id(task_spec.job_id());
    actor_table_data_.set_max_restarts(actor_creation_task_spec.max_actor_restarts());
    actor_table_data_.set_num_restarts(0);

    auto dummy_object = TaskSpecification(task_spec).ActorDummyObject().Binary();
    actor_table_data_.set_actor_creation_dummy_object_id(dummy_object);

    actor_table_data_.set_is_detached(actor_creation_task_spec.is_detached());
    actor_table_data_.set_name(actor_creation_task_spec.name());
    actor_table_data_.mutable_owner_address()->CopyFrom(task_spec.caller_address());

    actor_table_data_.set_state(rpc::ActorTableData::DEPENDENCIES_UNREADY);
    actor_table_data_.mutable_task_spec()->CopyFrom(task_spec);

    actor_table_data_.mutable_address()->set_raylet_id(ClientID::Nil().Binary());
    actor_table_data_.mutable_address()->set_worker_id(WorkerID::Nil().Binary());
  }

  /// Get the node id on which this actor is created.
  ClientID GetNodeID() const;
  /// Get the id of the worker on which this actor is created.
  WorkerID GetWorkerID() const;
  /// Get the actor's owner ID.
  WorkerID GetOwnerID() const;
  /// Get the node ID of the actor's owner.
  ClientID GetOwnerNodeID() const;
  /// Get the address of the actor's owner.
  const rpc::Address &GetOwnerAddress() const;

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
  /// Returns whether or not this is a detached actor.
  bool IsDetached() const;
  /// Get the name of this actor (only set if it's a detached actor).
  std::string GetName() const;
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
using CreateActorCallback = std::function<void(std::shared_ptr<GcsActor>)>;
/// GcsActorManager is responsible for managing the lifecycle of all actors.
/// This class is not thread-safe.
class GcsActorManager : public rpc::ActorInfoHandler {
 public:
  /// Create a GcsActorManager
  ///
  /// \param scheduler Used to schedule actor creation tasks.
  /// \param gcs_table_storage Used to flush actor data to storage.
  /// \param gcs_pub_sub Used to publish gcs message.
  GcsActorManager(std::shared_ptr<GcsActorSchedulerInterface> scheduler,
                  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
                  std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
                  const rpc::ClientFactoryFn &worker_client_factory = nullptr);

  ~GcsActorManager() = default;

  void HandleRegisterActor(const rpc::RegisterActorRequest &request,
                           rpc::RegisterActorReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void HandleCreateActor(const rpc::CreateActorRequest &request,
                         rpc::CreateActorReply *reply,
                         rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetActorInfo(const rpc::GetActorInfoRequest &request,
                          rpc::GetActorInfoReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetNamedActorInfo(const rpc::GetNamedActorInfoRequest &request,
                               rpc::GetNamedActorInfoReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllActorInfo(const rpc::GetAllActorInfoRequest &request,
                             rpc::GetAllActorInfoReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  void HandleRegisterActorInfo(const rpc::RegisterActorInfoRequest &request,
                               rpc::RegisterActorInfoReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  void HandleUpdateActorInfo(const rpc::UpdateActorInfoRequest &request,
                             rpc::UpdateActorInfoReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  void HandleAddActorCheckpoint(const rpc::AddActorCheckpointRequest &request,
                                rpc::AddActorCheckpointReply *reply,
                                rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetActorCheckpoint(const rpc::GetActorCheckpointRequest &request,
                                rpc::GetActorCheckpointReply *reply,
                                rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetActorCheckpointID(const rpc::GetActorCheckpointIDRequest &request,
                                  rpc::GetActorCheckpointIDReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  /// Register actor asynchronously.
  ///
  /// \param request Contains the meta info to create the actor.
  /// \param callback Will be invoked after the actor is created successfully or be
  /// invoked immediately if the actor is already registered to `registered_actors_` and
  /// its state is `ALIVE`.
  /// \return Status::Invalid if this is a named actor and an actor with the specified
  /// name already exists. The callback will not be called in this case.
  Status RegisterActor(const rpc::RegisterActorRequest &request,
                       RegisterActorCallback callback);

  /// Create actor asynchronously.
  ///
  /// \param request Contains the meta info to create the actor.
  /// \param callback Will be invoked after the actor is created successfully or be
  /// invoked immediately if the actor is already registered to `registered_actors_` and
  /// its state is `ALIVE`.
  /// \return Status::Invalid if this is a named actor and an actor with the specified
  /// name already exists. The callback will not be called in this case.
  Status CreateActor(const rpc::CreateActorRequest &request,
                     CreateActorCallback callback);

  /// Get the actor ID for the named actor. Returns nil if the actor was not found.
  /// \param name The name of the detached actor to look up.
  /// \returns ActorID The ID of the actor. Nil if the actor was not found.
  ActorID GetActorIDByName(const std::string &name);

  /// Schedule actors in the `pending_actors_` queue.
  /// This method should be called when new nodes are registered or resources
  /// change.
  void SchedulePendingActors();

  /// Handle a node death. This will restart all actors associated with the
  /// specified node id, including actors which are scheduled or have been
  /// created on this node. Actors whose owners have died (possibly due to this
  /// node being removed) will not be restarted. If any workers on this node
  /// owned an actor, those actors will be destroyed.
  ///
  /// \param node_id The specified node id.
  void OnNodeDead(const ClientID &node_id);

  /// Handle a worker failure. This will restart the associated actor, if any,
  /// which may be pending or already created. If the worker owned other
  /// actors, those actors will be destroyed.
  ///
  /// \param node_id ID of the node where the dead worker was located.
  /// \param worker_id ID of the dead worker.
  /// \param intentional_exit Whether the death was intentional. If yes and the
  /// worker was an actor, we should not attempt to restart the actor.
  void OnWorkerDead(const ClientID &node_id, const WorkerID &worker_id,
                    bool intentional_exit = false);

  /// Handle actor creation task failure. This should be called when scheduling
  /// an actor creation task is infeasible.
  ///
  /// \param actor The actor whose creation task is infeasible.
  void OnActorCreationFailed(std::shared_ptr<GcsActor> actor);

  /// Handle actor creation task success. This should be called when the actor
  /// creation task has been scheduled successfully.
  ///
  /// \param actor The actor that has been created.
  void OnActorCreationSuccess(const std::shared_ptr<GcsActor> &actor);

  /// Load initial data from gcs storage to memory cache asynchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param done Callback that will be called when load is complete.
  void LoadInitialData(const EmptyCallback &done);

  /// Delete non-detached actor information from durable storage once the associated job
  /// finishes.
  ///
  /// \param job_id The id of finished job.
  void OnJobFinished(const JobID &job_id);

  /// Get the created actors.
  ///
  /// \return The created actors.
  const absl::flat_hash_map<ClientID, absl::flat_hash_map<WorkerID, ActorID>>
      &GetCreatedActors() const;

  const absl::flat_hash_map<ActorID, std::shared_ptr<GcsActor>> &GetRegisteredActors()
      const;

  const absl::flat_hash_map<ActorID, std::vector<RegisterActorCallback>>
      &GetActorRegisterCallbacks() const;

 private:
  /// A data structure representing an actor's owner.
  struct Owner {
    Owner(std::shared_ptr<rpc::CoreWorkerClientInterface> client)
        : client(std::move(client)) {}
    /// A client that can be used to contact the owner.
    std::shared_ptr<rpc::CoreWorkerClientInterface> client;
    /// The IDs of actors owned by this worker.
    absl::flat_hash_set<ActorID> children_actor_ids;
  };

  /// Poll an actor's owner so that we will receive a notification when the
  /// actor has gone out of scope, or the owner has died. This should not be
  /// called for detached actors.
  void PollOwnerForActorOutOfScope(const std::shared_ptr<GcsActor> &actor);

  /// Destroy an actor that has gone out of scope. This cleans up all local
  /// state associated with the actor and marks the actor as dead. For owned
  /// actors, this should be called when all actor handles have gone out of
  /// scope or the owner has died.
  /// TODO: For detached actors, this should be called when the application
  /// deregisters the actor.
  void DestroyActor(const ActorID &actor_id);

  /// Get unresolved actors that were submitted from the specified node.
  absl::flat_hash_set<ActorID> GetUnresolvedActorsByOwnerNode(
      const ClientID &node_id) const;

  /// Get unresolved actors that were submitted from the specified worker.
  absl::flat_hash_set<ActorID> GetUnresolvedActorsByOwnerWorker(
      const ClientID &node_id, const WorkerID &worker_id) const;

 private:
  /// Reconstruct the specified actor.
  ///
  /// \param actor The target actor to be reconstructed.
  /// \param need_reschedule Whether to reschedule the actor creation task, sometimes
  /// users want to kill an actor intentionally and don't want it to be reconstructed
  /// again.
  void ReconstructActor(const ActorID &actor_id, bool need_reschedule = true);

  /// Callbacks of pending `RegisterActor` requests.
  /// Maps actor ID to actor registration callbacks, which is used to filter duplicated
  /// messages from a driver/worker caused by some network problems.
  absl::flat_hash_map<ActorID, std::vector<RegisterActorCallback>>
      actor_to_register_callbacks_;
  /// Callbacks of actor creation requests.
  /// Maps actor ID to actor creation callbacks, which is used to filter duplicated
  /// messages come from a Driver/Worker caused by some network problems.
  absl::flat_hash_map<ActorID, std::vector<CreateActorCallback>>
      actor_to_create_callbacks_;
  /// All registered actors (unresoved and pending actors are also included).
  /// TODO(swang): Use unique_ptr instead of shared_ptr.
  absl::flat_hash_map<ActorID, std::shared_ptr<GcsActor>> registered_actors_;
  /// Maps actor names to their actor ID for lookups by name.
  absl::flat_hash_map<std::string, ActorID> named_actors_;
  /// The actors which dependencies have not been resolved.
  /// Maps from worker ID to a client and the IDs of the actors owned by that worker.
  /// The actor whose dependencies are not resolved should be destroyed once it creator
  /// dies.
  absl::flat_hash_map<ClientID,
                      absl::flat_hash_map<WorkerID, absl::flat_hash_set<ActorID>>>
      unresolved_actors_;
  /// The pending actors which will not be scheduled until there's a resource change.
  std::vector<std::shared_ptr<GcsActor>> pending_actors_;
  /// Map contains the relationship of node and created actors. Each node ID
  /// maps to a map from worker ID to the actor created on that worker.
  absl::flat_hash_map<ClientID, absl::flat_hash_map<WorkerID, ActorID>> created_actors_;
  /// Map from worker ID to a client and the IDs of the actors owned by that
  /// worker. An owned actor should be destroyed once it has gone out of scope,
  /// according to its owner, or the owner dies.
  absl::flat_hash_map<ClientID, absl::flat_hash_map<WorkerID, Owner>> owners_;

  /// The scheduler to schedule all registered actors.
  std::shared_ptr<gcs::GcsActorSchedulerInterface> gcs_actor_scheduler_;
  /// Used to update actor information upon creation, deletion, etc.
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  /// A publisher for publishing gcs messages.
  std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub_;
  /// Factory to produce clients to workers. This is used to communicate with
  /// actors and their owners.
  rpc::ClientFactoryFn worker_client_factory_;
};

}  // namespace gcs
}  // namespace ray
