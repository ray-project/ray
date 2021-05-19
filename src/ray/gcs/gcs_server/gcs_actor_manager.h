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
#include "ray/common/runtime_env_manager.h"
#include "ray/common/task/task_execution_spec.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs/gcs_server/gcs_actor_scheduler.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
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
  explicit GcsActor(const ray::rpc::TaskSpec &task_spec, std::string ray_namespace) {
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

    actor_table_data_.mutable_address()->set_raylet_id(NodeID::Nil().Binary());
    actor_table_data_.mutable_address()->set_worker_id(WorkerID::Nil().Binary());

    actor_table_data_.set_ray_namespace(ray_namespace);
  }

  /// Get the node id on which this actor is created.
  NodeID GetNodeID() const;
  /// Get the id of the worker on which this actor is created.
  WorkerID GetWorkerID() const;
  /// Get the actor's owner ID.
  WorkerID GetOwnerID() const;
  /// Get the node ID of the actor's owner.
  NodeID GetOwnerNodeID() const;
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
  /// Get the name of this actor.
  std::string GetName() const;
  /// Get the namespace of this actor.
  std::string GetRayNamespace() const;
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
/// Actor State Transition Diagram:
///                                                        3
///  0                       1                   2        --->
/// --->DEPENDENCIES_UNREADY--->PENDING_CREATION--->ALIVE      RESTARTING
///             |                      |              |   <---      |
///           8 |                    7 |            6 |     4       | 5
///             |                      v              |             |
///              ------------------> DEAD <-------------------------
///
/// 0: When GCS receives a `RegisterActor` request from core worker, it will add an actor
/// to `registered_actors_` and `unresolved_actors_`.
/// 1: When GCS receives a `CreateActor` request from core worker, it will remove the
/// actor from `unresolved_actors_` and schedule the actor.
/// 2: GCS selects a node to lease worker. If the worker is successfully leased,
/// GCS will push actor creation task to the core worker, else GCS will select another
/// node to lease worker. If the actor is created successfully, GCS will add the actor to
/// `created_actors_`.
/// 3: When GCS detects that the worker/node of an actor is dead, it
/// will get actor from `registered_actors_` by actor id. If the actor's remaining
/// restarts number is greater than 0, it will reconstruct the actor.
/// 4: When the actor is successfully reconstructed, GCS will update its state to `ALIVE`.
/// 5: If the actor is restarting, GCS detects that its worker or node is dead and its
/// remaining restarts number is 0, it will update its state to `DEAD`. If the actor is
/// detached, GCS will remove it from `registered_actors_` and `created_actors_`. If the
/// actor is non-detached, when GCS detects that its owner is dead, GCS will remove it
/// from `registered_actors_`.
/// 6: When GCS detected that an actor is dead, GCS will
/// reconstruct it. If its remaining restarts number is 0, it will update its state to
/// `DEAD`. If the actor is detached, GCS will remove it from `registered_actors_` and
/// `created_actors_`. If the actor is non-detached, when GCS detects that its owner is
/// dead, it will destroy the actor and remove it from `registered_actors_` and
/// `created_actors_`.
/// 7: If the actor is non-detached, when GCS detects that its owner is
/// dead, it will destroy the actor and remove it from `registered_actors_` and
/// `created_actors_`.
/// 8: For both detached and non-detached actors, when GCS detects that
/// an actor's creator is dead, it will update its state to `DEAD` and remove it from
/// `registered_actors_` and `created_actors_`. Because in this case, the actor can never
/// be created. If the actor is non-detached, when GCS detects that its owner is dead, it
/// will update its state to `DEAD` and remove it from `registered_actors_` and
/// `created_actors_`.
class GcsActorManager : public rpc::ActorInfoHandler {
 public:
  /// Create a GcsActorManager
  ///
  /// \param scheduler Used to schedule actor creation tasks.
  /// \param gcs_table_storage Used to flush actor data to storage.
  /// \param gcs_pub_sub Used to publish gcs message.
  GcsActorManager(
      std::shared_ptr<GcsActorSchedulerInterface> scheduler,
      std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
      std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub, RuntimeEnvManager &runtime_env_manager,
      std::function<void(const ActorID &)> destroy_ownded_placement_group_if_needed,
      std::function<std::string(const JobID &)> get_ray_namespace,
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

  void HandleKillActorViaGcs(const rpc::KillActorViaGcsRequest &request,
                             rpc::KillActorViaGcsReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  /// Register actor asynchronously.
  ///
  /// \param request Contains the meta info to create the actor.
  /// \param success_callback Will be invoked after the actor is created successfully or
  /// be invoked immediately if the actor is already registered to `registered_actors_`
  /// and its state is `ALIVE`.
  /// \return Status::Invalid if this is a named actor and an
  /// actor with the specified name already exists. The callback will not be called in
  /// this case.
  Status RegisterActor(const rpc::RegisterActorRequest &request,
                       RegisterActorCallback success_callback);

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
  ActorID GetActorIDByName(const std::string &name,
                           const std::string &ray_namespace) const;

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
  void OnNodeDead(const NodeID &node_id);

  /// Handle a worker failure. This will restart the associated actor, if any,
  /// which may be pending or already created. If the worker owned other
  /// actors, those actors will be destroyed.
  ///
  /// \param node_id ID of the node where the dead worker was located.
  /// \param worker_id ID of the dead worker.
  /// \param exit_type exit reason of the dead worker.
  /// \param creation_task_exception if this arg is set, this worker is died because of an
  /// exception thrown in actor's creation task.
  void OnWorkerDead(
      const NodeID &node_id, const WorkerID &worker_id,
      const rpc::WorkerExitType disconnect_type,
      const std::shared_ptr<rpc::RayException> &creation_task_exception = nullptr);

  void OnWorkerDead(const NodeID &node_id, const WorkerID &worker_id);

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

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

  /// Delete non-detached actor information from durable storage once the associated job
  /// finishes.
  ///
  /// \param job_id The id of finished job.
  void OnJobFinished(const JobID &job_id);

  /// Get the created actors.
  ///
  /// \return The created actors.
  const absl::flat_hash_map<NodeID, absl::flat_hash_map<WorkerID, ActorID>>
      &GetCreatedActors() const;

  const absl::flat_hash_map<ActorID, std::shared_ptr<GcsActor>> &GetRegisteredActors()
      const;

  const absl::flat_hash_map<ActorID, std::vector<RegisterActorCallback>>
      &GetActorRegisterCallbacks() const;

  /// Collect stats from gcs actor manager in-memory data structures.
  void CollectStats() const;

  std::string DebugString() const;

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
  /// NOTE: This method can be called multiple times in out-of-order and should be
  /// idempotent.
  void DestroyActor(const ActorID &actor_id);

  /// Get unresolved actors that were submitted from the specified node.
  absl::flat_hash_set<ActorID> GetUnresolvedActorsByOwnerNode(
      const NodeID &node_id) const;

  /// Get unresolved actors that were submitted from the specified worker.
  absl::flat_hash_set<ActorID> GetUnresolvedActorsByOwnerWorker(
      const NodeID &node_id, const WorkerID &worker_id) const;

  /// Reconstruct the specified actor.
  ///
  /// \param actor The target actor to be reconstructed.
  /// \param need_reschedule Whether to reschedule the actor creation task, sometimes
  /// users want to kill an actor intentionally and don't want it to be reconstructed
  /// again.
  /// \param creation_task_exception Only applies when need_reschedule=false, decribing
  /// why this actor failed. If this arg is set, it means this actor died because of an
  /// exception thrown in creation task.
  void ReconstructActor(
      const ActorID &actor_id, bool need_reschedule,
      const std::shared_ptr<rpc::RayException> &creation_task_exception = nullptr);

  /// Reconstruct the specified actor and reschedule it.
  void ReconstructActor(const ActorID &actor_id);

  /// Remove the specified actor from `unresolved_actors_`.
  ///
  /// \param actor The actor to be removed.
  void RemoveUnresolvedActor(const std::shared_ptr<GcsActor> &actor);

  /// Remove the specified actor from owner.
  ///
  /// \param actor The actor to be removed.
  void RemoveActorFromOwner(const std::shared_ptr<GcsActor> &actor);

  /// Kill the specified actor.
  ///
  /// \param actor_id ID of the actor to kill.
  /// \param force_kill Whether to force kill an actor by killing the worker.
  /// \param no_restart If set to true, the killed actor will not be restarted anymore.
  void KillActor(const ActorID &actor_id, bool force_kill, bool no_restart);

  /// Notify CoreWorker to kill the specified actor.
  ///
  /// \param actor The actor to be killed.
  /// \param force_kill Whether to force kill an actor by killing the worker.
  /// \param no_restart If set to true, the killed actor will not be restarted anymore.
  void NotifyCoreWorkerToKillActor(const std::shared_ptr<GcsActor> &actor,
                                   bool force_kill = true, bool no_restart = true);

  /// Add the destroyed actor to the cache. If the cache is full, one actor is randomly
  /// evicted.
  ///
  /// \param actor The actor to be killed.
  void AddDestroyedActorToCache(const std::shared_ptr<GcsActor> &actor);

  std::shared_ptr<rpc::ActorTableData> GenActorDataOnlyWithStates(
      const rpc::ActorTableData &actor) {
    auto actor_delta = std::make_shared<rpc::ActorTableData>();
    actor_delta->set_state(actor.state());
    actor_delta->set_allocated_creation_task_exception(
        new rpc::RayException(actor.creation_task_exception()));
    actor_delta->mutable_address()->CopyFrom(actor.address());
    actor_delta->set_num_restarts(actor.num_restarts());
    actor_delta->set_timestamp(actor.timestamp());
    actor_delta->set_pid(actor.pid());
    return actor_delta;
  }

  /// Cancel actor which is either being scheduled or is pending scheduling.
  ///
  /// \param actor The actor to be cancelled.
  /// \param task_id The id of actor creation task to be cancelled.
  void CancelActorInScheduling(const std::shared_ptr<GcsActor> &actor,
                               const TaskID &task_id);

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
  /// All destroyed actors.
  absl::flat_hash_map<ActorID, std::shared_ptr<GcsActor>> destroyed_actors_;
  /// The actors are sorted according to the timestamp, and the oldest is at the head of
  /// the list.
  std::list<std::pair<ActorID, int64_t>> sorted_destroyed_actor_list_;
  /// Maps actor names to their actor ID for lookups by name, first keyed by their
  /// namespace.
  absl::flat_hash_map<std::string, absl::flat_hash_map<std::string, ActorID>>
      named_actors_;
  /// The actors which dependencies have not been resolved.
  /// Maps from worker ID to a client and the IDs of the actors owned by that worker.
  /// The actor whose dependencies are not resolved should be destroyed once it creator
  /// dies.
  absl::flat_hash_map<NodeID, absl::flat_hash_map<WorkerID, absl::flat_hash_set<ActorID>>>
      unresolved_actors_;
  /// The pending actors which will not be scheduled until there's a resource change.
  std::vector<std::shared_ptr<GcsActor>> pending_actors_;
  /// Map contains the relationship of node and created actors. Each node ID
  /// maps to a map from worker ID to the actor created on that worker.
  absl::flat_hash_map<NodeID, absl::flat_hash_map<WorkerID, ActorID>> created_actors_;
  /// Map from worker ID to a client and the IDs of the actors owned by that
  /// worker. An owned actor should be destroyed once it has gone out of scope,
  /// according to its owner, or the owner dies.
  absl::flat_hash_map<NodeID, absl::flat_hash_map<WorkerID, Owner>> owners_;

  /// The scheduler to schedule all registered actors.
  std::shared_ptr<gcs::GcsActorSchedulerInterface> gcs_actor_scheduler_;
  /// Used to update actor information upon creation, deletion, etc.
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  /// A publisher for publishing gcs messages.
  std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub_;
  /// Factory to produce clients to workers. This is used to communicate with
  /// actors and their owners.
  rpc::ClientFactoryFn worker_client_factory_;
  /// A callback that is used to destroy placemenet group owned by the actor.
  /// This method MUST BE IDEMPOTENT because it can be called multiple times during
  /// actor destroy process.
  std::function<void(const ActorID &)> destroy_owned_placement_group_if_needed_;
  /// A callback to get the namespace an actor belongs to based on its job id. This is
  /// necessary for actor creation.
  std::function<std::string(const JobID &)> get_ray_namespace_;

  RuntimeEnvManager &runtime_env_manager_;
  // Debug info.
  enum CountType {
    REGISTER_ACTOR_REQUEST = 0,
    CREATE_ACTOR_REQUEST = 1,
    GET_ACTOR_INFO_REQUEST = 2,
    GET_NAMED_ACTOR_INFO_REQUEST = 3,
    GET_ALL_ACTOR_INFO_REQUEST = 4,
    KILL_ACTOR_REQUEST = 5,
    CountType_MAX = 6,
  };
  uint64_t counts_[CountType::CountType_MAX] = {0};
};

}  // namespace gcs
}  // namespace ray
