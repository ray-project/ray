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

#include <gtest/gtest_prod.h>

#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/runtime_env_manager.h"
#include "ray/core_worker_rpc_client/core_worker_client_interface.h"
#include "ray/core_worker_rpc_client/core_worker_client_pool.h"
#include "ray/gcs/gcs_actor.h"
#include "ray/gcs/gcs_actor_scheduler.h"
#include "ray/gcs/gcs_function_manager.h"
#include "ray/gcs/gcs_init_data.h"
#include "ray/gcs/gcs_table_storage.h"
#include "ray/gcs/grpc_service_interfaces.h"
#include "ray/gcs/usage_stats_client.h"
#include "ray/observability/ray_event_recorder_interface.h"
#include "ray/pubsub/gcs_publisher.h"
#include "ray/util/counter_map.h"
#include "ray/util/thread_checker.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

/// GcsActorManager is responsible for managing the lifecycle of all actors.
/// This class is not thread-safe.
/// Actor State Transition Diagram:
///                                                        3
///  0                       1                   2        --->
/// --->DEPENDENCIES_UNREADY--->PENDING_CREATION--->ALIVE      RESTARTING
///             |                      |              |   <---      ^
///           8 |                    7 |            6 |     4       | 9
///             |                      v              |             |
///              ------------------> DEAD <-------------------------
///                                          5
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
/// 9: A dead actor caused by out-of-scope is lineage reconstructed.
class GcsActorManager : public rpc::ActorInfoGcsServiceHandler {
 public:
  /// Create a GcsActorManager
  ///
  /// \param scheduler Used to schedule actor creation tasks.
  /// \param gcs_table_storage Used to flush actor data to storage.
  /// \param gcs_publisher Used to publish gcs message.
  GcsActorManager(
      std::unique_ptr<GcsActorSchedulerInterface> scheduler,
      GcsTableStorage *gcs_table_storage,
      instrumented_io_context &io_context,
      pubsub::GcsPublisher *gcs_publisher,
      RuntimeEnvManager &runtime_env_manager,
      GCSFunctionManager &function_manager,
      std::function<void(const ActorID &)> destroy_owned_placement_group_if_needed,
      rpc::RayletClientPool &raylet_client_pool,
      rpc::CoreWorkerClientPool &worker_client_pool,
      observability::RayEventRecorderInterface &ray_event_recorder,
      const std::string &session_name,
      ray::observability::MetricInterface &actor_by_state_gauge,
      ray::observability::MetricInterface &gcs_actor_by_state_gauge);

  ~GcsActorManager() override = default;

  void HandleRegisterActor(rpc::RegisterActorRequest request,
                           rpc::RegisterActorReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void HandleRestartActorForLineageReconstruction(
      rpc::RestartActorForLineageReconstructionRequest request,
      rpc::RestartActorForLineageReconstructionReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleCreateActor(rpc::CreateActorRequest request,
                         rpc::CreateActorReply *reply,
                         rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetActorInfo(rpc::GetActorInfoRequest request,
                          rpc::GetActorInfoReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetNamedActorInfo(rpc::GetNamedActorInfoRequest request,
                               rpc::GetNamedActorInfoReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  void HandleListNamedActors(rpc::ListNamedActorsRequest request,
                             rpc::ListNamedActorsReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllActorInfo(rpc::GetAllActorInfoRequest request,
                             rpc::GetAllActorInfoReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  void HandleKillActorViaGcs(rpc::KillActorViaGcsRequest request,
                             rpc::KillActorViaGcsReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  void HandleReportActorOutOfScope(rpc::ReportActorOutOfScopeRequest request,
                                   rpc::ReportActorOutOfScopeReply *reply,
                                   rpc::SendReplyCallback send_reply_callback) override;

  /// Register actor asynchronously.
  ///
  /// \param request Contains the meta info to create the actor.
  /// \param success_callback Will be invoked after the actor is created successfully or
  /// be invoked immediately if the actor is already registered to `registered_actors_`
  /// and its state is `ALIVE`.
  /// \return Status::AlreadyExists if this is a named actor and an
  /// actor with the specified name already exists. The callback will not be called in
  /// this case.
  Status RegisterActor(const rpc::RegisterActorRequest &request,
                       std::function<void(Status)> success_callback);

  /// Set actors on the node as preempted and publish the actor information.
  /// If the node is already dead, this method is a no-op.
  void SetPreemptedAndPublish(const NodeID &node_id);

  /// Create actor asynchronously.
  ///
  /// \param request Contains the meta info to create the actor.
  /// \param callback Will be invoked after the actor is created successfully or if the
  /// actor creation is cancelled (e.g. due to the actor going out-of-scope or being
  /// killed before actor creation has been completed), or will be invoked immediately if
  /// the actor is already registered to `registered_actors_` and its state is `ALIVE`.
  /// \return Status::Invalid if this is a named actor and an actor with the specified
  /// name already exists. The callback will not be called in this case.
  Status CreateActor(const rpc::CreateActorRequest &request,
                     CreateActorCallback callback);

  /// Get the actor ID for the named actor. Returns nil if the actor was not found.
  /// \param name The name of the detached actor to look up.
  /// \returns ActorID The ID of the actor. Nil if the actor was not found.
  ActorID GetActorIDByName(const std::string &name,
                           const std::string &ray_namespace) const;

  /// Remove the actor name from the name registry if actor has the name.
  /// If the actor doesn't have the name, it is no-op.
  /// \param actor The actor to remove name from the entry.
  void RemoveActorNameFromRegistry(const std::shared_ptr<GcsActor> &actor);

  /// Get names of named actors.
  //
  /// \param[in] all_namespaces Whether to include actors from all Ray namespaces.
  /// \param[in] namespace The namespace to filter to if all_namespaces is false.
  /// \returns List of <namespace, name> pairs.
  std::vector<std::pair<std::string, std::string>> ListNamedActors(
      bool all_namespaces, const std::string &ray_namespace) const;

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
  /// \param node The specified node id.
  /// \param node_ip_address The ip address of the dead node.
  void OnNodeDead(std::shared_ptr<const rpc::GcsNodeInfo> node,
                  const std::string &node_ip_address);

  /// Handle a worker failure. This will restart the associated actor, if any,
  /// which may be pending or already created. If the worker owned other
  /// actors, those actors will be destroyed.
  ///
  /// \param node_id ID of the node where the dead worker was located.
  /// \param worker_id ID of the dead worker.
  /// \param exit_type exit reason of the dead worker.
  /// \param creation_task_exception if this arg is set, this worker is died because of an
  /// exception thrown in actor's creation task.
  void OnWorkerDead(const NodeID &node_id,
                    const WorkerID &worker_id,
                    const std::string &worker_ip,
                    const rpc::WorkerExitType disconnect_type,
                    const std::string &disconnect_detail,
                    const rpc::RayException *creation_task_exception = nullptr);

  /// Testing only.
  void OnWorkerDead(const NodeID &node_id, const WorkerID &worker_id);

  /// Handle actor creation task failure. This should be called
  /// - when scheduling an actor creation task is infeasible.
  /// - when actor cannot be created to the cluster (e.g., runtime environment ops
  /// failed).
  ///
  /// \param actor The actor whose creation task is infeasible.
  /// \param failure_type Scheduling failure type.
  /// \param scheduling_failure_message The scheduling failure error message.
  void OnActorSchedulingFailed(
      std::shared_ptr<GcsActor> actor,
      const rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
      const std::string &scheduling_failure_message);

  /// Handle actor creation task success. This should be called when the actor
  /// creation task has been scheduled successfully.
  ///
  /// \param actor The actor that has been created.
  /// \param reply The reply from the PushTask request from creation task executed on a
  /// remote worker.
  void OnActorCreationSuccess(const std::shared_ptr<GcsActor> &actor,
                              const rpc::PushTaskReply &reply);

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

  /// Get the created actors.
  ///
  /// \return The created actors.
  const absl::flat_hash_map<NodeID, absl::flat_hash_map<WorkerID, ActorID>>
      &GetCreatedActors() const;

  const absl::flat_hash_map<ActorID, std::shared_ptr<GcsActor>> &GetRegisteredActors()
      const;

  std::string DebugString() const;

  /// Collect stats from gcs actor manager in-memory data structures.
  void RecordMetrics() const;

  // Visible for testing.
  int64_t CountFor(rpc::ActorTableData::ActorState state, const std::string &name) const {
    return actor_state_counter_->Get(std::make_pair(state, name));
  }

  void SetUsageStatsClient(UsageStatsClient *usage_stats_client) {
    usage_stats_client_ = usage_stats_client;
  }

 private:
  const ray::rpc::ActorDeathCause GenNodeDiedCause(
      const ray::gcs::GcsActor *actor, std::shared_ptr<const rpc::GcsNodeInfo> node);
  /// A data structure representing an actor's owner.
  struct Owner {
    explicit Owner(rpc::Address address) : address_(std::move(address)) {}
    /// The address of the owner.
    rpc::Address address_;
    /// The IDs of actors owned by this worker.
    absl::flat_hash_set<ActorID> children_actor_ids_;
  };

  /// Poll an actor's owner so that we will receive a notification when the
  /// actor has no references, or the owner has died. This should not be
  /// called for detached actors.
  void PollOwnerForActorRefDeleted(const std::shared_ptr<GcsActor> &actor);

  /// Destroy an actor that has gone out of scope. This cleans up all local
  /// state associated with the actor and marks the actor as dead. For owned
  /// actors, this should be called when all actor handles have gone out of
  /// scope or the owner has died.
  /// NOTE: This method can be called multiple times in out-of-order and should be
  /// idempotent.
  ///
  /// \param[in] actor_id The actor id to destroy.
  /// \param[in] death_cause The reason why actor is destroyed.
  /// \param[in] force_kill Whether destory the actor forcelly.
  /// \param[in] done_callback Called when destroy finishes.
  void DestroyActor(const ActorID &actor_id,
                    const rpc::ActorDeathCause &death_cause,
                    bool force_kill = true,
                    std::function<void()> done_callback = nullptr);

  /// Get unresolved actors that were submitted from the specified node.
  absl::flat_hash_map<WorkerID, absl::flat_hash_set<ActorID>>
  GetUnresolvedActorsByOwnerNode(const NodeID &node_id) const;

  /// Get unresolved actors that were submitted from the specified worker.
  absl::flat_hash_set<ActorID> GetUnresolvedActorsByOwnerWorker(
      const NodeID &node_id, const WorkerID &worker_id) const;

  /// Reconstruct the specified actor.
  ///
  /// \param actor The target actor to be reconstructed.
  /// \param need_reschedule Whether to reschedule the actor creation task, sometimes
  /// users want to kill an actor intentionally and don't want it to be reconstructed
  /// again.
  /// \param death_cause Context about why this actor is dead. Should only be set when
  /// need_reschedule=false.
  void RestartActor(const ActorID &actor_id,
                    bool need_reschedule,
                    const rpc::ActorDeathCause &death_cause,
                    std::function<void()> done_callback = nullptr);

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
  void KillActor(const ActorID &actor_id, bool force_kill);

  /// Notify Raylet to kill the specified actor.
  ///
  /// \param actor The actor to be killed.
  /// \param death_cause Context about why this actor is dead.
  /// \param force_kill Whether to force kill an actor by killing the worker.
  void NotifyRayletToKillActor(const std::shared_ptr<GcsActor> &actor,
                               const rpc::ActorDeathCause &death_cause,
                               bool force_kill = true);

  /// Add the destroyed actor to the cache. If the cache is full, one actor is randomly
  /// evicted.
  ///
  /// \param actor The actor to be killed.
  void AddDestroyedActorToCache(const std::shared_ptr<GcsActor> &actor);

  rpc::ActorTableData GenActorDataOnlyWithStates(const rpc::ActorTableData &actor) {
    rpc::ActorTableData actor_delta;
    actor_delta.set_state(actor.state());
    actor_delta.mutable_death_cause()->CopyFrom(actor.death_cause());
    actor_delta.mutable_address()->CopyFrom(actor.address());
    actor_delta.set_num_restarts(actor.num_restarts());
    actor_delta.set_num_restarts_due_to_node_preemption(
        actor.num_restarts_due_to_node_preemption());
    actor_delta.set_max_restarts(actor.max_restarts());
    actor_delta.set_timestamp(actor.timestamp());
    actor_delta.set_pid(actor.pid());
    actor_delta.set_start_time(actor.start_time());
    actor_delta.set_end_time(actor.end_time());
    actor_delta.set_repr_name(actor.repr_name());
    actor_delta.set_preempted(actor.preempted());
    // Acotr's namespace and name are used for removing cached name when it's dead.
    if (!actor.ray_namespace().empty()) {
      actor_delta.set_ray_namespace(actor.ray_namespace());
    }
    if (!actor.name().empty()) {
      actor_delta.set_name(actor.name());
    }
    return actor_delta;
  }

  /// Cancel actor which is either being scheduled or is pending scheduling.
  ///
  /// \param actor The actor to be cancelled.
  /// \param lease_id The lease id of actor creation task to be cancelled.
  void CancelActorInScheduling(const std::shared_ptr<GcsActor> &actor);

  /// Get the alive or dead actor of the actor id.
  /// NOTE: The return value is not meant to be passed to other scope.
  /// This return value should be used only for a short-time usage.
  ///
  /// \param actor_id The id of the actor.
  /// \return Actor instance. The nullptr if the actor doesn't exist.
  ///
  const GcsActor *GetActor(const ActorID &actor_id) const;

  /// Remove a pending actor.
  ///
  /// \param actor The actor to be removed.
  /// \return True if the actor was successfully found and removed. Otherwise, return
  /// false.
  bool RemovePendingActor(std::shared_ptr<GcsActor> actor);

  /// Get the total count of pending actors.
  /// \return The total count of pending actors in all pending queues.
  size_t GetPendingActorsCount() const;

  /// Invoke the actor creation callbacks on the actor, and remove the callbacks stored.
  ///
  /// \param actor Actor.
  /// \param creation_task_reply The reply from the worker that handles the push task
  /// request of the creation task.
  /// \param creation_task_status The status of the actor creation task.
  void RunAndClearActorCreationCallbacks(const std::shared_ptr<GcsActor> &actor,
                                         const rpc::PushTaskReply &creation_task_reply,
                                         const Status &creation_task_status);

  /// Callbacks of pending `RegisterActor` requests.
  /// Maps actor ID to actor registration callbacks, which is used to filter duplicated
  /// messages from a driver/worker caused by some network problems.
  absl::flat_hash_map<ActorID, std::vector<std::function<void(Status)>>>
      actor_to_register_callbacks_;
  /// Callbacks of pending `RestartActorForLineageReconstruction` requests.
  /// Maps actor ID to actor restart callbacks, which is used to filter duplicated
  /// messages from a driver/worker caused by some network problems.
  absl::flat_hash_map<ActorID, std::vector<RestartActorForLineageReconstructionCallback>>
      actor_to_restart_for_lineage_reconstruction_callbacks_;
  /// Callbacks of actor creation requests.
  /// Maps actor ID to actor creation callbacks, which is used to filter duplicated
  /// messages come from a Driver/Worker caused by some network problems.
  absl::flat_hash_map<ActorID, std::vector<CreateActorCallback>>
      actor_to_create_callbacks_;
  /// All registered actors (unresolved and pending actors are also included).
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
  std::unique_ptr<GcsActorSchedulerInterface> gcs_actor_scheduler_;
  /// Used to update actor information upon creation, deletion, etc.
  GcsTableStorage *gcs_table_storage_;
  instrumented_io_context &io_context_;
  /// A publisher for publishing gcs messages.
  pubsub::GcsPublisher *gcs_publisher_;
  /// This is used to communicate with raylets where actors are located.
  rpc::RayletClientPool &raylet_client_pool_;
  /// This is used to communicate with actors and their owners.
  rpc::CoreWorkerClientPool &worker_client_pool_;
  /// Event recorder for emitting actor events
  observability::RayEventRecorderInterface &ray_event_recorder_;
  std::string session_name_;
  /// A callback that is used to destroy placemenet group owned by the actor.
  /// This method MUST BE IDEMPOTENT because it can be called multiple times during
  /// actor destroy process.
  std::function<void(const ActorID &)> destroy_owned_placement_group_if_needed_;
  /// Runtime environment manager for GC purpose
  RuntimeEnvManager &runtime_env_manager_;
  /// Function manager for GC purpose
  GCSFunctionManager &function_manager_;

  UsageStatsClient *usage_stats_client_;
  /// Run a function on a delay. This is useful for guaranteeing data will be
  /// accessible for a minimum amount of time.
  std::function<void(std::function<void(void)>, boost::posix_time::milliseconds)>
      run_delayed_;
  const boost::posix_time::milliseconds actor_gc_delay_;
  /// Counter of actors broken down by (State, ClassName).
  std::shared_ptr<CounterMap<std::pair<rpc::ActorTableData::ActorState, std::string>>>
      actor_state_counter_;
  ray::observability::MetricInterface &actor_by_state_gauge_;
  ray::observability::MetricInterface &gcs_actor_by_state_gauge_;

  /// Total number of successfully created actors in the cluster lifetime.
  int64_t liftime_num_created_actors_ = 0;

  // Make sure our unprotected maps are accessed from the same thread.
  // Currently protects actor_to_register_callbacks_.
  ThreadChecker thread_checker_;

  // Debug info.
  enum CountType {
    REGISTER_ACTOR_REQUEST = 0,
    CREATE_ACTOR_REQUEST = 1,
    GET_ACTOR_INFO_REQUEST = 2,
    GET_NAMED_ACTOR_INFO_REQUEST = 3,
    GET_ALL_ACTOR_INFO_REQUEST = 4,
    KILL_ACTOR_REQUEST = 5,
    LIST_NAMED_ACTORS_REQUEST = 6,
    CountType_MAX = 7,
  };
  uint64_t counts_[CountType::CountType_MAX] = {0};

  FRIEND_TEST(GcsActorManagerTest, TestKillActorWhenActorIsCreating);
  friend class GcsActorManagerTest;
};

}  // namespace gcs
}  // namespace ray
