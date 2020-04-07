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
#include <queue>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "gcs_actor_scheduler.h"

namespace ray {
namespace gcs {

/// GcsActor just wraps `ActorTableData` and provides some convenient interfaces to access
/// the fields inside `ActorTableData`.
/// This class is not thread-safety, do not share between multiple threads.
class GcsActor {
 public:
  /// Create a GcsActor
  ///
  /// \param actor_table_data Data of the actor (see gcs.proto).
  explicit GcsActor(rpc::ActorTableData actor_table_data)
      : actor_table_data_(std::move(actor_table_data)) {}

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

/// GcsActorManager is responsible for managing the lifecycle of all actors.
/// This class is not thread-safety, do not share between multiple threads.
class GcsActorManager {
 public:
  /// This constructor allows us to pass in a mocked GcsActorScheduler, which is very
  /// helpful for writing unit tests.
  ///
  /// \param redis_gcs_client Used to communicate with storage, it will be replaced with
  /// StorageClient later.
  /// \param gcs_node_manager The actor manager needs to listen to the node change events
  /// inside gcs_node_manager.
  /// \param gcs_actor_scheduler The scheduler to schedule the actor creation task.
  explicit GcsActorManager(std::shared_ptr<gcs::RedisGcsClient> redis_gcs_client,
                           gcs::GcsNodeManager &gcs_node_manager,
                           std::unique_ptr<gcs::GcsActorScheduler> gcs_actor_scheduler);

  /// Create a GcsActorManager
  ///
  /// \param io_context The main event loop.
  /// \param redis_gcs_client Used to communicate with storage, it will be replaced with
  /// StorageClient later.
  /// \param gcs_node_manager The actor manager needs to listen to the node change events
  /// inside gcs_node_manager.
  explicit GcsActorManager(boost::asio::io_context &io_context,
                           std::shared_ptr<gcs::RedisGcsClient> gcs_client,
                           gcs::GcsNodeManager &gcs_node_manager);

  virtual ~GcsActorManager() = default;

  /// Register actor asynchronously.
  ///
  /// \param request Contains the meta info to create the actor.
  /// \param callback Will be invoked after the meta info is flushed to the storage or be
  /// invoked immediately if the meta info already exists.
  void RegisterActor(const rpc::CreateActorRequest &request,
                     std::function<void(std::shared_ptr<GcsActor>)> callback);

  /// Reconstruct all actors associated with the specified node id, including actors that
  /// are being scheduled or have been created on this node
  ///
  /// \param node_id The specified node id.
  void ReconstructActorsOnNode(const ClientID &node_id);

  /// Reconstruct actor associated with the specified node_id and worker_id.
  /// The actor may be scheduling or has been created.
  ///
  /// \param node_id ID of the node where the worker is located
  /// \param worker_id  ID of the worker that the actor is creating/created on
  /// \param need_reschedule Weather to reschedule the actor creation task, sometimes
  /// users want to kill an actor intentionally and don't want it to be rescheduled
  /// again.
  void ReconstructActorOnWorker(const ClientID &node_id, const WorkerID &worker_id,
                                bool need_reschedule = true);

  /// Get all registered actors.
  const absl::flat_hash_map<ActorID, std::shared_ptr<GcsActor>> &GetAllRegisteredActors()
      const {
    return registered_actors_;
  }

 protected:
  /// Schedule actors in the `pending_actors_` queue.
  /// This method is triggered when new nodes are registered or resources change.
  void SchedulePendingActors();

  /// Reconstruct the specified actor.
  ///
  /// \param actor The target actor to be reconstructed.
  /// \param need_reschedule Weather to reschedule the actor creation task, sometimes
  /// users want to kill an actor intentionally and don't want it to be reconstructed
  /// again.
  void ReconstructActor(std::shared_ptr<GcsActor> actor, bool need_reschedule = true);

 private:
  /// All registered actors (pending actors are also included).
  absl::flat_hash_map<ActorID, std::shared_ptr<GcsActor>> registered_actors_;
  /// The pending actors which will not be scheduled until there's a resource change.
  std::queue<std::shared_ptr<GcsActor>> pending_actors_;
  /// Map contains the relationship of worker and created actor.
  absl::flat_hash_map<WorkerID, std::shared_ptr<GcsActor>> worker_to_created_actor_;
  /// Map contains the relationship of node and created actors.
  absl::flat_hash_map<ClientID, absl::flat_hash_map<ActorID, std::shared_ptr<GcsActor>>>
      node_to_created_actors_;
  /// The client to communicate with redis to access actor table data.
  std::shared_ptr<gcs::RedisGcsClient> redis_gcs_client_;
  /// The scheduler to schedule all registered actors.
  std::unique_ptr<gcs::GcsActorScheduler> gcs_actor_scheduler_;
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_ACTOR_MANAGER_H
