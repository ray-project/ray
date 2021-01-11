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

#include <queue>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/id.h"
#include "ray/common/task/task_execution_spec.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs/accessor.h"
#include "ray/gcs/gcs_server/gcs_actor_schedule_strategy.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/raylet_client/raylet_client.h"
#include "ray/rpc/node_manager/node_manager_client.h"
#include "ray/rpc/node_manager/node_manager_client_pool.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "ray/rpc/worker/core_worker_client_pool.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

class GcsActor;

class GcsActorSchedulerInterface {
 public:
  /// Schedule the specified actor.
  ///
  /// \param actor to be scheduled.
  virtual void Schedule(std::shared_ptr<GcsActor> actor) = 0;

  /// Reschedule the specified actor after gcs server restarts.
  ///
  /// \param actor to be scheduled.
  virtual void Reschedule(std::shared_ptr<GcsActor> actor) = 0;

  /// Cancel all actors that are being scheduled to the specified node.
  ///
  /// \param node_id ID of the node where the worker is located.
  /// \return ID list of actors associated with the specified node id.
  virtual std::vector<ActorID> CancelOnNode(const NodeID &node_id) = 0;

  /// Cancel a outstanding leasing request to raylets.
  ///
  /// \param node_id ID of the node where the actor leasing request has been sent.
  /// \param actor_id ID of an actor.
  virtual void CancelOnLeasing(const NodeID &node_id, const ActorID &actor_id) = 0;

  /// Cancel the actor that is being scheduled to the specified worker.
  ///
  /// \param node_id ID of the node where the worker is located.
  /// \param worker_id ID of the worker that the actor is creating on.
  /// \return ID of actor associated with the specified node id and worker id.
  virtual ActorID CancelOnWorker(const NodeID &node_id, const WorkerID &worker_id) = 0;

  /// Notify raylets to release unused workers.
  ///
  /// \param node_to_workers Workers used by each node.
  virtual void ReleaseUnusedWorkers(
      const std::unordered_map<NodeID, std::vector<WorkerID>> &node_to_workers) = 0;

  virtual ~GcsActorSchedulerInterface() {}
};

/// GcsActorScheduler is responsible for scheduling actors registered to GcsActorManager.
/// This class is not thread-safe.
class GcsActorScheduler : public GcsActorSchedulerInterface {
 public:
  /// Create a GcsActorScheduler
  ///
  /// \param io_context The main event loop.
  /// \param gcs_actor_table Used to flush actor info to storage.
  /// \param gcs_node_manager The node manager which is used when scheduling.
  /// \param schedule_failure_handler Invoked when there are no available nodes to
  /// schedule actors.
  /// \param schedule_success_handler Invoked when actors are created on the worker
  /// successfully.
  /// \param raylet_client_pool Raylet client pool to construct connections to raylets.
  /// \param actor_schedule_strategy Actor schedule strategy.
  /// \param client_factory Factory to create remote core worker client, default factor
  /// will be used if not set.
  explicit GcsActorScheduler(
      boost::asio::io_context &io_context, gcs::GcsActorTable &gcs_actor_table,
      const GcsNodeManager &gcs_node_manager, std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
      std::function<void(std::shared_ptr<GcsActor>)> schedule_failure_handler,
      std::function<void(std::shared_ptr<GcsActor>)> schedule_success_handler,
      std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
      std::shared_ptr<GcsActorScheduleStrategyInterface> actor_schedule_strategy,
      rpc::ClientFactoryFn client_factory = nullptr);
  virtual ~GcsActorScheduler() = default;

  /// Schedule the specified actor.
  /// If there is no available nodes then the `schedule_failed_handler_` will be
  /// triggered, otherwise the actor will be scheduled until succeed or canceled.
  ///
  /// \param actor to be scheduled.
  void Schedule(std::shared_ptr<GcsActor> actor) override;

  /// Reschedule the specified actor after gcs server restarts.
  ///
  /// \param actor to be scheduled.
  void Reschedule(std::shared_ptr<GcsActor> actor) override;

  /// Cancel all actors that are being scheduled to the specified node.
  ///
  /// \param node_id ID of the node where the worker is located.
  /// \return ID list of actors associated with the specified node id.
  std::vector<ActorID> CancelOnNode(const NodeID &node_id) override;

  /// Cancel a outstanding leasing request to raylets.
  ///
  /// NOTE: The current implementation does not actually send lease cancel request to
  /// raylet. This method must be only used to ignore incoming raylet lease request
  /// responses.
  ///
  /// \param node_id ID of the node where the actor leasing request has been sent.
  /// \param actor_id ID of an actor.
  void CancelOnLeasing(const NodeID &node_id, const ActorID &actor_id) override;

  /// Cancel the actor that is being scheduled to the specified worker.
  ///
  /// \param node_id ID of the node where the worker is located.
  /// \param worker_id ID of the worker that the actor is creating on.
  /// \return ID of actor associated with the specified node id and worker id.
  ActorID CancelOnWorker(const NodeID &node_id, const WorkerID &worker_id) override;

  /// Notify raylets to release unused workers.
  ///
  /// \param node_to_workers Workers used by each node.
  void ReleaseUnusedWorkers(
      const std::unordered_map<NodeID, std::vector<WorkerID>> &node_to_workers) override;

 protected:
  /// The GcsLeasedWorker is kind of abstraction of remote leased worker inside raylet. It
  /// contains the address of remote leased worker as well as the leased resources and the
  /// ID of the actor associated with this worker. Through this class, we can easily get
  /// the WorkerID, Endpoint, NodeID and the associated ActorID of the remote worker.
  class GcsLeasedWorker {
   public:
    /// Create a GcsLeasedWorker
    ///
    /// \param address the Address of the remote leased worker.
    /// \param resources the resources that leased from the remote node(raylet).
    /// \param actor_id ID of the actor associated with this leased worker.
    explicit GcsLeasedWorker(rpc::Address address,
                             std::vector<rpc::ResourceMapEntry> resources,
                             const ActorID &actor_id)
        : address_(std::move(address)),
          resources_(std::move(resources)),
          assigned_actor_id_(actor_id) {}
    virtual ~GcsLeasedWorker() = default;

    /// Get the Address of this leased worker.
    const rpc::Address &GetAddress() const { return address_; }

    /// Get the ip address of this leased worker.
    const std::string &GetIpAddress() const { return address_.ip_address(); }

    /// Get the listening port of the leased worker at remote side.
    uint16_t GetPort() const { return address_.port(); }

    /// Get the WorkerID of this leased worker.
    WorkerID GetWorkerID() const { return WorkerID::FromBinary(address_.worker_id()); }

    /// Get the NodeID of this leased worker.
    NodeID GetNodeID() const { return NodeID::FromBinary(address_.raylet_id()); }

    /// Get the id of the actor which is assigned to this leased worker.
    ActorID GetAssignedActorID() const { return assigned_actor_id_; }

    /// Get the leased resources.
    const std::vector<rpc::ResourceMapEntry> &GetLeasedResources() const {
      return resources_;
    }

   protected:
    /// The address of the remote leased worker.
    rpc::Address address_;
    /// The resources leased from remote node.
    std::vector<rpc::ResourceMapEntry> resources_;
    /// Id of the actor assigned to this worker.
    ActorID assigned_actor_id_;
  };

  /// Lease a worker from the specified node for the specified actor.
  ///
  /// \param actor A description of the actor to create. This object has the resource
  /// specification needed to lease workers from the specified node.
  /// \param node The node that the worker will be leased from.
  void LeaseWorkerFromNode(std::shared_ptr<GcsActor> actor,
                           std::shared_ptr<rpc::GcsNodeInfo> node);

  /// Retry leasing a worker from the specified node for the specified actor.
  /// Make it a virtual method so that the io_context_ could be mocked out.
  ///
  /// \param actor A description of the actor to create. This object has the resource
  /// specification needed to lease workers from the specified node.
  /// \param node The node that the worker will be leased from.
  virtual void RetryLeasingWorkerFromNode(std::shared_ptr<GcsActor> actor,
                                          std::shared_ptr<rpc::GcsNodeInfo> node);

  /// This method is only invoked inside `RetryLeasingWorkerFromNode`, the purpose of this
  /// is to make it easy to write unit tests.
  ///
  /// \param actor A description of the actor to create. This object has the resource
  /// specification needed to lease workers from the specified node.
  /// \param node The node that the worker will be leased from.
  void DoRetryLeasingWorkerFromNode(std::shared_ptr<GcsActor> actor,
                                    std::shared_ptr<rpc::GcsNodeInfo> node);

  /// Handler to process a granted lease.
  ///
  /// \param actor Contains the resources needed to lease workers from the specified node.
  /// \param reply The reply of `RequestWorkerLeaseRequest`.
  void HandleWorkerLeasedReply(std::shared_ptr<GcsActor> actor,
                               const rpc::RequestWorkerLeaseReply &reply);

  /// Create the specified actor on the specified worker.
  ///
  /// \param actor The actor to be created.
  /// \param worker The worker that the actor will created on.
  void CreateActorOnWorker(std::shared_ptr<GcsActor> actor,
                           std::shared_ptr<GcsLeasedWorker> worker);

  /// Retry creating the specified actor on the specified worker asynchoronously.
  /// Make it a virtual method so that the io_context_ could be mocked out.
  ///
  /// \param actor The actor to be created.
  /// \param worker The worker that the actor will created on.
  virtual void RetryCreatingActorOnWorker(std::shared_ptr<GcsActor> actor,
                                          std::shared_ptr<GcsLeasedWorker> worker);

  /// This method is only invoked inside `RetryCreatingActorOnWorker`, the purpose of this
  /// is to make it easy to write unit tests.
  ///
  /// \param actor The actor to be created.
  /// \param worker The worker that the actor will created on.
  void DoRetryCreatingActorOnWorker(std::shared_ptr<GcsActor> actor,
                                    std::shared_ptr<GcsLeasedWorker> worker);

  /// Select a node from alive nodes randomly.
  std::shared_ptr<rpc::GcsNodeInfo> SelectNodeRandomly() const;

  /// Get an existing lease client or connect a new one.
  std::shared_ptr<WorkerLeaseInterface> GetOrConnectLeaseClient(
      const rpc::Address &raylet_address);

 protected:
  /// The io loop that is used to delay execution of tasks (e.g.,
  /// execute_after).
  boost::asio::io_context &io_context_;
  /// The actor info accessor.
  gcs::GcsActorTable &gcs_actor_table_;
  /// Map from node ID to the set of actors for whom we are trying to acquire a lease from
  /// that node. This is needed so that we can retry lease requests from the node until we
  /// receive a reply or the node is removed.
  absl::flat_hash_map<NodeID, absl::flat_hash_set<ActorID>> node_to_actors_when_leasing_;
  /// Map from node ID to the workers on which we are trying to create actors. This is
  /// needed so that we can cancel actor creation requests if the worker is removed.
  absl::flat_hash_map<NodeID,
                      absl::flat_hash_map<WorkerID, std::shared_ptr<GcsLeasedWorker>>>
      node_to_workers_when_creating_;
  /// Reference of GcsNodeManager.
  const GcsNodeManager &gcs_node_manager_;
  /// A publisher for publishing gcs messages.
  std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub_;
  /// The handler to handle the scheduling failures.
  std::function<void(std::shared_ptr<GcsActor>)> schedule_failure_handler_;
  /// The handler to handle the successful scheduling.
  std::function<void(std::shared_ptr<GcsActor>)> schedule_success_handler_;
  /// Whether or not to report the backlog of actors waiting to be scheduled.
  bool report_worker_backlog_;
  /// The nodes which are releasing unused workers.
  absl::flat_hash_set<NodeID> nodes_of_releasing_unused_workers_;
  /// The cached raylet clients used to communicate with raylet.
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;
  /// The actor schedule strategy.
  std::shared_ptr<GcsActorScheduleStrategyInterface> actor_schedule_strategy_;
  /// The cached core worker clients which are used to communicate with leased worker.
  rpc::CoreWorkerClientPool core_worker_clients_;
};

}  // namespace gcs
}  // namespace ray
