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

#include <queue>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/raylet/scheduling/cluster_task_manager.h"
#include "ray/raylet/scheduling/scheduling_ids.h"
#include "ray/raylet_client/raylet_client.h"
#include "ray/rpc/node_manager/node_manager_client.h"
#include "ray/rpc/node_manager/node_manager_client_pool.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "ray/rpc/worker/core_worker_client_pool.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
using raylet::ClusterTaskManager;
namespace gcs {

class GcsActor;

using GcsActorSchedulerFailureCallback =
    std::function<void(std::shared_ptr<GcsActor>,
                       rpc::RequestWorkerLeaseReply::SchedulingFailureType,
                       const std::string &)>;
using GcsActorSchedulerSuccessCallback =
    std::function<void(std::shared_ptr<GcsActor>, const rpc::PushTaskReply &reply)>;

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
  virtual void CancelOnLeasing(const NodeID &node_id,
                               const ActorID &actor_id,
                               const TaskID &task_id) = 0;

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
      const absl::flat_hash_map<NodeID, std::vector<WorkerID>> &node_to_workers) = 0;

  /// Handle the destruction of an actor.
  ///
  /// \param actor The actor to be destoryed.
  virtual void OnActorDestruction(std::shared_ptr<GcsActor> actor) = 0;

  /// Get the count of pending actors.
  ///
  /// \return The count of pending actors.
  virtual size_t GetPendingActorsCount() const = 0;

  /// Cancel an in-flight actor scheduling.
  ///
  /// \param The actor to be cancelled.
  /// \return Whether the actor is cancelled successfully.
  virtual bool CancelInFlightActorScheduling(const std::shared_ptr<GcsActor> &actor) = 0;

  virtual std::string DebugString() const = 0;

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
  /// \param cluster_task_manager The task manager that queues and schedules actor.
  /// creation tasks.
  /// \param schedule_failure_handler Invoked when there are no available
  /// nodes to schedule actors.
  /// \param schedule_success_handler Invoked when actors are
  /// created on the worker successfully.
  /// \param raylet_client_pool Raylet client pool to
  /// construct connections to raylets.
  /// \param client_factory Factory to create remote
  /// core worker client, default factor will be used if not set.
  explicit GcsActorScheduler(
      instrumented_io_context &io_context,
      GcsActorTable &gcs_actor_table,
      const GcsNodeManager &gcs_node_manager,
      std::shared_ptr<ClusterTaskManager> cluster_task_manager_,
      GcsActorSchedulerFailureCallback schedule_failure_handler,
      GcsActorSchedulerSuccessCallback schedule_success_handler,
      std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
      rpc::ClientFactoryFn client_factory = nullptr,
      std::function<void(const NodeID &, const rpc::ResourcesData &)>
          normal_task_resources_changed_callback = nullptr);
  virtual ~GcsActorScheduler() = default;

  /// Schedule the specified actor.
  /// If there is no available nodes then the actor would be queued in the
  /// `cluster_task_manager_`.
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
  void CancelOnLeasing(const NodeID &node_id,
                       const ActorID &actor_id,
                       const TaskID &task_id) override;

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
      const absl::flat_hash_map<NodeID, std::vector<WorkerID>> &node_to_workers) override;

  /// Handle the destruction of an actor.
  ///
  /// \param actor The actor to be destoryed.
  void OnActorDestruction(std::shared_ptr<GcsActor> actor) override;

  std::string DebugString() const override;

  /// Get the count of pending actors, which considers both infeasible and waiting queues.
  ///
  /// \return The count of pending actors.
  size_t GetPendingActorsCount() const override;

  /// Cancel an in-flight actor scheduling.
  ///
  /// \param The actor to be cancelled.
  /// \return Whether the actor is cancelled successfully.
  bool CancelInFlightActorScheduling(const std::shared_ptr<GcsActor> &actor) override;

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

  /// Handler to process a worker lease reply.
  ///
  /// \param actor The actor to be scheduled.
  /// \param node The selected node at which a worker is to be leased.
  /// \param status Status of the reply of `RequestWorkerLeaseRequest`.
  /// \param reply The reply of `RequestWorkerLeaseRequest`.
  void HandleWorkerLeaseReply(std::shared_ptr<GcsActor> actor,
                              std::shared_ptr<rpc::GcsNodeInfo> node,
                              const Status &status,
                              const rpc::RequestWorkerLeaseReply &reply);

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
  void HandleWorkerLeaseGrantedReply(std::shared_ptr<GcsActor> actor,
                                     const rpc::RequestWorkerLeaseReply &reply);

  /// A rejected rely means resources were preempted by normal tasks. Then
  /// update the the cluster resource view and reschedule immediately.
  void HandleWorkerLeaseRejectedReply(std::shared_ptr<GcsActor> actor,
                                      const rpc::RequestWorkerLeaseReply &reply);

  /// Handler to request worker lease canceled.
  ///
  /// \param actor Contains the resources needed to lease workers from the specified node.
  /// \param node_id The node where the runtime env is failed to setup.
  /// \param failure_type The type of the canceling.
  /// \param scheduling_failure_message The scheduling failure error message.
  void HandleRequestWorkerLeaseCanceled(
      std::shared_ptr<GcsActor> actor,
      const NodeID &node_id,
      rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
      const std::string &scheduling_failure_message);

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

  /// Get an existing lease client or connect a new one.
  std::shared_ptr<WorkerLeaseInterface> GetOrConnectLeaseClient(
      const rpc::Address &raylet_address);

  /// Kill the actor on a node
  bool KillActorOnWorker(const rpc::Address &worker_address, ActorID actor_id);

  /// Schedule the actor at GCS. The target Raylet is selected by hybrid_policy by
  /// default.
  ///
  /// \param actor The actor to be scheduled.
  void ScheduleByGcs(std::shared_ptr<GcsActor> actor);

  /// Forward the actor to a Raylet for scheduling. The target Raylet is the same node for
  /// the actor's owner, or selected randomly.
  ///
  /// \param actor The actor to be scheduled.
  void ScheduleByRaylet(std::shared_ptr<GcsActor> actor);

  /// Return the resources acquired by the actor, which updates GCS' resource view.
  ///
  /// \param acthr The actor whose resources are being returned.
  void ReturnActorAcquiredResources(std::shared_ptr<GcsActor> actor);

 protected:
  /// The io loop that is used to delay execution of tasks (e.g.,
  /// execute_after).
  instrumented_io_context &io_context_;
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
  /// The cluster task manager.
  std::shared_ptr<ClusterTaskManager> cluster_task_manager_;
  /// The handler to handle the scheduling failures.
  GcsActorSchedulerFailureCallback schedule_failure_handler_;
  /// The handler to handle the successful scheduling.
  GcsActorSchedulerSuccessCallback schedule_success_handler_;
  /// The nodes which are releasing unused workers.
  absl::flat_hash_set<NodeID> nodes_of_releasing_unused_workers_;
  /// The cached raylet clients used to communicate with raylet.
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;
  /// The cached core worker clients which are used to communicate with leased worker.
  rpc::CoreWorkerClientPool core_worker_clients_;

  /// The resource changed listeners.
  std::vector<std::function<void()>> resource_changed_listeners_;

  /// Normal task resources changed callback.
  std::function<void(const NodeID &, const rpc::ResourcesData &)>
      normal_task_resources_changed_callback_;

  /// Select a node where the actor is forwarded (for queueing and scheduling).
  ///
  /// \param actor The actor to be forwarded.
  /// \return The selected node's ID. If the selection fails, NodeID::Nil() is returned.
  NodeID SelectForwardingNode(std::shared_ptr<GcsActor> actor);

  /// A helper function to select a node from alive nodes randomly.
  ///
  /// \return The selected node. If the selection fails, `nullptr` is returned.
  std::shared_ptr<rpc::GcsNodeInfo> SelectNodeRandomly() const;

  friend class GcsActorSchedulerTest;
  FRIEND_TEST(GcsActorSchedulerTest, TestScheduleFailedWithZeroNode);
  FRIEND_TEST(GcsActorSchedulerTest, TestScheduleActorSuccess);
  FRIEND_TEST(GcsActorSchedulerTest, TestScheduleRetryWhenLeasing);
  FRIEND_TEST(GcsActorSchedulerTest, TestScheduleRetryWhenCreating);
  FRIEND_TEST(GcsActorSchedulerTest, TestNodeFailedWhenLeasing);
  FRIEND_TEST(GcsActorSchedulerTest, TestLeasingCancelledWhenLeasing);
  FRIEND_TEST(GcsActorSchedulerTest, TestNodeFailedWhenCreating);
  FRIEND_TEST(GcsActorSchedulerTest, TestWorkerFailedWhenCreating);
  FRIEND_TEST(GcsActorSchedulerTest, TestSpillback);
  FRIEND_TEST(GcsActorSchedulerTest, TestReschedule);
  FRIEND_TEST(GcsActorSchedulerTest, TestReleaseUnusedWorkers);
  FRIEND_TEST(GcsActorSchedulerTest, TestScheduleFailedWithZeroNodeByGcs);
  FRIEND_TEST(GcsActorSchedulerTest, TestNotEnoughClusterResources);
  FRIEND_TEST(GcsActorSchedulerTest, TestScheduleAndDestroyOneActor);
  FRIEND_TEST(GcsActorSchedulerTest, TestBalancedSchedule);
  FRIEND_TEST(GcsActorSchedulerTest, TestRejectedRequestWorkerLeaseReply);
  FRIEND_TEST(GcsActorSchedulerTest, TestScheduleRetryWhenLeasingByGcs);
  FRIEND_TEST(GcsActorSchedulerTest, TestScheduleRetryWhenCreatingByGcs);
  FRIEND_TEST(GcsActorSchedulerTest, TestNodeFailedWhenLeasingByGcs);
  FRIEND_TEST(GcsActorSchedulerTest, TestLeasingCancelledWhenLeasingByGcs);
  FRIEND_TEST(GcsActorSchedulerTest, TestNodeFailedWhenCreatingByGcs);
  FRIEND_TEST(GcsActorSchedulerTest, TestWorkerFailedWhenCreatingByGcs);
  FRIEND_TEST(GcsActorSchedulerTest, TestRescheduleByGcs);
  FRIEND_TEST(GcsActorSchedulerTest, TestReleaseUnusedWorkersByGcs);

  friend class GcsActorSchedulerMockTest;
  FRIEND_TEST(GcsActorSchedulerMockTest, KillWorkerLeak1);
  FRIEND_TEST(GcsActorSchedulerMockTest, KillWorkerLeak2);
};

}  // namespace gcs
}  // namespace ray
