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

#ifndef RAY_GCS_ACTOR_SCHEDULER_H
#define RAY_GCS_ACTOR_SCHEDULER_H

#include <ray/common/id.h>
#include <ray/common/task/task_execution_spec.h>
#include <ray/common/task/task_spec.h>
#include <ray/gcs/redis_gcs_client.h>
#include <ray/protobuf/gcs_service.pb.h>
#include <ray/rpc/node_manager/node_manager_client.h>
#include <ray/rpc/worker/core_worker_client.h>
#include <queue>

#include "gcs_node_manager.h"

namespace ray {
namespace gcs {

class GcsActor;
class GcsActorScheduler {
 protected:
  /// The GcsLeasedWorker is kind of abstraction of remote leased worker inside raylet. It
  /// contains the address of remote leased worker as well as the leased resources.
  /// It provides interface called `CreateActor` to push a normal task to the remote
  /// worker to create the real actor. Through this class, we can easily get the WorkerID,
  /// Endpoint, NodeID and the associated actor of the remote worker.
  class GcsLeasedWorker : public std::enable_shared_from_this<GcsLeasedWorker> {
   public:
    /// Create a GcsLeasedWorker
    ///
    /// \param address the Address of the remote leased worker.
    /// \param resources the resources that leased from the remote node(raylet).
    /// \param client_call_manager is shared by all `CoreWorkerClient`s.
    explicit GcsLeasedWorker(rpc::Address address,
                             std::vector<rpc::ResourceMapEntry> &&resources,
                             boost::asio::io_context &io_context,
                             rpc::ClientCallManager &client_call_manager)
        : address_(std::move(address)),
          resources_(std::move(resources)),
          io_context_(io_context),
          client_call_manager_(client_call_manager) {}
    virtual ~GcsLeasedWorker() = default;

    /// Create the specified actor on the leased worker asynchronously.
    ///
    /// \param actor_creation_task The task specification to describe how to create the
    /// actor.
    /// \param on_done Callback of the create request, which will be invoked only
    /// when success, otherwise it will never be triggered.
    virtual void CreateActor(const TaskSpecification &actor_creation_task,
                             const std::function<void()> &on_done);

    /// Get the Address of this leased worker.
    const rpc::Address &GetAddress() const { return address_; }

    /// Get the ip address of this leased worker.
    const std::string &GetIpAddress() const { return address_.ip_address(); }

    /// Get the listening port of the leased worker at remote side.
    uint16_t GetPort() const { return address_.port(); }

    /// Get the WorkerID of this leased worker.
    WorkerID GetWorkerID() const { return WorkerID::FromBinary(address_.worker_id()); }

    /// Get the NodeID of this leased worker.
    ClientID GetNodeID() const { return ClientID::FromBinary(address_.raylet_id()); }

    /// Get the id of the actor which is assigned to this leased worker.
    ActorID GetAssignedActorID() const { return assigned_actor_id_; }

   private:
    /// Get or create CoreWorkerClient to communicate with the remote leased worker.
    /// If the client_ is nullptr then create and cache it, otherwise return the client_.
    std::shared_ptr<rpc::CoreWorkerClient> GetOrCreateClient() {
      if (client_ == nullptr) {
        rpc::Address address;
        address.set_ip_address(GetIpAddress());
        address.set_port(GetPort());
        client_ = std::make_shared<rpc::CoreWorkerClient>(address, client_call_manager_);
      }
      return client_;
    }

    /// Reset the client so that we can create a new one when retry creating actors.
    void ResetClient() { client_.reset(); }

   protected:
    /// The address of the remote leased worker.
    rpc::Address address_;
    /// The resources leased from remote node.
    std::vector<rpc::ResourceMapEntry> resources_;
    /// The io loop which is used to construct `client_call_manager_` and delay execution
    /// of tasks(e.g. execute_after).
    boost::asio::io_context &io_context_;
    /// The `ClientCallManager` object that is shared by all `CoreWorkerClient`s.
    rpc::ClientCallManager &client_call_manager_;
    /// The client to communicate with the remote leased worker.
    std::shared_ptr<rpc::CoreWorkerClient> client_;
    /// Id of the actor which is assigned to this worker.
    ActorID assigned_actor_id_;
  };

 public:
  /// Create a GcsActorScheduler
  ///
  /// \param io_context The main event loop.
  /// \param redis_gcs_client Used to communicate with storage, it will be replaced with
  /// StorageClient later.
  /// \param gcs_node_manager The node manager which is used when scheduling.
  explicit GcsActorScheduler(boost::asio::io_context &io_context,
                             std::shared_ptr<gcs::RedisGcsClient> redis_gcs_client,
                             const GcsNodeManager &gcs_node_manager);
  virtual ~GcsActorScheduler() = default;

  /// Schedule the specified actor.
  /// If there is no available nodes then the `schedule_failed_handler_` will be
  /// triggered, otherwise the actor will be scheduled until succeed or canceled.
  ///
  /// \param actor to be scheduled.
  void Schedule(std::shared_ptr<GcsActor> actor);

  /// Set the failed handler of scheduling.
  ///
  /// \param handler Invoked when there are no available nodes to schedule the actor.
  void SetScheduleFailureHandler(
      std::function<void(std::shared_ptr<GcsActor>)> &&handler) {
    RAY_CHECK(handler);
    schedule_failed_handler_ = std::move(handler);
  }

  /// Set the success handler of scheduling.
  ///
  /// \param handler Invoked when the actor is created on the worker successfully.
  void SetScheduleSuccessHandler(
      std::function<void(std::shared_ptr<GcsActor>)> &&handler) {
    RAY_CHECK(handler);
    schedule_success_handler_ = std::move(handler);
  }

  /// Cancel the scheduling actors associated with the specified node id.
  ///
  /// \param node_id ID of the node where the worker is located.
  /// \return ID list of actors associated with the specified node id.
  std::vector<ActorID> CancelOnNode(const ClientID &node_id);

  /// Cancel the scheduling actor associated with the specified node and worker.
  ///
  /// \param node_id ID of the node where the worker is located.
  /// \param worker_id ID of the worker that the actor is creating on.
  /// \return ID of actor associated with the specified node id and worker id.
  ActorID CancelOnWorker(const ClientID &node_id, const WorkerID &worker_id);

 protected:
  /// Lease a worker from the specified node for the specified actor.
  ///
  /// \param actor This object has the resources needed to lease workers from the
  /// specified node.
  /// \param node The node that the worker will be leased from.
  virtual void LeaseWorkerFromNode(std::shared_ptr<GcsActor> actor,
                                   std::shared_ptr<rpc::GcsNodeInfo> node);
  /// Handler to process `WorkerLeasedReply`.
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

  /// Select a node from alive nodes randomly.
  std::shared_ptr<rpc::GcsNodeInfo> SelectNodeRandomly() const;

  /// Get or create a node client by the specified node info.
  ///
  /// \param node_id ID of the specified node.
  /// \param node The node info which contains the node address.
  /// \return New node client if there is no cached client related to the node id, else
  /// the cached one.
  std::shared_ptr<rpc::NodeManagerWorkerClient> GetOrCreateNodeClient(
      const ClientID &node_id, const rpc::GcsNodeInfo &node);

  /// Add actor to `node_to_leasing_actor_` map.
  /// When leasing worker for actor, it need to record the information of the leasing
  /// relationship which is used when cancel the actor scheduling.
  ///
  /// \param actor The scheduling actor that we are leasing worker from a node for it.
  void AddActorInPhaseOfLeasing(std::shared_ptr<GcsActor> actor);

  /// Remove actor from `node_to_leasing_actor_` map.
  /// The record will be removed from `node_to_leasing_actor_` when succeed in receiving
  /// the reply from a node for the scheduling actor.
  ///
  /// \param actor The scheduling actor.
  void RemoveActorInPhaseOfLeasing(std::shared_ptr<GcsActor> actor);

  /// Add the leased worker which is creating actor to `node_to_leasing_worker_`.
  ///
  /// \param leased_worker The leased worker for an actor.
  void AddWorkerInPhaseOfCreating(std::shared_ptr<GcsLeasedWorker> leased_worker);

  /// Remove the leased worker which is creating actor by the specified node id and worker
  /// id.
  ///
  /// \param node_id ID of the node where the worker is located.
  /// \param worker_id ID of the worker that the actor is creating on.
  /// \return The actor created on the worker.
  ActorID RemoveWorkerInPhaseOfCreating(const ClientID &node_id,
                                        const WorkerID &worker_id);

  /// The factory of GcsLeasedWorker.
  ///
  /// \param address the Address of the remote leased worker.
  /// \param resources the resources that leased from the remote node(raylet).
  /// \return The instance of GcsLeasedWorker.
  virtual std::shared_ptr<GcsLeasedWorker> CreateLeasedWorker(
      rpc::Address address, std::vector<rpc::ResourceMapEntry> resources);

 protected:
  /// The io loop which is used to construct `client_call_manager_` and delay execution of
  /// tasks(e.g. execute_after).
  boost::asio::io_context &io_context_;
  /// The `ClientCallManager` object that is shared by all `NodeManagerWorkerClient`s.
  rpc::ClientCallManager client_call_manager_;
  /// The client to communicate with redis to access actor table data.
  std::shared_ptr<gcs::RedisGcsClient> redis_gcs_client_;
  /// Map which contains the relationship of node and actor id set in phase of leasing
  /// worker.
  std::unordered_map<ClientID, std::unordered_set<ActorID>> node_to_actors_when_leasing_;
  /// Map which contains the relationship of node and workers in phase of creating actor.
  std::unordered_map<ClientID,
                     std::unordered_map<WorkerID, std::shared_ptr<GcsLeasedWorker>>>
      node_to_workers_when_creating_;
  /// The cached node clients which are used to communicate with raylet to lease workers.
  std::unordered_map<ClientID, std::shared_ptr<rpc::NodeManagerWorkerClient>>
      node_to_client_;
  /// The handler to handle the scheduling failures.
  std::function<void(std::shared_ptr<GcsActor>)> schedule_failed_handler_;
  /// The handler to handle the successful scheduling.
  std::function<void(std::shared_ptr<GcsActor>)> schedule_success_handler_;
  /// Reference of GcsNodeManager.
  const GcsNodeManager &gcs_node_manager_;
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_ACTOR_SCHEDULER_H
