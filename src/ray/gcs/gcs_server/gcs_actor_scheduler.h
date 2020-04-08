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
#include <ray/gcs/accessor.h>
#include <ray/protobuf/gcs_service.pb.h>
#include <ray/raylet/raylet_client.h>
#include <ray/rpc/node_manager/node_manager_client.h>
#include <ray/rpc/worker/core_worker_client.h>
#include <queue>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "gcs_node_manager.h"

namespace ray {
namespace gcs {

using LeaseClientFactoryFn =
    std::function<std::shared_ptr<WorkerLeaseInterface>(const rpc::Address &address)>;

class GcsActor;
/// GcsActorScheduler is responsible for scheduling actors registered to GcsActorManager.
/// This class is not thread-safe.
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
    /// \param client_factory Factory to create CoreWorkerClient.
    explicit GcsLeasedWorker(rpc::Address address,
                             std::vector<rpc::ResourceMapEntry> resources,
                             boost::asio::io_context &io_context,
                             rpc::ClientFactoryFn client_factory)
        : address_(std::move(address)),
          resources_(std::move(resources)),
          io_context_(io_context),
          client_factory_(std::move(client_factory)) {}
    virtual ~GcsLeasedWorker() = default;

    /// Create the specified actor on the leased worker asynchronously.
    ///
    /// \param actor_creation_task The task specification to describe how to create the
    /// actor.
    /// \param on_done Callback of the create request, which will be invoked only
    /// when success, otherwise it will never be triggered.
    void CreateActor(const TaskSpecification &actor_creation_task,
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

    /// Get the id of the actor which is assigned to this leased worker and set the
    /// assigned_actor_id_ to nil.
    ActorID TakeAssignedActorID() {
      auto actor_id = assigned_actor_id_;
      assigned_actor_id_ = ActorID::Nil();
      return actor_id;
    }

   private:
    /// Get or create CoreWorkerClient to communicate with the remote leased worker.
    /// If the client_ is nullptr then create and cache it, otherwise return the client_.
    std::shared_ptr<rpc::CoreWorkerClientInterface> GetOrCreateClient() {
      if (client_ == nullptr) {
        rpc::Address address;
        address.set_ip_address(GetIpAddress());
        address.set_port(GetPort());
        client_ = client_factory_(address);
      }
      return client_;
    }

   protected:
    /// The address of the remote leased worker.
    rpc::Address address_;
    /// The resources leased from remote node.
    std::vector<rpc::ResourceMapEntry> resources_;
    /// The io loop which is used to construct `client_call_manager_` and delay execution
    /// of tasks(e.g. execute_after).
    boost::asio::io_context &io_context_;
    /// The client to communicate with the remote leased worker.
    std::shared_ptr<rpc::CoreWorkerClientInterface> client_;
    /// Factory for producing new core worker clients.
    rpc::ClientFactoryFn client_factory_;
    /// Id of the actor which is assigned to this worker.
    ActorID assigned_actor_id_;
  };

 public:
  /// Create a GcsActorScheduler
  ///
  /// \param io_context The main event loop.
  /// \param actor_info_accessor Used to flush actor info to storage.
  /// \param gcs_node_manager The node manager which is used when scheduling.
  /// \param schedule_failure_handler Invoked when there are no available nodes to
  /// schedule actors.
  /// \param schedule_success_handler Invoked when actors are created on the worker
  /// successfully.
  /// \param lease_client_factory Factory to create remote lease client, default factor
  /// will be used if not set.
  /// \param client_factory Factory to create remote core worker client, default factor
  /// will be used if not set.
  explicit GcsActorScheduler(
      boost::asio::io_context &io_context, gcs::ActorInfoAccessor &actor_info_accessor,
      const GcsNodeManager &gcs_node_manager,
      std::function<void(std::shared_ptr<GcsActor>)> schedule_failure_handler,
      std::function<void(std::shared_ptr<GcsActor>)> schedule_success_handler,
      LeaseClientFactoryFn lease_client_factory = nullptr,
      rpc::ClientFactoryFn client_factory = nullptr);
  virtual ~GcsActorScheduler() = default;

  /// Schedule the specified actor.
  /// If there is no available nodes then the `schedule_failed_handler_` will be
  /// triggered, otherwise the actor will be scheduled until succeed or canceled.
  ///
  /// \param actor to be scheduled.
  void Schedule(std::shared_ptr<GcsActor> actor);

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

  /// Get leased workers in phase of creating.
  /// This method is used in unit test.
  const absl::flat_hash_map<
      ClientID, absl::flat_hash_map<WorkerID, std::shared_ptr<GcsLeasedWorker>>>
      &GetWorkersInPhaseOfCreating() {
    return node_to_workers_when_creating_;
  }

 protected:
  /// Lease a worker from the specified node for the specified actor.
  ///
  /// \param actor This object has the resources needed to lease workers from the
  /// specified node.
  /// \param node The node that the worker will be leased from.
  void LeaseWorkerFromNode(std::shared_ptr<GcsActor> actor,
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

  /// Get an existing lease client or connect a new one.
  std::shared_ptr<WorkerLeaseInterface> GetOrConnectLeaseClient(
      const rpc::Address &raylet_address);

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

 protected:
  /// The io loop which is used to construct `client_call_manager_` and delay execution of
  /// tasks(e.g. execute_after).
  boost::asio::io_context &io_context_;
  /// The `ClientCallManager` object that is shared by all `NodeManagerWorkerClient`s.
  rpc::ClientCallManager client_call_manager_;
  /// The actor info accessor.
  gcs::ActorInfoAccessor &actor_info_accessor_;
  /// Map which contains the relationship of node and actor id set in phase of leasing
  /// worker.
  absl::flat_hash_map<ClientID, absl::flat_hash_set<ActorID>>
      node_to_actors_when_leasing_;
  /// Map which contains the relationship of node and workers in phase of creating actor.
  absl::flat_hash_map<ClientID,
                      absl::flat_hash_map<WorkerID, std::shared_ptr<GcsLeasedWorker>>>
      node_to_workers_when_creating_;
  /// The cached node clients which are used to communicate with raylet to lease workers.
  absl::flat_hash_map<ClientID, std::shared_ptr<WorkerLeaseInterface>>
      remote_lease_clients_;
  /// Reference of GcsNodeManager.
  const GcsNodeManager &gcs_node_manager_;
  /// The handler to handle the scheduling failures.
  std::function<void(std::shared_ptr<GcsActor>)> schedule_failure_handler_;
  /// The handler to handle the successful scheduling.
  std::function<void(std::shared_ptr<GcsActor>)> schedule_success_handler_;
  /// Factory for producing new clients to request leases from remote nodes.
  LeaseClientFactoryFn lease_client_factory_;
  /// Factory for producing new core worker clients.
  rpc::ClientFactoryFn client_factory_;
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_ACTOR_SCHEDULER_H
