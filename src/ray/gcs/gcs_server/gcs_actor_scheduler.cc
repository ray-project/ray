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

#include "gcs_actor_scheduler.h"
#include <ray/common/ray_config.h>
#include <ray/protobuf/node_manager.pb.h>
#include <ray/util/asio_util.h>
#include "gcs_actor_manager.h"

namespace ray {
namespace gcs {

GcsActorScheduler::GcsActorScheduler(
    boost::asio::io_context &io_context, gcs::ActorInfoAccessor &actor_info_accessor,
    const gcs::GcsNodeManager &gcs_node_manager,
    std::function<void(std::shared_ptr<GcsActor>)> schedule_failure_handler,
    std::function<void(std::shared_ptr<GcsActor>)> schedule_success_handler,
    LeaseClientFactoryFn lease_client_factory, rpc::ClientFactoryFn client_factory)
    : io_context_(io_context),
      client_call_manager_(io_context_),
      actor_info_accessor_(actor_info_accessor),
      gcs_node_manager_(gcs_node_manager),
      schedule_failure_handler_(std::move(schedule_failure_handler)),
      schedule_success_handler_(std::move(schedule_success_handler)),
      lease_client_factory_(std::move(lease_client_factory)),
      client_factory_(std::move(client_factory)) {
  RAY_CHECK(schedule_failure_handler_ != nullptr && schedule_success_handler_ != nullptr);
  if (lease_client_factory_ == nullptr) {
    lease_client_factory_ = [this](const rpc::Address &address) {
      auto node_manager_worker_client = rpc::NodeManagerWorkerClient::make(
          address.ip_address(), address.port(), client_call_manager_);
      return std::make_shared<raylet::RayletClient>(
          std::move(node_manager_worker_client));
    };
  }
  if (client_factory_ == nullptr) {
    client_factory_ = [this](const rpc::Address &address) {
      return std::make_shared<rpc::CoreWorkerClient>(address, client_call_manager_);
    };
  }
}

void GcsActorScheduler::Schedule(std::shared_ptr<GcsActor> actor) {
  auto node_id = actor->GetNodeID();
  if (!node_id.IsNil()) {
    if (auto node = gcs_node_manager_.GetNode(node_id)) {
      // If the actor is already tied to a node and the node is available, then record
      // the relationship of the node and actor and then lease worker directly from the
      // node.
      RAY_CHECK(node_to_actors_when_leasing_[actor->GetNodeID()]
                    .emplace(actor->GetActorID())
                    .second);
      LeaseWorkerFromNode(actor, node);
      return;
    }

    // The actor is already tied to a node which is unavailable now, so we should reset
    // the address.
    actor->UpdateAddress(rpc::Address());
  }

  // Select a node to lease worker for the actor.
  auto node = SelectNodeRandomly();
  if (node == nullptr) {
    // There are no available nodes to schedule the actor, so just trigger the failed
    // handler.
    schedule_failure_handler_(std::move(actor));
    return;
  }

  // Update the address of the actor as it is tied to a new node.
  rpc::Address address;
  address.set_raylet_id(node->node_id());
  actor->UpdateAddress(address);
  auto actor_table_data =
      std::make_shared<rpc::ActorTableData>(actor->GetActorTableData());
  // The backend storage is reliable in the future, so the status must be ok.
  RAY_CHECK_OK(actor_info_accessor_.AsyncUpdate(actor->GetActorID(), actor_table_data,
                                                [this, actor](Status status) {
                                                  RAY_CHECK_OK(status);
                                                  // There is no promise that the node the
                                                  // actor tied to is still alive as the
                                                  // flush is asynchronously, so just
                                                  // invoke `Schedule` which will lease
                                                  // worker directly if the node is still
                                                  // available or select a new one if not.
                                                  Schedule(actor);
                                                }));
}

std::vector<ActorID> GcsActorScheduler::CancelOnNode(const ClientID &node_id) {
  // Remove all the actors from the map associated with this node, and return them as they
  // will be reconstructed later.
  std::vector<ActorID> actor_ids;

  // Remove all actors in phase of leasing.
  {
    auto iter = node_to_actors_when_leasing_.find(node_id);
    if (iter != node_to_actors_when_leasing_.end()) {
      actor_ids.insert(actor_ids.end(), iter->second.begin(), iter->second.end());
      node_to_actors_when_leasing_.erase(iter);
    }
  }

  // Remove all actors in phase of creating.
  {
    auto iter = node_to_workers_when_creating_.find(node_id);
    if (iter != node_to_workers_when_creating_.end()) {
      for (auto &entry : iter->second) {
        actor_ids.emplace_back(entry.second->GetAssignedActorID());
        // Remove core worker client.
        RAY_CHECK(core_worker_clients_.erase(entry.first) != 0);
      }
      node_to_workers_when_creating_.erase(iter);
    }
  }

  // Remove the related remote lease client from remote_lease_clients_.
  // There is no need to check in this place, because it is possible that there are no
  // workers leased on this node.
  remote_lease_clients_.erase(node_id);

  return actor_ids;
}

ActorID GcsActorScheduler::CancelOnWorker(const ClientID &node_id,
                                          const WorkerID &worker_id) {
  // Remove the worker from creating map and return ID of the actor associated with the
  // removed worker if exist, else return NilID.
  ActorID assigned_actor_id;
  auto iter = node_to_workers_when_creating_.find(node_id);
  if (iter != node_to_workers_when_creating_.end()) {
    auto actor_iter = iter->second.find(worker_id);
    if (actor_iter != iter->second.end()) {
      assigned_actor_id = actor_iter->second->GetAssignedActorID();
      // Remove core worker client.
      RAY_CHECK(core_worker_clients_.erase(worker_id) != 0);
      iter->second.erase(actor_iter);
      if (iter->second.empty()) {
        node_to_workers_when_creating_.erase(iter);
      }
    }
  }
  return assigned_actor_id;
}

void GcsActorScheduler::LeaseWorkerFromNode(std::shared_ptr<GcsActor> actor,
                                            std::shared_ptr<rpc::GcsNodeInfo> node) {
  RAY_CHECK(actor && node);

  auto node_id = ClientID::FromBinary(node->node_id());
  RAY_LOG(INFO) << "Start leasing worker from node " << node_id << " for actor "
                << actor->GetActorID();

  rpc::Address remote_address;
  remote_address.set_raylet_id(node->node_id());
  remote_address.set_ip_address(node->node_manager_address());
  remote_address.set_port(node->node_manager_port());
  auto lease_client = GetOrConnectLeaseClient(remote_address);
  auto status = lease_client->RequestWorkerLease(
      actor->GetCreationTaskSpecification(),
      [this, node_id, actor, node](const Status &status,
                                   const rpc::RequestWorkerLeaseReply &reply) {
        // If the actor is still in the leasing map and the status is ok, remove the actor
        // from the leasing map and handle the reply. Otherwise, lease again, because it
        // may be a network exception.
        // If the actor is not in the leasing map, it means that the actor has been
        // cancelled as the node is dead, just do nothing in this case because the
        // gcs_actor_manager will reconstruct it again.
        auto iter = node_to_actors_when_leasing_.find(node_id);
        if (iter != node_to_actors_when_leasing_.end()) {
          // If the node is still available, the actor must be still in the leasing map as
          // it is erased from leasing map only when `CancelOnNode` or the
          // `RequestWorkerLeaseReply` is received from the node, so try lease again.
          auto actor_iter = iter->second.find(actor->GetActorID());
          RAY_CHECK(actor_iter != iter->second.end());
          if (status.ok()) {
            // Remove the actor from the leasing map as the reply is returned from the
            // remote node.
            iter->second.erase(actor_iter);
            if (iter->second.empty()) {
              node_to_actors_when_leasing_.erase(iter);
            }
            RAY_LOG(INFO) << "Finished leasing worker from " << node_id << " for actor "
                          << actor->GetActorID();
            HandleWorkerLeasedReply(actor, reply);
          } else {
            RetryLeasingWorkerFromNode(actor, node);
          }
        }
      });

  if (!status.ok()) {
    RetryLeasingWorkerFromNode(actor, node);
  }
}

void GcsActorScheduler::RetryLeasingWorkerFromNode(
    std::shared_ptr<GcsActor> actor, std::shared_ptr<rpc::GcsNodeInfo> node) {
  execute_after(io_context_,
                [this, node, actor] { DoRetryLeasingWorkerFromNode(actor, node); },
                RayConfig::instance().gcs_lease_worker_retry_interval_ms());
}

void GcsActorScheduler::DoRetryLeasingWorkerFromNode(
    std::shared_ptr<GcsActor> actor, std::shared_ptr<rpc::GcsNodeInfo> node) {
  auto iter = node_to_actors_when_leasing_.find(actor->GetNodeID());
  if (iter != node_to_actors_when_leasing_.end()) {
    // If the node is still available, the actor must be still in the
    // leasing map as it is erased from leasing map only when
    // `CancelOnNode` or the `RequestWorkerLeaseReply` is received from
    // the node, so try leasing again.
    RAY_CHECK(iter->second.count(actor->GetActorID()) != 0);
    RAY_LOG(INFO) << "Retry leasing worker from " << actor->GetNodeID() << " for actor "
                  << actor->GetActorID();
    LeaseWorkerFromNode(actor, node);
  }
}

void GcsActorScheduler::HandleWorkerLeasedReply(
    std::shared_ptr<GcsActor> actor, const ray::rpc::RequestWorkerLeaseReply &reply) {
  const auto &retry_at_raylet_address = reply.retry_at_raylet_address();
  const auto &worker_address = reply.worker_address();
  if (worker_address.raylet_id().empty()) {
    // The worker did not succeed in the lease, but the specified node returned a new
    // node, and then try again on the new node.
    RAY_CHECK(!retry_at_raylet_address.raylet_id().empty());
    actor->UpdateAddress(retry_at_raylet_address);
    auto actor_table_data =
        std::make_shared<rpc::ActorTableData>(actor->GetActorTableData());
    // The backend storage is reliable in the future, so the status must be ok.
    RAY_CHECK_OK(actor_info_accessor_.AsyncUpdate(actor->GetActorID(), actor_table_data,
                                                  [this, actor](Status status) {
                                                    RAY_CHECK_OK(status);
                                                    Schedule(actor);
                                                  }));
  } else {
    // The worker is leased successfully from the specified node.
    std::vector<rpc::ResourceMapEntry> resources;
    for (auto &resource : reply.resource_mapping()) {
      resources.emplace_back(resource);
    }
    auto leased_worker = std::make_shared<GcsLeasedWorker>(
        worker_address, std::move(resources), actor->GetActorID());
    auto node_id = leased_worker->GetNodeID();
    RAY_CHECK(node_to_workers_when_creating_[node_id]
                  .emplace(leased_worker->GetWorkerID(), leased_worker)
                  .second);
    actor->UpdateAddress(leased_worker->GetAddress());
    CreateActorOnWorker(actor, leased_worker);
  }
}

void GcsActorScheduler::CreateActorOnWorker(std::shared_ptr<GcsActor> actor,
                                            std::shared_ptr<GcsLeasedWorker> worker) {
  RAY_CHECK(actor && worker);
  RAY_LOG(INFO) << "Start creating actor " << actor->GetActorID() << " on worker "
                << worker->GetWorkerID() << " at node " << actor->GetNodeID();

  std::unique_ptr<rpc::PushTaskRequest> request(new rpc::PushTaskRequest());
  request->set_intended_worker_id(worker->GetWorkerID().Binary());
  request->mutable_task_spec()->CopyFrom(
      actor->GetCreationTaskSpecification().GetMessage());
  google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> resources;
  for (auto resource : worker->GetLeasedResources()) {
    resources.Add(std::move(resource));
  }
  request->mutable_resource_mapping()->CopyFrom(resources);

  auto client = GetOrConnectCoreWorkerClient(worker->GetAddress());
  auto status = client->PushNormalTask(
      std::move(request),
      [this, actor, worker](Status status, const rpc::PushTaskReply &reply) {
        RAY_UNUSED(reply);
        // If the actor is still in the creating map and the status is ok, remove the
        // actor from the creating map and invoke the schedule_success_handler_.
        // Otherwise, create again, because it may be a network exception.
        // If the actor is not in the creating map, it means that the actor has been
        // cancelled as the worker or node is dead, just do nothing in this case because
        // the gcs_actor_manager will reconstruct it again.
        auto iter = node_to_workers_when_creating_.find(actor->GetNodeID());
        if (iter != node_to_workers_when_creating_.end()) {
          auto worker_iter = iter->second.find(actor->GetWorkerID());
          if (worker_iter != iter->second.end()) {
            // The worker is still in the creating map.
            if (status.ok()) {
              // Remove related core worker client.
              RAY_CHECK(core_worker_clients_.erase(actor->GetWorkerID()) != 0);
              // Remove related worker in phase of creating.
              iter->second.erase(worker_iter);
              if (iter->second.empty()) {
                node_to_workers_when_creating_.erase(iter);
              }
              RAY_LOG(INFO) << "Succeeded in creating actor " << actor->GetActorID()
                            << " on worker " << worker->GetWorkerID() << " at node "
                            << actor->GetNodeID();
              schedule_success_handler_(actor);
            } else {
              RetryCreatingActorOnWorker(actor, worker);
            }
          }
        }
      });
  if (!status.ok()) {
    RetryCreatingActorOnWorker(actor, worker);
  }
}

void GcsActorScheduler::RetryCreatingActorOnWorker(
    std::shared_ptr<GcsActor> actor, std::shared_ptr<GcsLeasedWorker> worker) {
  execute_after(io_context_,
                [this, actor, worker] { DoRetryCreatingActorOnWorker(actor, worker); },
                RayConfig::instance().gcs_create_actor_retry_interval_ms());
}

void GcsActorScheduler::DoRetryCreatingActorOnWorker(
    std::shared_ptr<GcsActor> actor, std::shared_ptr<GcsLeasedWorker> worker) {
  auto iter = node_to_workers_when_creating_.find(actor->GetNodeID());
  if (iter != node_to_workers_when_creating_.end()) {
    auto worker_iter = iter->second.find(actor->GetWorkerID());
    if (worker_iter != iter->second.end()) {
      // The worker is still in the creating map, try create again.
      // The worker is erased from creating map only when `CancelOnNode`
      // or `CancelOnWorker` or the actor is created successfully.
      RAY_LOG(INFO) << "Retry creating actor " << actor->GetActorID() << " on worker "
                    << worker->GetWorkerID() << " at node " << actor->GetNodeID();
      CreateActorOnWorker(actor, worker);
    }
  }
}

std::shared_ptr<rpc::GcsNodeInfo> GcsActorScheduler::SelectNodeRandomly() const {
  auto &alive_nodes = gcs_node_manager_.GetAllAliveNodes();
  if (alive_nodes.empty()) {
    return nullptr;
  }

  static std::mt19937_64 gen_(
      std::chrono::high_resolution_clock::now().time_since_epoch().count());
  std::uniform_int_distribution<int> distribution(0, alive_nodes.size() - 1);
  int key_index = distribution(gen_);
  int index = 0;
  auto iter = alive_nodes.begin();
  for (; index != key_index && iter != alive_nodes.end(); ++index, ++iter)
    ;
  return iter->second;
}

std::shared_ptr<WorkerLeaseInterface> GcsActorScheduler::GetOrConnectLeaseClient(
    const rpc::Address &raylet_address) {
  auto node_id = ClientID::FromBinary(raylet_address.raylet_id());
  auto iter = remote_lease_clients_.find(node_id);
  if (iter == remote_lease_clients_.end()) {
    auto lease_client = lease_client_factory_(raylet_address);
    iter = remote_lease_clients_.emplace(node_id, std::move(lease_client)).first;
  }
  return iter->second;
}

std::shared_ptr<rpc::CoreWorkerClientInterface>
GcsActorScheduler::GetOrConnectCoreWorkerClient(const rpc::Address &worker_address) {
  auto worker_id = WorkerID::FromBinary(worker_address.worker_id());
  auto iter = core_worker_clients_.find(worker_id);
  if (iter == core_worker_clients_.end()) {
    iter = core_worker_clients_.emplace(worker_id, client_factory_(worker_address)).first;
  }
  return iter->second;
}

}  // namespace gcs
}  // namespace ray
