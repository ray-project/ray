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

#include "ray/gcs/gcs_server/gcs_actor_scheduler.h"

#include "ray/common/asio/asio_util.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_server/gcs_actor_manager.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {
namespace gcs {

GcsActorScheduler::GcsActorScheduler(
    instrumented_io_context &io_context, gcs::GcsActorTable &gcs_actor_table,
    gcs::GcsActorTaskSpecTable &gcs_actor_task_spec_table,
    const gcs::GcsNodeManager &gcs_node_manager,
    std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
    std::function<void(std::shared_ptr<GcsActor>)> schedule_failure_handler,
    std::function<void(std::shared_ptr<GcsActor>)> schedule_success_handler,
    std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
    std::shared_ptr<GcsActorScheduleStrategyInterface> actor_schedule_strategy,
    rpc::ClientFactoryFn client_factory)
    : io_context_(io_context),
      gcs_actor_table_(gcs_actor_table),
      gcs_actor_task_spec_table_(gcs_actor_task_spec_table),
      gcs_node_manager_(gcs_node_manager),
      gcs_pub_sub_(std::move(gcs_pub_sub)),
      schedule_failure_handler_(std::move(schedule_failure_handler)),
      schedule_success_handler_(std::move(schedule_success_handler)),
      report_worker_backlog_(RayConfig::instance().report_worker_backlog()),
      raylet_client_pool_(raylet_client_pool),
      actor_schedule_strategy_(actor_schedule_strategy),
      core_worker_clients_(client_factory) {
  RAY_CHECK(schedule_failure_handler_ != nullptr && schedule_success_handler_ != nullptr);
}

void GcsActorScheduler::Schedule(std::shared_ptr<GcsActor> actor) {
  RAY_CHECK(actor->GetNodeID().IsNil() && actor->GetWorkerID().IsNil());

  // Select a node to lease worker for the actor.
  const auto &node = actor_schedule_strategy_->Schedule(actor);

  if (node == nullptr) {
    // There are no available nodes to schedule the actor, so just trigger the failed
    // handler.
    schedule_failure_handler_(std::move(actor));
    return;
  }

  // Update the address of the actor as it is tied to a node.
  rpc::Address address;
  address.set_raylet_id(node->node_id());
  actor->UpdateAddress(address);

  RAY_CHECK(node_to_actors_when_leasing_[actor->GetNodeID()]
                .emplace(actor->GetActorID())
                .second);

  // Lease worker directly from the node.
  LeaseWorkerFromNode(actor, node);
}

void GcsActorScheduler::Reschedule(std::shared_ptr<GcsActor> actor) {
  if (!actor->GetWorkerID().IsNil()) {
    RAY_LOG(INFO) << "Actor " << actor->GetActorID()
                  << " is already tied to a leased worker. Create actor directly on "
                     "worker. Job id = "
                  << actor->GetActorID().JobId();
    auto leased_worker = std::make_shared<GcsLeasedWorker>(
        actor->GetAddress(),
        VectorFromProtobuf(actor->GetMutableActorTableData()->resource_mapping()),
        actor->GetActorID());
    auto iter_node = node_to_workers_when_creating_.find(actor->GetNodeID());
    if (iter_node != node_to_workers_when_creating_.end()) {
      if (0 == iter_node->second.count(leased_worker->GetWorkerID())) {
        iter_node->second.emplace(leased_worker->GetWorkerID(), leased_worker);
      }
    } else {
      node_to_workers_when_creating_[actor->GetNodeID()].emplace(
          leased_worker->GetWorkerID(), leased_worker);
    }
    CreateActorOnWorker(actor, leased_worker);
  } else {
    Schedule(actor);
  }
}

std::vector<ActorID> GcsActorScheduler::CancelOnNode(const NodeID &node_id) {
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
        core_worker_clients_.Disconnect(entry.first);
      }
      node_to_workers_when_creating_.erase(iter);
    }
  }

  raylet_client_pool_->Disconnect(node_id);

  return actor_ids;
}

void GcsActorScheduler::CancelOnLeasing(const NodeID &node_id, const ActorID &actor_id,
                                        const TaskID &task_id) {
  // NOTE: This method will cancel the outstanding lease request and remove leasing
  // information from the internal state.
  auto node_it = node_to_actors_when_leasing_.find(node_id);
  RAY_CHECK(node_it != node_to_actors_when_leasing_.end());
  node_it->second.erase(actor_id);
  if (node_it->second.empty()) {
    node_to_actors_when_leasing_.erase(node_it);
  }

  const auto &alive_nodes = gcs_node_manager_.GetAllAliveNodes();
  const auto &iter = alive_nodes.find(node_id);
  if (iter != alive_nodes.end()) {
    const auto &node_info = iter->second;
    rpc::Address address;
    address.set_raylet_id(node_info->node_id());
    address.set_ip_address(node_info->node_manager_address());
    address.set_port(node_info->node_manager_port());
    auto lease_client = GetOrConnectLeaseClient(address);
    lease_client->CancelWorkerLease(
        task_id, [](const Status &status, const rpc::CancelWorkerLeaseReply &reply) {});
  }
}

ActorID GcsActorScheduler::CancelOnWorker(const NodeID &node_id,
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
      core_worker_clients_.Disconnect(worker_id);
      iter->second.erase(actor_iter);
      if (iter->second.empty()) {
        node_to_workers_when_creating_.erase(iter);
      }
    }
  }
  return assigned_actor_id;
}

void GcsActorScheduler::ReleaseUnusedWorkers(
    const std::unordered_map<NodeID, std::vector<WorkerID>> &node_to_workers) {
  // The purpose of this function is to release leased workers that may be leaked.
  // When GCS restarts, it doesn't know which workers it has leased in the previous
  // lifecycle. In this case, GCS will send a list of worker ids that are still needed.
  // And Raylet will release other leased workers.
  // If the node is dead, there is no need to send the request of release unused
  // workers.
  const auto &alive_nodes = gcs_node_manager_.GetAllAliveNodes();
  for (const auto &alive_node : alive_nodes) {
    const auto &node_id = alive_node.first;
    nodes_of_releasing_unused_workers_.insert(node_id);

    rpc::Address address;
    address.set_raylet_id(alive_node.second->node_id());
    address.set_ip_address(alive_node.second->node_manager_address());
    address.set_port(alive_node.second->node_manager_port());
    auto lease_client = GetOrConnectLeaseClient(address);
    auto release_unused_workers_callback =
        [this, node_id](const Status &status,
                        const rpc::ReleaseUnusedWorkersReply &reply) {
          nodes_of_releasing_unused_workers_.erase(node_id);
        };
    auto iter = node_to_workers.find(alive_node.first);

    // When GCS restarts, the reply of RequestWorkerLease may not be processed, so some
    // nodes do not have leased workers. In this case, GCS will send an empty list.
    auto workers_in_use =
        iter != node_to_workers.end() ? iter->second : std::vector<WorkerID>{};
    lease_client->ReleaseUnusedWorkers(workers_in_use, release_unused_workers_callback);
  }
}

void GcsActorScheduler::LeaseWorkerFromNode(std::shared_ptr<GcsActor> actor,
                                            std::shared_ptr<rpc::GcsNodeInfo> node) {
  RAY_CHECK(actor && node);

  auto node_id = NodeID::FromBinary(node->node_id());
  RAY_LOG(INFO) << "Start leasing worker from node " << node_id << " for actor "
                << actor->GetActorID() << ", job id = " << actor->GetActorID().JobId();

  // We need to ensure that the RequestWorkerLease won't be sent before the reply of
  // ReleaseUnusedWorkers is returned.
  if (nodes_of_releasing_unused_workers_.contains(node_id)) {
    RetryLeasingWorkerFromNode(actor, node);
    return;
  }

  rpc::Address remote_address;
  remote_address.set_raylet_id(node->node_id());
  remote_address.set_ip_address(node->node_manager_address());
  remote_address.set_port(node->node_manager_port());
  auto lease_client = GetOrConnectLeaseClient(remote_address);
  // Actor leases should be sent to the raylet immediately, so we should never build up a
  // backlog in GCS.
  int backlog_size = report_worker_backlog_ ? 0 : -1;
  lease_client->RequestWorkerLease(
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
          auto actor_iter = iter->second.find(actor->GetActorID());
          if (actor_iter == iter->second.end()) {
            // if actor is not in leasing state, it means it is cancelled.
            RAY_LOG(INFO)
                << "Raylet granted a lease request, but the outstanding lease "
                   "request for "
                << actor->GetActorID()
                << " has been already cancelled. The response will be ignored. Job id = "
                << actor->GetActorID().JobId();
            return;
          }

          if (status.ok()) {
            if (reply.worker_address().raylet_id().empty() &&
                reply.retry_at_raylet_address().raylet_id().empty()) {
              // Actor creation task has been cancelled. It is triggered by `ray.kill`. If
              // the number of remaining restarts of the actor is not equal to 0, GCS will
              // reschedule the actor, so it return directly here.
              RAY_LOG(DEBUG) << "Actor " << actor->GetActorID()
                             << " creation task has been cancelled.";
              return;
            }

            // Remove the actor from the leasing map as the reply is returned from the
            // remote node.
            iter->second.erase(actor_iter);
            if (iter->second.empty()) {
              node_to_actors_when_leasing_.erase(iter);
            }
            RAY_LOG(INFO) << "Finished leasing worker from " << node_id << " for actor "
                          << actor->GetActorID()
                          << ", job id = " << actor->GetActorID().JobId();
            HandleWorkerLeasedReply(actor, reply);
          } else {
            RetryLeasingWorkerFromNode(actor, node);
          }
        }
      },
      backlog_size);
}

void GcsActorScheduler::RetryLeasingWorkerFromNode(
    std::shared_ptr<GcsActor> actor, std::shared_ptr<rpc::GcsNodeInfo> node) {
  RAY_UNUSED(execute_after(
      io_context_, [this, node, actor] { DoRetryLeasingWorkerFromNode(actor, node); },
      RayConfig::instance().gcs_lease_worker_retry_interval_ms()));
}

void GcsActorScheduler::DoRetryLeasingWorkerFromNode(
    std::shared_ptr<GcsActor> actor, std::shared_ptr<rpc::GcsNodeInfo> node) {
  auto iter = node_to_actors_when_leasing_.find(actor->GetNodeID());
  if (iter != node_to_actors_when_leasing_.end()) {
    // If the node is still available, the actor must be still in the
    // leasing map as it is erased from leasing map only when
    // `CancelOnNode`, `RequestWorkerLeaseReply` or `CancelOnLeasing` is received, so try
    // leasing again.
    if (iter->second.count(actor->GetActorID())) {
      RAY_LOG(INFO) << "Retry leasing worker from " << actor->GetNodeID() << " for actor "
                    << actor->GetActorID()
                    << ", job id = " << actor->GetActorID().JobId();
      LeaseWorkerFromNode(actor, node);
    }
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
    auto spill_back_node_id = NodeID::FromBinary(retry_at_raylet_address.raylet_id());
    auto maybe_spill_back_node = gcs_node_manager_.GetAliveNode(spill_back_node_id);
    if (maybe_spill_back_node.has_value()) {
      auto spill_back_node = maybe_spill_back_node.value();
      actor->UpdateAddress(retry_at_raylet_address);
      RAY_CHECK(node_to_actors_when_leasing_[actor->GetNodeID()]
                    .emplace(actor->GetActorID())
                    .second);
      LeaseWorkerFromNode(actor, spill_back_node);
    } else {
      // If the spill back node is dead, we need to schedule again.
      actor->UpdateAddress(rpc::Address());
      actor->GetMutableActorTableData()->clear_resource_mapping();
      Schedule(actor);
    }
  } else {
    // The worker is leased successfully from the specified node.
    std::vector<rpc::ResourceMapEntry> resources;
    for (auto &resource : reply.resource_mapping()) {
      resources.emplace_back(resource);
      actor->GetMutableActorTableData()->add_resource_mapping()->CopyFrom(resource);
    }
    auto leased_worker = std::make_shared<GcsLeasedWorker>(
        worker_address, std::move(resources), actor->GetActorID());
    auto node_id = leased_worker->GetNodeID();
    RAY_CHECK(node_to_workers_when_creating_[node_id]
                  .emplace(leased_worker->GetWorkerID(), leased_worker)
                  .second);
    actor->UpdateAddress(leased_worker->GetAddress());
    actor->GetMutableActorTableData()->set_pid(reply.worker_pid());
    // Make sure to connect to the client before persisting actor info to GCS.
    // Without this, there could be a possible race condition. Related issues:
    // https://github.com/ray-project/ray/pull/9215/files#r449469320
    core_worker_clients_.GetOrConnect(leased_worker->GetAddress());
    RAY_CHECK_OK(gcs_actor_task_spec_table_.Put(
        actor->GetActorID(), actor->GetCreationTaskSpecification().GetMessage(),
        [this, actor, leased_worker](const Status &status) {
          RAY_CHECK_OK(gcs_actor_table_.Put(actor->GetActorID(),
                                            actor->GetActorTableData(),
                                            [this, actor, leased_worker](Status status) {
                                              RAY_CHECK_OK(status);
                                              CreateActorOnWorker(actor, leased_worker);
                                            }));
        }));
  }
}

void GcsActorScheduler::CreateActorOnWorker(std::shared_ptr<GcsActor> actor,
                                            std::shared_ptr<GcsLeasedWorker> worker) {
  RAY_CHECK(actor && worker);
  RAY_LOG(INFO) << "Start creating actor " << actor->GetActorID() << " on worker "
                << worker->GetWorkerID() << " at node " << actor->GetNodeID()
                << ", job id = " << actor->GetActorID().JobId();

  std::unique_ptr<rpc::PushTaskRequest> request(new rpc::PushTaskRequest());
  request->set_intended_worker_id(worker->GetWorkerID().Binary());
  request->mutable_task_spec()->CopyFrom(
      actor->GetCreationTaskSpecification().GetMessage());
  google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> resources;
  for (auto resource : worker->GetLeasedResources()) {
    resources.Add(std::move(resource));
  }
  request->mutable_resource_mapping()->CopyFrom(resources);

  auto client = core_worker_clients_.GetOrConnect(worker->GetAddress());
  client->PushNormalTask(
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
              core_worker_clients_.Disconnect(actor->GetWorkerID());
              // Remove related worker in phase of creating.
              iter->second.erase(worker_iter);
              if (iter->second.empty()) {
                node_to_workers_when_creating_.erase(iter);
              }
              RAY_LOG(INFO) << "Succeeded in creating actor " << actor->GetActorID()
                            << " on worker " << worker->GetWorkerID() << " at node "
                            << actor->GetNodeID()
                            << ", job id = " << actor->GetActorID().JobId();
              schedule_success_handler_(actor);
            } else {
              RetryCreatingActorOnWorker(actor, worker);
            }
          }
        }
      });
}

void GcsActorScheduler::RetryCreatingActorOnWorker(
    std::shared_ptr<GcsActor> actor, std::shared_ptr<GcsLeasedWorker> worker) {
  RAY_UNUSED(execute_after(
      io_context_, [this, actor, worker] { DoRetryCreatingActorOnWorker(actor, worker); },
      RayConfig::instance().gcs_create_actor_retry_interval_ms()));
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
                    << worker->GetWorkerID() << " at node " << actor->GetNodeID()
                    << ", job id = " << actor->GetActorID().JobId();
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
  return raylet_client_pool_->GetOrConnectByAddress(raylet_address);
}

}  // namespace gcs
}  // namespace ray
