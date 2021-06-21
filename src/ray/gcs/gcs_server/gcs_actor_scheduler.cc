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
#include "ray/common/task/scheduling_resources.h"
#include "ray/common/task/scheduling_resources_util.h"
#include "ray/gcs/gcs_server/gcs_actor_manager.h"
#include "ray/util/event.h"
#include "ray/util/event_label.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {
namespace gcs {

GcsActorScheduler::GcsActorScheduler(
    instrumented_io_context &io_context, gcs::GcsActorTable &gcs_actor_table,
    const gcs::GcsNodeManager &gcs_node_manager,
    std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
    std::function<void(std::shared_ptr<GcsActor>)> schedule_failure_handler,
    std::function<void(std::shared_ptr<GcsActor>)> schedule_success_handler,
    std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
    rpc::ClientFactoryFn client_factory)
    : io_context_(io_context),
      gcs_actor_table_(gcs_actor_table),
      gcs_node_manager_(gcs_node_manager),
      gcs_pub_sub_(std::move(gcs_pub_sub)),
      schedule_failure_handler_(std::move(schedule_failure_handler)),
      schedule_success_handler_(std::move(schedule_success_handler)),
      report_worker_backlog_(RayConfig::instance().report_worker_backlog()),
      raylet_client_pool_(raylet_client_pool),
      core_worker_clients_(client_factory) {
  RAY_CHECK(schedule_failure_handler_ != nullptr && schedule_success_handler_ != nullptr);
}

void GcsActorScheduler::Schedule(std::shared_ptr<GcsActor> actor) {
  RAY_CHECK(actor->GetNodeID().IsNil() && actor->GetWorkerID().IsNil());

  // Select a node to lease worker for the actor.
  const auto &node_id = SelectNode(actor);

  auto node = gcs_node_manager_.GetAliveNode(node_id);
  if (!node.has_value()) {
    // There are no available nodes to schedule the actor, so just trigger the failed
    // handler.
    schedule_failure_handler_(std::move(actor));
    return;
  }

  // Update the address of the actor as it is tied to a node.
  rpc::Address address;
  address.set_raylet_id(node.value()->node_id());
  actor->UpdateAddress(address);

  RAY_CHECK(node_to_actors_when_leasing_[actor->GetNodeID()]
                .emplace(actor->GetActorID())
                .second);

  // Lease worker directly from the node.
  LeaseWorkerFromNode(actor, node.value());
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
      [this, actor, node](const Status &status,
                          const rpc::RequestWorkerLeaseReply &reply) {
        HandleWorkerLeaseReply(actor, node, status, reply);
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

void GcsActorScheduler::HandleWorkerLeaseGrantedReply(
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
    RAY_CHECK_OK(gcs_actor_table_.Put(actor->GetActorID(), actor->GetActorTableData(),
                                      [this, actor, leased_worker](Status status) {
                                        RAY_CHECK_OK(status);
                                        CreateActorOnWorker(actor, leased_worker);
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

std::shared_ptr<WorkerLeaseInterface> GcsActorScheduler::GetOrConnectLeaseClient(
    const rpc::Address &raylet_address) {
  return raylet_client_pool_->GetOrConnectByAddress(raylet_address);
}

NodeID RayletBasedActorScheduler::SelectNode(std::shared_ptr<GcsActor> actor) {
  // Select a node to lease worker for the actor.
  std::shared_ptr<rpc::GcsNodeInfo> node;

  // If an actor has resource requirements, we will try to schedule it on the same node as
  // the owner if possible.
  const auto &task_spec = actor->GetCreationTaskSpecification();
  if (!task_spec.GetRequiredResources().IsEmpty()) {
    auto maybe_node = gcs_node_manager_.GetAliveNode(actor->GetOwnerNodeID());
    node = maybe_node.has_value() ? maybe_node.value() : SelectNodeRandomly();
  } else {
    node = SelectNodeRandomly();
  }

  return node ? NodeID::FromBinary(node->node_id()) : NodeID::Nil();
}

std::shared_ptr<rpc::GcsNodeInfo> RayletBasedActorScheduler::SelectNodeRandomly() const {
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

void RayletBasedActorScheduler::HandleWorkerLeaseReply(
    std::shared_ptr<GcsActor> actor, std::shared_ptr<rpc::GcsNodeInfo> node,
    const Status &status, const rpc::RequestWorkerLeaseReply &reply) {
  // If the actor is still in the leasing map and the status is ok, remove the actor
  // from the leasing map and handle the reply. Otherwise, lease again, because it
  // may be a network exception.
  // If the actor is not in the leasing map, it means that the actor has been
  // cancelled as the node is dead, just do nothing in this case because the
  // gcs_actor_manager will reconstruct it again.
  auto node_id = NodeID::FromBinary(node->node_id());
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
      HandleWorkerLeaseGrantedReply(actor, reply);
    } else {
      RetryLeasingWorkerFromNode(actor, node);
    }
  }
}

NodeID GcsBasedActorScheduler::SelectNode(std::shared_ptr<GcsActor> actor) {
  RAY_CHECK(actor->GetActorWorkerAssignmentID().IsNil());
  bool need_sole_actor_worker_assignment =
      ray::NeedSoleActorWorkerAssignment(actor->GetCreationTaskSpecification());
  if (auto selected_actor_worker_assignment = SelectOrAllocateActorWorkerAssignment(
          actor, need_sole_actor_worker_assignment)) {
    // If succeed in selecting an available actor worker assignment then just assign the
    // actor, it will consume a slot inside the actor worker assignment.
    RAY_CHECK(selected_actor_worker_assignment->AssignActor(actor->GetActorID()))
        << ", actor id = " << actor->GetActorID()
        << ", actor_worker_assignment = " << selected_actor_worker_assignment->ToString();
    // Bind the actor worker assignment id to the physical worker process.
    actor->SetActorWorkerAssignmentID(
        selected_actor_worker_assignment->GetActorWorkerAssignmentID());

    std::ostringstream ss;
    ss << "Finished selecting node " << selected_actor_worker_assignment->GetNodeID()
       << " to schedule actor " << actor->GetActorID()
       << " with actor_worker_assignment_id = "
       << selected_actor_worker_assignment->GetActorWorkerAssignmentID();
    RAY_EVENT(INFO, EVENT_LABEL_ACTOR_NODE_SCHEDULED)
            .WithField("job_id", actor->GetActorID().JobId().Hex())
            .WithField("actor_id", actor->GetActorID().Hex())
        << ss.str();
    RAY_LOG(INFO) << ss.str();
    return selected_actor_worker_assignment->GetNodeID();
  }

  // If failed to select an available actor worker assignment then just return a nil node
  // id.
  std::ostringstream ss;
  ss << "There are no available resources to schedule the actor " << actor->GetActorID()
     << ", need sole actor worker assignment = " << need_sole_actor_worker_assignment;
  RAY_EVENT(ERROR, EVENT_LABEL_ACTOR_NODE_SCHEDULED)
          .WithField("job_id", actor->GetActorID().JobId().Hex())
          .WithField("actor_id", actor->GetActorID().Hex())
      << ss.str();
  RAY_LOG(WARNING) << ss.str();
  return NodeID::Nil();
}

std::shared_ptr<GcsActorWorkerAssignment>
GcsBasedActorScheduler::SelectOrAllocateActorWorkerAssignment(
    std::shared_ptr<GcsActor> actor, bool need_sole_actor_worker_assignment) {
  auto job_id = actor->GetActorID().JobId();
  auto job_scheduling_context =
      gcs_job_distribution_->FindOrCreateJobSchedulingContext(job_id);

  const auto &task_spec = actor->GetCreationTaskSpecification();
  auto required_resources = task_spec.GetRequiredPlacementResources();

  if (need_sole_actor_worker_assignment) {
    // If the task needs a sole actor worker assignment then allocate a new one.
    return AllocateNewActorWorkerAssignment(job_scheduling_context, required_resources,
                                            /*is_shared=*/false, task_spec);
  }

  std::shared_ptr<GcsActorWorkerAssignment> selected_actor_worker_assignment;

  // Otherwise, the task needs a shared actor worker assignment.
  // If there are unused slots in the allocated shared actor worker assignment, select the
  // one with the largest number of slots.
  const auto &shared_actor_worker_assignments =
      job_scheduling_context->GetSharedActorWorkerAssignments();
  // Select a actor worker assignment with the largest number of available slots.
  size_t max_available_slot_count = 0;
  for (const auto &entry : shared_actor_worker_assignments) {
    const auto &shared_actor_worker_assignment = entry.second;
    if (max_available_slot_count <
        shared_actor_worker_assignment->GetAvailableSlotCount()) {
      max_available_slot_count = shared_actor_worker_assignment->GetAvailableSlotCount();
      selected_actor_worker_assignment = shared_actor_worker_assignment;
    }
  }

  if (selected_actor_worker_assignment && !selected_actor_worker_assignment->IsDummy()) {
    return selected_actor_worker_assignment;
  }

  // If the resources required do not contain `kMemory_ResourceLabel` then add one
  // with the value of `worker_process_default_memory_units`
  RAY_CHECK(task_spec.GetLanguage() == rpc::Language::JAVA);
  const auto &job_config = job_scheduling_context->GetJobConfig();
  required_resources.AddOrUpdateResource(
      kMemory_ResourceLabel, job_config.java_worker_process_default_memory_units_);

  if (selected_actor_worker_assignment == nullptr) {
    // If there are no existing shared actor worker assignment then allocate a new one.
    selected_actor_worker_assignment =
        AllocateNewActorWorkerAssignment(job_scheduling_context, required_resources,
                                         /*is_shared=*/true, task_spec);
  } else {
    RAY_CHECK(selected_actor_worker_assignment->IsDummy());
    // If an existing shared actor worker assignment is selected and the `NodeID` of the
    // assignment is `Nil`, it means that a initial actor worker assignment is selected.
    // The initial actor worker assignment only deduct resources from job available
    // resources when initialized, not from the cluster resource pool, so we need
    // allocate now.
    auto selected_node_id = AllocateResources(required_resources);
    if (!selected_node_id.IsNil()) {
      // If the resources are allocated successfully then update the status of the
      // initial actor worker assignment as well as the job scheduling context.
      selected_actor_worker_assignment->SetNodeID(selected_node_id);
      selected_actor_worker_assignment->SetResources(required_resources);
      RAY_CHECK(gcs_job_distribution_->UpdateNodeToJob(selected_actor_worker_assignment));
    } else {
      WarnResourceAllocationFailure(job_scheduling_context, task_spec,
                                    required_resources);
      selected_actor_worker_assignment = nullptr;
    }
  }

  return selected_actor_worker_assignment;
}

std::shared_ptr<GcsActorWorkerAssignment>
GcsBasedActorScheduler::AllocateNewActorWorkerAssignment(
    std::shared_ptr<ray::gcs::GcsJobSchedulingContext> job_scheduling_context,
    const ResourceSet &required_resources, bool is_shared,
    const TaskSpecification &task_spec) {
  const auto &job_config = job_scheduling_context->GetJobConfig();
  // Figure out the `num_workers_per_process` and `slot_capacity`.
  int num_workers_per_process = 1;
  const auto &language = task_spec.GetLanguage();
  if (language == rpc::Language::JAVA) {
    num_workers_per_process = job_config.num_java_workers_per_process_;
  }
  auto slot_capacity = is_shared ? num_workers_per_process : 1;

  RAY_LOG(INFO) << "Allocating new actor worker assignment for job " << job_config.job_id_
                << ", language = " << rpc::Language_Name(language)
                << ", is_shared = " << is_shared << ", slot_capacity = " << slot_capacity
                << "\nrequired_resources = " << required_resources.ToString();
  RAY_LOG(DEBUG) << "Current cluster resources = " << gcs_resource_manager_->ToString();

  // TODO(Chong-Li): Check whether the job claimed resources satifies this new allocation.

  // Allocate resources from cluster.
  auto selected_node_id = AllocateResources(required_resources);
  if (selected_node_id.IsNil()) {
    WarnResourceAllocationFailure(job_scheduling_context, task_spec, required_resources);
    return nullptr;
  }

  // Create a new gcs actor worker assignment.
  auto gcs_actor_worker_assignment =
      GcsActorWorkerAssignment::Create(selected_node_id, job_config.job_id_, language,
                                       required_resources, is_shared, slot_capacity);

  // Add the gcs actor worker assignment to the job scheduling context which manager the
  // lifetime of the actor worker assignment.
  RAY_CHECK(gcs_job_distribution_->AddActorWorkerAssignment(gcs_actor_worker_assignment));
  RAY_LOG(INFO) << "Succeed in allocating new actor worker assignment for job "
                << job_config.job_id_ << " from node " << selected_node_id
                << ", actor_worker_assignment = "
                << gcs_actor_worker_assignment->ToString();

  return gcs_actor_worker_assignment;
}

NodeID GcsBasedActorScheduler::AllocateResources(const ResourceSet &required_resources) {
  auto selected_nodes =
      gcs_resource_scheduler_->Schedule({required_resources}, SchedulingType::SPREAD);

  if (selected_nodes.size() == 0) {
    RAY_LOG(INFO)
        << "Scheduling resources failed, schedule type = SchedulingType::SPREAD";
    return NodeID::Nil();
  }

  RAY_CHECK(selected_nodes.size() == 1);

  auto selected_node_id = selected_nodes[0];
  if (!selected_node_id.IsNil()) {
    // Acquire the resources from the selected node.
    RAY_CHECK(
        gcs_resource_manager_->AcquireResources(selected_node_id, required_resources));
  }

  return selected_node_id;
}

NodeID GcsBasedActorScheduler::GetHighestScoreNodeResource(
    const ResourceSet &required_resources) const {
  const auto &cluster_map = gcs_resource_manager_->GetClusterResources();

  /// Get the highest score node
  LeastResourceScorer scorer;

  double highest_score = -10;
  auto highest_score_node = NodeID::Nil();
  for (const auto &pair : cluster_map) {
    double least_resource_val = scorer.Score(required_resources, pair.second);
    if (least_resource_val > highest_score) {
      highest_score = least_resource_val;
      highest_score_node = pair.first;
    }
  }

  return highest_score_node;
}

void GcsBasedActorScheduler::WarnResourceAllocationFailure(
    std::shared_ptr<GcsJobSchedulingContext> job_scheduling_context,
    const TaskSpecification &task_spec, const ResourceSet &required_resources) const {
  const auto &job_config = job_scheduling_context->GetJobConfig();
  auto scheduling_node_id = GetHighestScoreNodeResource(required_resources);
  const SchedulingResources *scheduling_resource = nullptr;
  auto iter = gcs_resource_manager_->GetClusterResources().find(scheduling_node_id);
  if (iter != gcs_resource_manager_->GetClusterResources().end()) {
    scheduling_resource = &iter->second;
  }
  const std::string &scheduling_resource_str =
      scheduling_resource ? scheduling_resource->DebugString() : "None";
  // Return nullptr if the cluster resources are not enough.
  std::ostringstream ostr;
  ostr << "No enough resources for creating actor " << task_spec.ActorCreationId()
       << "\nActor class: " << task_spec.FunctionDescriptor()->ToString()
       << "\nJob id: " << job_config.job_id_
       << "\nRequired resources: " << required_resources.ToString()
       << "\nThe node with the most resources is:"
       << "\n   Node id: " << scheduling_node_id
       << "\n   Node resources: " << scheduling_resource_str;

  std::string message = ostr.str();

  RAY_LOG(WARNING) << message;
  RAY_LOG(DEBUG) << "Cluster resources: " << gcs_resource_manager_->ToString();

  RAY_EVENT(ERROR, EVENT_LABEL_JOB_FAILED_TO_ALLOCATE_RESOURCE)
          .WithField("job_id", job_config.job_id_.Hex())
      << message;
}

void GcsBasedActorScheduler::HandleWorkerLeaseReply(
    std::shared_ptr<GcsActor> actor, std::shared_ptr<rpc::GcsNodeInfo> node,
    const Status &status, const rpc::RequestWorkerLeaseReply &reply) {
  auto node_id = NodeID::FromBinary(node->node_id());
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
      // Remove the actor from the leasing map as the reply is returned from the
      // remote node.
      iter->second.erase(actor_iter);
      if (iter->second.empty()) {
        node_to_actors_when_leasing_.erase(iter);
      }
      if (reply.rejected()) {
        std::ostringstream ss;
        ss << "Failed to lease worker from node " << node_id << " for actor "
           << actor->GetActorID()
           << " as the resources are seized by normal tasks, job id = "
           << actor->GetActorID().JobId();
        RAY_LOG(INFO) << ss.str();
        HandleWorkerLeaseRejectedReply(actor, reply);
      } else {
        std::ostringstream ss;
        ss << "Finished leasing worker from node " << node_id << " for actor "
           << actor->GetActorID() << ", job id = " << actor->GetActorID().JobId();
        RAY_LOG(INFO) << ss.str();
        HandleWorkerLeaseGrantedReply(actor, reply);
      }
    } else {
      std::ostringstream ss;
      ss << "Failed to lease worker from node " << node_id << " for actor "
         << actor->GetActorID() << ", status = " << status
         << ", job id = " << actor->GetActorID().JobId();
      RAY_LOG(WARNING) << ss.str();
      RetryLeasingWorkerFromNode(actor, node);
    }
  }
}

void GcsBasedActorScheduler::HandleWorkerLeaseRejectedReply(
    std::shared_ptr<GcsActor> actor, const rpc::RequestWorkerLeaseReply &reply) {
  // The request was rejected because of insufficient resources.
  auto node_id = actor->GetNodeID();
  gcs_resource_manager_->UpdateNodeNormalTaskResources(node_id, reply.resources_data());
  CancelOnActorWorkerAssignment(actor->GetActorID(), actor->GetActorWorkerAssignmentID());
  actor->UpdateAddress(rpc::Address());
  actor->SetActorWorkerAssignmentID(UniqueID::Nil());
  Reschedule(actor);
}

void GcsBasedActorScheduler::CancelOnActorWorkerAssignment(
    const ActorID &actor_id, const UniqueID &actor_worker_assignment_id) {
  RAY_LOG(INFO) << "Removing actor " << actor_id << " from assignment "
                << actor_worker_assignment_id << ", job id = " << actor_id.JobId();
  if (auto actor_worker_assignment = gcs_job_distribution_->GetActorWorkerAssignmentById(
          actor_id.JobId(), actor_worker_assignment_id)) {
    if (actor_worker_assignment->RemoveActor(actor_id)) {
      RAY_LOG(INFO) << "Finished removing actor " << actor_id << " from assignment "
                    << actor_worker_assignment_id << ", job id = " << actor_id.JobId();
      if (actor_worker_assignment->GetUsedSlotCount() == 0) {
        auto node_id = actor_worker_assignment->GetNodeID();
        RAY_LOG(INFO) << "Remove actor worker assignment " << actor_worker_assignment_id
                      << " from node " << node_id
                      << " as there are no more actors bind to it.";
        // Recycle this actor worker assignment.
        auto removed_actor_worker_assignment =
            gcs_job_distribution_->RemoveActorWorkerAssignmentByActorWorkerAssignmentID(
                actor_worker_assignment->GetNodeID(), actor_worker_assignment_id,
                actor_id.JobId());
        RAY_CHECK(removed_actor_worker_assignment == actor_worker_assignment);
        if (gcs_resource_manager_->ReleaseResources(
                node_id, removed_actor_worker_assignment->GetResources())) {
          // TODO(Chong-Li): Notify Cluster Resource Changed here.
        }
      }
    } else {
      RAY_LOG(WARNING) << "Failed to remove actor " << actor_id << " from assignment "
                       << actor_worker_assignment_id
                       << " as the actor is already removed from this assignment.";
    }
  } else {
    RAY_LOG(WARNING) << "Failed to remove actor " << actor_id << " from assignment "
                     << actor_worker_assignment_id
                     << " as the assignment does not exist.";
  }
}

}  // namespace gcs
}  // namespace ray
