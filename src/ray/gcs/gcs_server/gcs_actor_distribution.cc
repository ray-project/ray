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

#include "ray/gcs/gcs_server/gcs_actor_distribution.h"

#include "ray/util/event.h"

namespace ray {

namespace gcs {

GcsActorWorkerAssignment::GcsActorWorkerAssignment(
    const NodeID &node_id, const ResourceRequest &acquired_resources)
    : node_id_(node_id), acquired_resources_(acquired_resources) {}

const NodeID &GcsActorWorkerAssignment::GetNodeID() const { return node_id_; }

const ResourceRequest &GcsActorWorkerAssignment::GetResources() const {
  return acquired_resources_;
}

GcsBasedActorScheduler::GcsBasedActorScheduler(
    instrumented_io_context &io_context,
    GcsActorTable &gcs_actor_table,
    const GcsNodeManager &gcs_node_manager,
    std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler,
    GcsActorSchedulerFailureCallback schedule_failure_handler,
    GcsActorSchedulerSuccessCallback schedule_success_handler,
    std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
    rpc::ClientFactoryFn client_factory,
    std::function<void(const NodeID &, const rpc::ResourcesData &)>
        normal_task_resources_changed_callback)
    : GcsActorScheduler(io_context,
                        gcs_actor_table,
                        gcs_node_manager,
                        schedule_failure_handler,
                        schedule_success_handler,
                        raylet_client_pool,
                        client_factory),
      cluster_resource_scheduler_(std::move(cluster_resource_scheduler)),
      normal_task_resources_changed_callback_(normal_task_resources_changed_callback) {}

NodeID GcsBasedActorScheduler::SelectNode(std::shared_ptr<GcsActor> actor) {
  if (actor->GetActorWorkerAssignment()) {
    ResetActorWorkerAssignment(actor.get());
  }

  if (auto actor_worker_assignment =
          AllocateActorWorkerAssignment(actor->GetCreationTaskSpecification())) {
    auto node_id = actor_worker_assignment->GetNodeID();
    actor->SetActorWorkerAssignment(std::move(actor_worker_assignment));
    return node_id;
  }
  return NodeID::Nil();
}

std::unique_ptr<GcsActorWorkerAssignment>
GcsBasedActorScheduler::AllocateActorWorkerAssignment(
    const TaskSpecification &task_spec) {
  // Allocate resources from cluster.
  auto selected_node_id = AllocateResources(task_spec);
  if (selected_node_id.IsNil()) {
    WarnResourceAllocationFailure(task_spec);
    return nullptr;
  }

  auto required_resources = ResourceMapToResourceRequest(
      task_spec.GetRequiredResources().GetResourceMap(), false);
  // Create a new gcs actor worker assignment.
  auto gcs_actor_worker_assignment = std::make_unique<GcsActorWorkerAssignment>(
      NodeID::FromBinary(selected_node_id.Binary()), required_resources);

  return gcs_actor_worker_assignment;
}

scheduling::NodeID GcsBasedActorScheduler::AllocateResources(
    const TaskSpecification &task_spec) {
  bool is_infeasible = false;
  auto scheduling_node_id = cluster_resource_scheduler_->GetBestSchedulableNode(
      task_spec,
      /*prioritize_local_node*/ false,
      /*exclude_local_node*/ true,
      /*requires_object_store_memory*/ false,
      &is_infeasible);

  if (scheduling_node_id.IsNil()) {
    RAY_LOG(INFO) << "No node found to schedule a task " << task_spec.TaskId()
                  << " is infeasible?" << is_infeasible;
    return scheduling_node_id;
  }

  auto &cluster_resource_manager =
      cluster_resource_scheduler_->GetClusterResourceManager();
  auto required_resources = ResourceMapToResourceRequest(
      task_spec.GetRequiredResources().GetResourceMap(), false);
  // Acquire the resources from the selected node.
  RAY_CHECK(cluster_resource_manager.SubtractNodeAvailableResources(scheduling_node_id,
                                                                    required_resources));

  return scheduling_node_id;
}

void GcsBasedActorScheduler::WarnResourceAllocationFailure(
    const TaskSpecification &task_spec) const {
  auto &cluster_resource_manager =
      cluster_resource_scheduler_->GetClusterResourceManager();
  auto required_placement_resources = ResourceMapToResourceRequest(
      task_spec.GetRequiredPlacementResources().GetResourceMap(), false);

  RAY_LOG(WARNING) << "No enough resources for creating actor "
                   << task_spec.ActorCreationId()
                   << "\nActor class: " << task_spec.FunctionDescriptor()->ToString()
                   << "\nRequired placement resources: "
                   << required_placement_resources.DebugString();

  std::stringstream ostr;
  cluster_resource_manager.DebugString(ostr);
  RAY_LOG(DEBUG) << "Cluster resources: " << ostr.str();
}

void GcsBasedActorScheduler::HandleWorkerLeaseReply(
    std::shared_ptr<GcsActor> actor,
    std::shared_ptr<rpc::GcsNodeInfo> node,
    const Status &status,
    const rpc::RequestWorkerLeaseReply &reply) {
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
      if (reply.worker_address().raylet_id().empty() &&
          reply.retry_at_raylet_address().raylet_id().empty() && !reply.rejected()) {
        // Actor creation task has been cancelled. It is triggered by `ray.kill`. If
        // the number of remaining restarts of the actor is not equal to 0, GCS will
        // reschedule the actor, so it return directly here.
        RAY_LOG(DEBUG) << "Actor " << actor->GetActorID()
                       << " creation task has been cancelled.";
        ResetActorWorkerAssignment(actor.get());
        return;
      }
      // Remove the actor from the leasing map as the reply is returned from the
      // remote node.
      iter->second.erase(actor_iter);
      if (iter->second.empty()) {
        node_to_actors_when_leasing_.erase(iter);
      }
      if (reply.canceled()) {
        // TODO(sang): Should properly update the failure message.
        HandleRequestWorkerLeaseCanceled(actor,
                                         node_id,
                                         reply.failure_type(),
                                         /*scheduling_failure_message*/ "");
      } else if (reply.rejected()) {
        RAY_LOG(INFO) << "Failed to lease worker from node " << node_id << " for actor "
                      << actor->GetActorID()
                      << " as the resources are seized by normal tasks, job id = "
                      << actor->GetActorID().JobId();
        HandleWorkerLeaseRejectedReply(actor, reply);
      } else {
        RAY_LOG(INFO) << "Finished leasing worker from node " << node_id << " for actor "
                      << actor->GetActorID()
                      << ", job id = " << actor->GetActorID().JobId();
        HandleWorkerLeaseGrantedReply(actor, reply);
      }
    } else {
      RAY_LOG(WARNING) << "Failed to lease worker from node " << node_id << " for actor "
                       << actor->GetActorID() << ", status = " << status
                       << ", job id = " << actor->GetActorID().JobId();
      RetryLeasingWorkerFromNode(actor, node);
    }
  }
}

void GcsBasedActorScheduler::HandleWorkerLeaseRejectedReply(
    std::shared_ptr<GcsActor> actor, const rpc::RequestWorkerLeaseReply &reply) {
  // The request was rejected because of insufficient resources.
  auto node_id = actor->GetNodeID();
  auto &cluster_resource_manager =
      cluster_resource_scheduler_->GetClusterResourceManager();
  cluster_resource_manager.AddNodeAvailableResources(
      scheduling::NodeID(actor->GetActorWorkerAssignment()->GetNodeID().Binary()),
      actor->GetActorWorkerAssignment()->GetResources());
  if (normal_task_resources_changed_callback_) {
    normal_task_resources_changed_callback_(node_id, reply.resources_data());
  }
  actor->UpdateAddress(rpc::Address());
  actor->SetActorWorkerAssignment(nullptr);
  Reschedule(actor);
}

void GcsBasedActorScheduler::AddResourcesChangedListener(std::function<void()> listener) {
  RAY_CHECK(listener != nullptr);
  resource_changed_listeners_.emplace_back(std::move(listener));
}

void GcsBasedActorScheduler::NotifyClusterResourcesChanged() {
  for (auto &listener : resource_changed_listeners_) {
    listener();
  }
}

void GcsBasedActorScheduler::ResetActorWorkerAssignment(GcsActor *actor) {
  if (!actor->GetActorWorkerAssignment()) {
    return;
  }

  auto &cluster_resource_manager =
      cluster_resource_scheduler_->GetClusterResourceManager();
  if (cluster_resource_manager.AddNodeAvailableResources(
          scheduling::NodeID(actor->GetActorWorkerAssignment()->GetNodeID().Binary()),
          actor->GetActorWorkerAssignment()->GetResources())) {
    NotifyClusterResourcesChanged();
  };
  actor->SetActorWorkerAssignment(nullptr);
}

void GcsBasedActorScheduler::OnActorDestruction(std::shared_ptr<GcsActor> actor) {
  if (actor) {
    ResetActorWorkerAssignment(actor.get());
  }
}

}  // namespace gcs
}  // namespace ray