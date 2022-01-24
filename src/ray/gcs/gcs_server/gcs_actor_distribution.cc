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

GcsActorWorkerAssignment::GcsActorWorkerAssignment(const NodeID &node_id,
                                                   const ResourceSet &acquired_resources,
                                                   bool is_shared)
    : node_id_(node_id), acquired_resources_(acquired_resources), is_shared_(is_shared) {}

const NodeID &GcsActorWorkerAssignment::GetNodeID() const { return node_id_; }

const ResourceSet &GcsActorWorkerAssignment::GetResources() const {
  return acquired_resources_;
}

bool GcsActorWorkerAssignment::IsShared() const { return is_shared_; }

GcsBasedActorScheduler::GcsBasedActorScheduler(
    instrumented_io_context &io_context, GcsActorTable &gcs_actor_table,
    const GcsNodeManager &gcs_node_manager,
    std::shared_ptr<GcsResourceManager> gcs_resource_manager,
    std::shared_ptr<GcsResourceScheduler> gcs_resource_scheduler,
    std::function<void(std::shared_ptr<GcsActor>,
                       rpc::RequestWorkerLeaseReply::SchedulingFailureType)>
        schedule_failure_handler,
    std::function<void(std::shared_ptr<GcsActor>, const rpc::PushTaskReply &reply)>
        schedule_success_handler,
    std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
    rpc::ClientFactoryFn client_factory)
    : GcsActorScheduler(io_context, gcs_actor_table, gcs_node_manager,
                        schedule_failure_handler, schedule_success_handler,
                        raylet_client_pool, client_factory),
      gcs_resource_manager_(std::move(gcs_resource_manager)),
      gcs_resource_scheduler_(std::move(gcs_resource_scheduler)) {}

NodeID GcsBasedActorScheduler::SelectNode(std::shared_ptr<GcsActor> actor) {
  if (actor->GetActorWorkerAssignment()) {
    ResetActorWorkerAssignment(actor.get());
  }
  // TODO(Chong-Li): Java actors may not need a sole assignment (worker process).
  bool need_sole_actor_worker_assignment = true;
  if (auto selected_actor_worker_assignment = SelectOrAllocateActorWorkerAssignment(
          actor, need_sole_actor_worker_assignment)) {
    auto node_id = selected_actor_worker_assignment->GetNodeID();
    actor->SetActorWorkerAssignment(std::move(selected_actor_worker_assignment));
    return node_id;
  }
  return NodeID::Nil();
}

std::unique_ptr<GcsActorWorkerAssignment>
GcsBasedActorScheduler::SelectOrAllocateActorWorkerAssignment(
    std::shared_ptr<GcsActor> actor, bool need_sole_actor_worker_assignment) {
  const auto &task_spec = actor->GetCreationTaskSpecification();
  auto required_resources = task_spec.GetRequiredPlacementResources();

  // If the task needs a sole actor worker assignment then allocate a new one.
  return AllocateNewActorWorkerAssignment(required_resources, /*is_shared=*/false,
                                          task_spec);

  // TODO(Chong-Li): code path for actors that do not need a sole assignment.
}

std::unique_ptr<GcsActorWorkerAssignment>
GcsBasedActorScheduler::AllocateNewActorWorkerAssignment(
    const ResourceSet &required_resources, bool is_shared,
    const TaskSpecification &task_spec) {
  // Allocate resources from cluster.
  auto selected_node_id = AllocateResources(required_resources);
  if (selected_node_id.IsNil()) {
    WarnResourceAllocationFailure(task_spec, required_resources);
    return nullptr;
  }

  // Create a new gcs actor worker assignment.
  auto gcs_actor_worker_assignment = std::make_unique<GcsActorWorkerAssignment>(
      selected_node_id, required_resources, is_shared);

  return gcs_actor_worker_assignment;
}

NodeID GcsBasedActorScheduler::AllocateResources(const ResourceSet &required_resources) {
  auto selected_nodes =
      gcs_resource_scheduler_->Schedule({required_resources}, SchedulingType::SPREAD)
          .second;

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

  double highest_score = std::numeric_limits<double>::lowest();
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
    const TaskSpecification &task_spec, const ResourceSet &required_resources) const {
  auto scheduling_node_id = GetHighestScoreNodeResource(required_resources);
  const SchedulingResources *scheduling_resource = nullptr;
  auto iter = gcs_resource_manager_->GetClusterResources().find(scheduling_node_id);
  if (iter != gcs_resource_manager_->GetClusterResources().end()) {
    scheduling_resource = &iter->second;
  }
  std::string scheduling_resource_str =
      scheduling_resource ? scheduling_resource->DebugString() : "None";
  // Return nullptr if the cluster resources are not enough.
  RAY_LOG(WARNING) << "No enough resources for creating actor "
                   << task_spec.ActorCreationId()
                   << "\nActor class: " << task_spec.FunctionDescriptor()->ToString()
                   << "\nRequired resources: " << required_resources.ToString()
                   << "\nThe node with the most resources is:"
                   << "\n   Node id: " << scheduling_node_id
                   << "\n   Node resources: " << scheduling_resource_str;

  RAY_LOG(DEBUG) << "Cluster resources: " << gcs_resource_manager_->ToString();
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
      if (reply.canceled()) {
        HandleRequestWorkerLeaseCanceled(actor, node_id, reply.failure_type());
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
  gcs_resource_manager_->UpdateNodeNormalTaskResources(node_id, reply.resources_data());
  gcs_resource_manager_->ReleaseResources(
      actor->GetActorWorkerAssignment()->GetNodeID(),
      actor->GetActorWorkerAssignment()->GetResources());
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
  if (gcs_resource_manager_->ReleaseResources(
          actor->GetActorWorkerAssignment()->GetNodeID(),
          actor->GetActorWorkerAssignment()->GetResources())) {
    NotifyClusterResourcesChanged();
  };
  actor->SetActorWorkerAssignment(nullptr);
}

void GcsBasedActorScheduler::OnActorDestruction(std::shared_ptr<GcsActor> actor) {
  if (actor && actor->GetActorWorkerAssignment()) {
    ResetActorWorkerAssignment(actor.get());
  }
}

}  // namespace gcs
}  // namespace ray