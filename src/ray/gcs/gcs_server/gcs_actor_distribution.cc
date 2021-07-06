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

#include "ray/common/task/scheduling_resources_util.h"
#include "ray/util/event.h"

namespace ray {

namespace gcs {

const UniqueID &GcsActorWorkerAssignment::GetActorWorkerAssignmentID() const {
  return actor_worker_assignment_id_;
}

const NodeID &GcsActorWorkerAssignment::GetNodeID() const { return node_id_; }

void GcsActorWorkerAssignment::SetNodeID(const NodeID &node_id) { node_id_ = node_id; }

const JobID &GcsActorWorkerAssignment::GetJobID() const { return job_id_; }

const Language &GcsActorWorkerAssignment::GetLanguage() const { return language_; }

size_t GcsActorWorkerAssignment::GetAvailableSlotCount() const {
  return slot_capacity_ - actor_ids_.size();
}

size_t GcsActorWorkerAssignment::GetUsedSlotCount() const { return actor_ids_.size(); }

bool GcsActorWorkerAssignment::IsShared() const { return is_shared_; }

bool GcsActorWorkerAssignment::IsDummy() const { return node_id_.IsNil(); }

bool GcsActorWorkerAssignment::AssignActor(const ActorID &actor_id) {
  return actor_ids_.size() < slot_capacity_ && actor_ids_.emplace(actor_id).second;
}

bool GcsActorWorkerAssignment::RemoveActor(const ActorID &actor_id) {
  return actor_ids_.erase(actor_id) != 0;
}

const ResourceSet &GcsActorWorkerAssignment::GetResources() const {
  return acquired_resources_;
}

void GcsActorWorkerAssignment::SetResources(const ResourceSet &acquired_resources) {
  acquired_resources_ = acquired_resources;
}

std::string GcsActorWorkerAssignment::ToString(int indent /* = 0*/) const {
  std::ostringstream ostr;
  std::string indent_0(indent + 0 * 2, ' ');
  std::string indent_1(indent + 1 * 2, ' ');

  ostr << "{\n";
  ostr << indent_1 << "actor_worker_assignment_id = " << actor_worker_assignment_id_
       << ",\n";
  ostr << indent_1 << "node_id = " << node_id_ << ",\n";
  ostr << indent_1 << "job_id = " << job_id_ << ",\n";
  ostr << indent_1 << "language = " << rpc::Language_Name(language_) << ",\n";
  ostr << indent_1 << "is_shared = " << is_shared_ << ",\n";
  ostr << indent_1 << "slot_capacity = " << slot_capacity_ << ",\n";
  ostr << indent_1 << "available_slot_count = " << GetAvailableSlotCount() << ",\n";
  ostr << indent_1 << "acquired_resources = " << acquired_resources_.ToString() << "\n";
  ostr << indent_0 << "},";
  return ostr.str();
}

//////////////////////////////////////////////////////////////////////
GcsJobSchedulingContext::GcsJobSchedulingContext(const GcsJobConfig &job_config)
    : job_config_(job_config) {
  std::unordered_map<std::string, double> resource_mapping;
  resource_mapping.emplace(kMemory_ResourceLabel, job_config_.total_memory_units_);
  scheduling_resources_ = SchedulingResources(ResourceSet(resource_mapping));

  for (uint32_t i = 0; i < job_config.num_initial_java_worker_processes_; ++i) {
    RAY_CHECK(AddDummySharedActorWorkerAssignment());
  }
}

bool GcsJobSchedulingContext::AddDummySharedActorWorkerAssignment() {
  std::unordered_map<std::string, FixedPoint> resource_mapping;
  resource_mapping.emplace(kMemory_ResourceLabel,
                           job_config_.java_worker_process_default_memory_units_);
  ResourceSet constraint_resources(resource_mapping);
  auto actor_worker_assignment = GcsActorWorkerAssignment::Create(
      NodeID::Nil(), job_config_.job_id_, Language::JAVA, constraint_resources,
      /*is_shared=*/true, job_config_.num_java_workers_per_process_);
  return AddActorWorkerAssignment(std::move(actor_worker_assignment));
}

bool GcsJobSchedulingContext::AddActorWorkerAssignment(
    std::shared_ptr<GcsActorWorkerAssignment> actor_worker_assignment) {
  RAY_CHECK(actor_worker_assignment != nullptr);

  if (actor_worker_assignment->IsShared()) {
    shared_actor_worker_assignments_.emplace(
        actor_worker_assignment->GetActorWorkerAssignmentID(), actor_worker_assignment);
  } else {
    sole_actor_worker_assignments_.emplace(
        actor_worker_assignment->GetActorWorkerAssignmentID(), actor_worker_assignment);
  }

  // Update the distribution of actor worker assignment on each node.
  UpdateNodeToActorWorkerAssignment(actor_worker_assignment);

  // TODO(Chong-Li):
  // Acquire resources from job scheduling resources.
  // The resources should be return back to the `scheduling_resources_` when the worker
  // process is removed.

  return true;
}

const GcsJobConfig &GcsJobSchedulingContext::GetJobConfig() const { return job_config_; }

const ActorWorkerAssignmentMap &GcsJobSchedulingContext::GetSharedActorWorkerAssignments()
    const {
  return shared_actor_worker_assignments_;
}

const NodeToActorWorkerAssignmentsMap &
GcsJobSchedulingContext::GetNodeToActorWorkerAssignments() const {
  return node_to_actor_worker_assignments_;
}

bool GcsJobSchedulingContext::UpdateNodeToActorWorkerAssignment(
    std::shared_ptr<GcsActorWorkerAssignment> actor_worker_assignment) {
  if (actor_worker_assignment == nullptr || actor_worker_assignment->IsDummy()) {
    return false;
  }

  if (actor_worker_assignment->IsShared()) {
    if (!shared_actor_worker_assignments_.contains(
            actor_worker_assignment->GetActorWorkerAssignmentID())) {
      return false;
    }
  } else {
    if (!sole_actor_worker_assignments_.contains(
            actor_worker_assignment->GetActorWorkerAssignmentID())) {
      return false;
    }
  }

  // Update the distribution of actor worker assignments on related node.
  return node_to_actor_worker_assignments_[actor_worker_assignment->GetNodeID()]
      .emplace(actor_worker_assignment->GetActorWorkerAssignmentID(),
               actor_worker_assignment)
      .second;
}

std::shared_ptr<GcsActorWorkerAssignment>
GcsJobSchedulingContext::RemoveActorWorkerAssignmentByActorWorkerAssignmentID(
    const NodeID &node_id, const UniqueID &actor_worker_assignment_id) {
  std::shared_ptr<GcsActorWorkerAssignment> removed_actor_worker_assignment;
  auto iter = node_to_actor_worker_assignments_.find(node_id);
  if (iter != node_to_actor_worker_assignments_.end()) {
    auto &actor_worker_assignments = iter->second;
    auto actor_worker_assignment_iter =
        actor_worker_assignments.find(actor_worker_assignment_id);
    if (actor_worker_assignment_iter != actor_worker_assignments.end()) {
      removed_actor_worker_assignment = actor_worker_assignment_iter->second;
      // Remove actor worker assignment associated with the specified node.
      if (removed_actor_worker_assignment->IsShared()) {
        RAY_CHECK(shared_actor_worker_assignments_.erase(
            removed_actor_worker_assignment->GetActorWorkerAssignmentID()));
      } else {
        RAY_CHECK(sole_actor_worker_assignments_.erase(
            removed_actor_worker_assignment->GetActorWorkerAssignmentID()));
      }
      // Remove actor worker assignment from `actor_worker_assignments` to update the
      // actor worker assignment distribution on the specified node.
      actor_worker_assignments.erase(actor_worker_assignment_iter);
      if (actor_worker_assignments.empty()) {
        // Remove entry from `node_to_actor_worker_assignments_` as
        // `actor_worker_assignments` is empty.
        node_to_actor_worker_assignments_.erase(iter);
      }
      // TODO(Chong-Li):
      // Return the resources of the removed actor worker assignment back to the job
      // scheduling resources.
    }
  }
  return removed_actor_worker_assignment;
}

std::shared_ptr<GcsActorWorkerAssignment>
GcsJobSchedulingContext::GetActorWorkerAssignmentById(
    const UniqueID &actor_worker_assignment_id) const {
  auto iter = shared_actor_worker_assignments_.find(actor_worker_assignment_id);
  if (iter != shared_actor_worker_assignments_.end()) {
    return iter->second;
  }

  iter = sole_actor_worker_assignments_.find(actor_worker_assignment_id);
  if (iter != sole_actor_worker_assignments_.end()) {
    return iter->second;
  }

  return nullptr;
}

//////////////////////////////////////////////////////////////////////
GcsJobDistribution::GcsJobDistribution(
    std::function<std::shared_ptr<GcsJobSchedulingContext>(const JobID &)>
        gcs_job_scheduling_factory)
    : gcs_job_scheduling_factory_(std::move(gcs_job_scheduling_factory)) {
  RAY_CHECK(gcs_job_scheduling_factory_ != nullptr);
}

bool GcsJobDistribution::AddActorWorkerAssignment(
    std::shared_ptr<GcsActorWorkerAssignment> actor_worker_assignment) {
  RAY_CHECK(actor_worker_assignment != nullptr);

  if (actor_worker_assignment->IsDummy()) {
    return false;
  }

  auto job_id = actor_worker_assignment->GetJobID();
  auto job_scheduling_context = GetJobSchedulingContext(job_id);
  if (job_scheduling_context == nullptr ||
      !job_scheduling_context->AddActorWorkerAssignment(actor_worker_assignment)) {
    return false;
  }

  // Update the worker distribution on the related node.
  node_to_jobs_[actor_worker_assignment->GetNodeID()].emplace(job_id);
  return true;
}

bool GcsJobDistribution::UpdateNodeToJob(
    std::shared_ptr<GcsActorWorkerAssignment> actor_worker_assignment) {
  if (actor_worker_assignment == nullptr) {
    return false;
  }

  auto job_scheduling_context =
      GetJobSchedulingContext(actor_worker_assignment->GetJobID());
  if (job_scheduling_context == nullptr ||
      !job_scheduling_context->UpdateNodeToActorWorkerAssignment(
          actor_worker_assignment)) {
    return false;
  }

  // Update the worker distribution on the related node.
  node_to_jobs_[actor_worker_assignment->GetNodeID()].emplace(
      actor_worker_assignment->GetJobID());
  return true;
}

std::shared_ptr<GcsJobSchedulingContext> GcsJobDistribution::GetJobSchedulingContext(
    const JobID &job_id) const {
  auto iter = job_scheduling_contexts_.find(job_id);
  return iter == job_scheduling_contexts_.end() ? nullptr : iter->second;
}

std::shared_ptr<GcsJobSchedulingContext>
GcsJobDistribution::FindOrCreateJobSchedulingContext(const JobID &job_id) {
  auto iter = job_scheduling_contexts_.find(job_id);
  if (iter == job_scheduling_contexts_.end()) {
    auto job_scheduling_context = gcs_job_scheduling_factory_(job_id);
    RAY_LOG(INFO) << "Create a new job scheduling context: "
                  << job_scheduling_context->GetJobConfig().ToString();
    iter = job_scheduling_contexts_.emplace(job_id, job_scheduling_context).first;
  }
  return iter->second;
}

std::shared_ptr<GcsActorWorkerAssignment>
GcsJobDistribution::RemoveActorWorkerAssignmentByActorWorkerAssignmentID(
    const NodeID &node_id, const UniqueID &actor_worker_assignment_id,
    const JobID &job_id) {
  std::shared_ptr<GcsActorWorkerAssignment> removed_actor_worker_assignment = nullptr;
  if (auto job_scheduling_context = GetJobSchedulingContext(job_id)) {
    // Remove actor worker assignment associated with this node id and actor worker
    // assignment id.
    removed_actor_worker_assignment =
        job_scheduling_context->RemoveActorWorkerAssignmentByActorWorkerAssignmentID(
            node_id, actor_worker_assignment_id);
    if (removed_actor_worker_assignment != nullptr) {
      if (removed_actor_worker_assignment->IsShared()) {
        // Add new dummy shared worker prcess to replace the removed one.
        // This way can make scheduling more balanced.
        RAY_CHECK(job_scheduling_context->AddDummySharedActorWorkerAssignment());
      }
      // Update the job distribution on each node.
      auto &node_to_actor_worker_assignments =
          job_scheduling_context->GetNodeToActorWorkerAssignments();
      if (!node_to_actor_worker_assignments.contains(node_id)) {
        RemoveJobFromNode(job_id, node_id);
      }
    }
  }
  return removed_actor_worker_assignment;
}

void GcsJobDistribution::RemoveJobFromNode(const JobID &job_id, const NodeID &node_id) {
  auto iter = node_to_jobs_.find(node_id);
  if (iter != node_to_jobs_.end()) {
    if (iter->second.erase(job_id) && iter->second.empty()) {
      node_to_jobs_.erase(iter);
    }
  }
}

std::shared_ptr<GcsActorWorkerAssignment>
GcsJobDistribution::GetActorWorkerAssignmentById(
    const JobID &job_id, const UniqueID &actor_worker_assignment_id) const {
  std::shared_ptr<GcsActorWorkerAssignment> actor_worker_assignment = nullptr;
  if (auto job_scheduling_context = GetJobSchedulingContext(job_id)) {
    actor_worker_assignment =
        job_scheduling_context->GetActorWorkerAssignmentById(actor_worker_assignment_id);
  }
  return actor_worker_assignment;
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