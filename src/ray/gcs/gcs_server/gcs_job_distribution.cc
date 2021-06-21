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

#include "ray/gcs/gcs_server/gcs_job_distribution.h"

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

}  // namespace gcs
}  // namespace ray