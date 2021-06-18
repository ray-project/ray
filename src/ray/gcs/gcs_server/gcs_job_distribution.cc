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

const ResourceSet &GcsActorWorkerAssignment::GetResources() const { return acquired_resources_; }

void GcsActorWorkerAssignment::SetResources(const ResourceSet &acquired_resources) {
  acquired_resources_ = acquired_resources;
}

std::string GcsActorWorkerAssignment::ToString(int indent /* = 0*/) const {
  std::ostringstream ostr;
  std::string indent_0(indent + 0 * 2, ' ');
  std::string indent_1(indent + 1 * 2, ' ');

  ostr << "{\n";
  ostr << indent_1 << "worker_process_id = " << worker_process_id_ << ",\n";
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
  std::unordered_map<std::string, FractionalResourceQuantity> resource_mapping;
  resource_mapping.emplace(kMemory_ResourceLabel,
                           job_config_.java_worker_process_default_memory_units_);
  ResourceSet constraint_resources(resource_mapping);
  auto worker_process = GcsActorWorkerAssignment::Create(
      NodeID::Nil(), job_config_.job_id_, Language::JAVA, constraint_resources,
      /*is_shared=*/true, job_config_.num_java_workers_per_process_);
  return AddActorWorkerAssignment(std::move(worker_process));
}

bool GcsJobSchedulingContext::AddActorWorkerAssignment(
    std::shared_ptr<GcsActorWorkerAssignment> worker_process) {
  RAY_CHECK(worker_process != nullptr);

  if (worker_process->IsShared()) {
    shared_worker_processes_.emplace(worker_process->GetActorWorkerAssignmentID(),
                                     worker_process);
  } else {
    sole_worker_processes_.emplace(worker_process->GetActorWorkerAssignmentID(), worker_process);
  }

  // Update the distribution of worker process on each node.
  UpdateNodeToActorWorkerAssignment(worker_process);

  // Acquire resources from job scheduling resources.
  // The resources should be return back to the `scheduling_resources_` when the worker
  // process is removed.
  scheduling_resources_.Acquire(constraint_resources);
  return true;
}

const GcsJobConfig &GcsJobSchedulingContext::GetJobConfig() const { return job_config_; }

const ActorWorkerAssignmentMap &GcsJobSchedulingContext::GetSharedActorWorkerAssignments() const {
  return shared_actor_worker_assignments_;
}

const NodeToActorWorkerAssignmentsMap &GcsJobSchedulingContext::GetNodeToActorWorkerAssignments()
    const {
  return node_to_worker_processes_;
}

bool GcsJobSchedulingContext::UpdateNodeToActorWorkerAssignment(
    std::shared_ptr<GcsActorWorkerAssignment> worker_process) {
  if (worker_process == nullptr || worker_process->IsDummy()) {
    return false;
  }

  if (worker_process->IsShared()) {
    if (!shared_worker_processes_.contains(worker_process->GetActorWorkerAssignmentID())) {
      return false;
    }
  } else {
    if (!sole_worker_processes_.contains(worker_process->GetActorWorkerAssignmentID())) {
      return false;
    }
  }

  // Update the distribution of worker processes on related node.
  return node_to_worker_processes_[worker_process->GetNodeID()]
      .emplace(worker_process->GetActorWorkerAssignmentID(), worker_process)
      .second;
}

std::shared_ptr<GcsActorWorkerAssignment>
GcsJobSchedulingContext::RemoveActorWorkerAssignmentByActorWorkerAssignmentID(
    const NodeID &node_id, const UniqueID &worker_process_id) {
  std::shared_ptr<GcsActorWorkerAssignment> removed_worker_process;
  auto iter = node_to_worker_processes_.find(node_id);
  if (iter != node_to_worker_processes_.end()) {
    auto &worker_processes = iter->second;
    auto worker_process_iter = worker_processes.find(worker_process_id);
    if (worker_process_iter != worker_processes.end()) {
      removed_worker_process = worker_process_iter->second;
      // Remove worker process associated with the specified node.
      if (removed_worker_process->IsShared()) {
        RAY_CHECK(
            shared_worker_processes_.erase(removed_worker_process->GetActorWorkerAssignmentID()));
      } else {
        RAY_CHECK(
            sole_worker_processes_.erase(removed_worker_process->GetActorWorkerAssignmentID()));
      }
      // Remove worker process from `worker_processes` to update the worker process
      // distribution on the specified node.
      worker_processes.erase(worker_process_iter);
      if (worker_processes.empty()) {
        // Remove entry from `node_to_worker_processes_` as `worker_processes` is
        // empty.
        node_to_worker_processes_.erase(iter);
      }
      // Return the resources of the removed worker process back to the job scheduling
      // resources.
      auto constraint_resources = removed_worker_process->GetConstraintResources();
      scheduling_resources_.Release(constraint_resources);
      RAY_LOG(DEBUG) << "Removed worker process "
                     << removed_worker_process->GetActorWorkerAssignmentID()
                     << ", Worker constraint resources: "
                     << constraint_resources.ToString() << ", Job constraint resources: "
                     << scheduling_resources_.GetAvailableResources().ToString();
    }
  }
  return removed_worker_process;
}

std::shared_ptr<GcsActorWorkerAssignment> GcsJobSchedulingContext::GetActorWorkerAssignmentById(
    const UniqueID &worker_process_id) const {
  auto iter = shared_worker_processes_.find(worker_process_id);
  if (iter != shared_worker_processes_.end()) {
    return iter->second;
  }

  iter = sole_worker_processes_.find(worker_process_id);
  if (iter != sole_worker_processes_.end()) {
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
    std::shared_ptr<GcsActorWorkerAssignment> worker_process) {
  RAY_CHECK(worker_process != nullptr);

  if (worker_process->IsDummy()) {
    return false;
  }

  auto job_id = worker_process->GetJobID();
  auto job_scheduling_context = GetJobSchedulingContext(job_id);
  if (job_scheduling_context == nullptr ||
      !job_scheduling_context->AddActorWorkerAssignment(worker_process)) {
    return false;
  }

  // Update the worker distribution on the related node.
  node_to_jobs_[worker_process->GetNodeID()].emplace(job_id);
  return true;
}

bool GcsJobDistribution::UpdateNodeToJob(
    std::shared_ptr<GcsActorWorkerAssignment> worker_process) {
  if (worker_process == nullptr) {
    return false;
  }

  auto job_scheduling_context = GetJobSchedulingContext(worker_process->GetJobID());
  if (job_scheduling_context == nullptr ||
      !job_scheduling_context->UpdateNodeToActorWorkerAssignment(worker_process)) {
    return false;
  }

  // Update the worker distribution on the related node.
  node_to_jobs_[worker_process->GetNodeID()].emplace(worker_process->GetJobID());
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
    const NodeID &node_id, const UniqueID &worker_process_id, const JobID &job_id) {
  std::shared_ptr<GcsActorWorkerAssignment> removed_worker_process = nullptr;
  if (auto job_scheduling_context = GetJobSchedulingContext(job_id)) {
    // Remove worker process associated with this node id and worker process id.
    removed_worker_process = job_scheduling_context->RemoveActorWorkerAssignmentByActorWorkerAssignmentID(
        node_id, worker_process_id);
    if (removed_worker_process != nullptr) {
      if (removed_worker_process->IsShared()) {
        // Add new dummy shared worker prcess to replace the removed one.
        // This way can make scheduling more balanced.
        RAY_CHECK(job_scheduling_context->AddDummySharedActorWorkerAssignment());
      }
      // Update the job distribution on each node.
      auto &node_to_worker_processes = job_scheduling_context->GetNodeToActorWorkerAssignments();
      if (!node_to_worker_processes.contains(node_id)) {
        RemoveJobFromNode(job_id, node_id);
      }
    }
  }
  return removed_worker_process;
}

void GcsJobDistribution::RemoveJobFromNode(const JobID &job_id, const NodeID &node_id) {
  auto iter = node_to_jobs_.find(node_id);
  if (iter != node_to_jobs_.end()) {
    if (iter->second.erase(job_id) && iter->second.empty()) {
      node_to_jobs_.erase(iter);
    }
  }
}

std::shared_ptr<GcsActorWorkerAssignment> GcsJobDistribution::GetActorWorkerAssignmentById(
    const JobID &job_id, const UniqueID &worker_process_id) const {
  std::shared_ptr<GcsActorWorkerAssignment> worker_process = nullptr;
  if (auto job_scheduling_context = GetJobSchedulingContext(job_id)) {
    worker_process = job_scheduling_context->GetActorWorkerAssignmentById(worker_process_id);
  }
  return worker_process;
}

}  // namespace gcs
}  // namespace ray