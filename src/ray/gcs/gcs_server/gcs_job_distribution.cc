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
#include "ray/util/resource_util.h"

namespace ray {

namespace gcs {

const UniqueID &GcsActorWorkerAssignment::GetActorWorkerAssignmentID() const {
  return actor_worker_assignment_id_;
}

const int32_t &GcsWorkerProcess::GetPID() const { return pid_; }

void GcsWorkerProcess::SetPID(const int32_t &pid) { pid_ = pid; }

const NodeID &GcsWorkerProcess::GetNodeID() const { return node_id_; }

void GcsWorkerProcess::SetNodeID(const NodeID &node_id) { node_id_ = node_id; }

const JobID &GcsWorkerProcess::GetJobID() const { return job_id_; }

const Language &GcsWorkerProcess::GetLanguage() const { return language_; }

size_t GcsWorkerProcess::GetAvailableSlotCount() const {
  return slot_capacity_ - actor_ids_.size();
}

size_t GcsWorkerProcess::GetUsedSlotCount() const { return actor_ids_.size(); }

bool GcsWorkerProcess::IsShared() const { return is_shared_; }

bool GcsWorkerProcess::IsDummy() const { return node_id_.IsNil(); }

size_t GcsWorkerProcess::GetSlotCapacity() const { return slot_capacity_; }

bool GcsWorkerProcess::AssignActor(const ActorID &actor_id) {
  return actor_ids_.size() < slot_capacity_ && actor_ids_.emplace(actor_id).second;
}

const absl::flat_hash_set<ActorID> &GcsWorkerProcess::GetAssignedActors() const {
  return actor_ids_;
}

bool GcsWorkerProcess::RemoveActor(const ActorID &actor_id) {
  return actor_ids_.erase(actor_id) != 0;
}

const ResourceSet &GcsWorkerProcess::GetResources() const { return acquired_resources_; }

void GcsWorkerProcess::SetResources(const ResourceSet &acquired_resources) {
  acquired_resources_ = acquired_resources;
}

ResourceSet GcsWorkerProcess::GetConstraintResources() {
  return ray::GetConstraintResources(acquired_resources_);
}

bool GcsWorkerProcess::UpdateResources(const ResourceSet &constraint_resources) {
  if (constraint_resources == GetConstraintResources()) {
    return false;
  }
  auto old_resources = acquired_resources_;
  for (auto &entry : constraint_resources.GetResourceAmountMap()) {
    acquired_resources_.AddOrUpdateResource(entry.first, entry.second);
  }
  for (auto &listener : listeners_) {
    listener(node_id_, old_resources, acquired_resources_);
  }
  return true;
}

void GcsWorkerProcess::AddResourceChangedListener(
    WorkerProcessResourceChangedListener listener) {
  RAY_CHECK(listener != nullptr);
  listeners_.emplace_back(std::move(listener));
}

std::string GcsWorkerProcess::ToString(int indent /* = 0*/) const {
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

void GcsWorkerProcess::AsyncFlush(
    const std::shared_ptr<gcs::GcsTableStorage> &gcs_table_storage,
    std::function<void(const ray::Status &)> callback) {
  if (worker_process_status_ == WorkerProcessStatus::FLUSHED && callback) {
    callback(Status::OK());
    return;
  }

  flush_callbacks_.emplace_back(std::move(callback));
  if (worker_process_status_ == WorkerProcessStatus::IN_MEMORY) {
    worker_process_status_ = WorkerProcessStatus::FLUSHING;
    auto worker_process_data = BuildWorkerProcessData();
    std::weak_ptr<GcsWorkerProcess> wk_worker_process(shared_from_this());
    RAY_CHECK_OK(gcs_table_storage->WorkerProcessTable().Put(
        worker_process_id_, *worker_process_data,
        [this, wk_worker_process](const Status &status) {
          RAY_CHECK_OK(status);
          if (auto worker_process = wk_worker_process.lock()) {
            worker_process_status_ = WorkerProcessStatus::FLUSHED;
            for (auto &callback : flush_callbacks_) {
              if (callback) {
                callback(Status::OK());
              }
            }
            flush_callbacks_.clear();
          }
        }));
  }
}

bool GcsWorkerProcess::EqualsTo(std::shared_ptr<GcsWorkerProcess> other) {
  if (other == nullptr) {
    return false;
  }

  return worker_process_id_ == other->worker_process_id_ && node_id_ == other->node_id_ &&
         job_id_ == other->job_id_ && is_shared_ == other->is_shared_ &&
         slot_capacity_ == other->slot_capacity_ &&
         acquired_resources_.IsEqual(other->acquired_resources_);
}

std::shared_ptr<rpc::GcsWorkerProcessTableData> GcsWorkerProcess::BuildWorkerProcessData()
    const {
  auto worker_process_data = std::make_shared<rpc::GcsWorkerProcessTableData>();
  worker_process_data->set_worker_process_id(worker_process_id_.Binary());
  worker_process_data->set_node_id(node_id_.Binary());
  worker_process_data->set_job_id(job_id_.Binary());
  worker_process_data->set_language(language_);
  worker_process_data->set_is_shared(is_shared_);
  worker_process_data->set_slot_capacity(slot_capacity_);
  worker_process_data->set_pid(pid_);

  auto mutable_acquired_resources = worker_process_data->mutable_acquired_resources();
  for (auto &pair : acquired_resources_.GetResourceAmountMap()) {
    (*mutable_acquired_resources)[pair.first] = pair.second.ToDouble();
  }
  return worker_process_data;
}

//////////////////////////////////////////////////////////////////////
GcsJobSchedulingContext::GcsJobSchedulingContext(
    const GcsJobConfig &job_config, std::shared_ptr<ScheduleOptions> schedule_options)
    : job_config_(job_config), schedule_options_(std::move(schedule_options)) {
  std::unordered_map<std::string, double> resource_mapping;
  resource_mapping.emplace(kMemory_ResourceLabel, job_config_.total_memory_units_);
  scheduling_resources_ = SchedulingResources(ResourceSet(resource_mapping));

  for (uint32_t i = 0; i < job_config.num_initial_java_worker_processes_; ++i) {
    RAY_CHECK(AddDummySharedWorkerProcess());
  }
}

bool GcsJobSchedulingContext::AddDummySharedWorkerProcess() {
  std::unordered_map<std::string, FractionalResourceQuantity> resource_mapping;
  resource_mapping.emplace(kMemory_ResourceLabel,
                           job_config_.java_worker_process_default_memory_units_);
  ResourceSet constraint_resources(resource_mapping);
  auto worker_process = GcsWorkerProcess::Create(
      NodeID::Nil(), job_config_.job_id_, Language::JAVA, constraint_resources,
      /*is_shared=*/true, job_config_.num_java_workers_per_process_);
  return AddWorkerProcess(std::move(worker_process));
}

std::shared_ptr<GcsWorkerProcess>
GcsJobSchedulingContext::TryRemoveOneDummySharedWorkerProcess() {
  std::shared_ptr<GcsWorkerProcess> dummy_worker_process;
  auto iter = std::find_if(
      shared_worker_processes_.begin(), shared_worker_processes_.end(),
      [](const std::pair<UniqueID, std::shared_ptr<GcsWorkerProcess>> &entry) {
        return entry.second->IsDummy();
      });
  if (iter != shared_worker_processes_.end()) {
    // Return the resources of the worker process back to the job scheduling resources.
    dummy_worker_process = iter->second;
    auto constraint_resources = dummy_worker_process->GetConstraintResources();
    scheduling_resources_.Release(constraint_resources);
    shared_worker_processes_.erase(iter);
  }
  return dummy_worker_process;
}

bool GcsJobSchedulingContext::AddWorkerProcess(
    std::shared_ptr<GcsWorkerProcess> worker_process) {
  RAY_CHECK(worker_process != nullptr);

  const auto &constraint_resources = worker_process->GetConstraintResources();
  if (!constraint_resources.IsSubset(scheduling_resources_.GetAvailableResources())) {
    // Return false if the job available resources are not satisfied with the worker
    // process.
    return false;
  }

  if (worker_process->IsShared()) {
    shared_worker_processes_.emplace(worker_process->GetWorkerProcessID(),
                                     worker_process);
  } else {
    sole_worker_processes_.emplace(worker_process->GetWorkerProcessID(), worker_process);
  }

  // Update the distribution of worker process on each node.
  UpdateNodeToWorkerProcess(worker_process);

  // Acquire resources from job scheduling resources.
  // The resources should be return back to the `scheduling_resources_` when the worker
  // process is removed.
  scheduling_resources_.Acquire(constraint_resources);
  return true;
}

const ResourceSet &GcsJobSchedulingContext::GetAvailableResources() const {
  return scheduling_resources_.GetAvailableResources();
}

const SchedulingResources &GcsJobSchedulingContext::GetSchedulingResources() const {
  return scheduling_resources_;
}

const GcsJobConfig &GcsJobSchedulingContext::GetJobConfig() const { return job_config_; }

GcsJobConfig *GcsJobSchedulingContext::GetMutableJobConfig() { return &job_config_; }

std::shared_ptr<ScheduleOptions> GcsJobSchedulingContext::GetScheduleOptions() const {
  return schedule_options_;
}

Status GcsJobSchedulingContext::UpdateJobTotalResources(
    const rpc::JobTableData &job_table_data) {
  std::unordered_map<std::string, double> resource_map;
  resource_map.emplace(kMemory_ResourceLabel,
                       job_table_data.config().total_memory_units());
  // resource_map.emplace("cpu", job_table_data.config().total_cpus());
  // resource_map.emplace("gpu", job_table_data.config().total_gpus());
  ResourceSet new_total_memory(resource_map);

  auto total_resources = scheduling_resources_.GetTotalResources();
  auto available_resources = scheduling_resources_.GetAvailableResources();

  ResourceSet used_resources = total_resources;
  used_resources.SubtractResources(available_resources);

  ResourceSet total_memory = total_resources.GetMemory();
  ResourceSet available_memory = available_resources.GetMemory();
  ResourceSet current_used_memory = used_resources.GetMemory();

  RAY_LOG(INFO) << "UpdateJobTotalResources:"
                << "\n -- total_memory: " << total_memory.ToString()
                << "\n -- available_memory: " << available_memory.ToString()
                << "\n -- used_memory: " << current_used_memory.ToString();

  if (!current_used_memory.IsSubset(new_total_memory)) {
    std::ostringstream ss;
    ss << "Failed to udpate job total resources as the required resource are smaller "
          "than the job's currently-used resources."
       << "\nJobID: " << JobID::FromBinary(job_table_data.job_id())
       << "\nRequired resources: " << new_total_memory.ToString()
       << "\nCurrently-used resources: " << current_used_memory.ToString();
    std::string message = ss.str();
    RAY_LOG(WARNING) << message;
    RAY_EVENT(ERROR, EVENT_LABEL_UPDATE_JOB_TOTAL_RESOURCES_FAILED) << message;
    return Status::Invalid(message);
  }

  total_resources.AddOrUpdateResource(
      kMemory_ResourceLabel, new_total_memory.GetResource(kMemory_ResourceLabel));
  scheduling_resources_.SetTotalResources(std::move(total_resources));

  ResourceSet new_available_memory(new_total_memory);
  // Should substract used memory, the current_used_memory contains both PG used memory
  // and Non-PG used memory.
  new_available_memory.SubtractResources(current_used_memory);
  available_resources.AddOrUpdateResource(
      kMemory_ResourceLabel, new_available_memory.GetResource(kMemory_ResourceLabel));
  scheduling_resources_.SetAvailableResources(std::move(available_resources));

  job_config_.total_memory_units_ = job_table_data.config().total_memory_units();
  return Status::OK();
}

const WorkerProcessMap &GcsJobSchedulingContext::GetSharedWorkerProcesses() const {
  return shared_worker_processes_;
}

const WorkerProcessMap &GcsJobSchedulingContext::GetSoleWorkerProcesses() const {
  return sole_worker_processes_;
}

const NodeToWorkerProcessesMap &GcsJobSchedulingContext::GetNodeToWorkerProcesses()
    const {
  return node_to_worker_processes_;
}

bool GcsJobSchedulingContext::UpdateNodeToWorkerProcess(
    std::shared_ptr<GcsWorkerProcess> worker_process) {
  if (worker_process == nullptr || worker_process->IsDummy()) {
    return false;
  }

  if (worker_process->IsShared()) {
    if (!shared_worker_processes_.contains(worker_process->GetWorkerProcessID())) {
      return false;
    }
  } else {
    if (!sole_worker_processes_.contains(worker_process->GetWorkerProcessID())) {
      return false;
    }
  }

  // Update the distribution of worker processes on related node.
  return node_to_worker_processes_[worker_process->GetNodeID()]
      .emplace(worker_process->GetWorkerProcessID(), worker_process)
      .second;
}

absl::flat_hash_set<std::shared_ptr<GcsWorkerProcess>>
GcsJobSchedulingContext::RemoveWorkerProcessesByNodeID(const NodeID &node_id) {
  absl::flat_hash_set<std::shared_ptr<GcsWorkerProcess>> removed_worker_processes;
  auto iter = node_to_worker_processes_.find(node_id);
  if (iter != node_to_worker_processes_.end()) {
    auto worker_processes = std::move(iter->second);
    for (auto &entry : worker_processes) {
      removed_worker_processes.emplace(entry.second);
      // Remove worker process associated with the specified node.
      if (entry.second->IsShared()) {
        RAY_CHECK(shared_worker_processes_.erase(entry.second->GetWorkerProcessID()));
      } else {
        RAY_CHECK(sole_worker_processes_.erase(entry.second->GetWorkerProcessID()));
      }

      // Return the resources of the worker process back to the job scheduling resources.
      auto constraint_resources = entry.second->GetConstraintResources();
      scheduling_resources_.Release(constraint_resources);
      RAY_LOG(DEBUG) << "Removed worker process " << entry.first
                     << ", Worker constraint resources: "
                     << constraint_resources.ToString() << ", Job constraint resources: "
                     << scheduling_resources_.GetAvailableResources().ToString();
    }
    // Remove the entry from `node_to_worker_processes_`.
    node_to_worker_processes_.erase(iter);
  }
  return removed_worker_processes;
}

std::shared_ptr<GcsWorkerProcess>
GcsJobSchedulingContext::RemoveWorkerProcessByWorkerProcessID(
    const NodeID &node_id, const UniqueID &worker_process_id) {
  std::shared_ptr<GcsWorkerProcess> removed_worker_process;
  auto iter = node_to_worker_processes_.find(node_id);
  if (iter != node_to_worker_processes_.end()) {
    auto &worker_processes = iter->second;
    auto worker_process_iter = worker_processes.find(worker_process_id);
    if (worker_process_iter != worker_processes.end()) {
      removed_worker_process = worker_process_iter->second;
      // Remove worker process associated with the specified node.
      if (removed_worker_process->IsShared()) {
        RAY_CHECK(
            shared_worker_processes_.erase(removed_worker_process->GetWorkerProcessID()));
      } else {
        RAY_CHECK(
            sole_worker_processes_.erase(removed_worker_process->GetWorkerProcessID()));
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
                     << removed_worker_process->GetWorkerProcessID()
                     << ", Worker constraint resources: "
                     << constraint_resources.ToString() << ", Job constraint resources: "
                     << scheduling_resources_.GetAvailableResources().ToString();
    }
  }
  return removed_worker_process;
}

std::shared_ptr<GcsWorkerProcess> GcsJobSchedulingContext::GetWorkerProcess(
    const NodeID &node_id, const UniqueID &worker_process_id) const {
  auto iter = node_to_worker_processes_.find(node_id);
  if (iter != node_to_worker_processes_.end()) {
    auto &worker_processes = iter->second;
    auto worker_process_iter = worker_processes.find(worker_process_id);
    if (worker_process_iter != worker_processes.end()) {
      return worker_process_iter->second;
    }
  }
  return nullptr;
}

bool GcsJobSchedulingContext::ReserveBundlesResources(
    std::vector<std::shared_ptr<BundleSpecification>> bundles) {
  std::vector<std::shared_ptr<BundleSpecification>> prepared_bundles;
  prepared_bundles.reserve(bundles.size());

  for (auto &bundle : bundles) {
    const auto &bundle_id = bundle->BundleId();
    auto required_resources = ray::GetConstraintResources(bundle->GetRequiredResources());
    auto prepare_res = scheduling_resources_.PrepareBundleResources(
        bundle_id.first, bundle_id.second, required_resources);
    if (prepare_res) {
      // If prepare resource successful, we must first commit bundle or we can't return
      // resource when prepare failed.
      scheduling_resources_.CommitBundleResources(bundle_id.first, bundle_id.second,
                                                  required_resources);
      prepared_bundles.emplace_back(bundle);
    } else {
      // If prepare resource failed, we will release the previous prepared bundle.
      ReturnBundlesResources(prepared_bundles);
      return false;
    }
  }

  return true;
}

void GcsJobSchedulingContext::ReturnBundlesResources(
    std::vector<std::shared_ptr<BundleSpecification>> bundles) {
  for (auto &bundle : bundles) {
    const auto &bundle_id = bundle->BundleId();
    auto required_resources = ray::GetConstraintResources(bundle->GetRequiredResources());
    scheduling_resources_.ReturnBundleResources(bundle_id.first, bundle_id.second);
  }
}

void GcsJobSchedulingContext::OnJobResourcesInsufficiant(
    const ResourceSet &constraint_resources, const TaskSpecification &task_spec) {
  auto get_current_actor_num = [this] {
    int shared_actor_num = std::accumulate(
        std::begin(shared_worker_processes_), std::end(shared_worker_processes_), 0,
        [](int value, const WorkerProcessMap::value_type &p) {
          return value + p.second->GetAssignedActors().size();
        });

    int solo_actor_num = std::accumulate(
        std::begin(sole_worker_processes_), std::end(sole_worker_processes_), 0,
        [](int value, const WorkerProcessMap::value_type &p) {
          return value + p.second->GetAssignedActors().size();
        });

    return shared_actor_num + solo_actor_num;
  };

  std::ostringstream ostr;
  ostr << "Failed to schedule the actor " << task_spec.ActorCreationId()
       << " as the declared total resources of the job are too few."
       << "\nActor class: " << task_spec.FunctionDescriptor()->ToString()
       << "\nJob id: " << job_config_.job_id_
       << "\nAssigned actor count: " << get_current_actor_num()
       << "\nRequired resources: " << constraint_resources.ToString()
       << "\nJob available resources: "
       << scheduling_resources_.GetAvailableResources().ToString()
       << "\nJob total resources: "
       << scheduling_resources_.GetTotalResources().ToString();
  std::string message = ostr.str();
  RAY_LOG(WARNING) << message;

  uint64_t now = current_time_ms();
  uint64_t report_interval_ms =
      RayConfig::instance().job_resources_exhausted_report_interval_ms();
  if (last_report_time_ms_ + report_interval_ms < now) {
    RAY_EVENT(ERROR, EVENT_LABEL_JOB_RESOURCES_ARE_EXHAUSTED)
            .WithField("job_id", job_config_.job_id_.Hex())
        << message;
    last_report_time_ms_ = now;
  }
}

std::string GcsJobSchedulingContext::ToString(int indent /* = 0*/) const {
  std::ostringstream ostr;
  std::string indent_0(indent + 0 * 2, ' ');
  std::string indent_1(indent + 1 * 2, ' ');
  std::string indent_2(indent + 2 * 2, ' ');

  ostr << "{\n";
  ostr << indent_1 << "job_config = {\n";
  ostr << indent_2 << "job_id = " << job_config_.job_id_ << ",\n";
  ostr << indent_2 << "num_initial_java_worker_processes = "
       << job_config_.num_initial_java_worker_processes_ << ",\n";
  ostr << indent_2
       << "num_java_workers_per_process = " << job_config_.num_java_workers_per_process_
       << ",\n";
  ostr << indent_2 << "java_worker_process_default_memory_gb = "
       << FromMemoryUnitsToGiB(job_config_.java_worker_process_default_memory_units_)
       << ",\n";
  ostr << indent_2 << "python_worker_process_default_memory_gb = "
       << FromMemoryUnitsToGiB(job_config_.python_worker_process_default_memory_units_)
       << ",\n";
  ostr << indent_2
       << "total_memory_gb = " << FromMemoryUnitsToGiB(job_config_.total_memory_units_)
       << ",\n";
  ostr << indent_1 << "},\n";

  ostr << indent_1 << "shared_worker_processes = [";
  for (const auto &entry : shared_worker_processes_) {
    ostr << entry.second->ToString(4);
  }
  ostr << indent_1 << "],\n";

  ostr << indent_1 << "sole_worker_processes = [";
  for (const auto &entry : sole_worker_processes_) {
    ostr << entry.second->ToString(4);
  }
  ostr << "\n" << indent_1 << "],\n";

  ostr << indent_0 << "},\n";
  return ostr.str();
}

std::shared_ptr<GcsWorkerProcess> GcsJobSchedulingContext::GetWorkerProcessById(
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

bool GcsJobDistribution::AddWorkerProcess(
    std::shared_ptr<GcsWorkerProcess> worker_process) {
  RAY_CHECK(worker_process != nullptr);

  if (worker_process->IsDummy()) {
    return false;
  }

  auto job_id = worker_process->GetJobID();
  auto job_scheduling_context = GetJobSchedulingContext(job_id);
  if (job_scheduling_context == nullptr ||
      !job_scheduling_context->AddWorkerProcess(worker_process)) {
    return false;
  }

  // Update the worker distribution on the related node.
  node_to_jobs_[worker_process->GetNodeID()].emplace(job_id);
  return true;
}

bool GcsJobDistribution::UpdateNodeToJob(
    std::shared_ptr<GcsWorkerProcess> worker_process) {
  if (worker_process == nullptr) {
    return false;
  }

  auto job_scheduling_context = GetJobSchedulingContext(worker_process->GetJobID());
  if (job_scheduling_context == nullptr ||
      !job_scheduling_context->UpdateNodeToWorkerProcess(worker_process)) {
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

std::shared_ptr<GcsJobSchedulingContext> GcsJobDistribution::RemoveJobSchedulingContext(
    const JobID &job_id) {
  std::shared_ptr<GcsJobSchedulingContext> job_scheduling_context;
  // Remove the job scheduling context to prevent memory from leaking.
  auto iter = job_scheduling_contexts_.find(job_id);
  if (iter != job_scheduling_contexts_.end()) {
    job_scheduling_context = std::move(iter->second);
    job_scheduling_contexts_.erase(iter);
  }

  if (job_scheduling_context != nullptr) {
    // Update the job distribution on each node.
    const auto &node_to_worker_processes =
        job_scheduling_context->GetNodeToWorkerProcesses();
    for (auto &entry : node_to_worker_processes) {
      RemoveJobFromNode(job_id, entry.first);
    }
  }
  return job_scheduling_context;
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

absl::flat_hash_set<std::shared_ptr<GcsWorkerProcess>>
GcsJobDistribution::RemoveWorkerProcessesByNodeID(const NodeID &node_id) {
  absl::flat_hash_set<std::shared_ptr<GcsWorkerProcess>> removed_worker_processes;
  auto node_to_jobs_iter = node_to_jobs_.find(node_id);
  if (node_to_jobs_iter != node_to_jobs_.end()) {
    // Update the job distribution on the specified node.
    for (const auto &job_id : node_to_jobs_iter->second) {
      auto job_scheduling_context = GetJobSchedulingContext(job_id);
      RAY_CHECK(job_scheduling_context != nullptr);
      // Remove worker processes associated with this node id.
      auto worker_processes =
          job_scheduling_context->RemoveWorkerProcessesByNodeID(node_id);
      for (auto &worker_process : worker_processes) {
        if (worker_process->IsShared()) {
          // Add new dummy shared worker prcess to replace the removed one.
          // This way can make scheduling more balanced.
          RAY_CHECK(job_scheduling_context->AddDummySharedWorkerProcess());
        }
      }
      removed_worker_processes.insert(worker_processes.begin(), worker_processes.end());
    }
    node_to_jobs_.erase(node_to_jobs_iter);
  }
  return removed_worker_processes;
}

std::shared_ptr<GcsWorkerProcess>
GcsJobDistribution::RemoveWorkerProcessByWorkerProcessID(
    const NodeID &node_id, const UniqueID &worker_process_id, const JobID &job_id) {
  std::shared_ptr<GcsWorkerProcess> removed_worker_process = nullptr;
  if (auto job_scheduling_context = GetJobSchedulingContext(job_id)) {
    // Remove worker process associated with this node id and worker process id.
    removed_worker_process = job_scheduling_context->RemoveWorkerProcessByWorkerProcessID(
        node_id, worker_process_id);
    if (removed_worker_process != nullptr) {
      if (removed_worker_process->IsShared()) {
        // Add new dummy shared worker prcess to replace the removed one.
        // This way can make scheduling more balanced.
        RAY_CHECK(job_scheduling_context->AddDummySharedWorkerProcess());
      }
      // Update the job distribution on each node.
      auto &node_to_worker_processes = job_scheduling_context->GetNodeToWorkerProcesses();
      if (!node_to_worker_processes.contains(node_id)) {
        RemoveJobFromNode(job_id, node_id);
      }
    }
  }
  return removed_worker_process;
}

std::vector<rpc::NodeInfo> GcsJobDistribution::GetJobDistribution() const {
  std::unordered_map<NodeID, rpc::NodeInfo> node_info_map;
  for (const auto &it : node_to_jobs_) {
    rpc::NodeInfo n;
    n.set_node_id(it.first.Binary());
    node_info_map[it.first] = n;
  }
  for (const auto &jsc : job_scheduling_contexts_) {
    WorkerProcessMap worker_processes;
    worker_processes.insert(jsc.second->GetSharedWorkerProcesses().begin(),
                            jsc.second->GetSharedWorkerProcesses().end());
    worker_processes.insert(jsc.second->GetSoleWorkerProcesses().begin(),
                            jsc.second->GetSoleWorkerProcesses().end());
    std::unordered_map<NodeID, rpc::JobInfo> node_to_job_info;
    for (const auto &entry : worker_processes) {
      const auto &worker_process = entry.second;
      if (worker_process->IsDummy()) {
        continue;
      }
      rpc::WorkerInfo worker_info;
      auto node_id = worker_process->GetNodeID();
      auto job_id = worker_process->GetJobID();
      worker_info.set_worker_process_id(worker_process->GetWorkerProcessID().Binary());
      worker_info.set_is_shared(worker_process->IsShared());
      worker_info.set_slot_capacity(worker_process->GetSlotCapacity());
      worker_info.set_pid(worker_process->GetPID());
      worker_info.set_language(worker_process->GetLanguage());

      for (auto r : worker_process->GetResources().GetResourceMap()) {
        (*worker_info.mutable_acquired_resources())[r.first] = r.second;
      }

      auto it = node_to_job_info.find(node_id);
      if (it == node_to_job_info.end()) {
        rpc::JobInfo job_info;
        job_info.set_job_id(job_id.Binary());
        it = node_to_job_info.emplace(node_id, std::move(job_info)).first;
      }
      it->second.add_worker_info_list()->CopyFrom(worker_info);
    }

    for (const auto &it : node_to_job_info) {
      node_info_map[it.first].add_job_info_list()->CopyFrom(it.second);
    }
  }

  std::vector<rpc::NodeInfo> node_info_list;
  for (const auto &it : node_info_map) {
    node_info_list.push_back(it.second);
  }
  return node_info_list;
}

std::vector<rpc::NodeInfoAgg> GcsJobDistribution::GetJobDistributionAgg() const {
  std::unordered_map<NodeID, rpc::NodeInfoAgg> node_info_map;
  for (const auto &it : node_to_jobs_) {
    rpc::NodeInfoAgg n;
    n.set_node_id(it.first.Binary());
    node_info_map[it.first] = n;
  }
  for (const auto &jsc : job_scheduling_contexts_) {
    WorkerProcessMap worker_processes;
    worker_processes.insert(jsc.second->GetSharedWorkerProcesses().begin(),
                            jsc.second->GetSharedWorkerProcesses().end());
    worker_processes.insert(jsc.second->GetSoleWorkerProcesses().begin(),
                            jsc.second->GetSoleWorkerProcesses().end());
    std::unordered_map<NodeID, rpc::JobInfoAgg> node_to_job_info;
    for (const auto &entry : worker_processes) {
      const auto &worker_process = entry.second;
      if (worker_process->IsDummy()) {
        continue;
      }
      auto node_id = worker_process->GetNodeID();
      auto job_id = worker_process->GetJobID();

      auto it = node_to_job_info.find(node_id);
      if (it == node_to_job_info.end()) {
        rpc::JobInfoAgg job_info;
        job_info.set_job_id(job_id.Binary());
        it = node_to_job_info.emplace(node_id, std::move(job_info)).first;
      }

      std::unordered_map<std::string, std::unordered_map<uint32_t, uint32_t>> histogram;
      for (auto resource : worker_process->GetResources().GetResourceMap()) {
        std::string resource_label = resource.first;
        uint32_t resource_capacity = (uint32_t)resource.second;
        if (histogram.count(resource_label) == 1) {
          if (histogram[resource_label].count(resource_capacity) == 1) {
            histogram[resource_label][resource_capacity] += 1;
          } else {
            histogram[resource_label][resource_capacity] = 1;
          }
        } else {
          histogram[resource_label][resource_capacity] = 1;
        }
      }

      for (const auto &h : histogram) {
        rpc::WorkerInfoAgg worker_info;
        worker_info.set_resource_name(h.first);
        for (const auto &c : h.second) {
          rpc::ResourceInfo resource_info;
          resource_info.set_capacity(c.first);
          resource_info.set_count(c.second);
          worker_info.add_resource_info_list()->CopyFrom(resource_info);
        }
        it->second.add_worker_info_agg_list()->CopyFrom(worker_info);
      }
    }

    for (const auto &it : node_to_job_info) {
      node_info_map[it.first].add_job_info_agg_list()->CopyFrom(it.second);
    }
  }

  std::vector<rpc::NodeInfoAgg> node_hist_list;
  for (const auto &it : node_info_map) {
    node_hist_list.push_back(it.second);
  }
  return node_hist_list;
}

const absl::flat_hash_map<NodeID, absl::flat_hash_set<JobID>>
    &GcsJobDistribution::GetNodeToJobs() const {
  return node_to_jobs_;
}

const absl::flat_hash_map<JobID, std::shared_ptr<GcsJobSchedulingContext>>
    &GcsJobDistribution::GetAllJobSchedulingContexts() const {
  return job_scheduling_contexts_;
}

void GcsJobDistribution::RemoveJobFromNode(const JobID &job_id, const NodeID &node_id) {
  auto iter = node_to_jobs_.find(node_id);
  if (iter != node_to_jobs_.end()) {
    if (iter->second.erase(job_id) && iter->second.empty()) {
      node_to_jobs_.erase(iter);
    }
  }
}

absl::flat_hash_set<JobID> GcsJobDistribution::GetJobsByNodeID(
    const NodeID &node_id) const {
  auto iter = node_to_jobs_.find(node_id);
  if (iter != node_to_jobs_.end()) {
    return iter->second;
  }
  return absl::flat_hash_set<JobID>();
}

std::shared_ptr<GcsWorkerProcess> GcsJobDistribution::GetWorkerProcessById(
    const JobID &job_id, const UniqueID &worker_process_id) const {
  std::shared_ptr<GcsWorkerProcess> worker_process = nullptr;
  if (auto job_scheduling_context = GetJobSchedulingContext(job_id)) {
    worker_process = job_scheduling_context->GetWorkerProcessById(worker_process_id);
  }
  return worker_process;
}

}  // namespace gcs
}  // namespace ray