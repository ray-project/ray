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

#include "ray/core_worker/task_manager.h"

#include "ray/common/buffer.h"
#include "ray/common/common_protocol.h"
#include "ray/common/constants.h"
#include "ray/util/util.h"

namespace ray {
namespace core {

// Start throttling task failure logs once we hit this threshold.
const int64_t kTaskFailureThrottlingThreshold = 50;

// Throttle task failure logs to once this interval.
const int64_t kTaskFailureLoggingFrequencyMillis = 5000;

std::vector<rpc::ObjectReference> TaskManager::AddPendingTask(
    const rpc::Address &caller_address,
    const TaskSpecification &spec,
    const std::string &call_site,
    int max_retries) {
  RAY_LOG(DEBUG) << "Adding pending task " << spec.TaskId() << " with " << max_retries
                 << " retries";

  // Add references for the dependencies to the task.
  std::vector<ObjectID> task_deps;
  for (size_t i = 0; i < spec.NumArgs(); i++) {
    if (spec.ArgByRef(i)) {
      task_deps.push_back(spec.ArgId(i));
      RAY_LOG(DEBUG) << "Adding arg ID " << spec.ArgId(i);
    } else {
      const auto &inlined_refs = spec.ArgInlinedRefs(i);
      for (const auto &inlined_ref : inlined_refs) {
        const auto inlined_id = ObjectID::FromBinary(inlined_ref.object_id());
        task_deps.push_back(inlined_id);
        RAY_LOG(DEBUG) << "Adding inlined ID " << inlined_id;
      }
    }
  }
  if (spec.IsActorTask()) {
    const auto actor_creation_return_id = spec.ActorCreationDummyObjectId();
    task_deps.push_back(actor_creation_return_id);
  }

  // Add new owned objects for the return values of the task.
  size_t num_returns = spec.NumReturns();
  if (spec.IsActorTask()) {
    num_returns--;
  }
  std::vector<rpc::ObjectReference> returned_refs;
  std::vector<ObjectID> return_ids;
  for (size_t i = 0; i < num_returns; i++) {
    auto return_id = spec.ReturnId(i);
    if (!spec.IsActorCreationTask()) {
      bool is_reconstructable = max_retries != 0;
      // We pass an empty vector for inner IDs because we do not know the return
      // value of the task yet. If the task returns an ID(s), the worker will
      // publish the WaitForRefRemoved message that we are now a borrower for
      // the inner IDs. Note that this message can be received *before* the
      // PushTaskReply.
      // NOTE(swang): We increment the local ref count to ensure that the
      // object is considered in scope before we return the ObjectRef to the
      // language frontend. Note that the language bindings should set
      // skip_adding_local_ref=True to avoid double referencing the object.
      reference_counter_->AddOwnedObject(return_id,
                                         /*inner_ids=*/{},
                                         caller_address,
                                         call_site,
                                         -1,
                                         /*is_reconstructable=*/is_reconstructable,
                                         /*add_local_ref=*/true);
    }

    return_ids.push_back(return_id);
    rpc::ObjectReference ref;
    ref.set_object_id(spec.ReturnId(i).Binary());
    ref.mutable_owner_address()->CopyFrom(caller_address);
    ref.set_call_site(call_site);
    returned_refs.push_back(std::move(ref));
  }

  reference_counter_->UpdateSubmittedTaskReferences(return_ids, task_deps);

  {
    absl::MutexLock lock(&mu_);
    auto inserted = submissible_tasks_.emplace(spec.TaskId(),
                                               TaskEntry(spec, max_retries, num_returns));
    RAY_CHECK(inserted.second);
    num_pending_tasks_++;
  }

  return returned_refs;
}

bool TaskManager::ResubmitTask(const TaskID &task_id, std::vector<ObjectID> *task_deps) {
  TaskSpecification spec;
  bool resubmit = false;
  std::vector<ObjectID> return_ids;
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    if (it == submissible_tasks_.end()) {
      // This can happen when the task has already been
      // retried up to its max attempts.
      return false;
    }

    if (!it->second.IsPending()) {
      resubmit = true;
      it->second.status = rpc::TaskStatus::WAITING_FOR_DEPENDENCIES;
      num_pending_tasks_++;

      // The task is pending again, so it's no longer counted as lineage. If
      // the task finishes and we still need the spec, we'll add the task back
      // to the footprint sum.
      total_lineage_footprint_bytes_ -= it->second.lineage_footprint_bytes;
      it->second.lineage_footprint_bytes = 0;

      if (it->second.num_retries_left > 0) {
        it->second.num_retries_left--;
      } else {
        RAY_CHECK(it->second.num_retries_left == -1);
      }
      spec = it->second.spec;

      for (const auto &return_id : it->second.reconstructable_return_ids) {
        return_ids.push_back(return_id);
      }
    }
  }

  if (resubmit) {
    for (size_t i = 0; i < spec.NumArgs(); i++) {
      if (spec.ArgByRef(i)) {
        task_deps->push_back(spec.ArgId(i));
      } else {
        const auto &inlined_refs = spec.ArgInlinedRefs(i);
        for (const auto &inlined_ref : inlined_refs) {
          task_deps->push_back(ObjectID::FromBinary(inlined_ref.object_id()));
        }
      }
    }

    reference_counter_->UpdateResubmittedTaskReferences(return_ids, *task_deps);
    if (spec.IsActorTask()) {
      const auto actor_creation_return_id = spec.ActorCreationDummyObjectId();
      reference_counter_->UpdateResubmittedTaskReferences(return_ids,
                                                          {actor_creation_return_id});
    }

    retry_task_callback_(spec, /*delay=*/false);
  }

  return true;
}

void TaskManager::DrainAndShutdown(std::function<void()> shutdown) {
  bool has_pending_tasks = false;
  {
    absl::MutexLock lock(&mu_);
    if (num_pending_tasks_ > 0) {
      has_pending_tasks = true;
      RAY_LOG(WARNING)
          << "This worker is still managing " << submissible_tasks_.size()
          << " in flight tasks, waiting for them to finish before shutting down.";
      shutdown_hook_ = shutdown;
    }
  }

  // Do not hold the lock when calling callbacks.
  if (!has_pending_tasks) {
    shutdown();
  }
}

bool TaskManager::IsTaskSubmissible(const TaskID &task_id) const {
  absl::MutexLock lock(&mu_);
  return submissible_tasks_.count(task_id) > 0;
}

bool TaskManager::IsTaskPending(const TaskID &task_id) const {
  absl::MutexLock lock(&mu_);
  const auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return false;
  }
  return it->second.IsPending();
}

size_t TaskManager::NumSubmissibleTasks() const {
  absl::MutexLock lock(&mu_);
  return submissible_tasks_.size();
}

size_t TaskManager::NumPendingTasks() const {
  absl::MutexLock lock(&mu_);
  return num_pending_tasks_;
}

void TaskManager::CompletePendingTask(const TaskID &task_id,
                                      const rpc::PushTaskReply &reply,
                                      const rpc::Address &worker_addr) {
  RAY_LOG(DEBUG) << "Completing task " << task_id;

  // Objects that were stored in plasma upon the first successful execution of
  // this task. These objects will get stored in plasma again, even if they
  // were returned directly in the worker's reply. This ensures that any
  // reference holders that are already scheduled at the raylet can retrieve
  // these objects through plasma.
  absl::flat_hash_set<ObjectID> store_in_plasma_ids = {};
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    RAY_CHECK(it != submissible_tasks_.end())
        << "Tried to complete task that was not pending " << task_id;
    if (it->second.num_successful_executions > 0) {
      store_in_plasma_ids = it->second.reconstructable_return_ids;
    }
  }

  std::vector<ObjectID> direct_return_ids;
  for (int i = 0; i < reply.return_objects_size(); i++) {
    const auto &return_object = reply.return_objects(i);
    ObjectID object_id = ObjectID::FromBinary(return_object.object_id());
    reference_counter_->UpdateObjectSize(object_id, return_object.size());
    RAY_LOG(DEBUG) << "Task return object " << object_id << " has size "
                   << return_object.size();

    const auto nested_refs =
        VectorFromProtobuf<rpc::ObjectReference>(return_object.nested_inlined_refs());
    if (return_object.in_plasma()) {
      // Mark it as in plasma with a dummy object.
      RAY_CHECK(
          in_memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id));
      const auto pinned_at_raylet_id = NodeID::FromBinary(worker_addr.raylet_id());
      reference_counter_->UpdateObjectPinnedAtRaylet(object_id, pinned_at_raylet_id);
    } else {
      // NOTE(swang): If a direct object was promoted to plasma, then we do not
      // record the node ID that it was pinned at, which means that we will not
      // be able to reconstruct it if the plasma object copy is lost. However,
      // this is okay because the pinned copy is on the local node, so we will
      // fate-share with the object if the local node fails.
      std::shared_ptr<LocalMemoryBuffer> data_buffer;
      if (return_object.data().size() > 0) {
        data_buffer = std::make_shared<LocalMemoryBuffer>(
            const_cast<uint8_t *>(
                reinterpret_cast<const uint8_t *>(return_object.data().data())),
            return_object.data().size());
      }
      std::shared_ptr<LocalMemoryBuffer> metadata_buffer;
      if (return_object.metadata().size() > 0) {
        metadata_buffer = std::make_shared<LocalMemoryBuffer>(
            const_cast<uint8_t *>(
                reinterpret_cast<const uint8_t *>(return_object.metadata().data())),
            return_object.metadata().size());
      }

      RayObject object(data_buffer, metadata_buffer, nested_refs);
      if (store_in_plasma_ids.count(object_id)) {
        put_in_local_plasma_callback_(object, object_id);
      } else {
        bool stored_in_direct_memory = in_memory_store_->Put(object, object_id);
        if (stored_in_direct_memory) {
          direct_return_ids.push_back(object_id);
        }
      }
    }

    rpc::Address owner_address;
    if (reference_counter_->GetOwner(object_id, &owner_address) && !nested_refs.empty()) {
      std::vector<ObjectID> nested_ids;
      for (const auto &nested_ref : nested_refs) {
        nested_ids.emplace_back(ObjectRefToId(nested_ref));
      }
      reference_counter_->AddNestedObjectIds(object_id, nested_ids, owner_address);
    }
  }

  TaskSpecification spec;
  bool release_lineage = true;
  int64_t min_lineage_bytes_to_evict = 0;
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    RAY_CHECK(it != submissible_tasks_.end())
        << "Tried to complete task that was not pending " << task_id;
    spec = it->second.spec;

    // Release the lineage for any non-plasma return objects.
    for (const auto &direct_return_id : direct_return_ids) {
      RAY_LOG(DEBUG) << "Task " << it->first << " returned direct object "
                     << direct_return_id << ", now has "
                     << it->second.reconstructable_return_ids.size()
                     << " plasma returns in scope";
      it->second.reconstructable_return_ids.erase(direct_return_id);
    }
    RAY_LOG(DEBUG) << "Task " << it->first << " now has "
                   << it->second.reconstructable_return_ids.size()
                   << " plasma returns in scope";
    it->second.num_successful_executions++;

    it->second.status = rpc::TaskStatus::FINISHED;
    num_pending_tasks_--;

    // A finished task can only be re-executed if it has some number of
    // retries left and returned at least one object that is still in use and
    // stored in plasma.
    bool task_retryable = it->second.num_retries_left != 0 &&
                          !it->second.reconstructable_return_ids.empty();
    if (task_retryable) {
      // Pin the task spec if it may be retried again.
      release_lineage = false;
      it->second.lineage_footprint_bytes = it->second.spec.GetMessage().ByteSizeLong();
      total_lineage_footprint_bytes_ += it->second.lineage_footprint_bytes;
      if (total_lineage_footprint_bytes_ > max_lineage_bytes_) {
        RAY_LOG(INFO) << "Total lineage size is " << total_lineage_footprint_bytes_ / 1e6
                      << "MB, which exceeds the limit of " << max_lineage_bytes_ / 1e6
                      << "MB";
        min_lineage_bytes_to_evict =
            total_lineage_footprint_bytes_ - (max_lineage_bytes_ / 2);
      }
    } else {
      submissible_tasks_.erase(it);
    }
  }

  RemoveFinishedTaskReferences(spec, release_lineage, worker_addr, reply.borrowed_refs());
  if (min_lineage_bytes_to_evict > 0) {
    // Evict at least half of the current lineage.
    auto bytes_evicted = reference_counter_->EvictLineage(min_lineage_bytes_to_evict);
    RAY_LOG(INFO) << "Evicted " << bytes_evicted / 1e6 << "MB of task lineage.";
  }

  ShutdownIfNeeded();
}

bool TaskManager::RetryTaskIfPossible(const TaskID &task_id) {
  int num_retries_left = 0;
  TaskSpecification spec;
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    RAY_CHECK(it != submissible_tasks_.end())
        << "Tried to retry task that was not pending " << task_id;
    RAY_CHECK(it->second.IsPending())
        << "Tried to retry task that was not pending " << task_id;
    spec = it->second.spec;
    num_retries_left = it->second.num_retries_left;
    if (num_retries_left > 0) {
      it->second.num_retries_left--;
    } else {
      RAY_CHECK(num_retries_left == 0 || num_retries_left == -1);
    }
  }

  // We should not hold the lock during these calls because they may trigger
  // callbacks in this or other classes.
  if (num_retries_left != 0) {
    std::ostringstream stream;
    auto num_retries_left_str =
        num_retries_left == -1 ? "infinite" : std::to_string(num_retries_left);
    stream << num_retries_left_str << " retries left for task " << spec.TaskId()
           << ", attempting to resubmit.";
    RAY_LOG(INFO) << stream.str();
    retry_task_callback_(spec, /*delay=*/true);
    return true;
  } else {
    return false;
  }
}

void TaskManager::FailPendingTask(const TaskID &task_id,
                                  rpc::ErrorType error_type,
                                  const Status *status,
                                  const rpc::RayErrorInfo *ray_error_info,
                                  bool mark_task_object_failed) {
  // Note that this might be the __ray_terminate__ task, so we don't log
  // loudly with ERROR here.
  RAY_LOG(DEBUG) << "Task " << task_id << " failed with error "
                 << rpc::ErrorType_Name(error_type);

  TaskSpecification spec;
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    RAY_CHECK(it != submissible_tasks_.end())
        << "Tried to fail task that was not pending " << task_id;
    RAY_CHECK(it->second.IsPending())
        << "Tried to fail task that was not pending " << task_id;
    spec = it->second.spec;
    submissible_tasks_.erase(it);
    num_pending_tasks_--;

    // Throttled logging of task failure errors.
    auto debug_str = spec.DebugString();
    if (debug_str.find("__ray_terminate__") == std::string::npos &&
        (num_failure_logs_ < kTaskFailureThrottlingThreshold ||
         (current_time_ms() - last_log_time_ms_) > kTaskFailureLoggingFrequencyMillis)) {
      if (num_failure_logs_++ == kTaskFailureThrottlingThreshold) {
        RAY_LOG(WARNING) << "Too many failure logs, throttling to once every "
                         << kTaskFailureLoggingFrequencyMillis << " millis.";
      }
      last_log_time_ms_ = current_time_ms();
      if (status != nullptr) {
        RAY_LOG(INFO) << "Task failed: " << *status << ": " << spec.DebugString();
      } else {
        RAY_LOG(INFO) << "Task failed: " << spec.DebugString();
      }
    }
  }

  // The worker failed to execute the task, so it cannot be borrowing any
  // objects.
  RemoveFinishedTaskReferences(spec,
                               /*release_lineage=*/true,
                               rpc::Address(),
                               ReferenceCounter::ReferenceTableProto());
  if (mark_task_object_failed) {
    MarkTaskReturnObjectsFailed(spec, error_type, ray_error_info);
  }

  ShutdownIfNeeded();
}

bool TaskManager::FailOrRetryPendingTask(const TaskID &task_id,
                                         rpc::ErrorType error_type,
                                         const Status *status,
                                         const rpc::RayErrorInfo *ray_error_info,
                                         bool mark_task_object_failed) {
  // Note that this might be the __ray_terminate__ task, so we don't log
  // loudly with ERROR here.
  RAY_LOG(DEBUG) << "Task attempt " << task_id << " failed with error "
                 << rpc::ErrorType_Name(error_type);
  const bool will_retry = RetryTaskIfPossible(task_id);
  if (!will_retry) {
    FailPendingTask(task_id, error_type, status, ray_error_info, mark_task_object_failed);
  }

  ShutdownIfNeeded();

  return will_retry;
}

void TaskManager::ShutdownIfNeeded() {
  std::function<void()> shutdown_hook = nullptr;
  {
    absl::MutexLock lock(&mu_);
    if (shutdown_hook_ && num_pending_tasks_ == 0) {
      RAY_LOG(WARNING) << "All in flight tasks finished, worker will shut down after "
                          "draining references.";
      std::swap(shutdown_hook_, shutdown_hook);
    }
  }
  // Do not hold the lock when calling callbacks.
  if (shutdown_hook != nullptr) {
    shutdown_hook();
  }
}

void TaskManager::OnTaskDependenciesInlined(
    const std::vector<ObjectID> &inlined_dependency_ids,
    const std::vector<ObjectID> &contained_ids) {
  std::vector<ObjectID> deleted;
  reference_counter_->UpdateSubmittedTaskReferences(
      /*return_ids=*/{},
      /*argument_ids_to_add=*/contained_ids,
      /*argument_ids_to_remove=*/inlined_dependency_ids,
      &deleted);
  in_memory_store_->Delete(deleted);
}

void TaskManager::RemoveFinishedTaskReferences(
    TaskSpecification &spec,
    bool release_lineage,
    const rpc::Address &borrower_addr,
    const ReferenceCounter::ReferenceTableProto &borrowed_refs) {
  std::vector<ObjectID> plasma_dependencies;
  for (size_t i = 0; i < spec.NumArgs(); i++) {
    if (spec.ArgByRef(i)) {
      plasma_dependencies.push_back(spec.ArgId(i));
    } else {
      const auto &inlined_refs = spec.ArgInlinedRefs(i);
      for (const auto &inlined_ref : inlined_refs) {
        plasma_dependencies.push_back(ObjectID::FromBinary(inlined_ref.object_id()));
      }
    }
  }
  if (spec.IsActorTask()) {
    const auto actor_creation_return_id = spec.ActorCreationDummyObjectId();
    plasma_dependencies.push_back(actor_creation_return_id);
  }

  std::vector<ObjectID> return_ids;
  size_t num_returns = spec.NumReturns();
  if (spec.IsActorTask()) {
    num_returns--;
  }
  for (size_t i = 0; i < num_returns; i++) {
    return_ids.push_back(spec.ReturnId(i));
  }

  std::vector<ObjectID> deleted;
  reference_counter_->UpdateFinishedTaskReferences(return_ids,
                                                   plasma_dependencies,
                                                   release_lineage,
                                                   borrower_addr,
                                                   borrowed_refs,
                                                   &deleted);
  in_memory_store_->Delete(deleted);
}

int64_t TaskManager::RemoveLineageReference(const ObjectID &object_id,
                                            std::vector<ObjectID> *released_objects) {
  absl::MutexLock lock(&mu_);
  const int64_t total_lineage_footprint_bytes_prev(total_lineage_footprint_bytes_);

  const TaskID &task_id = object_id.TaskId();
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    RAY_LOG(DEBUG) << "No lineage for object " << object_id;
    return 0;
  }

  RAY_LOG(DEBUG) << "Plasma object " << object_id << " out of scope";
  for (const auto &plasma_id : it->second.reconstructable_return_ids) {
    RAY_LOG(DEBUG) << "Task " << task_id << " has " << plasma_id << " in scope";
  }
  it->second.reconstructable_return_ids.erase(object_id);
  RAY_LOG(DEBUG) << "Task " << task_id << " now has "
                 << it->second.reconstructable_return_ids.size()
                 << " plasma returns in scope";

  if (it->second.reconstructable_return_ids.empty() && !it->second.IsPending()) {
    // If the task can no longer be retried, decrement the lineage ref count
    // for each of the task's args.
    for (size_t i = 0; i < it->second.spec.NumArgs(); i++) {
      if (it->second.spec.ArgByRef(i)) {
        released_objects->push_back(it->second.spec.ArgId(i));
      } else {
        const auto &inlined_refs = it->second.spec.ArgInlinedRefs(i);
        for (const auto &inlined_ref : inlined_refs) {
          released_objects->push_back(ObjectID::FromBinary(inlined_ref.object_id()));
        }
      }
    }

    total_lineage_footprint_bytes_ -= it->second.lineage_footprint_bytes;
    // The task has finished and none of the return IDs are in scope anymore,
    // so it is safe to remove the task spec.
    submissible_tasks_.erase(it);
  }

  return total_lineage_footprint_bytes_ - total_lineage_footprint_bytes_prev;
}

bool TaskManager::MarkTaskCanceled(const TaskID &task_id) {
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it != submissible_tasks_.end()) {
    it->second.num_retries_left = 0;
  }
  return it != submissible_tasks_.end();
}

void TaskManager::MarkTaskReturnObjectsFailed(const TaskSpecification &spec,
                                              rpc::ErrorType error_type,
                                              const rpc::RayErrorInfo *ray_error_info) {
  const TaskID task_id = spec.TaskId();
  RAY_LOG(DEBUG) << "Treat task as failed. task_id: " << task_id
                 << ", error_type: " << ErrorType_Name(error_type);
  int64_t num_returns = spec.NumReturns();
  for (int i = 0; i < num_returns; i++) {
    const auto object_id = ObjectID::FromIndex(task_id, /*index=*/i + 1);
    if (ray_error_info == nullptr) {
      RAY_UNUSED(in_memory_store_->Put(RayObject(error_type), object_id));
    } else {
      RAY_UNUSED(in_memory_store_->Put(RayObject(error_type, ray_error_info), object_id));
    }
  }
}

absl::optional<TaskSpecification> TaskManager::GetTaskSpec(const TaskID &task_id) const {
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return absl::optional<TaskSpecification>();
  }
  return it->second.spec;
}

std::vector<TaskID> TaskManager::GetPendingChildrenTasks(
    const TaskID &parent_task_id) const {
  std::vector<TaskID> ret_vec;
  absl::MutexLock lock(&mu_);
  for (auto it : submissible_tasks_) {
    if (it.second.IsPending() && (it.second.spec.ParentTaskId() == parent_task_id)) {
      ret_vec.push_back(it.first);
    }
  }
  return ret_vec;
}

void TaskManager::AddTaskStatusInfo(rpc::CoreWorkerStats *stats) const {
  absl::MutexLock lock(&mu_);
  for (int64_t i = 0; i < stats->object_refs_size(); i++) {
    auto ref = stats->mutable_object_refs(i);
    const auto obj_id = ObjectID::FromBinary(ref->object_id());
    const auto task_id = obj_id.TaskId();
    const auto it = submissible_tasks_.find(task_id);
    if (it == submissible_tasks_.end()) {
      continue;
    }
    ref->set_task_status(it->second.status);
    ref->set_attempt_number(it->second.spec.AttemptNumber());
  }
}

void TaskManager::MarkDependenciesResolved(const TaskID &task_id) {
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return;
  }
  if (it->second.status == rpc::TaskStatus::WAITING_FOR_DEPENDENCIES) {
    it->second.status = rpc::TaskStatus::SCHEDULED;
  }
}

}  // namespace core
}  // namespace ray
