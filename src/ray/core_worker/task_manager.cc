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
#include "ray/util/exponential_backoff.h"
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
  int32_t max_oom_retries =
      (max_retries != 0) ? RayConfig::instance().task_oom_retries() : 0;
  RAY_LOG(DEBUG) << "Adding pending task " << spec.TaskId() << " with " << max_retries
                 << " retries, " << max_oom_retries << " oom retries";

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
                                         is_reconstructable,
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
    auto inserted = submissible_tasks_.try_emplace(
        spec.TaskId(), spec, max_retries, num_returns, task_counter_, max_oom_retries);
    RAY_CHECK(inserted.second);
    num_pending_tasks_++;
    RecordTaskStatusEvent(inserted.first->second, rpc::TaskStatus::PENDING_ARGS_AVAIL);
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
      SetTaskStatus(it->second, rpc::TaskStatus::PENDING_ARGS_AVAIL);
      it->second.MarkRetryOnResubmit();
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

    for (const auto &task_dep : *task_deps) {
      bool was_freed = reference_counter_->TryMarkFreedObjectInUseAgain(task_dep);
      if (was_freed) {
        RAY_LOG(DEBUG) << "Dependency " << task_dep << " of task " << task_id
                       << " was freed";
        // We do not keep around copies for objects that were freed, but now that
        // they're needed for recovery, we need to generate and pin a new copy.
        // Delete the old in-memory marker that indicated that the object was
        // freed. Now workers that attempt to get the object will be able to get
        // the reconstructed value.
        in_memory_store_->Delete({task_dep});
      }
    }
    if (spec.IsActorTask()) {
      const auto actor_creation_return_id = spec.ActorCreationDummyObjectId();
      reference_counter_->UpdateResubmittedTaskReferences(return_ids,
                                                          {actor_creation_return_id});
    }

    RAY_LOG(INFO) << "Resubmitting task that produced lost plasma object, attempt #"
                  << spec.AttemptNumber() + 1 << ": " << spec.DebugString();
    retry_task_callback_(spec, /*object_recovery*/ true, /*delay_ms*/ 0);
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

bool TaskManager::IsTaskWaitingForExecution(const TaskID &task_id) const {
  absl::MutexLock lock(&mu_);
  const auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return false;
  }
  return it->second.IsWaitingForExecution();
}

size_t TaskManager::NumSubmissibleTasks() const {
  absl::MutexLock lock(&mu_);
  return submissible_tasks_.size();
}

size_t TaskManager::NumPendingTasks() const {
  absl::MutexLock lock(&mu_);
  return num_pending_tasks_;
}

bool TaskManager::HandleTaskReturn(const ObjectID &object_id,
                                   const rpc::ReturnObject &return_object,
                                   const NodeID &worker_raylet_id,
                                   bool store_in_plasma) {
  bool direct_return = false;
  reference_counter_->UpdateObjectSize(object_id, return_object.size());
  RAY_LOG(DEBUG) << "Task return object " << object_id << " has size "
                 << return_object.size();

  const auto nested_refs =
      VectorFromProtobuf<rpc::ObjectReference>(return_object.nested_inlined_refs());
  if (return_object.in_plasma()) {
    // NOTE(swang): We need to add the location of the object before marking
    // it as local in the in-memory store so that the data locality policy
    // will choose the right raylet for any queued dependent tasks.
    reference_counter_->UpdateObjectPinnedAtRaylet(object_id, worker_raylet_id);
    // Mark it as in plasma with a dummy object.
    RAY_CHECK(
        in_memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id));
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
    if (store_in_plasma) {
      put_in_local_plasma_callback_(object, object_id);
    } else {
      direct_return = in_memory_store_->Put(object, object_id);
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
  return direct_return;
}

void TaskManager::CompletePendingTask(const TaskID &task_id,
                                      const rpc::PushTaskReply &reply,
                                      const rpc::Address &worker_addr,
                                      bool is_application_error) {
  RAY_LOG(DEBUG) << "Completing task " << task_id;

  bool first_execution = false;
  const auto store_in_plasma_ids =
      GetTaskReturnObjectsToStoreInPlasma(task_id, &first_execution);
  std::vector<ObjectID> dynamic_return_ids;
  std::vector<ObjectID> dynamic_returns_in_plasma;
  std::vector<ObjectID> direct_return_ids;
  if (reply.dynamic_return_objects_size() > 0) {
    RAY_CHECK(reply.return_objects_size() == 1)
        << "Dynamic generators only supported for num_returns=1";
    const auto generator_id = ObjectID::FromBinary(reply.return_objects(0).object_id());
    for (const auto &return_object : reply.dynamic_return_objects()) {
      const auto object_id = ObjectID::FromBinary(return_object.object_id());
      if (first_execution) {
        reference_counter_->AddDynamicReturn(object_id, generator_id);
        dynamic_return_ids.push_back(object_id);
      }
      if (!HandleTaskReturn(object_id,
                            return_object,
                            NodeID::FromBinary(worker_addr.raylet_id()),
                            store_in_plasma_ids.count(object_id))) {
        if (first_execution) {
          dynamic_returns_in_plasma.push_back(object_id);
        }
      }
    }
  }

  for (const auto &return_object : reply.return_objects()) {
    const auto object_id = ObjectID::FromBinary(return_object.object_id());
    if (HandleTaskReturn(object_id,
                         return_object,
                         NodeID::FromBinary(worker_addr.raylet_id()),
                         store_in_plasma_ids.count(object_id))) {
      direct_return_ids.push_back(object_id);
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

    // Record any dynamically returned objects. We need to store these with the
    // task spec so that the worker will recreate them if the task gets
    // re-executed.
    if (first_execution) {
      for (const auto &dynamic_return_id : dynamic_return_ids) {
        RAY_LOG(DEBUG) << "Task " << task_id << " produced dynamic return object "
                       << dynamic_return_id;
        spec.AddDynamicReturnId(dynamic_return_id);
      }
      for (const auto &dynamic_return_id : dynamic_returns_in_plasma) {
        it->second.reconstructable_return_ids.insert(dynamic_return_id);
      }
    }

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

    if (is_application_error) {
      SetTaskStatus(it->second, rpc::TaskStatus::FAILED);
    } else {
      SetTaskStatus(it->second, rpc::TaskStatus::FINISHED);
    }
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

bool TaskManager::RetryTaskIfPossible(const TaskID &task_id,
                                      bool task_failed_due_to_oom) {
  TaskSpecification spec;
  bool will_retry = false;
  int32_t num_retries_left = 0;
  int32_t num_oom_retries_left = 0;
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    RAY_CHECK(it != submissible_tasks_.end())
        << "Tried to retry task that was not pending " << task_id;
    RAY_CHECK(it->second.IsPending())
        << "Tried to retry task that was not pending " << task_id;
    spec = it->second.spec;
    num_retries_left = it->second.num_retries_left;
    num_oom_retries_left = it->second.num_oom_retries_left;
    if (task_failed_due_to_oom) {
      if (num_oom_retries_left > 0) {
        will_retry = true;
        it->second.num_oom_retries_left--;
      }
    } else {
      if (num_retries_left > 0) {
        will_retry = true;
        it->second.num_retries_left--;
      } else if (num_retries_left == -1) {
        will_retry = true;
      } else {
        RAY_CHECK(num_retries_left == 0);
      }
    }
    if (will_retry) {
      SetTaskStatus(it->second, rpc::TaskStatus::PENDING_NODE_ASSIGNMENT);
      it->second.MarkRetryOnFailed();
    }
  }

  // We should not hold the lock during these calls because they may trigger
  // callbacks in this or other classes.
  std::ostringstream stream;
  std::string num_retries_left_str =
      num_retries_left == -1 ? "infinite" : std::to_string(num_retries_left);
  RAY_LOG(INFO) << "task " << spec.TaskId() << " retries left: " << num_retries_left_str
                << ", oom retries left: " << num_oom_retries_left
                << ", task failed due to oom: " << task_failed_due_to_oom;
  if (will_retry) {
    RAY_LOG(INFO) << "Attempting to resubmit task " << spec.TaskId();
    // TODO(clarng): clean up and remove task_retry_delay_ms that is relied
    // on by some tests.
    int32_t delay_ms = task_failed_due_to_oom
                           ? ExponentialBackoff::GetBackoffMs(
                                 spec.AttemptNumber(),
                                 RayConfig::instance().task_oom_retry_delay_base_ms())
                           : RayConfig::instance().task_retry_delay_ms();
    retry_task_callback_(spec, /*object_recovery*/ false, delay_ms);
    return true;
  } else {
    RAY_LOG(INFO) << "No retries left for task " << spec.TaskId()
                  << ", not going to resubmit.";
    return false;
  }
}

void TaskManager::FailPendingTask(const TaskID &task_id,
                                  rpc::ErrorType error_type,
                                  const Status *status,
                                  const rpc::RayErrorInfo *ray_error_info) {
  // Note that this might be the __ray_terminate__ task, so we don't log
  // loudly with ERROR here.
  RAY_LOG(DEBUG) << "Task " << task_id << " failed with error "
                 << rpc::ErrorType_Name(error_type);

  TaskSpecification spec;
  // Check whether the error should be stored in plasma or not.
  const auto store_in_plasma_ids = GetTaskReturnObjectsToStoreInPlasma(task_id);
  {
    absl::MutexLock lock(&mu_);
    auto it = submissible_tasks_.find(task_id);
    RAY_CHECK(it != submissible_tasks_.end())
        << "Tried to fail task that was not pending " << task_id;
    RAY_CHECK(it->second.IsPending())
        << "Tried to fail task that was not pending " << task_id;
    spec = it->second.spec;
    SetTaskStatus(it->second, rpc::TaskStatus::FAILED);
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
      RAY_LOG(DEBUG) << "Runtime env for task " << spec.TaskId() << " is "
                     << spec.RuntimeEnvDebugString();
    }
  }

  // The worker failed to execute the task, so it cannot be borrowing any
  // objects.
  RemoveFinishedTaskReferences(spec,
                               /*release_lineage=*/true,
                               rpc::Address(),
                               ReferenceCounter::ReferenceTableProto());

  MarkTaskReturnObjectsFailed(spec, error_type, ray_error_info, store_in_plasma_ids);

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
  const bool will_retry = RetryTaskIfPossible(
      task_id, /*task_failed_due_to_oom*/ error_type == rpc::ErrorType::OUT_OF_MEMORY);
  if (!will_retry && mark_task_object_failed) {
    FailPendingTask(task_id, error_type, status, ray_error_info);
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
  if (spec.ReturnsDynamic()) {
    for (const auto &dynamic_return_id : spec.DynamicReturnIds()) {
      return_ids.push_back(dynamic_return_id);
    }
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
    it->second.num_oom_retries_left = 0;
  }
  return it != submissible_tasks_.end();
}

absl::flat_hash_set<ObjectID> TaskManager::GetTaskReturnObjectsToStoreInPlasma(
    const TaskID &task_id, bool *first_execution_out) const {
  bool first_execution;
  absl::flat_hash_set<ObjectID> store_in_plasma_ids = {};
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  RAY_CHECK(it != submissible_tasks_.end())
      << "Tried to store return values for task that was not pending " << task_id;
  first_execution = it->second.num_successful_executions == 0;
  if (!first_execution) {
    store_in_plasma_ids = it->second.reconstructable_return_ids;
  }
  if (first_execution_out != nullptr) {
    *first_execution_out = first_execution;
  }
  return store_in_plasma_ids;
}

void TaskManager::MarkTaskReturnObjectsFailed(
    const TaskSpecification &spec,
    rpc::ErrorType error_type,
    const rpc::RayErrorInfo *ray_error_info,
    const absl::flat_hash_set<ObjectID> &store_in_plasma_ids) {
  const TaskID task_id = spec.TaskId();
  RayObject error(error_type, ray_error_info);
  RAY_LOG(DEBUG) << "Treat task as failed. task_id: " << task_id
                 << ", error_type: " << ErrorType_Name(error_type);
  int64_t num_returns = spec.NumReturns();
  for (int i = 0; i < num_returns; i++) {
    const auto object_id = ObjectID::FromIndex(task_id, /*index=*/i + 1);
    if (store_in_plasma_ids.count(object_id)) {
      put_in_local_plasma_callback_(error, object_id);
    } else {
      in_memory_store_->Put(error, object_id);
    }
  }
  if (spec.ReturnsDynamic()) {
    for (const auto &dynamic_return_id : spec.DynamicReturnIds()) {
      if (store_in_plasma_ids.count(dynamic_return_id)) {
        put_in_local_plasma_callback_(error, dynamic_return_id);
      } else {
        in_memory_store_->Put(error, dynamic_return_id);
      }
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
    ref->set_task_status(it->second.GetStatus());
    ref->set_attempt_number(it->second.spec.AttemptNumber());
  }
}

void TaskManager::MarkDependenciesResolved(const TaskID &task_id) {
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return;
  }
  if (it->second.GetStatus() == rpc::TaskStatus::PENDING_ARGS_AVAIL) {
    SetTaskStatus(it->second, rpc::TaskStatus::PENDING_NODE_ASSIGNMENT);
  }
}

void TaskManager::MarkTaskWaitingForExecution(const TaskID &task_id,
                                              const NodeID &node_id) {
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return;
  }
  RAY_CHECK(it->second.GetStatus() == rpc::TaskStatus::PENDING_NODE_ASSIGNMENT);
  it->second.SetNodeId(node_id);
  SetTaskStatus(it->second, rpc::TaskStatus::SUBMITTED_TO_WORKER);
}

void TaskManager::SetTaskStatus(TaskEntry &task_entry, rpc::TaskStatus status) {
  task_entry.SetStatus(status);
  RecordTaskStatusEvent(task_entry, status);
}

rpc::TaskInfoEntry TaskManager::MakeTaskInfoEntry(
    const TaskSpecification &task_spec) const {
  rpc::TaskInfoEntry task_info;
  rpc::TaskType type;
  if (task_spec.IsNormalTask()) {
    type = rpc::TaskType::NORMAL_TASK;
  } else if (task_spec.IsActorCreationTask()) {
    type = rpc::TaskType::ACTOR_CREATION_TASK;
    task_info.set_actor_id(task_spec.ActorCreationId().Binary());
  } else {
    RAY_CHECK(task_spec.IsActorTask());
    type = rpc::TaskType::ACTOR_TASK;
    task_info.set_actor_id(task_spec.ActorId().Binary());
  }
  task_info.set_type(type);
  task_info.set_name(task_spec.GetName());
  task_info.set_language(task_spec.GetLanguage());
  task_info.set_func_or_class_name(task_spec.FunctionDescriptor()->CallString());
  // NOTE(rickyx): we will have scheduling states recorded in the events list.
  task_info.set_scheduling_state(rpc::TaskStatus::NIL);
  task_info.set_job_id(task_spec.JobId().Binary());

  task_info.set_task_id(task_spec.TaskId().Binary());
  task_info.set_parent_task_id(task_spec.ParentTaskId().Binary());
  const auto &resources_map = task_spec.GetRequiredResources().GetResourceMap();
  task_info.mutable_required_resources()->insert(resources_map.begin(),
                                                 resources_map.end());
  task_info.mutable_runtime_env_info()->CopyFrom(task_spec.RuntimeEnvInfo());

  return task_info;
}

void TaskManager::FillTaskInfo(rpc::GetCoreWorkerStatsReply *reply,
                               const int64_t limit) const {
  absl::MutexLock lock(&mu_);
  auto total = submissible_tasks_.size();
  auto count = 0;
  for (const auto &task_it : submissible_tasks_) {
    if (limit != -1 && count >= limit) {
      break;
    }
    count += 1;

    const auto &task_entry = task_it.second;
    auto entry = reply->add_owned_task_info_entries();
    const auto &task_spec = task_entry.spec;
    const auto &task_state = task_entry.GetStatus();
    const auto &node_id = task_entry.GetNodeId();
    rpc::TaskType type;
    if (task_spec.IsNormalTask()) {
      type = rpc::TaskType::NORMAL_TASK;
    } else if (task_spec.IsActorCreationTask()) {
      type = rpc::TaskType::ACTOR_CREATION_TASK;
      entry->set_actor_id(task_spec.ActorCreationId().Binary());
    } else {
      RAY_CHECK(task_spec.IsActorTask());
      type = rpc::TaskType::ACTOR_TASK;
      entry->set_actor_id(task_spec.ActorId().Binary());
    }
    entry->set_type(type);
    entry->set_name(task_spec.GetName());
    entry->set_language(task_spec.GetLanguage());
    entry->set_func_or_class_name(task_spec.FunctionDescriptor()->CallString());
    entry->set_scheduling_state(task_state);
    entry->set_job_id(task_spec.JobId().Binary());
    if (!node_id.IsNil()) {
      entry->set_node_id(node_id.Binary());
    }
    entry->set_task_id(task_spec.TaskId().Binary());
    entry->set_parent_task_id(task_spec.ParentTaskId().Binary());
    const auto &resources_map = task_spec.GetRequiredResources().GetResourceMap();
    entry->mutable_required_resources()->insert(resources_map.begin(),
                                                resources_map.end());
    entry->mutable_runtime_env_info()->CopyFrom(task_spec.RuntimeEnvInfo());
  }
  reply->set_tasks_total(total);
}

void TaskManager::RecordMetrics() {
  absl::MutexLock lock(&mu_);
  task_counter_.FlushOnChangeCallbacks();
}

void TaskManager::RecordTaskStatusEvent(const TaskEntry &task_entry,
                                        rpc::TaskStatus status) {
  if (!task_event_buffer_.Enabled()) {
    return;
  }
  // Make task event
  rpc::TaskEvents task_event;
  task_event.set_task_id(task_entry.spec.TaskId().Binary());
  task_event.set_job_id(task_entry.spec.JobId().Binary());
  task_event.set_attempt_number(task_entry.spec.AttemptNumber());
  auto state_updates = task_event.mutable_state_updates();
  switch (status) {
  case rpc::TaskStatus::PENDING_ARGS_AVAIL: {
    // Initialize a new TaskInfoEntry
    auto task_info = MakeTaskInfoEntry(task_entry.spec);
    task_event.mutable_task_info()->Swap(&task_info);
    state_updates->set_pending_args_avail_ts(absl::GetCurrentTimeNanos());
    break;
  }
  case rpc::TaskStatus::SUBMITTED_TO_WORKER: {
    RAY_CHECK(!task_entry.GetNodeId().IsNil())
        << "Node ID should have been set on the TaskEntry before updating it's status "
           "to "
           "SUBMITTED_TO_WORKER.";
    // Update the node id
    state_updates->set_node_id(task_entry.GetNodeId().Binary());
    state_updates->set_submitted_to_worker_ts(absl::GetCurrentTimeNanos());
    break;
  }
  case rpc::TaskStatus::PENDING_NODE_ASSIGNMENT: {
    state_updates->set_pending_node_assignment_ts(absl::GetCurrentTimeNanos());
    break;
  }
  case rpc::TaskStatus::FINISHED: {
    state_updates->set_finished_ts(absl::GetCurrentTimeNanos());
    break;
  }
  case rpc::TaskStatus::FAILED: {
    state_updates->set_failed_ts(absl::GetCurrentTimeNanos());
    break;
  }
  default: {
    // NOTE: Other task status (e.g. TaskStatus::RUNNING_IN_XXX), should not be set by the
    // TaskManager.
    UNREACHABLE;
  }
  }
  task_event_buffer_.AddTaskEvent(std::move(task_event));
}

ObjectID TaskManager::TaskGeneratorId(const TaskID &task_id) const {
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return ObjectID::Nil();
  }
  if (!it->second.spec.ReturnsDynamic()) {
    return ObjectID::Nil();
  }
  return it->second.spec.ReturnId(0);
}

}  // namespace core
}  // namespace ray
