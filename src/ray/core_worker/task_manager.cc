#include "ray/core_worker/task_manager.h"

#include "ray/util/util.h"

namespace ray {

// Start throttling task failure logs once we hit this threshold.
const int64_t kTaskFailureThrottlingThreshold = 50;

// Throttle task failure logs to once this interval.
const int64_t kTaskFailureLoggingFrequencyMillis = 5000;

void TaskManager::AddPendingTask(const TaskID &caller_id,
                                 const rpc::Address &caller_address,
                                 const TaskSpecification &spec, int max_retries) {
  RAY_LOG(DEBUG) << "Adding pending task " << spec.TaskId();
  absl::MutexLock lock(&mu_);
  RAY_CHECK(pending_tasks_.emplace(spec.TaskId(), TaskEntry(spec, max_retries)).second);

  // Add references for the dependencies to the task.
  std::vector<ObjectID> task_deps;
  for (size_t i = 0; i < spec.NumArgs(); i++) {
    if (spec.ArgByRef(i)) {
      for (size_t j = 0; j < spec.ArgIdCount(i); j++) {
        task_deps.push_back(spec.ArgId(i, j));
        RAY_LOG(DEBUG) << "Adding arg ID " << spec.ArgId(i, j);
      }
    } else {
      const auto &inlined_ids = spec.ArgInlinedIds(i);
      for (const auto &inlined_id : inlined_ids) {
        task_deps.push_back(inlined_id);
        RAY_LOG(DEBUG) << "Adding inlined ID " << inlined_id;
      }
    }
  }
  reference_counter_->UpdateSubmittedTaskReferences(
      task_deps, lineage_pinning_enabled_ ? spec.NumReturns() : 0);

  // Add new owned objects for the return values of the task.
  size_t num_returns = spec.NumReturns();
  if (spec.IsActorCreationTask() || spec.IsActorTask()) {
    num_returns--;
  }
  for (size_t i = 0; i < num_returns; i++) {
    // We pass an empty vector for inner IDs because we do not know the return
    // value of the task yet. If the task returns an ID(s), the worker will
    // notify us via the WaitForRefRemoved RPC that we are now a borrower for
    // the inner IDs. Note that this RPC can be received *before* the
    // PushTaskReply.
    reference_counter_->AddOwnedObject(spec.ReturnId(i, TaskTransportType::DIRECT),
                                       /*inner_ids=*/{}, caller_id, caller_address);
  }
}

void TaskManager::DrainAndShutdown(std::function<void()> shutdown) {
  absl::MutexLock lock(&mu_);
  if (pending_tasks_.empty()) {
    shutdown();
  } else {
    RAY_LOG(WARNING)
        << "This worker is still managing " << pending_tasks_.size()
        << " in flight tasks, waiting for them to finish before shutting down.";
  }
  shutdown_hook_ = shutdown;
}

bool TaskManager::IsTaskPending(const TaskID &task_id) const {
  absl::MutexLock lock(&mu_);
  return pending_tasks_.count(task_id) > 0;
}

void TaskManager::CompletePendingTask(const TaskID &task_id,
                                      const rpc::PushTaskReply &reply,
                                      const rpc::Address &worker_addr) {
  RAY_LOG(DEBUG) << "Completing task " << task_id;

  size_t num_direct_returns = 0;
  for (int i = 0; i < reply.return_objects_size(); i++) {
    const auto &return_object = reply.return_objects(i);
    ObjectID object_id = ObjectID::FromBinary(return_object.object_id());

    if (return_object.in_plasma()) {
      // Mark it as in plasma with a dummy object.
      RAY_CHECK_OK(
          in_memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id));
    } else {
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
      RAY_CHECK_OK(in_memory_store_->Put(
          RayObject(data_buffer, metadata_buffer,
                    IdVectorFromProtobuf<ObjectID>(return_object.nested_inlined_ids())),
          object_id));
      num_direct_returns++;
    }
  }

  TaskSpecification spec;
  size_t num_returns_to_release = 0;
  {
    absl::MutexLock lock(&mu_);
    auto it = pending_tasks_.find(task_id);
    RAY_CHECK(it != pending_tasks_.end())
        << "Tried to complete task that was not pending " << task_id;
    spec = it->second.spec;

    if (it->second.num_executions == 0) {
      // This is the first time that the task has finished. Release the lineage
      // for any non-plasma return IDs.
      it->second.num_plasma_returns_in_scope -= num_direct_returns;
      num_returns_to_release += num_direct_returns;
    }
    it->second.num_executions++;

    if (!lineage_pinning_enabled_) {
      pending_tasks_.erase(it);
    } else if (it->second.num_retries_left == 0) {
      // Erase the task.
      num_returns_to_release += it->second.num_plasma_returns_in_scope;
      pending_tasks_.erase(it);
    } else if (it->second.num_plasma_returns_in_scope == 0) {
      // Erase the task.
      pending_tasks_.erase(it);
    }
  }

  // TODO: Move this inside the lock? I don't think this is correct otherwise.
  RemoveFinishedTaskReferences(spec, num_returns_to_release, worker_addr,
                               reply.borrowed_refs());

  ShutdownIfNeeded();
}

void TaskManager::PendingTaskFailed(const TaskID &task_id, rpc::ErrorType error_type,
                                    Status *status) {
  // Note that this might be the __ray_terminate__ task, so we don't log
  // loudly with ERROR here.
  RAY_LOG(DEBUG) << "Task " << task_id << " failed with error "
                 << rpc::ErrorType_Name(error_type);
  int num_retries_left = 0;
  TaskSpecification spec;
  size_t num_plasma_returns_in_scope = 0;
  {
    absl::MutexLock lock(&mu_);
    auto it = pending_tasks_.find(task_id);
    RAY_CHECK(it != pending_tasks_.end())
        << "Tried to complete task that was not pending " << task_id;
    spec = it->second.spec;
    num_retries_left = it->second.num_retries_left;
    if (num_retries_left == 0) {
      num_plasma_returns_in_scope = it->second.num_plasma_returns_in_scope;
      pending_tasks_.erase(it);
    } else {
      RAY_CHECK(it->second.num_retries_left > 0);
      it->second.num_retries_left--;
    }
  }

  // We should not hold the lock during these calls because they may trigger
  // callbacks in this or other classes.
  if (num_retries_left > 0) {
    RAY_LOG(ERROR) << num_retries_left << " retries left for task " << spec.TaskId()
                   << ", attempting to resubmit.";
    retry_task_callback_(spec);
  } else {
    // Throttled logging of task failure errors.
    {
      absl::MutexLock lock(&mu_);
      auto debug_str = spec.DebugString();
      if (debug_str.find("__ray_terminate__") == std::string::npos &&
          (num_failure_logs_ < kTaskFailureThrottlingThreshold ||
           (current_time_ms() - last_log_time_ms_) >
               kTaskFailureLoggingFrequencyMillis)) {
        if (num_failure_logs_++ == kTaskFailureThrottlingThreshold) {
          RAY_LOG(ERROR) << "Too many failure logs, throttling to once every "
                         << kTaskFailureLoggingFrequencyMillis << " millis.";
        }
        last_log_time_ms_ = current_time_ms();
        if (status != nullptr) {
          RAY_LOG(ERROR) << "Task failed: " << *status << ": " << spec.DebugString();
        } else {
          RAY_LOG(ERROR) << "Task failed: " << spec.DebugString();
        }
      }
    }
    // The worker failed to execute the task, so it cannot be borrowing any
    // objects.
    RemoveFinishedTaskReferences(spec, num_plasma_returns_in_scope, rpc::Address(),
                                 ReferenceCounter::ReferenceTableProto());
    MarkPendingTaskFailed(task_id, spec, error_type);
  }

  ShutdownIfNeeded();
}

void TaskManager::ShutdownIfNeeded() {
  absl::MutexLock lock(&mu_);
  if (shutdown_hook_ && pending_tasks_.empty()) {
    RAY_LOG(WARNING) << "All in flight tasks finished, shutting down worker.";
    shutdown_hook_();
  }
}

void TaskManager::OnTaskDependenciesInlined(
    const std::vector<ObjectID> &inlined_dependency_ids,
    const std::vector<ObjectID> &contained_ids, size_t num_plasma_returns) {
  std::vector<ObjectID> deleted;
  reference_counter_->UpdateSubmittedTaskReferences(
      /*argument_ids_to_add=*/contained_ids,
      lineage_pinning_enabled_ ? num_plasma_returns : 0,
      /*argument_ids_to_remove=*/inlined_dependency_ids, &deleted);
  in_memory_store_->Delete(deleted);
}

void TaskManager::RemoveFinishedTaskReferences(
    TaskSpecification &spec, size_t num_returns_to_release,
    const rpc::Address &borrower_addr,
    const ReferenceCounter::ReferenceTableProto &borrowed_refs) {
  std::vector<ObjectID> plasma_dependencies;
  for (size_t i = 0; i < spec.NumArgs(); i++) {
    if (spec.ArgByRef(i)) {
      for (size_t j = 0; j < spec.ArgIdCount(i); j++) {
        plasma_dependencies.push_back(spec.ArgId(i, j));
      }
    } else {
      const auto &inlined_ids = spec.ArgInlinedIds(i);
      plasma_dependencies.insert(plasma_dependencies.end(), inlined_ids.begin(),
                                 inlined_ids.end());
    }
  }

  std::vector<ObjectID> deleted;
  reference_counter_->UpdateFinishedTaskReferences(
      plasma_dependencies, lineage_pinning_enabled_ ? num_returns_to_release : 0,
      borrower_addr, borrowed_refs, &deleted);
  in_memory_store_->Delete(deleted);
}

void TaskManager::RemoveLineageReference(const ObjectID &object_id,
                                         std::vector<ObjectID> *released_objects) {
  RAY_CHECK(lineage_pinning_enabled_);

  absl::MutexLock lock(&mu_);
  const TaskID &task_id = object_id.TaskId();
  auto it = pending_tasks_.find(task_id);
  if (it == pending_tasks_.end()) {
    RAY_LOG(WARNING) << "Failed to find lineage for object " << object_id;
    return;
  }

  RAY_CHECK(it->second.num_plasma_returns_in_scope > 0);
  it->second.num_plasma_returns_in_scope--;

  // Decrement the lineage ref count for each of the task's args once.
  for (size_t i = 0; i < it->second.spec.NumArgs(); i++) {
    if (it->second.spec.ArgByRef(i)) {
      for (size_t j = 0; j < it->second.spec.ArgIdCount(i); j++) {
        released_objects->push_back(it->second.spec.ArgId(i, j));
      }
    } else {
      const auto &inlined_ids = it->second.spec.ArgInlinedIds(i);
      released_objects->insert(released_objects->end(), inlined_ids.begin(),
                               inlined_ids.end());
    }
  }

  if (it->second.num_plasma_returns_in_scope == 0 && it->second.num_executions > 0) {
    // The task has finished and none of the return IDs are in scope anymore,
    // so it is safe to remove the task spec.
    pending_tasks_.erase(it);
  }
}

void TaskManager::MarkPendingTaskFailed(const TaskID &task_id,
                                        const TaskSpecification &spec,
                                        rpc::ErrorType error_type) {
  RAY_LOG(DEBUG) << "Treat task as failed. task_id: " << task_id
                 << ", error_type: " << ErrorType_Name(error_type);
  int64_t num_returns = spec.NumReturns();
  for (int i = 0; i < num_returns; i++) {
    const auto object_id = ObjectID::ForTaskReturn(
        task_id, /*index=*/i + 1,
        /*transport_type=*/static_cast<int>(TaskTransportType::DIRECT));
    RAY_CHECK_OK(in_memory_store_->Put(RayObject(error_type), object_id));
  }

  if (spec.IsActorCreationTask()) {
    // Publish actor death if actor creation task failed after
    // a number of retries.
    actor_manager_->PublishTerminatedActor(spec);
  }
}

TaskSpecification TaskManager::GetTaskSpec(const TaskID &task_id) const {
  absl::MutexLock lock(&mu_);
  auto it = pending_tasks_.find(task_id);
  RAY_CHECK(it != pending_tasks_.end());
  return it->second.spec;
}

}  // namespace ray
