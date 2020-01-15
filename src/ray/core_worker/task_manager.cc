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
  std::pair<TaskSpecification, int> entry = {spec, max_retries};
  RAY_CHECK(pending_tasks_.emplace(spec.TaskId(), std::move(entry)).second);

  // Add references for the dependencies to the task.
  std::vector<ObjectID> task_deps;
  for (size_t i = 0; i < spec.NumArgs(); i++) {
    if (spec.ArgByRef(i)) {
      for (size_t j = 0; j < spec.ArgIdCount(i); j++) {
        task_deps.push_back(spec.ArgId(i, j));
      }
    }
  }
  reference_counter_->AddSubmittedTaskReferences(task_deps);

  // Add new owned objects for the return values of the task.
  size_t num_returns = spec.NumReturns();
  if (spec.IsActorCreationTask() || spec.IsActorTask()) {
    num_returns--;
  }
  for (size_t i = 0; i < num_returns; i++) {
    reference_counter_->AddOwnedObject(spec.ReturnId(i, TaskTransportType::DIRECT),
                                       caller_id, caller_address);
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
                                      const rpc::Address *actor_addr) {
  RAY_LOG(DEBUG) << "Completing task " << task_id;
  TaskSpecification spec;
  {
    absl::MutexLock lock(&mu_);
    auto it = pending_tasks_.find(task_id);
    RAY_CHECK(it != pending_tasks_.end())
        << "Tried to complete task that was not pending " << task_id;
    spec = it->second.first;
    pending_tasks_.erase(it);
  }

  RemovePlasmaSubmittedTaskReferences(spec);

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
      RAY_CHECK_OK(
          in_memory_store_->Put(RayObject(data_buffer, metadata_buffer), object_id));
    }
  }

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
  {
    absl::MutexLock lock(&mu_);
    auto it = pending_tasks_.find(task_id);
    RAY_CHECK(it != pending_tasks_.end())
        << "Tried to complete task that was not pending " << task_id;
    spec = it->second.first;
    num_retries_left = it->second.second;
    if (num_retries_left == 0) {
      pending_tasks_.erase(it);
    } else {
      RAY_CHECK(num_retries_left > 0);
      it->second.second--;
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
    RemovePlasmaSubmittedTaskReferences(spec);
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

void TaskManager::RemoveSubmittedTaskReferences(const std::vector<ObjectID> &object_ids) {
  std::vector<ObjectID> deleted;
  reference_counter_->RemoveSubmittedTaskReferences(object_ids, &deleted);
  in_memory_store_->Delete(deleted);
}

void TaskManager::OnTaskDependenciesInlined(const std::vector<ObjectID> &object_ids) {
  RemoveSubmittedTaskReferences(object_ids);
}

void TaskManager::RemovePlasmaSubmittedTaskReferences(TaskSpecification &spec) {
  std::vector<ObjectID> plasma_dependencies;
  for (size_t i = 0; i < spec.NumArgs(); i++) {
    auto count = spec.ArgIdCount(i);
    if (count > 0) {
      const auto &id = spec.ArgId(i, 0);
      plasma_dependencies.push_back(id);
    }
  }
  RemoveSubmittedTaskReferences(plasma_dependencies);
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
  return it->second.first;
}

}  // namespace ray
