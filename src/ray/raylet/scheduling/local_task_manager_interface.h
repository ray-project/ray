// Copyright 2020-2021 The Ray Authors.
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

#pragma once

#include <deque>
#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "ray/common/task/task.h"
#include "ray/raylet/scheduling/internal.h"

namespace ray {
namespace raylet {

// Forward declaration
class WorkerInterface;

/// Manages the lifetime of a task on the local node. It receives request from
/// cluster_task_manager and tries to execute the task locally.
/// Read raylet/local_task_manager.h for more information.
class ILocalTaskManager {
 public:
  virtual ~ILocalTaskManager() = default;

  /// Queue task and schedule.
  virtual void QueueAndScheduleTask(std::shared_ptr<internal::Work> work) = 0;

  // Schedule and dispatch tasks.
  virtual void ScheduleAndDispatchTasks() = 0;

  /// Attempt to cancel all queued tasks that match the predicate.
  ///
  /// \param predicate: A function that returns true if a task needs to be cancelled.
  /// \param failure_type: The reason for cancellation.
  /// \param scheduling_failure_message: The reason message for cancellation.
  /// \return True if any task was successfully cancelled.
  virtual bool CancelTasks(
      std::function<bool(const std::shared_ptr<internal::Work> &)> predicate,
      rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
      const std::string &scheduling_failure_message) = 0;

  virtual const absl::flat_hash_map<SchedulingClass,
                                    std::deque<std::shared_ptr<internal::Work>>>
      &GetTaskToDispatch() const = 0;

  virtual const absl::flat_hash_map<SchedulingClass,
                                    absl::flat_hash_map<WorkerID, int64_t>>
      &GetBackLogTracker() const = 0;

  virtual void SetWorkerBacklog(SchedulingClass scheduling_class,
                                const WorkerID &worker_id,
                                int64_t backlog_size) = 0;

  virtual void ClearWorkerBacklog(const WorkerID &worker_id) = 0;

  virtual const RayTask *AnyPendingTasksForResourceAcquisition(
      int *num_pending_actor_creation, int *num_pending_tasks) const = 0;

  virtual void TasksUnblocked(const std::vector<TaskID> &ready_ids) = 0;

  virtual void TaskFinished(std::shared_ptr<WorkerInterface> worker, RayTask *task) = 0;

  virtual void ReleaseWorkerResources(std::shared_ptr<WorkerInterface> worker) = 0;

  virtual bool ReleaseCpuResourcesFromBlockedWorker(
      std::shared_ptr<WorkerInterface> worker) = 0;

  virtual bool ReturnCpuResourcesToUnblockedWorker(
      std::shared_ptr<WorkerInterface> worker) = 0;

  virtual ResourceSet CalcNormalTaskResources() const = 0;

  virtual void RecordMetrics() const = 0;

  virtual void DebugStr(std::stringstream &buffer) const = 0;

  virtual size_t GetNumTaskSpilled() const = 0;
  virtual size_t GetNumWaitingTaskSpilled() const = 0;
  virtual size_t GetNumUnschedulableTaskSpilled() const = 0;
};

/// A noop local task manager. It is a no-op class. We need this because there's no
/// "LocalTaskManager" when the `ClusterTaskManager` is used within GCS. In the long term,
/// we should make `ClusterTaskManager` not aware of `LocalTaskManager`.
class NoopLocalTaskManager : public ILocalTaskManager {
 public:
  NoopLocalTaskManager() = default;

  /// Queue task and schedule.
  void QueueAndScheduleTask(std::shared_ptr<internal::Work> work) override {
    RAY_CHECK(false)
        << "This function should never be called by gcs' local task manager.";
  }

  // Schedule and dispatch tasks.
  void ScheduleAndDispatchTasks() override {}

  bool CancelTasks(std::function<bool(const std::shared_ptr<internal::Work> &)> predicate,
                   rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
                   const std::string &scheduling_failure_message) override {
    return false;
  }

  const absl::flat_hash_map<SchedulingClass, std::deque<std::shared_ptr<internal::Work>>>
      &GetTaskToDispatch() const override {
    static const absl::flat_hash_map<SchedulingClass,
                                     std::deque<std::shared_ptr<internal::Work>>>
        tasks_to_dispatch;
    return tasks_to_dispatch;
  }

  const absl::flat_hash_map<SchedulingClass, absl::flat_hash_map<WorkerID, int64_t>>
      &GetBackLogTracker() const override {
    static const absl::flat_hash_map<SchedulingClass,
                                     absl::flat_hash_map<WorkerID, int64_t>>
        backlog_tracker;
    return backlog_tracker;
  }

  void SetWorkerBacklog(SchedulingClass scheduling_class,
                        const WorkerID &worker_id,
                        int64_t backlog_size) override {}

  void ClearWorkerBacklog(const WorkerID &worker_id) override {}

  const RayTask *AnyPendingTasksForResourceAcquisition(
      int *num_pending_actor_creation, int *num_pending_tasks) const override {
    return nullptr;
  }

  void TasksUnblocked(const std::vector<TaskID> &ready_ids) override {}

  void TaskFinished(std::shared_ptr<WorkerInterface> worker, RayTask *task) override {}

  void ReleaseWorkerResources(std::shared_ptr<WorkerInterface> worker) override {}

  bool ReleaseCpuResourcesFromBlockedWorker(
      std::shared_ptr<WorkerInterface> worker) override {
    return false;
  }

  bool ReturnCpuResourcesToUnblockedWorker(
      std::shared_ptr<WorkerInterface> worker) override {
    return false;
  }

  ResourceSet CalcNormalTaskResources() const override { return ResourceSet(); }

  void RecordMetrics() const override{};

  void DebugStr(std::stringstream &buffer) const override {}

  size_t GetNumTaskSpilled() const override { return 0; }
  size_t GetNumWaitingTaskSpilled() const override { return 0; }
  size_t GetNumUnschedulableTaskSpilled() const override { return 0; }
};

}  // namespace raylet
}  // namespace ray
