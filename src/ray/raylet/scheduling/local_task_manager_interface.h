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

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/ray_object.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_common.h"
#include "ray/raylet/scheduling/internal.h"

namespace ray {
namespace raylet {

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

  /// Attempt to cancel an already queued task.
  ///
  /// \param task_id: The id of the task to remove.
  /// \param failure_type: The failure type.
  ///
  /// \return True if task was successfully removed. This function will return
  /// false if the task is already running.
  virtual bool CancelTask(
      const TaskID &task_id,
      rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type =
          rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED,
      const std::string &scheduling_failure_message = "") = 0;

  virtual const absl::flat_hash_map<SchedulingClass,
                                    std::deque<std::shared_ptr<internal::Work>>>
      &GetTaskToDispatch() const = 0;

  virtual const absl::flat_hash_map<SchedulingClass,
                                    absl::flat_hash_map<WorkerID, int64_t>>
      &GetBackLogTracker() const = 0;

  virtual bool AnyPendingTasksForResourceAcquisition(RayTask *example,
                                                     bool *any_pending,
                                                     int *num_pending_actor_creation,
                                                     int *num_pending_tasks) const = 0;

  virtual void RecordMetrics() const = 0;

  virtual void DebugStr(std::stringstream &buffer) const = 0;

  virtual size_t GetNumTaskSpilled() const = 0;
  virtual size_t GetNumWaitingTaskSpilled() const = 0;
  virtual size_t GetNumUnschedulableTaskSpilled() const = 0;
};
}  // namespace raylet
}  // namespace ray
