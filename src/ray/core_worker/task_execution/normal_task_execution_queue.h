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

#pragma once

#include <deque>
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/core_worker/task_execution/common.h"
#include "ray/rpc/rpc_callback_types.h"

namespace ray {
namespace core {

/// Used to implement the non-actor task queue. These tasks do not have ordering
/// constraints.
class NormalTaskExecutionQueue {
 public:
  NormalTaskExecutionQueue();

  void Stop();

  void EnqueueTask(TaskToExecute task);

  /// Search for a TaskToExecute associated with the task that we are trying to cancel.
  /// If found, remove the TaskToExecute from the queue and return true. Else,
  /// return false.
  bool CancelTaskIfFound(TaskID task_id);

  /// Execute as many queued tasks as possible.
  void ExecuteQueuedTasks();

 private:
  /// Cancel all tasks queued for execution.
  void CancelAllQueuedTasks(const std::string &msg);

  // Get the next queued task to execute if available.
  std::optional<TaskToExecute> TryPopQueuedTask();

  /// Protects access to the dequeue below.
  mutable absl::Mutex mu_;

  /// Queue of tasks to execute.
  std::deque<TaskToExecute> pending_normal_tasks_ ABSL_GUARDED_BY(mu_);
};

}  // namespace core
}  // namespace ray
