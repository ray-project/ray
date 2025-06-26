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

#include "ray/core_worker/core_worker_shutdown_dependencies.h"

#include "ray/core_worker/core_worker.h"

namespace ray {
namespace core {

CoreWorkerShutdownDependencies::CoreWorkerShutdownDependencies(CoreWorker& core_worker)
    : core_worker_(core_worker) {
}

void CoreWorkerShutdownDependencies::DisconnectRaylet() {
  // Delegate to existing CoreWorker method
  core_worker_.Disconnect();
}

void CoreWorkerShutdownDependencies::DisconnectGcs() {
  // For now, GCS disconnection is handled as part of Disconnect()
  // TODO: Add specific GCS disconnect method if needed
}

void CoreWorkerShutdownDependencies::ShutdownTaskManager(bool force) {
  // Delegate to task manager shutdown
  if (core_worker_.task_manager_) {
    core_worker_.task_manager_->Stop();
  }
}

void CoreWorkerShutdownDependencies::ShutdownObjectRecovery() {
  // Delegate to object recovery manager shutdown
  if (core_worker_.object_recovery_manager_) {
    core_worker_.object_recovery_manager_->Stop();
  }
}

void CoreWorkerShutdownDependencies::CancelPendingTasks(bool force) {
  // Cancel all pending tasks
  if (core_worker_.task_manager_) {
    core_worker_.task_manager_->CancelAllPendingTasks();
  }
}

void CoreWorkerShutdownDependencies::CleanupActorState() {
  // Clean up actor-specific state
  if (core_worker_.GetWorkerType() == WorkerType::WORKER && 
      core_worker_.actor_manager_) {
    // Only clean up if this is an actor worker
    core_worker_.actor_manager_->Stop();
  }
}

void CoreWorkerShutdownDependencies::FlushMetrics() {
  // Flush any remaining metrics/logs
  // TODO: Add specific metrics flushing if needed
}

size_t CoreWorkerShutdownDependencies::GetPendingTaskCount() const {
  // Get current pending task count
  if (core_worker_.task_manager_) {
    return core_worker_.task_manager_->GetPendingTasksCount();
  }
  return 0;
}

bool CoreWorkerShutdownDependencies::IsGracefulShutdownTimedOut(
    std::chrono::steady_clock::time_point start_time,
    std::chrono::milliseconds timeout) const {
  auto now = std::chrono::steady_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time);
  return elapsed >= timeout;
}

}  // namespace core
}  // namespace ray 