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

#include "ray/core_worker/shutdown_coordinator.h"

namespace ray {
namespace core {

// Forward declarations
class CoreWorker;

/// Concrete implementation of ShutdownDependencies for production use.
/// This class wraps the real CoreWorker services and delegates shutdown
/// operations to the appropriate CoreWorker methods.
class CoreWorkerShutdownDependencies : public ShutdownDependencies {
 public:
  /// Constructor that takes a reference to the CoreWorker instance.
  /// \param core_worker Reference to the CoreWorker that owns this instance
  explicit CoreWorkerShutdownDependencies(CoreWorker& core_worker);

  ~CoreWorkerShutdownDependencies() override = default;

  // Non-copyable
  CoreWorkerShutdownDependencies(const CoreWorkerShutdownDependencies&) = delete;
  CoreWorkerShutdownDependencies& operator=(const CoreWorkerShutdownDependencies&) = delete;

  /// Disconnect from raylet
  void DisconnectRaylet() override;

  /// Disconnect from GCS
  void DisconnectGcs() override;

  /// Shutdown task manager gracefully
  void ShutdownTaskManager(bool force) override;

  /// Shutdown object recovery manager
  void ShutdownObjectRecovery() override;

  /// Cancel all pending tasks
  void CancelPendingTasks(bool force) override;

  /// Clean up actor state (if actor worker)
  void CleanupActorState() override;

  /// Flush any remaining logs/metrics
  void FlushMetrics() override;

  /// Get pending task count for graceful shutdown decisions
  size_t GetPendingTaskCount() const override;

  /// Check if graceful shutdown timeout has elapsed
  bool IsGracefulShutdownTimedOut(
      std::chrono::steady_clock::time_point start_time,
      std::chrono::milliseconds timeout) const override;

 private:
  /// Reference to the CoreWorker instance that owns this dependencies object
  CoreWorker& core_worker_;
};

}  // namespace core
}  // namespace ray 