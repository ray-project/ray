// Copyright 2025 The Ray Authors.
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

#include <memory>
#include <string_view>

#include "ray/core_worker/shutdown_coordinator.h"

namespace ray {

namespace core {

class CoreWorker;

/// Concrete implementation of `ShutdownExecutorInterface` that executes actual
/// shutdown operations for `CoreWorker`.
///
/// Semantics overview:
/// - Graceful shutdown (ExecuteGracefulShutdown): stop accepting new work, drain ongoing
/// work, flush task
///   events, stop services (task execution service, gRPC server, IO service),
///   disconnect from the GCS/raylet, and join the IO thread if safe. This path
///   attempts best-effort cleanup to preserve observability and avoid resource
///   leaks. It may take up to `timeout_ms` for certain steps.
/// - Force shutdown (ExecuteForceShutdown): immediately kill child processes, disconnect
/// services, and
///   terminate the process without draining or cleanup. This path is used to
///   break out of hung or long-running shutdowns and should be considered
///   preemptive; it sacrifices cleanup for determinism.
/// - Worker exit (ExecuteWorkerExit): worker-type-specific graceful
///   shutdown that handles task draining and optional actor creation failure
///   payloads, then proceeds with the graceful sequence.
/// - Handle exit (ExecuteHandleExit): conditional exit that first checks worker
///   idleness and only proceeds when idle; otherwise it is ignored.
class CoreWorkerShutdownExecutor : public ShutdownExecutorInterface {
 public:
  /// Constructor with CoreWorker reference for accessing internals
  /// \param core_worker Reference to the CoreWorker instance
  explicit CoreWorkerShutdownExecutor(CoreWorker *core_worker);

  ~CoreWorkerShutdownExecutor() override = default;

  /// Execute graceful shutdown sequence.
  /// Stops task execution, flushes task events, stops IO/gRPC services, joins IO
  /// thread when not self, and disconnects from GCS. Best-effort cleanup.
  void ExecuteGracefulShutdown(std::string_view exit_type,
                               std::string_view detail,
                               std::chrono::milliseconds timeout_ms) override;

  /// Execute force shutdown sequence.
  /// Kills child processes, disconnects services, and terminates the process.
  /// Skips draining/cleanup for fast, deterministic termination.
  void ExecuteForceShutdown(std::string_view exit_type, std::string_view detail) override;

  /// Execute worker exit sequence with task draining.
  /// Drains tasks/references as applicable for worker mode, then performs
  /// graceful shutdown.
  void ExecuteExit(std::string_view exit_type,
                   std::string_view detail,
                   std::chrono::milliseconds timeout_ms,
                   const std::shared_ptr<LocalMemoryBuffer>
                       &creation_task_exception_pb_bytes) override;

  /// Execute exit sequence only if the worker is currently idle; otherwise, it
  /// logs and returns without action.
  void ExecuteExitIfIdle(std::string_view exit_type,
                         std::string_view detail,
                         std::chrono::milliseconds timeout_ms) override;

  void KillChildProcessesImmediately() override;

  bool ShouldWorkerIdleExit() const override;

 private:
  /// Reference to CoreWorker for accessing shutdown operations
  CoreWorker *core_worker_;

  void DisconnectServices(
      std::string_view exit_type,
      std::string_view detail,
      const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes);
  void QuickExit();
};
}  // namespace core
}  // namespace ray
