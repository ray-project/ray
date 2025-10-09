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

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "absl/synchronization/mutex.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
class LocalMemoryBuffer;
}  // namespace ray

namespace ray {

namespace core {

/// Interface for executing shutdown operations that the coordinator invokes.
class ShutdownExecutorInterface {
 public:
  virtual ~ShutdownExecutorInterface() = default;

  virtual void ExecuteGracefulShutdown(std::string_view exit_type,
                                       std::string_view detail,
                                       std::chrono::milliseconds timeout_ms) = 0;

  virtual void ExecuteForceShutdown(std::string_view exit_type,
                                    std::string_view detail) = 0;

  virtual void ExecuteExit(std::string_view exit_type,
                           std::string_view detail,
                           std::chrono::milliseconds timeout_ms,
                           const std::shared_ptr<::ray::LocalMemoryBuffer>
                               &creation_task_exception_pb_bytes) = 0;

  virtual void ExecuteExitIfIdle(std::string_view exit_type,
                                 std::string_view detail,
                                 std::chrono::milliseconds timeout_ms) = 0;

  // Best-effort cleanup of child processes spawned by this worker process to
  // avoid leaked subprocesses holding expensive resources (e.g., CUDA contexts).
  //
  // - Intended to be called during shutdown (including force paths).
  // - Only targets direct children of the current process; crash paths can still leak
  //   (subreaper not yet used).
  // - No-ops when disabled by configuration
  //   (RayConfig::kill_child_processes_on_worker_exit()).
  // - Platform-dependent: process enumeration may be unavailable on some OSes.
  virtual void KillChildProcessesImmediately() = 0;

  virtual bool ShouldWorkerIdleExit() const = 0;
};

/// Reasons for worker shutdown. Used for observability and debugging.
enum class ShutdownReason : std::uint8_t {
  kNone = 0,
  kIntentionalShutdown = 1,
  kUnexpectedError = 2,
  kIdleTimeout = 3,
  kGracefulExit = 4,
  kForcedExit = 5,
  kUserError = 6,
  kOutOfMemory = 7,
  kJobFinished = 8,
  kActorKilled = 9,
  kActorCreationFailed = 10
};

/// Shutdown state representing the current lifecycle phase of worker shutdown.
/// The state machine supports two paths with only forward transitions:
///
/// Normal shutdown:  kRunning -> kShuttingDown -> kDisconnecting -> kShutdown
/// Force shutdown:   kRunning -> kShuttingDown -> kShutdown (bypasses kDisconnecting)
///
/// State semantics:
/// - kRunning: Normal operation, accepting new work
/// - kShuttingDown: Shutdown initiated, draining existing work, no new work accepted
/// - kDisconnecting: Disconnecting from services (raylet, GCS), cleanup phase
/// - kShutdown: Final state, all cleanup complete, ready for process termination
enum class ShutdownState : std::uint8_t {
  kRunning = 0,
  kShuttingDown = 1,
  kDisconnecting = 2,
  kShutdown = 3
};

/// Thread-safe coordinator for managing worker shutdown state and transitions.
///
/// Uses a single mutex to serialize state transitions and to capture the shutdown
/// reason exactly once. We favor simple, readable synchronization because shutdown is
/// control-path, not throughput-critical.
///
/// Key features:
/// - Atomic state transitions with integrated reason tracking
/// - Idempotent shutdown operations
/// - Performance optimized for hot-path checks
/// - Thread-safe from any thread context
///
/// Usage:
///   auto coordinator = std::make_unique<ShutdownCoordinator>();
///
///   // Try to initiate shutdown (only the first caller succeeds)
///   if (coordinator->TryInitiateShutdown(ShutdownReason::kGracefulExit)) {
///     // This thread should execute shutdown sequence
///   }
///
///   // Fast check for early exit in performance-critical paths
///   if (coordinator->ShouldEarlyExit()) {
///     return Status::Invalid("Worker is shutting down");
///   }
class ShutdownCoordinator {
 public:
  static constexpr std::chrono::milliseconds kInfiniteTimeout{-1};
  /// Constructor
  ///
  /// \param executor Shutdown executor implementation
  /// \param worker_type Type of worker for shutdown behavior customization
  explicit ShutdownCoordinator(std::unique_ptr<ShutdownExecutorInterface> executor,
                               rpc::WorkerType worker_type = rpc::WorkerType::WORKER);

  ~ShutdownCoordinator() = default;

  // Non-copyable and non-movable for safety
  ShutdownCoordinator(const ShutdownCoordinator &) = delete;
  ShutdownCoordinator &operator=(const ShutdownCoordinator &) = delete;
  ShutdownCoordinator(ShutdownCoordinator &&) = delete;
  ShutdownCoordinator &operator=(ShutdownCoordinator &&) = delete;

  /// Request shutdown with configurable timeout and fallback behavior.
  ///
  /// Single entry-point that captures the first shutdown reason, chooses the
  /// worker-type-specific path, and optionally falls back to force. Additional
  /// graceful requests are ignored; a concurrent force may override the reason
  /// and proceed.
  ///
  /// \param force_shutdown If true, force immediate shutdown; if false, graceful shutdown
  /// \param reason The reason for shutdown initiation
  /// \param detail Optional detailed explanation
  /// \param timeout_ms Timeout for graceful shutdown (-1 = no timeout)
  /// \return true if this call initiated shutdown, false if already shutting down
  bool RequestShutdown(bool force_shutdown,
                       ShutdownReason reason,
                       std::string_view detail = "",
                       std::chrono::milliseconds timeout_ms = kInfiniteTimeout,
                       const std::shared_ptr<::ray::LocalMemoryBuffer>
                           &creation_task_exception_pb_bytes = nullptr);

  /// Get the current shutdown state (mutex-protected, fast path safe).
  ///
  /// \return Current shutdown state
  ShutdownState GetState() const;

  /// Get the shutdown reason.
  ///
  /// The reason is set when shutdown is first initiated and remains
  /// constant throughout the shutdown process.
  ///
  /// \return Shutdown reason (kNone if not shutting down)
  ShutdownReason GetReason() const;

  /// Check if worker should early-exit from operations.
  ///
  /// Recommended hot-path check; returns true for any non-running state.
  ///
  /// \return true if operations should be aborted, false if normal operation
  bool ShouldEarlyExit() const;

  /// Check if worker is in running state.
  ///
  /// \return true if in kRunning state, false otherwise
  bool IsRunning() const;

  /// Check if shutdown has been initiated.
  ///
  /// \return true if in any shutdown state, false if still running
  bool IsShuttingDown() const;

  /// Check if worker has completed shutdown.
  ///
  /// \return true if in kShutdown state, false otherwise
  bool IsShutdown() const;

  /// Get string representation of current state.
  ///
  /// \return Human-readable state description
  std::string GetStateString() const;

  /// Get string representation of exit type based on shutdown reason.
  std::string GetExitTypeString() const;

  /// Get string representation of shutdown reason.
  ///
  /// \return Human-readable reason description
  std::string GetReasonString() const;

 private:
  /// Attempt to transition to disconnecting state.
  /// Begins the disconnection/cleanup phase (e.g., GCS/raylet disconnect). Only
  /// valid from kShuttingDown.
  /// \return true if transition succeeded, false if invalid state
  bool TryTransitionToDisconnecting();

  /// Attempt to transition to final shutdown state.
  /// Finalizes shutdown. Allowed from kDisconnecting (normal) or kShuttingDown
  /// (force path).
  /// \return true if transition succeeded, false if invalid state
  bool TryTransitionToShutdown();

  /// Execute shutdown sequence based on worker type and mode
  void ExecuteShutdownSequence(
      bool force_shutdown,
      std::string_view detail,
      std::chrono::milliseconds timeout_ms,
      const std::shared_ptr<::ray::LocalMemoryBuffer> &creation_task_exception_pb_bytes);

  /// Executes graceful path; transitions to Disconnecting/Shutdown
  void ExecuteGracefulShutdown(std::string_view detail,
                               std::chrono::milliseconds timeout_ms);

  /// Executes force path; guarded to run at most once
  void ExecuteForceShutdown(std::string_view detail);

  void ExecuteDriverShutdown(bool force_shutdown,
                             std::string_view detail,
                             std::chrono::milliseconds timeout_ms);
  /// Worker-type specific shutdown behavior
  /// - Honors kActorCreationFailed with serialized exception payloads
  /// - Uses worker-idle checks for idle exits
  /// - Drains tasks/references before disconnect
  void ExecuteWorkerShutdown(
      bool force_shutdown,
      std::string_view detail,
      std::chrono::milliseconds timeout_ms,
      const std::shared_ptr<::ray::LocalMemoryBuffer> &creation_task_exception_pb_bytes);

  // Executor and configuration
  std::unique_ptr<ShutdownExecutorInterface> executor_;
  rpc::WorkerType worker_type_;

  // Mutex-guarded shutdown state
  mutable absl::Mutex mu_;
  ShutdownState state_ ABSL_GUARDED_BY(mu_) = ShutdownState::kRunning;
  ShutdownReason reason_ ABSL_GUARDED_BY(mu_) = ShutdownReason::kNone;
  bool force_executed_ ABSL_GUARDED_BY(mu_) = false;
  bool force_started_ ABSL_GUARDED_BY(mu_) = false;

  /// Shutdown detail for observability (set once during shutdown initiation)
  std::string shutdown_detail_ ABSL_GUARDED_BY(mu_);
};
}  // namespace core
}  // namespace ray
