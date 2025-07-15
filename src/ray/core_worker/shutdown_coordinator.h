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

#include <atomic>
#include <chrono>
#include <memory>
#include <string>

#include "ray/core_worker/common.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

namespace core {

/// Interface for executing shutdown operations and provides the shutdown executor
/// that the coordinator invokes. CoreWorkerShutdownExecutor executes real work.
class ShutdownExecutorInterface {
 public:
  virtual ~ShutdownExecutorInterface() = default;

  /// Execute complete graceful shutdown sequence
  virtual void ExecuteGracefulShutdown(const std::string &exit_type,
                                       const std::string &detail,
                                       std::chrono::milliseconds timeout_ms) = 0;

  /// Execute complete force shutdown sequence
  virtual void ExecuteForceShutdown(const std::string &exit_type,
                                    const std::string &detail) = 0;

  /// Execute worker exit sequence with task draining
  virtual void ExecuteWorkerExit(const std::string &exit_type,
                                 const std::string &detail,
                                 std::chrono::milliseconds timeout_ms) = 0;

  /// Execute handle exit sequence with idle checking
  virtual void ExecuteHandleExit(const std::string &exit_type,
                                 const std::string &detail,
                                 std::chrono::milliseconds timeout_ms) = 0;

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
/// The state machine executes in order with only forward transitions possible:
/// kRunning -> kShuttingDown -> kDisconnecting -> kShutdown
enum class ShutdownState : std::uint8_t {
  kRunning = 0,
  kShuttingDown = 1,
  kDisconnecting = 2,
  kShutdown = 3
};

/// Thread-safe coordinator for managing worker shutdown state and transitions.
///
/// This class provides atomic state management for shutdown operations using a
/// single 64-bit atomic variable that packs both state and reason information.
/// This design ensures consistent reads of both state and reason together,
/// eliminating race conditions in multi-threaded shutdown scenarios.
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
  /// Constructor
  ///
  /// \param executor Shutdown executor implementation
  /// \param worker_type Type of worker for shutdown behavior customization
  explicit ShutdownCoordinator(std::unique_ptr<ShutdownExecutorInterface> executor,
                               WorkerType worker_type = WorkerType::WORKER);

  ~ShutdownCoordinator() = default;

  // Non-copyable and non-movable for safety
  ShutdownCoordinator(const ShutdownCoordinator &) = delete;
  ShutdownCoordinator &operator=(const ShutdownCoordinator &) = delete;
  ShutdownCoordinator(ShutdownCoordinator &&) = delete;
  ShutdownCoordinator &operator=(ShutdownCoordinator &&) = delete;

  /// Request shutdown with configurable timeout and fallback behavior.
  ///
  /// This is the main entry point for all shutdown operations. It will:
  /// 1. Atomically transition to shutting down state (idempotent)
  /// 2. Execute appropriate shutdown sequence based on mode and worker type
  /// 3. Handle graceful vs force shutdown behavior with caller-specified timeout
  ///
  /// \param force_shutdown If true, force immediate shutdown; if false, graceful shutdown
  /// \param reason The reason for shutdown initiation
  /// \param detail Optional detailed explanation
  /// \param timeout_ms Timeout for graceful shutdown (0 = no timeout, immediate force
  /// fallback) \param force_on_timeout If true, fallback to force shutdown on timeout; if
  /// false, wait indefinitely
  /// \return true if this call initiated shutdown, false if already shutting down
  bool RequestShutdown(
      bool force_shutdown,
      ShutdownReason reason,
      std::string_view detail = "",
      std::chrono::milliseconds timeout_ms = std::chrono::milliseconds{0},
      bool force_on_timeout = false);

  /// Legacy method for compatibility - delegates to RequestShutdown
  /// \param reason The reason for shutdown initiation
  /// \return true if this call initiated shutdown, false if already shutting down
  bool TryInitiateShutdown(ShutdownReason reason);

  /// Attempt to transition to disconnecting state.
  ///
  /// This should be called when beginning disconnection from raylet/GCS.
  /// Can only succeed if currently in kShuttingDown state.
  ///
  /// \return true if transition succeeded, false if invalid state
  bool TryTransitionToDisconnecting();

  /// Attempt to transition to final shutdown state.
  ///
  /// This should be called when shutdown sequence is complete.
  /// Can succeed from either kShuttingDown or kDisconnecting states.
  ///
  /// \return true if transition succeeded, false if invalid state
  bool TryTransitionToShutdown();

  /// Get the current shutdown state.
  ///
  /// This is a fast, lock-free operation suitable for hot paths.
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
  /// This is the recommended way to check shutdown status in performance-critical
  /// paths. Returns true for any state other than kRunning.
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
  /// Execute shutdown sequence based on worker type and mode
  void ExecuteShutdownSequence(bool force_shutdown,
                               const std::string &detail,
                               std::chrono::milliseconds timeout_ms,
                               bool force_on_timeout);

  /// Execute graceful shutdown with timeout
  void ExecuteGracefulShutdown(const std::string &detail,
                               std::chrono::milliseconds timeout_ms);

  /// Execute force shutdown immediately
  void ExecuteForceShutdown(const std::string &detail);

  /// Worker-type specific shutdown behavior
  void ExecuteDriverShutdown(bool force_shutdown,
                             const std::string &detail,
                             std::chrono::milliseconds timeout_ms,
                             bool force_on_timeout);
  void ExecuteWorkerShutdown(bool force_shutdown,
                             const std::string &detail,
                             std::chrono::milliseconds timeout_ms,
                             bool force_on_timeout);

  /// Pack state and reason into a single 64-bit value for atomic operations.
  /// Layout: [32-bit state][32-bit reason]
  static uint64_t PackStateReason(ShutdownState state, ShutdownReason reason);

  /// Extract state from packed 64-bit value.
  static ShutdownState UnpackState(uint64_t packed);

  /// Extract reason from packed 64-bit value.
  static ShutdownReason UnpackReason(uint64_t packed);

  // Executor and configuration
  std::unique_ptr<ShutdownExecutorInterface> executor_;
  WorkerType worker_type_;

  /// Single atomic variable holding both state and reason.
  /// This ensures atomic updates of both fields together, preventing
  /// inconsistent intermediate states during concurrent shutdown requests.
  std::atomic<uint64_t> state_and_reason_;

  /// Shutdown detail for observability (set once during shutdown initiation)
  std::string shutdown_detail_;

  // Constants for bit manipulation
  static constexpr uint32_t STATE_MASK = 0xFFFFFFFF;
  static constexpr uint32_t REASON_MASK = 0xFFFFFFFF;
  static constexpr uint32_t STATE_SHIFT = 0;
  static constexpr uint32_t REASON_SHIFT = 32;
};
}  // namespace core
}  // namespace ray
