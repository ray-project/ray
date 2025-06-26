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

#include <atomic>
#include <chrono>
#include <string>

namespace ray {
namespace core {

/// Reasons for worker shutdown. Used for observability and debugging.
enum class ShutdownReason : uint32_t {
  kNone = 0,
  kIntentionalShutdown = 1,
  kUnexpectedError = 2,
  kSystemShutdown = 3,
  kIdleTimeout = 4,
  kWorkerExitRequestReceived = 5,
  kActorExitRequestReceived = 6,
  kGracefulExit = 7,
  kForcedExit = 8,
  kRayletFailure = 9,
  kNodeFailure = 10,
  kUserError = 11,
  kOutOfMemory = 12,
  kJobFinished = 13,
  kActorDiedError = 14,
  kActorKilled = 15,
  kTaskCancelled = 16,
  kRuntimeEnvFailed = 17,
  kWorkerRestartDueToUserCodeCrash = 18,
  kWorkerUnexpectedExit = 19,
  kActorCreationFailed = 20,
  kActorRestartError = 21,
  kDriverShutdown = 22,
  kLocalRayletDied = 23,
  kCreationTaskError = 24
};

/// Shutdown state representing the current lifecycle phase of worker shutdown.
/// States are ordered by progression, transitions must be monotonic.
enum class ShutdownState : uint32_t {
  kRunning = 0,
  kShuttingDown = 1,
  kDisconnecting = 2,
  kShutdown = 3
};

/// Thread-safe coordinator for managing worker shutdown state and transitions.
/// 
/// This class provides atomic state management for shutdown operations using a
/// single 64-bit atomic variable that packs both state and reason information.
/// This design minimizes cache line contention and ensures consistent state
/// transitions across multiple threads.
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
///   // Try to initiate shutdown (only first caller succeeds)
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
  ShutdownCoordinator();
  ~ShutdownCoordinator() = default;

  // Non-copyable and non-movable for safety
  ShutdownCoordinator(const ShutdownCoordinator &) = delete;
  ShutdownCoordinator &operator=(const ShutdownCoordinator &) = delete;
  ShutdownCoordinator(ShutdownCoordinator &&) = delete;
  ShutdownCoordinator &operator=(ShutdownCoordinator &&) = delete;

  /// Attempt to initiate shutdown from running state.
  /// 
  /// This is an atomic operation that will only succeed for the first caller.
  /// Subsequent calls will return false, ensuring idempotent behavior.
  ///
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

  /// Get string representation of shutdown reason.
  ///
  /// \return Human-readable reason description  
  std::string GetReasonString() const;

 private:
  /// Pack state and reason into a single 64-bit value for atomic operations.
  /// Layout: [32-bit state][32-bit reason]
  static uint64_t PackStateReason(ShutdownState state, ShutdownReason reason);

  /// Extract state from packed 64-bit value.
  static ShutdownState UnpackState(uint64_t packed);

  /// Extract reason from packed 64-bit value.
  static ShutdownReason UnpackReason(uint64_t packed);

  /// Validate state transition is allowed.
  static bool IsValidTransition(ShutdownState from, ShutdownState to);

  /// Single atomic variable holding both state and reason.
  /// This design minimizes memory overhead and ensures atomic updates
  /// of both fields together, preventing inconsistent intermediate states.
  std::atomic<uint64_t> state_and_reason_;

  // Constants for bit manipulation
  static constexpr uint32_t STATE_MASK = 0xFFFFFFFF;
  static constexpr uint32_t REASON_MASK = 0xFFFFFFFF; 
  static constexpr uint32_t STATE_SHIFT = 0;
  static constexpr uint32_t REASON_SHIFT = 32;
};

}  // namespace core
}  // namespace ray 