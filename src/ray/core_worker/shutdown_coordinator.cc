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

#include "ray/core_worker/shutdown_coordinator.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
namespace ray {

namespace core {

ShutdownCoordinator::ShutdownCoordinator(
    std::unique_ptr<ShutdownExecutorInterface> executor, WorkerType worker_type)
    : executor_(std::move(executor)), worker_type_(worker_type) {
  RAY_CHECK(executor_) << "ShutdownExecutor cannot be null";
  state_and_reason_.store(PackStateReason(ShutdownState::kRunning, ShutdownReason::kNone),
                          std::memory_order_relaxed);
}

bool ShutdownCoordinator::RequestShutdown(bool force_shutdown,
                                          ShutdownReason reason,
                                          std::string_view detail,
                                          std::chrono::milliseconds timeout_ms,
                                          bool force_on_timeout) {
  uint16_t expected = state_and_reason_.load(std::memory_order_acquire);

  while (true) {
    ShutdownState current_state = UnpackState(expected);

    if (current_state == ShutdownState::kShutdown) {
      return false;
    }

    if (!force_shutdown && current_state != ShutdownState::kRunning) {
      // Graceful shutdown is only allowed from the `Running` state.
      return false;
    }

    uint16_t desired = PackStateReason(ShutdownState::kShuttingDown, reason);

    if (state_and_reason_.compare_exchange_strong(
            expected, desired, std::memory_order_acq_rel, std::memory_order_acquire)) {
      shutdown_detail_ = detail;
      ExecuteShutdownSequence(force_shutdown, detail, timeout_ms, force_on_timeout);
      return true;
    }
  }
}

bool ShutdownCoordinator::TryInitiateShutdown(ShutdownReason reason) {
  // Legacy compatibility - delegate to graceful shutdown by default
  return RequestShutdown(false, reason, "");
}

bool ShutdownCoordinator::TryTransitionToDisconnecting() {
  uint16_t current = state_and_reason_.load(std::memory_order_acquire);
  ShutdownState current_state = UnpackState(current);
  ShutdownReason current_reason = UnpackReason(current);

  if (current_state != ShutdownState::kShuttingDown) {
    return false;
  }

  uint16_t expected = current;
  uint16_t desired = PackStateReason(ShutdownState::kDisconnecting, current_reason);

  return state_and_reason_.compare_exchange_strong(
      expected, desired, std::memory_order_acq_rel, std::memory_order_acquire);
}

bool ShutdownCoordinator::TryTransitionToShutdown() {
  uint16_t current = state_and_reason_.load(std::memory_order_acquire);
  ShutdownState current_state = UnpackState(current);
  ShutdownReason current_reason = UnpackReason(current);

  // Can transition from kDisconnecting (normal flow) or kShuttingDown (force shutdown)
  if (current_state != ShutdownState::kShuttingDown &&
      current_state != ShutdownState::kDisconnecting) {
    return false;
  }

  uint16_t expected = current;
  uint16_t desired = PackStateReason(ShutdownState::kShutdown, current_reason);

  return state_and_reason_.compare_exchange_strong(
      expected, desired, std::memory_order_acq_rel, std::memory_order_acquire);
}

ShutdownState ShutdownCoordinator::GetState() const {
  return UnpackState(state_and_reason_.load(std::memory_order_acquire));
}

ShutdownReason ShutdownCoordinator::GetReason() const {
  return UnpackReason(state_and_reason_.load(std::memory_order_acquire));
}

bool ShutdownCoordinator::ShouldEarlyExit() const {
  return UnpackState(state_and_reason_.load(std::memory_order_acquire)) !=
         ShutdownState::kRunning;
}

bool ShutdownCoordinator::IsRunning() const {
  return GetState() == ShutdownState::kRunning;
}

bool ShutdownCoordinator::IsShuttingDown() const {
  return GetState() != ShutdownState::kRunning;
}

bool ShutdownCoordinator::IsShutdown() const {
  return GetState() == ShutdownState::kShutdown;
}

std::string ShutdownCoordinator::GetStateString() const {
  switch (GetState()) {
  case ShutdownState::kRunning:
    return "Running";
  case ShutdownState::kShuttingDown:
    return "ShuttingDown";
  case ShutdownState::kDisconnecting:
    return "Disconnecting";
  case ShutdownState::kShutdown:
    return "Shutdown";
  default:
    return "Unknown";
  }
}

// Shutdown execution methods

void ShutdownCoordinator::ExecuteShutdownSequence(bool force_shutdown,
                                                  std::string_view detail,
                                                  std::chrono::milliseconds timeout_ms,
                                                  bool force_on_timeout) {
  switch (worker_type_) {
  case WorkerType::DRIVER:
    ExecuteDriverShutdown(force_shutdown, detail, timeout_ms, force_on_timeout);
    break;
  case WorkerType::WORKER:
  case WorkerType::SPILL_WORKER:
  case WorkerType::RESTORE_WORKER:
    ExecuteWorkerShutdown(force_shutdown, detail, timeout_ms, force_on_timeout);
    break;
  default:
    RAY_LOG(FATAL) << "Unknown worker type: " << static_cast<int>(worker_type_);
    break;
  }
}

void ShutdownCoordinator::ExecuteGracefulShutdown(std::string_view detail,
                                                  std::chrono::milliseconds timeout_ms) {
  TryTransitionToDisconnecting();
  executor_->ExecuteGracefulShutdown(GetExitTypeString(), detail, timeout_ms);
  TryTransitionToShutdown();
}

void ShutdownCoordinator::ExecuteForceShutdown(std::string_view detail) {
  RAY_LOG(WARNING) << "ExecuteForceShutdown called: detail=" << detail;

  // Force shutdown bypasses normal state transitions and terminates immediately
  // This ensures that force shutdowns can interrupt hanging graceful shutdowns
  RAY_LOG(WARNING) << "ExecuteForceShutdown: calling executor_->ExecuteForceShutdown";
  executor_->ExecuteForceShutdown(GetExitTypeString(), detail);
  RAY_LOG(WARNING) << "ExecuteForceShutdown: executor_->ExecuteForceShutdown completed";

  // Only update state if we're not already in final state
  // (force shutdown should have terminated the process by now)
  TryTransitionToShutdown();
}

void ShutdownCoordinator::ExecuteDriverShutdown(bool force_shutdown,
                                                std::string_view detail,
                                                std::chrono::milliseconds timeout_ms,
                                                bool force_on_timeout) {
  if (force_shutdown) {
    ExecuteForceShutdown(detail);
  } else {
    ExecuteGracefulShutdown(detail, timeout_ms);
    // Handle timeout fallback if needed
    if (force_on_timeout && GetState() != ShutdownState::kShutdown) {
      ExecuteForceShutdown(std::string("Graceful shutdown timeout: ") +
                           std::string(detail));
    }
  }
}

void ShutdownCoordinator::ExecuteWorkerShutdown(bool force_shutdown,
                                                std::string_view detail,
                                                std::chrono::milliseconds timeout_ms,
                                                bool force_on_timeout) {
  if (force_shutdown) {
    ExecuteForceShutdown(detail);
    return;
  }

  ShutdownReason reason = GetReason();

  if (reason == ShutdownReason::kUserError || reason == ShutdownReason::kGracefulExit ||
      reason == ShutdownReason::kIntentionalShutdown ||
      reason == ShutdownReason::kUnexpectedError ||
      reason == ShutdownReason::kOutOfMemory ||
      reason == ShutdownReason::kActorCreationFailed ||
      reason == ShutdownReason::kActorKilled) {
    TryTransitionToDisconnecting();
    executor_->ExecuteWorkerExit(GetExitTypeString(), detail, timeout_ms);
  } else if (reason == ShutdownReason::kIdleTimeout ||
             reason == ShutdownReason::kJobFinished) {
    TryTransitionToDisconnecting();
    executor_->ExecuteHandleExit(GetExitTypeString(), detail, timeout_ms);
  } else {
    ExecuteGracefulShutdown(detail, timeout_ms);
  }

  if (force_on_timeout && GetState() != ShutdownState::kShutdown) {
    ExecuteForceShutdown(std::string("Graceful shutdown timeout: ") +
                         std::string(detail));
  }
}

std::string ShutdownCoordinator::GetExitTypeString() const {
  switch (GetReason()) {
  case ShutdownReason::kIdleTimeout:
  case ShutdownReason::kIntentionalShutdown:
    return "INTENDED_SYSTEM_EXIT";
  case ShutdownReason::kUserError:
    return "USER_ERROR";
  case ShutdownReason::kUnexpectedError:
    return "SYSTEM_ERROR";
  case ShutdownReason::kOutOfMemory:
    return "NODE_OUT_OF_MEMORY";
  case ShutdownReason::kForcedExit:
  case ShutdownReason::kGracefulExit:
  default:
    return "INTENDED_USER_EXIT";
  }
}

std::string ShutdownCoordinator::GetReasonString() const {
  switch (GetReason()) {
  case ShutdownReason::kNone:
    return "None";
  case ShutdownReason::kIntentionalShutdown:
    return "IntentionalShutdown";
  case ShutdownReason::kUnexpectedError:
    return "UnexpectedError";
  case ShutdownReason::kIdleTimeout:
    return "IdleTimeout";
  case ShutdownReason::kGracefulExit:
    return "GracefulExit";
  case ShutdownReason::kForcedExit:
    return "ForcedExit";
  case ShutdownReason::kUserError:
    return "UserError";
  case ShutdownReason::kOutOfMemory:
    return "OutOfMemory";
  case ShutdownReason::kJobFinished:
    return "JobFinished";
  case ShutdownReason::kActorKilled:
    return "ActorKilled";
  case ShutdownReason::kActorCreationFailed:
    return "ActorCreationFailed";
  default:
    return "Unknown";
  }
}

// Private helper methods

uint16_t ShutdownCoordinator::PackStateReason(ShutdownState state,
                                              ShutdownReason reason) {
  StateReasonPacked packed;
  packed.fields.state = static_cast<uint8_t>(state);
  packed.fields.reason = static_cast<uint8_t>(reason);
  return packed.packed;
}

ShutdownState ShutdownCoordinator::UnpackState(uint16_t packed_value) const {
  StateReasonPacked packed;
  packed.packed = packed_value;
  return static_cast<ShutdownState>(packed.fields.state);
}

ShutdownReason ShutdownCoordinator::UnpackReason(uint16_t packed_value) const {
  StateReasonPacked packed;
  packed.packed = packed_value;
  return static_cast<ShutdownReason>(packed.fields.reason);
}
}  // namespace core
}  // namespace ray
