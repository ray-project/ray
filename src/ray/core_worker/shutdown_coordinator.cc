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

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "ray/common/buffer.h"  // LocalMemoryBuffer
namespace ray {

namespace core {

ShutdownCoordinator::ShutdownCoordinator(
    std::unique_ptr<ShutdownExecutorInterface> executor, rpc::WorkerType worker_type)
    : executor_(std::move(executor)), worker_type_(worker_type) {
  RAY_CHECK(executor_)
      << "ShutdownCoordinator requires a non-null ShutdownExecutorInterface. "
      << "This indicates a construction-time bug. "
      << "Pass a concrete executor (e.g., CoreWorkerShutdownExecutor) "
      << "when creating the coordinator.";
}

bool ShutdownCoordinator::RequestShutdown(
    bool force_shutdown,
    ShutdownReason reason,
    std::string_view detail,
    std::chrono::milliseconds timeout_ms,
    const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes) {
  bool should_execute = false;
  bool execute_force = force_shutdown;
  {
    absl::MutexLock lock(&mu_);
    if (state_ == ShutdownState::kShutdown) {
      return false;
    }
    // If a force request arrives, latch it immediately to guarantee single execution.
    if (force_shutdown) {
      if (force_started_) {
        return false;
      }
      force_started_ = true;
      reason_ = reason;
      shutdown_detail_ = std::string(detail);
      if (state_ == ShutdownState::kRunning) {
        state_ = ShutdownState::kShuttingDown;
      }
      should_execute = true;
    } else {
      if (state_ != ShutdownState::kRunning) {
        return false;
      }
      state_ = ShutdownState::kShuttingDown;
      reason_ = reason;
      shutdown_detail_ = std::string(detail);
      should_execute = true;
    }
  }

  if (!should_execute) {
    return false;
  }

  ExecuteShutdownSequence(
      execute_force, detail, timeout_ms, creation_task_exception_pb_bytes);
  return true;
}

bool ShutdownCoordinator::TryTransitionToDisconnecting() {
  absl::MutexLock lock(&mu_);
  if (state_ != ShutdownState::kShuttingDown) {
    return false;
  }
  state_ = ShutdownState::kDisconnecting;
  return true;
}

bool ShutdownCoordinator::TryTransitionToShutdown() {
  absl::MutexLock lock(&mu_);
  if (state_ != ShutdownState::kShuttingDown && state_ != ShutdownState::kDisconnecting) {
    return false;
  }
  state_ = ShutdownState::kShutdown;
  return true;
}

ShutdownState ShutdownCoordinator::GetState() const {
  absl::ReaderMutexLock lock(&mu_);
  return state_;
}

ShutdownReason ShutdownCoordinator::GetReason() const {
  absl::ReaderMutexLock lock(&mu_);
  return reason_;
}

bool ShutdownCoordinator::ShouldEarlyExit() const {
  absl::ReaderMutexLock lock(&mu_);
  return state_ != ShutdownState::kRunning;
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

// Methods that execute shutdown logic

void ShutdownCoordinator::ExecuteShutdownSequence(
    bool force_shutdown,
    std::string_view detail,
    std::chrono::milliseconds timeout_ms,
    const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes) {
  switch (worker_type_) {
  case rpc::WorkerType::DRIVER:
    ExecuteDriverShutdown(force_shutdown, detail, timeout_ms);
    break;
  case rpc::WorkerType::WORKER:
  case rpc::WorkerType::SPILL_WORKER:
  case rpc::WorkerType::RESTORE_WORKER:
    ExecuteWorkerShutdown(
        force_shutdown, detail, timeout_ms, creation_task_exception_pb_bytes);
    break;
  default:
    RAY_LOG(FATAL) << "Unknown worker type: " << static_cast<int>(worker_type_)
                   << ". This should be unreachable. Please file a bug at "
                   << "https://github.com/ray-project/ray/issues.";
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
  // Force shutdown bypasses normal state transitions and terminates immediately
  // This ensures that force shutdowns can interrupt hanging graceful shutdowns
  {
    absl::MutexLock lock(&mu_);
    if (force_executed_) {
      return;
    }
    force_executed_ = true;
  }
  executor_->ExecuteForceShutdown(GetExitTypeString(), detail);

  // Only update state if we're not already in final state
  // (force shutdown should have terminated the process by now)
  TryTransitionToShutdown();
}

void ShutdownCoordinator::ExecuteDriverShutdown(bool force_shutdown,
                                                std::string_view detail,
                                                std::chrono::milliseconds timeout_ms) {
  if (force_shutdown) {
    ExecuteForceShutdown(detail);
  } else {
    ExecuteGracefulShutdown(detail, timeout_ms);
  }
}

void ShutdownCoordinator::ExecuteWorkerShutdown(
    bool force_shutdown,
    std::string_view detail,
    std::chrono::milliseconds timeout_ms,
    const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes) {
  if (force_shutdown) {
    ExecuteForceShutdown(detail);
    return;
  }

  ShutdownReason reason = GetReason();

  if (reason == ShutdownReason::kActorCreationFailed) {
    TryTransitionToDisconnecting();
    executor_->ExecuteExit(
        GetExitTypeString(), detail, timeout_ms, creation_task_exception_pb_bytes);
  } else if (reason == ShutdownReason::kUserError ||
             reason == ShutdownReason::kGracefulExit ||
             reason == ShutdownReason::kIntentionalShutdown ||
             reason == ShutdownReason::kUnexpectedError ||
             reason == ShutdownReason::kOutOfMemory ||
             reason == ShutdownReason::kActorKilled) {
    TryTransitionToDisconnecting();
    executor_->ExecuteExit(
        GetExitTypeString(), detail, timeout_ms, creation_task_exception_pb_bytes);
  } else if (reason == ShutdownReason::kIdleTimeout ||
             reason == ShutdownReason::kJobFinished) {
    TryTransitionToDisconnecting();
    executor_->ExecuteExitIfIdle(GetExitTypeString(), detail, timeout_ms);
  } else {
    ExecuteGracefulShutdown(detail, timeout_ms);
  }
}

std::string ShutdownCoordinator::GetExitTypeString() const {
  switch (GetReason()) {
  case ShutdownReason::kIdleTimeout:
  case ShutdownReason::kIntentionalShutdown:
    return "INTENDED_SYSTEM_EXIT";
  case ShutdownReason::kUserError:
    return "USER_ERROR";
  case ShutdownReason::kActorCreationFailed:
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

}  // namespace core
}  // namespace ray
