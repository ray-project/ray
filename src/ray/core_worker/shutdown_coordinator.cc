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

#include "ray/core_worker/shutdown_coordinator.h"

#include <sstream>

namespace ray {
namespace core {

ShutdownCoordinator::ShutdownCoordinator() {
  // Initialize to running state with no reason
  state_and_reason_.store(PackStateReason(ShutdownState::kRunning, ShutdownReason::kNone),
                          std::memory_order_release);
}

bool ShutdownCoordinator::TryInitiateShutdown(ShutdownReason reason) {
  uint64_t expected = PackStateReason(ShutdownState::kRunning, ShutdownReason::kNone);
  uint64_t desired = PackStateReason(ShutdownState::kShuttingDown, reason);
  
  // Use strong compare_exchange with acquire-release semantics for proper ordering
  return state_and_reason_.compare_exchange_strong(
      expected, desired, std::memory_order_acq_rel, std::memory_order_acquire);
}

bool ShutdownCoordinator::TryTransitionToDisconnecting() {
  uint64_t current = state_and_reason_.load(std::memory_order_acquire);
  ShutdownState current_state = UnpackState(current);
  ShutdownReason current_reason = UnpackReason(current);
  
  if (current_state != ShutdownState::kShuttingDown) {
    return false;
  }
  
  uint64_t expected = current;
  uint64_t desired = PackStateReason(ShutdownState::kDisconnecting, current_reason);
  
  return state_and_reason_.compare_exchange_strong(
      expected, desired, std::memory_order_acq_rel, std::memory_order_acquire);
}

bool ShutdownCoordinator::TryTransitionToShutdown() {
  uint64_t current = state_and_reason_.load(std::memory_order_acquire);
  ShutdownState current_state = UnpackState(current);
  ShutdownReason current_reason = UnpackReason(current);
  
  // Can transition from either kShuttingDown or kDisconnecting
  if (current_state != ShutdownState::kShuttingDown && 
      current_state != ShutdownState::kDisconnecting) {
    return false;
  }
  
  uint64_t expected = current;
  uint64_t desired = PackStateReason(ShutdownState::kShutdown, current_reason);
  
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
  // Fast path: single atomic load with relaxed ordering for performance
  return UnpackState(state_and_reason_.load(std::memory_order_relaxed)) != 
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

std::string ShutdownCoordinator::GetReasonString() const {
  switch (GetReason()) {
    case ShutdownReason::kNone:
      return "None";
    case ShutdownReason::kIntentionalShutdown:
      return "IntentionalShutdown";
    case ShutdownReason::kUnexpectedError:
      return "UnexpectedError";
    case ShutdownReason::kSystemShutdown:
      return "SystemShutdown";
    case ShutdownReason::kIdleTimeout:
      return "IdleTimeout";
    case ShutdownReason::kWorkerExitRequestReceived:
      return "WorkerExitRequestReceived";
    case ShutdownReason::kActorExitRequestReceived:
      return "ActorExitRequestReceived";
    case ShutdownReason::kGracefulExit:
      return "GracefulExit";
    case ShutdownReason::kForcedExit:
      return "ForcedExit";
    case ShutdownReason::kRayletFailure:
      return "RayletFailure";
    case ShutdownReason::kNodeFailure:
      return "NodeFailure";
    case ShutdownReason::kUserError:
      return "UserError";
    case ShutdownReason::kOutOfMemory:
      return "OutOfMemory";
    case ShutdownReason::kJobFinished:
      return "JobFinished";
    case ShutdownReason::kActorDiedError:
      return "ActorDiedError";
    case ShutdownReason::kActorKilled:
      return "ActorKilled";
    case ShutdownReason::kTaskCancelled:
      return "TaskCancelled";
    case ShutdownReason::kRuntimeEnvFailed:
      return "RuntimeEnvFailed";
    case ShutdownReason::kWorkerRestartDueToUserCodeCrash:
      return "WorkerRestartDueToUserCodeCrash";
    case ShutdownReason::kWorkerUnexpectedExit:
      return "WorkerUnexpectedExit";
    case ShutdownReason::kActorCreationFailed:
      return "ActorCreationFailed";
    case ShutdownReason::kActorRestartError:
      return "ActorRestartError";
    case ShutdownReason::kDriverShutdown:
      return "DriverShutdown";
    case ShutdownReason::kLocalRayletDied:
      return "LocalRayletDied";
    case ShutdownReason::kCreationTaskError:
      return "CreationTaskError";
    default:
      return "Unknown";
  }
}

// Private helper methods

uint64_t ShutdownCoordinator::PackStateReason(ShutdownState state, ShutdownReason reason) {
  uint64_t packed_state = static_cast<uint64_t>(static_cast<uint32_t>(state));
  uint64_t packed_reason = static_cast<uint64_t>(static_cast<uint32_t>(reason));
  return (packed_reason << REASON_SHIFT) | (packed_state << STATE_SHIFT);
}

ShutdownState ShutdownCoordinator::UnpackState(uint64_t packed) {
  return static_cast<ShutdownState>((packed >> STATE_SHIFT) & STATE_MASK);
}

ShutdownReason ShutdownCoordinator::UnpackReason(uint64_t packed) {
  return static_cast<ShutdownReason>((packed >> REASON_SHIFT) & REASON_MASK);
}

bool ShutdownCoordinator::IsValidTransition(ShutdownState from, ShutdownState to) {
  // Only allow monotonic forward transitions
  return static_cast<uint32_t>(to) > static_cast<uint32_t>(from);
}

}  // namespace core
}  // namespace ray 