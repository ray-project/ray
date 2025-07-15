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

#include <string>

#include "ray/core_worker/shutdown_coordinator.h"

namespace ray {

namespace core {

class CoreWorker;

/// Concrete implementation of ShutdownExecutorInterface that executes actual
/// shutdown operations for CoreWorker.
class CoreWorkerShutdownExecutor : public ShutdownExecutorInterface {
 public:
  /// Constructor with CoreWorker reference for accessing internals
  /// \param core_worker Reference to the CoreWorker instance
  explicit CoreWorkerShutdownExecutor(CoreWorker *core_worker);

  ~CoreWorkerShutdownExecutor() override = default;

  /// Execute complete graceful shutdown sequence
  void ExecuteGracefulShutdown(const std::string &exit_type,
                               const std::string &detail,
                               std::chrono::milliseconds timeout_ms) override;

  /// Execute complete force shutdown sequence
  void ExecuteForceShutdown(const std::string &exit_type,
                            const std::string &detail) override;

  /// Execute worker exit sequence with task draining
  void ExecuteWorkerExit(const std::string &exit_type,
                         const std::string &detail,
                         std::chrono::milliseconds timeout_ms) override;

  /// Execute handle exit sequence with idle checking
  void ExecuteHandleExit(const std::string &exit_type,
                         const std::string &detail,
                         std::chrono::milliseconds timeout_ms) override;

  void KillChildProcessesImmediately() override;

  bool ShouldWorkerIdleExit() const override;

 private:
  /// Reference to CoreWorker for accessing shutdown operations
  CoreWorker *core_worker_;

  void DisconnectServices(const std::string &exit_type, const std::string &detail);
  void QuickExit();
};
}  // namespace core
}  // namespace ray
