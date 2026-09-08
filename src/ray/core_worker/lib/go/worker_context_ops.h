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

#ifndef SRC_RAY_CORE_WORKER_LIB_GO_WORKER_CONTEXT_OPS_H_
#define SRC_RAY_CORE_WORKER_LIB_GO_WORKER_CONTEXT_OPS_H_

#include <memory>
#include <string>

#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/go/core_worker_provider.h"

namespace ray {
namespace go {

/**
 * @brief Business logic operations for WorkerContext.
 *
 * This class encapsulates all WorkerContext-related operations,
 * providing a clean interface separated from CGO boundary concerns.
 * It uses dependency injection for CoreWorker access, enabling
 * unit testing without full CoreWorker initialization.
 */
class WorkerContextOperations {
 public:
  /**
   * @brief Construct with optional CoreWorker provider.
   * @param provider Shared pointer to CoreWorker provider.
   *                 If nullptr, uses DefaultCoreWorkerProvider.
   */
  explicit WorkerContextOperations(
      std::shared_ptr<ICoreWorkerProvider> provider = nullptr)
      : provider_(provider ? provider : std::make_shared<DefaultCoreWorkerProvider>()) {}

  /**
   * @brief Get the current worker ID.
   * @return WorkerID of the current worker.
   */
  WorkerID GetWorkerId() {
    return provider_->GetCoreWorker().GetWorkerContext().GetWorkerID();
  }

  /**
   * @brief Get the current job ID.
   * @return JobID of the current job.
   */
  JobID GetJobId() {
    return provider_->GetCoreWorker().GetWorkerContext().GetCurrentJobID();
  }

  /**
   * @brief Get the current actor ID (if running as an actor).
   * @return ActorID of the current actor, or nil if not an actor.
   */
  ActorID GetCurrentActorId() {
    return provider_->GetCoreWorker().GetWorkerContext().GetCurrentActorID();
  }

  /**
   * @brief Get the current task ID.
   * @return TaskID of the current task.
   */
  TaskID GetCurrentTaskId() {
    return provider_->GetCoreWorker().GetWorkerContext().GetCurrentTaskID();
  }

  /**
   * @brief Get the current task type.
   * @return TaskType enum value.
   */
  rpc::TaskType GetCurrentTaskType() {
    auto task = provider_->GetCoreWorker().GetWorkerContext().GetCurrentTask();
    if (task == nullptr) {
      throw std::runtime_error("Current task is not set");
    }
    return task->GetMessage().type();
  }

  /**
   * @brief Check if current task is set.
   * @return true if task is set, false otherwise.
   */
  bool IsCurrentTaskSet() {
    return provider_->GetCoreWorker().GetWorkerContext().GetCurrentTask() != nullptr;
  }

  /**
   * @brief Get the RPC address of the current worker.
   * @return Serialized RPC address as string.
   */
  std::string GetRpcAddress() {
    return provider_->GetCoreWorker().GetRpcAddress().SerializeAsString();
  }

  /**
   * @brief Get the serialized runtime environment.
   * @return Serialized runtime environment string.
   */
  std::string GetSerializedRuntimeEnv() {
    if (provider_->GetCoreWorker().GetWorkerType() == ray::core::WorkerType::DRIVER) {
      return provider_->GetCoreWorker()
          .GetJobConfig()
          .runtime_env_info()
          .serialized_runtime_env();
    } else {
      return provider_->GetCoreWorker()
          .GetWorkerContext()
          .GetCurrentSerializedRuntimeEnv();
    }
    // Should never reach here, but required to avoid compiler warning
    return "";
  }

  /**
   * @brief Get the current namespace.
   * @return Namespace string.
   */
  std::string GetNamespace() {
    // Note: GetNamespace() is not available in WorkerContext,
    // so we return an empty string for now.
    // TODO(daiping8): Implement proper namespace support if needed.
    return "";
  }

  /**
   * @brief Get the current node ID.
   * @return NodeID of the current node.
   */
  NodeID GetCurrentNodeId() { return provider_->GetCoreWorker().GetCurrentNodeId(); }

 private:
  std::shared_ptr<ICoreWorkerProvider> provider_;
};

}  // namespace go
}  // namespace ray

#endif  // SRC_RAY_CORE_WORKER_LIB_GO_WORKER_CONTEXT_OPS_H_
