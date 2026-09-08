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

#ifndef SRC_RAY_CORE_WORKER_LIB_GO_TASK_EXECUTOR_OPS_H_
#define SRC_RAY_CORE_WORKER_LIB_GO_TASK_EXECUTOR_OPS_H_

/**
 * @file task_executor_ops.h
 * @brief Business logic layer for task execution operations
 *
 * This file contains the pure C++ business logic for task execution,
 * separated from CGO boundary concerns. It handles the interaction
 * between the CoreWorker and Go runtime for task execution.
 *
 * Design principles:
 * - No CGO types in this file (pure C++ types only)
 * - No try-catch blocks (error handling done at CGO boundary)
 * - Thread-safe operations using atomic variables
 * - Fully testable with mock CoreWorker
 *
 * Usage pattern:
 *   auto& ops = TaskExecutorOperations::GetInstance();
 *   ops.SetExecutorCallback(callback);
 */

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/go/core_worker_provider.h"
#include "ray/core_worker/lib/go/task_argument.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
namespace go {

// Forward declaration
struct CSerializedObjectArray;
class TaskArgument;

/**
 * @brief Task execution callback type
 *
 * This callback is called by CoreWorker when a task needs to be executed.
 * The callback should populate the returns vector with the task results.
 *
 * Parameters:
 *   - task_type: Type of task (NORMAL_TASK or ACTOR_TASK)
 *   - function_descriptor: Function descriptor components
 *   - args: Task arguments as RayObjects
 *   - arg_refs: Argument references (for actor tasks)
 *   - returns: Output vector for return values
 *   - application_error: Error message if task execution fails
 *   - is_retryable_error: Whether the error is retryable
 */
using TaskExecutionCallback = std::function<void(
    ray::rpc::TaskType task_type,
    const std::vector<std::string> &function_descriptor,
    const std::vector<std::shared_ptr<ray::RayObject>> &args,
    const std::vector<ray::rpc::ObjectReference> &arg_refs,
    std::vector<std::pair<ray::ObjectID, std::shared_ptr<ray::RayObject>>> *returns,
    std::string *application_error,
    bool *is_retryable_error)>;

/**
 * @brief Business logic operations for task execution
 *
 * This class provides pure C++ methods for task execution operations.
 * All CGO-specific concerns (type conversion, error handling, memory
 * allocation) are handled at the boundary layer.
 *
 * Thread safety: This class is thread-safe. The executor callback
 * is stored in an atomic variable and all operations are thread-safe.
 */
class TaskExecutorOperations {
 public:
  /**
   * @brief Get singleton instance
   * @return Reference to singleton instance
   */
  static TaskExecutorOperations &GetInstance();

  /**
   * @brief Set the CoreWorker provider
   *
   * This allows injecting a custom CoreWorker provider for testing.
   * By default, uses DefaultCoreWorkerProvider.
   *
   * @param provider Shared pointer to CoreWorker provider
   */
  static void SetCoreWorkerProvider(std::shared_ptr<ICoreWorkerProvider> provider);

  /**
   * @brief Get the current CoreWorker provider
   * @return Reference to current provider
   */
  static ICoreWorkerProvider &GetCoreWorkerProvider();

  /**
   * @brief Set the task execution callback
   *
   * This callback is called by CoreWorker when a task needs to be executed.
   * It should be set during Go runtime initialization.
   *
   * @param callback Task execution callback function
   */
  void SetExecutorCallback(TaskExecutionCallback callback);

  /**
   * @brief Get the current executor callback
   * @return Current callback function (may be nullptr)
   */
  TaskExecutionCallback GetExecutorCallback() const;

  /**
   * @brief Check if executor callback is registered
   * @return true if callback is set, false otherwise
   */
  bool HasExecutorCallback() const;

  /**
   * @brief Execute a task synchronously
   *
   * This method submits a task and returns the results. It's used for
   * direct task execution (not through the callback mechanism).
   *
   * @param function_descriptor Function descriptor components
   * @param args Task arguments
   * @param num_returns Number of return values
   * @return Vector of result objects (empty on error)
   * @throws std::exception on error
   */
  std::vector<std::shared_ptr<ray::RayObject>> ExecuteTask(
      const std::vector<std::string> &function_descriptor,
      const std::vector<std::unique_ptr<ray::go::TaskArgument>> &args,
      int num_returns);

  /**
   * @brief Execute an actor task synchronously
   *
   * @param actor_id Target actor ID
   * @param function_descriptor Function descriptor components
   * @param args Task arguments
   * @param num_returns Number of return values
   * @return Vector of result objects (empty on error)
   * @throws std::exception on error
   */
  std::vector<std::shared_ptr<ray::RayObject>> ExecuteActorTask(
      const ray::ActorID &actor_id,
      const std::vector<std::string> &function_descriptor,
      const std::vector<std::unique_ptr<ray::go::TaskArgument>> &args,
      int num_returns);

 private:
  TaskExecutorOperations() = default;
  ~TaskExecutorOperations() = default;

  // Non-copyable
  TaskExecutorOperations(const TaskExecutorOperations &) = delete;
  TaskExecutorOperations &operator=(const TaskExecutorOperations &) = delete;

  /**
   * @brief Get CoreWorker from provider
   */
  ray::core::CoreWorker &GetCoreWorker() const {
    return GetCoreWorkerProvider().GetCoreWorker();
  }

  /**
   * @brief Storage for executor callback (protected by mutex)
   */
  mutable std::mutex callback_mutex_;
  TaskExecutionCallback executor_callback_{nullptr};
};

}  // namespace go
}  // namespace ray

#endif  // SRC_RAY_CORE_WORKER_LIB_GO_TASK_EXECUTOR_OPS_H_
