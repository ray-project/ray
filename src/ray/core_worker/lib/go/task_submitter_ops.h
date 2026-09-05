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

#ifndef SRC_RAY_CORE_WORKER_LIB_GO_TASK_SUBMITTER_OPS_H_
#define SRC_RAY_CORE_WORKER_LIB_GO_TASK_SUBMITTER_OPS_H_

/**
 * @file task_submitter_ops.h
 * @brief Business logic layer for task submission operations
 *
 * This file contains the pure C++ business logic for task submission,
 * separated from CGO boundary concerns. It uses dependency injection
 * via CoreWorkerProvider to access CoreWorker functionality.
 *
 * Design principles:
 * - No CGO types in this file (pure C++ types only)
 * - No try-catch blocks (error handling done at CGO boundary)
 * - No memory allocation for C types (done at CGO boundary)
 * - Fully testable with mock CoreWorkerProvider
 *
 * Usage pattern:
 *   auto ops = TaskSubmitterOperations::GetInstance();
 *   auto return_refs = ops.SubmitTask(...);
 */

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/go/core_worker_provider.h"
#include "ray/core_worker/lib/go/task_argument.h"

namespace ray {
namespace go {

/**
 * @brief Task submission options (simplified version of CTaskOptions)
 */
struct TaskSubmitOptions {
  std::unordered_map<std::string, double> resources;
  std::string serialized_runtime_env_info;
  int num_returns = 1;
  int max_retries = 0;
  std::string placement_group_id_hex;
  int bundle_index = -1;
};

/**
 * @brief Actor creation options (simplified version of CActorCreationOptions)
 */
struct ActorCreateOptions {
  int max_restarts = 0;
  int max_task_retries = 0;
  std::unordered_map<std::string, double> resources;
  std::string name;
  std::string namespace_;
  std::string serialized_runtime_env_info;
};

/**
 * @brief Business logic operations for task submission
 *
 * This class provides pure C++ methods for task submission operations.
 * All CGO-specific concerns (type conversion, error handling, memory
 * allocation) are handled at the boundary layer.
 *
 * Thread safety: This class is thread-safe as it only uses stateless
 * operations and the CoreWorker is thread-safe.
 */
class TaskSubmitterOperations {
 public:
  /**
   * @brief Get singleton instance
   * @return Reference to singleton instance
   */
  static TaskSubmitterOperations &GetInstance();

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
   * @brief Submit a remote task
   *
   * @param function_descriptor Function descriptor components
   * @param args Task arguments
   * @param options Task options
   * @return Vector of ObjectReferences for return values
   * @throws std::exception on error
   */
  std::vector<ray::rpc::ObjectReference> SubmitTask(
      const std::vector<std::string> &function_descriptor,
      const std::vector<std::unique_ptr<TaskArgument>> &args,
      const TaskSubmitOptions &options);

  /**
   * @brief Create an actor
   *
   * @param function_descriptor Function descriptor components
   * @param args Constructor arguments
   * @param options Actor creation options
   * @return Created ActorID
   * @throws std::exception on error
   */
  ray::ActorID CreateActor(const std::vector<std::string> &function_descriptor,
                           const std::vector<std::unique_ptr<TaskArgument>> &args,
                           const ActorCreateOptions &options);

  /**
   * @brief Submit a task to an actor
   *
   * @param actor_id Target actor ID
   * @param function_descriptor Function descriptor components
   * @param args Task arguments
   * @param options Task options
   * @return Vector of ObjectReferences for return values
   * @throws std::exception on error
   */
  std::vector<ray::rpc::ObjectReference> SubmitActorTask(
      const ray::ActorID &actor_id,
      const std::vector<std::string> &function_descriptor,
      const std::vector<std::unique_ptr<TaskArgument>> &args,
      const TaskSubmitOptions &options);

  /**
   * @brief Parse resources string to map
   *
   * @param resources_str Format: "CPU:2.0,GPU:1.0,memory:1073741824"
   * @return Resource map
   */
  static std::unordered_map<std::string, double> ParseResources(
      const std::string &resources_str);

  /**
   * @brief Convert hex string to binary
   *
   * @param hex_str Hex string (e.g., "ff00aa")
   * @return Binary string
   * @throws std::invalid_argument on invalid hex
   */
  static std::string HexToBinary(const std::string &hex_str);

 private:
  TaskSubmitterOperations() = default;
  ~TaskSubmitterOperations() = default;

  // Non-copyable
  TaskSubmitterOperations(const TaskSubmitterOperations &) = delete;
  TaskSubmitterOperations &operator=(const TaskSubmitterOperations &) = delete;

  /**
   * @brief Build RayFunction from descriptor
   */
  ray::core::RayFunction BuildRayFunction(
      const std::vector<std::string> &descriptor) const;

  /**
   * @brief Convert TaskArgument vector to Ray TaskArg vector
   */
  std::vector<std::unique_ptr<ray::TaskArg>> ConvertTaskArgs(
      const std::vector<std::unique_ptr<TaskArgument>> &args) const;

  /**
   * @brief Build TaskOptions from options
   */
  ray::core::TaskOptions BuildTaskOptions(const TaskSubmitOptions &options) const;

  /**
   * @brief Build ActorCreationOptions from options
   */
  ray::core::ActorCreationOptions BuildActorOptions(
      const ActorCreateOptions &options) const;

  /**
   * @brief Build SchedulingStrategy from placement group info
   */
  ray::rpc::SchedulingStrategy BuildSchedulingStrategy(const std::string &pg_id_hex,
                                                       int bundle_index) const;

  /**
   * @brief Get CoreWorker from provider
   */
  ray::core::CoreWorker &GetCoreWorker() const {
    return GetCoreWorkerProvider().GetCoreWorker();
  }
};

}  // namespace go
}  // namespace ray

#endif  // SRC_RAY_CORE_WORKER_LIB_GO_TASK_SUBMITTER_OPS_H_
