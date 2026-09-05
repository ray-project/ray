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

#ifndef SRC_RAY_CORE_WORKER_LIB_GO_TASK_ARGUMENT_H_
#define SRC_RAY_CORE_WORKER_LIB_GO_TASK_ARGUMENT_H_

/**
 * @file task_argument.h
 * @brief Task argument definition for both task submission and execution
 *
 * This file contains the TaskArgument class which is used by both
 * TaskSubmitterOperations and TaskExecutorOperations. It's extracted
 * to a separate file to avoid circular dependencies.
 *
 * Design principles:
 * - No CGO types in this file (pure C++ types only)
 * - No dependency on submitter or executor operations
 * - Fully testable with mock objects
 */

#include <memory>
#include <string>

#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/task/task_util.h"
#include "ray/core_worker/common.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
namespace go {

/**
 * @brief Task argument (wrapper around ray::TaskArg)
 *
 * This class stores task argument parameters and creates Ray task arguments
 * on demand. This approach is necessary because Ray's TaskArg classes don't
 * provide getter methods to extract parameters.
 */
class TaskArgument {
 public:
  /**
   * @brief Create a task argument by value
   *
   * @param data_buffer Data buffer containing serialized object
   * @param metadata_buffer Optional metadata buffer
   * @return Unique pointer to TaskArgument
   */
  static std::unique_ptr<TaskArgument> ByValue(
      std::shared_ptr<ray::Buffer> data_buffer,
      std::shared_ptr<ray::Buffer> metadata_buffer = nullptr);

  /**
   * @brief Create a task argument by reference
   *
   * @param object_id Object ID to reference
   * @param owner_address Owner address (optional, for remote objects)
   * @param call_site Call site information (optional)
   * @return Unique pointer to TaskArgument
   */
  static std::unique_ptr<TaskArgument> ByReference(
      const ray::ObjectID &object_id,
      const ray::rpc::Address &owner_address = ray::rpc::Address(),
      const std::string &call_site = "");

  /**
   * @brief Convert to Ray TaskArg
   *
   * @return Unique pointer to ray::TaskArg
   */
  std::unique_ptr<ray::TaskArg> ToRayTaskArg() const;

 private:
  TaskArgument() = default;

  // Store parameters instead of Ray task argument
  bool is_by_value_ = false;
  std::shared_ptr<ray::Buffer> data_buffer_;
  std::shared_ptr<ray::Buffer> metadata_buffer_;
  ray::ObjectID object_id_;
  ray::rpc::Address owner_address_;
  std::string call_site_;
};

}  // namespace go
}  // namespace ray

#endif  // SRC_RAY_CORE_WORKER_LIB_GO_TASK_ARGUMENT_H_
