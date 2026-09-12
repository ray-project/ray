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

#include "ray/core_worker/lib/go/task_executor_ops.h"

#include <memory>
#include <string>
#include <vector>

#include "absl/strings/str_join.h"
#include "ray/common/buffer.h"
#include "ray/common/status.h"
#include "ray/core_worker/core_worker.h"
#include "ray/util/logging.h"

namespace ray {
namespace go {

/// @brief Create an error object with the given error message and type
/// @param error_message Error message to include in the object
/// @param error_type Error type (reference: ray::rpc::ErrorType)
/// @return Serialized error RayObject
std::shared_ptr<ray::RayObject> CreateErrorObject(const std::string &error_message,
                                                  ray::rpc::ErrorType error_type) {
  // Create error metadata (stores error type as string)
  std::string meta_str = std::to_string(static_cast<int>(error_type));
  auto meta_buffer = std::make_shared<ray::LocalMemoryBuffer>(
      reinterpret_cast<uint8_t *>(const_cast<char *>(meta_str.data())),
      meta_str.size(),
      true);

  // Serialize error message to data buffer
  // Note: In production, you might want to use msgpack or other serialization
  auto data_buffer = std::make_shared<ray::LocalMemoryBuffer>(
      reinterpret_cast<uint8_t *>(const_cast<char *>(error_message.data())),
      error_message.size(),
      true);

  // Create RayObject with proper constructor signature
  // RayObject(data, metadata, nested_refs, copy_data, tensor_transport)
  std::vector<ray::rpc::ObjectReference> nested_refs;
  return std::make_shared<ray::RayObject>(
      data_buffer, meta_buffer, nested_refs, true, std::nullopt);
}

// Static provider - defaults to DefaultCoreWorkerProvider
static std::shared_ptr<ICoreWorkerProvider> g_core_worker_provider =
    std::make_shared<DefaultCoreWorkerProvider>();

TaskExecutorOperations &TaskExecutorOperations::GetInstance() {
  static TaskExecutorOperations instance;
  return instance;
}

void TaskExecutorOperations::SetCoreWorkerProvider(
    std::shared_ptr<ICoreWorkerProvider> provider) {
  g_core_worker_provider = provider;
}

ICoreWorkerProvider &TaskExecutorOperations::GetCoreWorkerProvider() {
  return *g_core_worker_provider;
}

void TaskExecutorOperations::SetExecutorCallback(TaskExecutionCallback callback) {
  std::lock_guard<std::mutex> lock(callback_mutex_);
  executor_callback_ = callback;
  RAY_LOG(INFO) << "Task executor callback registered";
}

TaskExecutionCallback TaskExecutorOperations::GetExecutorCallback() const {
  std::lock_guard<std::mutex> lock(callback_mutex_);
  return executor_callback_;
}

bool TaskExecutorOperations::HasExecutorCallback() const {
  std::lock_guard<std::mutex> lock(callback_mutex_);
  return executor_callback_ != nullptr;
}

}  // namespace go
}  // namespace ray
