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

#include "task_executor_ops.h"

#include "absl/strings/str_join.h"
#include "ray/common/buffer.h"
#include "ray/common/status.h"
#include "ray/core_worker/core_worker.h"
#include "ray/util/logging.h"

namespace ray {
namespace go {

// ============================================================================
// Anonymous namespace - Helper functions (business logic layer)
// ============================================================================
namespace {

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

/// @brief Check if an object is an exception object
/// @param obj RayObject pointer
/// @return true if the object represents an exception
bool IsExceptionObject(const std::shared_ptr<ray::RayObject> &obj) {
  if (obj == nullptr || obj->GetMetadata() == nullptr) {
    return false;
  }

  // Check if metadata is an error type
  std::string meta_str(reinterpret_cast<const char *>(obj->GetMetadata()->Data()),
                       obj->GetMetadata()->Size());

  // Try to parse as error type
  try {
    int error_type = std::stoi(meta_str);
    // Validate error type range (TASK_EXECUTION_EXCEPTION = 1, ACTOR_DIED = 2, etc.)
    return error_type >= 1 && error_type <= 10;
  } catch (...) {
    return false;
  }
}

}  // anonymous namespace

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

std::vector<std::shared_ptr<ray::RayObject>> TaskExecutorOperations::ExecuteTask(
    const std::vector<std::string> &function_descriptor,
    const std::vector<std::unique_ptr<ray::go::TaskArgument>> &args,
    int num_returns) {
  auto &core_worker = GetCoreWorker();

  // 1. Build RayFunction
  ray::FunctionDescriptor func_descriptor =
      ray::FunctionDescriptorBuilder::FromVector(ray::Language::GO, function_descriptor);
  ray::core::RayFunction ray_function(ray::Language::GO, func_descriptor);

  // 2. Build task arguments
  std::vector<std::unique_ptr<ray::TaskArg>> task_args;
  for (const auto &arg : args) {
    task_args.push_back(arg->ToRayTaskArg());
  }

  // 3. Submit task
  // Note: We explicitly set generator_backpressure_num_objects to -1 to indicate
  // that backpressure is not enabled. Using TaskOptions{} would initialize it to 0,
  // which would trigger an assertion failure in
  // TaskSpecification::GeneratorBackpressureNumObjects() if this task is a streaming
  // generator (RAY_CHECK_NE(result, 0) in task_spec.cc:248).
  ray::core::TaskOptions task_options;
  task_options.generator_backpressure_num_objects = -1;
  std::vector<ray::rpc::ObjectReference> return_refs =
      core_worker.SubmitTask(ray_function,
                             task_args,
                             task_options,
                             /*max_retries=*/0,
                             /*retry_exceptions=*/false,
                             ray::rpc::SchedulingStrategy(),
                             /*debugger_breakpoint=*/"",
                             /*serialized_retry_exception_allowlist=*/"",
                             /*call_site=*/"");

  // 4. Get actual objects from object store (complete implementation)
  std::vector<std::shared_ptr<ray::RayObject>> results;

  if (!return_refs.empty()) {
    // 4.1 Extract ObjectID list
    std::vector<ray::ObjectID> object_ids;
    object_ids.reserve(return_refs.size());
    for (const auto &ref : return_refs) {
      object_ids.push_back(ray::ObjectID::FromBinary(ref.object_id()));
    }

    // 4.2 Call CoreWorker.Get() to retrieve objects (5 second timeout)
    std::vector<std::shared_ptr<ray::RayObject>> get_results;
    const int64_t timeout_ms =
        5000;  // 5 seconds timeout, aligned with C++ implementation

    ray::Status status = core_worker.Get(object_ids, timeout_ms, get_results);

    if (!status.ok()) {
      // 4.3 Handle retrieval failure
      RAY_LOG(ERROR) << "Failed to get task results: " << status.ToString()
                     << " function: " << absl::StrJoin(function_descriptor, ".");

      // Create error objects as return values
      for (size_t i = 0; i < return_refs.size(); ++i) {
        auto error_object =
            CreateErrorObject("Task execution failed: " + status.ToString(),
                              ray::rpc::ErrorType::TASK_EXECUTION_EXCEPTION);
        results.push_back(error_object);
      }
    } else {
      // 4.4 Validate return results
      for (size_t i = 0; i < get_results.size(); ++i) {
        if (get_results[i] == nullptr) {
          RAY_LOG(WARNING) << "Object " << object_ids[i].Hex()
                           << " is null, creating error placeholder";
          auto error_object = CreateErrorObject("Object retrieval returned null",
                                                ray::rpc::ErrorType::OBJECT_LOST);
          results.push_back(error_object);
        } else {
          // 4.5 Check if it's an exception object
          if (IsExceptionObject(get_results[i])) {
            RAY_LOG(INFO) << "Task returned exception for function: "
                          << absl::StrJoin(function_descriptor, ".");
          }
          results.push_back(get_results[i]);
        }
      }
    }
  }

  // 5. Ensure return count matches num_returns
  while (static_cast<int>(results.size()) < num_returns) {
    results.push_back(nullptr);
  }

  return results;
}

std::vector<std::shared_ptr<ray::RayObject>> TaskExecutorOperations::ExecuteActorTask(
    const ray::ActorID &actor_id,
    const std::vector<std::string> &function_descriptor,
    const std::vector<std::unique_ptr<ray::go::TaskArgument>> &args,
    int num_returns) {
  auto &core_worker = GetCoreWorker();

  // 1. Build RayFunction
  ray::FunctionDescriptor func_descriptor =
      ray::FunctionDescriptorBuilder::FromVector(ray::Language::GO, function_descriptor);
  ray::core::RayFunction ray_function(ray::Language::GO, func_descriptor);

  // 2. Build task arguments
  std::vector<std::unique_ptr<ray::TaskArg>> task_args;
  for (const auto &arg : args) {
    task_args.push_back(arg->ToRayTaskArg());
  }

  // 3. Submit actor task
  // Note: We explicitly set generator_backpressure_num_objects to -1 to indicate
  // that backpressure is not enabled. Using TaskOptions{} would initialize it to 0,
  // which would trigger an assertion failure in
  // TaskSpecification::GeneratorBackpressureNumObjects() if this task is a streaming
  // generator (RAY_CHECK_NE(result, 0) in task_spec.cc:248).
  ray::core::TaskOptions task_options;
  task_options.generator_backpressure_num_objects = -1;
  std::vector<ray::rpc::ObjectReference> return_refs;
  ray::Status submit_status =
      core_worker.SubmitActorTask(actor_id,
                                  ray_function,
                                  task_args,
                                  task_options,
                                  /*max_retries=*/0,
                                  /*retry_exceptions=*/false,
                                  /*serialized_retry_exception_allowlist=*/"",
                                  /*call_site=*/"",
                                  return_refs);

  if (!submit_status.ok()) {
    RAY_LOG(ERROR) << "Failed to submit actor task: " << submit_status.ToString()
                   << " actor_id: " << actor_id.Hex()
                   << " function: " << absl::StrJoin(function_descriptor, ".");

    // Return error objects
    std::vector<std::shared_ptr<ray::RayObject>> error_results;
    for (int i = 0; i < num_returns; ++i) {
      error_results.push_back(
          CreateErrorObject("Actor task submission failed: " + submit_status.ToString(),
                            ray::rpc::ErrorType::ACTOR_DIED));
    }
    return error_results;
  }

  // 4. Get actual objects from object store (same logic as ExecuteTask)
  std::vector<std::shared_ptr<ray::RayObject>> results;

  if (!return_refs.empty()) {
    std::vector<ray::ObjectID> object_ids;
    object_ids.reserve(return_refs.size());
    for (const auto &ref : return_refs) {
      object_ids.push_back(ray::ObjectID::FromBinary(ref.object_id()));
    }

    std::vector<std::shared_ptr<ray::RayObject>> get_results;
    const int64_t timeout_ms = 5000;  // 5 seconds timeout

    ray::Status status = core_worker.Get(object_ids, timeout_ms, get_results);

    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to get actor task results: " << status.ToString()
                     << " actor_id: " << actor_id.Hex()
                     << " function: " << absl::StrJoin(function_descriptor, ".");

      for (size_t i = 0; i < return_refs.size(); ++i) {
        auto error_object =
            CreateErrorObject("Actor task execution failed: " + status.ToString(),
                              ray::rpc::ErrorType::TASK_EXECUTION_EXCEPTION);
        results.push_back(error_object);
      }
    } else {
      for (size_t i = 0; i < get_results.size(); ++i) {
        if (get_results[i] == nullptr) {
          RAY_LOG(WARNING) << "Actor task object " << object_ids[i].Hex() << " is null";
          auto error_object =
              CreateErrorObject("Actor task object retrieval returned null",
                                ray::rpc::ErrorType::OBJECT_LOST);
          results.push_back(error_object);
        } else {
          results.push_back(get_results[i]);
        }
      }
    }
  }

  // 5. Ensure return count matches num_returns
  while (static_cast<int>(results.size()) < num_returns) {
    results.push_back(nullptr);
  }

  return results;
}

}  // namespace go
}  // namespace ray
