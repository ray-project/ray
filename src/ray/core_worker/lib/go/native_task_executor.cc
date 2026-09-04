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

// CGO wrapper implementation for TaskExecutor functions.
// This file contains all TaskExecutor-related CGO functions, separated from
// native_runtime.cc for better modularity and maintainability.
// Pattern: Similar to io_ray_runtime_task_NativeTaskExecutor.cc in Java runtime.

#include "native_task_executor.h"
#include "cgo_wrapper.h"
#include "task_argument.h"
#include "task_executor_ops.h"

#include <atomic>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/common/task/task_common.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/task/task_util.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/core_worker_process.h"
#include "ray/util/logging.h"

// ============================================================================
// Global State
// ============================================================================

namespace {

// Static variable to store the C callback pointer
// The callback type is defined in native_task_executor.h
static std::atomic<GoTaskExecutorCallback> g_go_task_executor_callback_static{nullptr};

// ============================================================================
// Helper Functions
// ============================================================================
}  // anonymous namespace


// ============================================================================
// CGO Exports - TaskExecutor Functions
// ============================================================================

// RegisterGoTaskExecutorCallback registers the Go task executor with C++.
// This function is called from Go code during runtime initialization.
// It wraps CNativeTaskExecutor_RegisterCallback and passes the GoExecuteTask callback.
extern "C" void RegisterGoTaskExecutorCallback() {
  CNativeTaskExecutor_RegisterCallback(GoExecuteTask);
}

// Register Go task executor callback
// This function is called from Go to register the task executor callback
extern "C" void CNativeTaskExecutor_RegisterCallback(
    GoTaskExecutorCallback callback) {
  g_go_task_executor_callback_static.store(callback);
  RAY_LOG(INFO) << "Go task executor callback registered";
}

extern "C" CSerializedObjectArray* CNativeTaskExecutor_Execute(
    const char** function_descriptor,
    int function_descriptor_count,
    const CFunctionArg* args,
    int args_count,
    int num_returns) {
  return ray::go::CgoErrorHandler::Execute(
      "CNativeTaskExecutor_Execute",
      [&]() -> CSerializedObjectArray* {
        // Convert C types to C++ types
        std::vector<std::string> func_desc = CNativeCommon_ConvertToStringVector(
            function_descriptor, function_descriptor_count);
        if (func_desc.empty()) {
          throw std::runtime_error("Empty function descriptor");
        }

        // Build task arguments using helper function
        std::vector<std::unique_ptr<ray::go::TaskArgument>> task_args = ray::go::BuildTaskArgs(args, args_count);

        // Call business logic layer
        auto& ops = ray::go::TaskExecutorOperations::GetInstance();
        auto results = ops.ExecuteTask(func_desc, task_args, num_returns);

        // Convert C++ result to C types
        if (results.empty()) {
          return nullptr;
        }

        // Use RAII pointer for automatic cleanup on error
        // Caller is responsible for freeing the returned CSerializedObjectArray using CNativeCommon_FreeCSerializedObjectArray
        using CSerializedObjectArrayPtr = ray::go::CgoUniquePtr<CSerializedObjectArray, &CNativeCommon_FreeCSerializedObjectArray>;
        auto result_ptr = CSerializedObjectArrayPtr(static_cast<CSerializedObjectArray*>(malloc(sizeof(CSerializedObjectArray))));
        if (!result_ptr) {
          throw std::runtime_error("Failed to allocate memory for result array");
        }

        result_ptr->count = static_cast<int>(results.size());
        result_ptr->objects = static_cast<CSerializedObject*>(
            malloc(sizeof(CSerializedObject) * result_ptr->count));
        if (!result_ptr->objects) {
          throw std::runtime_error("Failed to allocate memory for objects");
        }

        for (int i = 0; i < result_ptr->count; ++i) {
          const auto& obj = results[i];
          if (obj && obj->HasData()) {
            const auto& data = obj->GetData();
            result_ptr->objects[i].data = static_cast<char*>(malloc(data->Size()));
            if (!result_ptr->objects[i].data) {
              throw std::runtime_error("Failed to allocate memory for object data");
            }
            memcpy(result_ptr->objects[i].data, data->Data(), data->Size());
            result_ptr->objects[i].data_size = static_cast<int>(data->Size());

            const auto& metadata = obj->GetMetadata();
            if (metadata && metadata->Size() > 0) {
              result_ptr->objects[i].metadata = static_cast<char*>(malloc(metadata->Size()));
              if (!result_ptr->objects[i].metadata) {
                throw std::runtime_error("Failed to allocate memory for metadata");
              }
              memcpy(result_ptr->objects[i].metadata, metadata->Data(), metadata->Size());
              result_ptr->objects[i].metadata_size = static_cast<int>(metadata->Size());
            } else {
              result_ptr->objects[i].metadata = nullptr;
              result_ptr->objects[i].metadata_size = 0;
            }
          } else {
            result_ptr->objects[i].data = nullptr;
            result_ptr->objects[i].data_size = 0;
            result_ptr->objects[i].metadata = nullptr;
            result_ptr->objects[i].metadata_size = 0;
          }
        }

        // Release ownership and return raw pointer to caller
        return result_ptr.release();
      });
}

extern "C" CSerializedObjectArray* CNativeTaskExecutor_ExecuteActorTask(
    const char* actor_id_data,
    int actor_id_size,
    const char** function_descriptor,
    int function_descriptor_count,
    const CFunctionArg* args,
    int args_count,
    int num_returns) {
  return ray::go::CgoErrorHandler::Execute(
      "CNativeTaskExecutor_ExecuteActorTask",
      [&]() -> CSerializedObjectArray* {
        // Validate and convert actor ID
        if (!actor_id_data || actor_id_size <= 0) {
          throw std::runtime_error("Invalid actor ID");
        }
        ray::ActorID actor_id =
            ray::ActorID::FromBinary(std::string(actor_id_data, actor_id_size));

        // Convert function descriptor
        std::vector<std::string> func_desc = CNativeCommon_ConvertToStringVector(
            function_descriptor, function_descriptor_count);
        if (func_desc.empty()) {
          throw std::runtime_error("Empty function descriptor for actor task");
        }

        // Convert task arguments using helper function
        std::vector<std::unique_ptr<ray::go::TaskArgument>> task_args = ray::go::BuildTaskArgs(args, args_count);

        // Call business logic layer
        auto& ops = ray::go::TaskExecutorOperations::GetInstance();
        auto results = ops.ExecuteActorTask(actor_id, func_desc, task_args, num_returns);

        // Convert C++ result to C types
        if (results.empty()) {
          return nullptr;
        }

        // Use RAII pointer for automatic cleanup on error
        // Caller is responsible for freeing the returned CSerializedObjectArray using CNativeCommon_FreeCSerializedObjectArray
        using CSerializedObjectArrayPtr = ray::go::CgoUniquePtr<CSerializedObjectArray, &CNativeCommon_FreeCSerializedObjectArray>;
        auto result_ptr = CSerializedObjectArrayPtr(static_cast<CSerializedObjectArray*>(malloc(sizeof(CSerializedObjectArray))));
        if (!result_ptr) {
          throw std::runtime_error("Failed to allocate memory for result array");
        }

        result_ptr->count = static_cast<int>(results.size());
        result_ptr->objects = static_cast<CSerializedObject*>(
            malloc(sizeof(CSerializedObject) * result_ptr->count));
        if (!result_ptr->objects) {
          throw std::runtime_error("Failed to allocate memory for objects");
        }

        for (int i = 0; i < result_ptr->count; ++i) {
          const auto& obj = results[i];
          if (obj && obj->HasData()) {
            const auto& data = obj->GetData();
            result_ptr->objects[i].data = static_cast<char*>(malloc(data->Size()));
            if (!result_ptr->objects[i].data) {
              throw std::runtime_error("Failed to allocate memory for object data");
            }
            memcpy(result_ptr->objects[i].data, data->Data(), data->Size());
            result_ptr->objects[i].data_size = static_cast<int>(data->Size());

            const auto& metadata = obj->GetMetadata();
            if (metadata && metadata->Size() > 0) {
              result_ptr->objects[i].metadata = static_cast<char*>(malloc(metadata->Size()));
              if (!result_ptr->objects[i].metadata) {
                throw std::runtime_error("Failed to allocate memory for metadata");
              }
              memcpy(result_ptr->objects[i].metadata, metadata->Data(), metadata->Size());
              result_ptr->objects[i].metadata_size = static_cast<int>(metadata->Size());
            } else {
              result_ptr->objects[i].metadata = nullptr;
              result_ptr->objects[i].metadata_size = 0;
            }
          } else {
            result_ptr->objects[i].data = nullptr;
            result_ptr->objects[i].data_size = 0;
            result_ptr->objects[i].metadata = nullptr;
            result_ptr->objects[i].metadata_size = 0;
          }
        }

        // Release ownership and return raw pointer to caller
        return result_ptr.release();
      });
}
