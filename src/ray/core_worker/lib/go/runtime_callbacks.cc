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

#include "runtime_callbacks.h"

#include "ray/common/function_descriptor.h"
#include "ray/util/logging.h"
#include "ray/util/time.h"
#include "ray/core_worker/core_worker_process.h"
#include "ray/core_worker/common.h"
#include "cgo_wrapper.h"
#include "native_task_executor.h"

#include <atomic>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <functional>

#include "absl/synchronization/mutex.h"

namespace ray::go {

// ============================================================================
// Public API Functions
// ============================================================================

std::function<void()> CreateGCCollectCallback() {
  // Rate limit GC calls to at most once per second (1000ms).
  // Implemented inline to avoid a dependency on the throttler helper, whose
  // location and constructor differ between Ray release lines.
  static int64_t last_run_ms = 0;
  static absl::Mutex mutex;

  return []() {
    absl::MutexLock lock(&mutex);
    int64_t now_ms = ray::current_time_ms();
    if (now_ms - last_run_ms >= 1000) {
      last_run_ms = now_ms;
      int64_t start = ray::current_time_ms();
      try {
        GoTriggerGC();
        int64_t end = ray::current_time_ms();
        RAY_LOG(DEBUG) << "GC finished in "
                       << static_cast<double>(end - start) / 1000
                       << " seconds.";
      } catch (const std::exception& e) {
        RAY_LOG(ERROR) << "GC callback failed: " << e.what();
      } catch (...) {
        RAY_LOG(ERROR) << "GC callback failed with unknown exception";
      }
    }
  };
}

std::function<ray::Status(
    const ray::rpc::Address &caller_address,
    ray::rpc::TaskType task_type,
    const std::string task_name,
    const ray::core::RayFunction &ray_function,
    const std::unordered_map<std::string, double> &required_resources,
    const std::vector<std::shared_ptr<ray::RayObject>> &args,
    const std::vector<ray::rpc::ObjectReference> &arg_refs,
    const std::string &debugger_breakpoint,
    const std::string &serialized_retry_exception_allowlist,
    std::vector<std::pair<ray::ObjectID, std::shared_ptr<ray::RayObject>>> *returns,
    std::vector<std::pair<ray::ObjectID, std::shared_ptr<ray::RayObject>>> *dynamic_returns,
    std::vector<std::pair<ray::ObjectID, bool>> *streaming_generator_returns,
    std::shared_ptr<ray::LocalMemoryBuffer> &creation_task_exception_pb_bytes,
    bool *is_retryable_error,
    std::string *actor_repr_name,
    std::string *application_error,
    const std::vector<ray::ConcurrencyGroup> &defined_concurrency_groups,
    const std::string name_of_concurrency_group_to_execute,
    bool is_reattempt,
    bool is_streaming_generator,
    bool retry_exception,
    int64_t generator_backpressure_num_objects,
    int64_t num_objects_per_yield,
    const std::optional<std::string> &tensor_transport)> CreateTaskExecutionCallback() {
  return [](const ray::rpc::Address &caller_address,
            ray::rpc::TaskType task_type,
            const std::string task_name,
            const ray::core::RayFunction &ray_function,
            const std::unordered_map<std::string, double> &required_resources,
            const std::vector<std::shared_ptr<ray::RayObject>> &args,
            const std::vector<ray::rpc::ObjectReference> &arg_refs,
            const std::string &debugger_breakpoint,
            const std::string &serialized_retry_exception_allowlist,
            std::vector<std::pair<ray::ObjectID, std::shared_ptr<ray::RayObject>>> *returns,
            std::vector<std::pair<ray::ObjectID, std::shared_ptr<ray::RayObject>>> *dynamic_returns,
            std::vector<std::pair<ray::ObjectID, bool>> *streaming_generator_returns,
            std::shared_ptr<ray::LocalMemoryBuffer> &creation_task_exception_pb_bytes,
            bool *is_retryable_error,
            std::string *actor_repr_name,
            std::string *application_error,
            const std::vector<ray::ConcurrencyGroup> &defined_concurrency_groups,
            const std::string name_of_concurrency_group_to_execute,
            bool is_reattempt,
            bool is_streaming_generator,
            bool retry_exception,
            int64_t generator_backpressure_num_objects,
            int64_t num_objects_per_yield,
            const std::optional<std::string> &tensor_transport) {
    RAY_UNUSED(defined_concurrency_groups);
    RAY_UNUSED(name_of_concurrency_group_to_execute);
    RAY_UNUSED(actor_repr_name);
    RAY_UNUSED(tensor_transport);
    *is_retryable_error = false;

    auto function_descriptor = ray_function.GetFunctionDescriptor();
    // Convert function descriptor to string list based on its type
    std::vector<std::string> func_desc_list;
    auto fd_type = function_descriptor->Type();
    if (fd_type == ray::FunctionDescriptorType::kCppFunctionDescriptor) {
      auto typed_fd = function_descriptor->As<ray::CppFunctionDescriptor>();
      func_desc_list = {typed_fd->FunctionName(),
                       typed_fd->Caller(),
                       typed_fd->ClassName()};
    } else if (fd_type == ray::FunctionDescriptorType::kPythonFunctionDescriptor) {
      auto typed_fd = function_descriptor->As<ray::PythonFunctionDescriptor>();
      func_desc_list = {typed_fd->ModuleName(),
                       typed_fd->ClassName(),
                       typed_fd->FunctionName(),
                       typed_fd->FunctionHash()};
    } else if (fd_type == ray::FunctionDescriptorType::kJavaFunctionDescriptor) {
      auto typed_fd = function_descriptor->As<ray::JavaFunctionDescriptor>();
      func_desc_list = {typed_fd->ClassName(),
                       typed_fd->FunctionName(),
                       typed_fd->Signature()};
    } else if (fd_type == ray::FunctionDescriptorType::kGoFunctionDescriptor) {
      auto typed_fd = function_descriptor->As<ray::GoFunctionDescriptor>();
      // Go function descriptor has 4 elements: [module_name, package_path, function_name, method_name]
      // This matches the format expected by Go's FunctionDescriptorFromList
      func_desc_list = {typed_fd->ModuleName(),
                       typed_fd->PackagePath(),
                       typed_fd->FunctionName(),
                       typed_fd->MethodName()};
    } else {
      RAY_LOG(ERROR) << "Unknown function descriptor type: " << fd_type
                     << ". This should not happen in normal operation.";
      return ray::Status::Invalid("Unknown function descriptor type");
    }

    std::vector<const char*> func_desc_cstrs;
    for (const auto& s : func_desc_list) {
      func_desc_cstrs.push_back(s.c_str());
    }

    std::vector<CFunctionArg> c_args;
    for (const auto& arg : args) {
      CFunctionArg c_arg;
      memset(&c_arg, 0, sizeof(c_arg));
      if (arg->HasData()) {
        const auto& data = arg->GetData();
        const auto& metadata = arg->GetMetadata();
        CFunctionArg_SetValue(
            &c_arg,
            reinterpret_cast<const char*>(data->Data()),
            static_cast<int>(data->Size()),
            metadata ? reinterpret_cast<const char*>(metadata->Data()) : nullptr,
            metadata ? static_cast<int>(metadata->Size()) : 0);
      }
      c_args.push_back(c_arg);
    }

    // For now, always pass null for actor ID. Actor tasks will be handled separately.
    const char* actor_id_data = nullptr;
    int actor_id_size = 0;

    // Log returns size before calling Go
    RAY_LOG(INFO) << "C++ calling GoExecuteTask: returns size=" << returns->size();

    // Note: const_cast is necessary because Go exports functions with non-const parameters,
    // but C++ code maintains const correctness. The Go function does not modify the data.
    CSerializedObjectArray* c_results = GoExecuteTask(
        static_cast<int>(task_type),
        const_cast<char**>(func_desc_cstrs.data()),
        static_cast<int>(func_desc_cstrs.size()),
        c_args.data(),
        static_cast<int>(c_args.size()),
        static_cast<int>(returns->size()),
        const_cast<char*>(actor_id_data),
        actor_id_size);

    // Log c_results from GoExecuteTask
    RAY_LOG(INFO) << "C++ received c_results from GoExecuteTask: "
                  << (c_results != nullptr ? "count=" + std::to_string(c_results->count) : "nullptr");

    if (c_results == nullptr) {
      *application_error = "Task execution failed in Go runtime";
      return ray::Status::Invalid("Task execution failed");
    }

    // Use RAII pointer for automatic cleanup (CSerializedObjectArrayPtr is defined in cgo_wrapper.h)
    auto result_ptr = CSerializedObjectArrayPtr(c_results);

    if (result_ptr->count > 0 && !returns->empty()) {
      // Validate that objects array is not null when count > 0
      if (result_ptr->objects == nullptr) {
        *application_error = "Invalid result from GoExecuteTask";
        return ray::Status::Invalid("GoExecuteTask returned invalid result structure");
      }

      for (int i = 0; i < result_ptr->count && i < static_cast<int>(returns->size()); i++) {
        const CSerializedObject& c_obj = result_ptr->objects[i];
        auto& result_id = (*returns)[i].first;
        auto& result_ptr_obj = (*returns)[i].second;

        std::shared_ptr<ray::LocalMemoryBuffer> data_buffer;
        if (c_obj.data_size > 0 && c_obj.data != nullptr) {
          data_buffer = std::make_shared<ray::LocalMemoryBuffer>(
              reinterpret_cast<uint8_t*>(const_cast<char*>(c_obj.data)),
              static_cast<size_t>(c_obj.data_size));
        }

        // Copy the metadata into an owned buffer: the C array returned by GoExecuteTask is
        // freed (CNativeCommon_FreeCSerializedObjectArray) as soon as this callback returns,
        // while the return object stored by AllocateReturnObject must outlive it. Data is
        // explicitly copied below, but metadata was not, leaving the stored return object
        // referencing freed C memory (surfaced as garbage metadata on the driver side).
        std::shared_ptr<ray::LocalMemoryBuffer> metadata_buffer;
        if (c_obj.metadata_size > 0 && c_obj.metadata != nullptr) {
          metadata_buffer = std::make_shared<ray::LocalMemoryBuffer>(
              reinterpret_cast<uint8_t*>(const_cast<char*>(c_obj.metadata)),
              static_cast<size_t>(c_obj.metadata_size),
              /*copy_data=*/true);
        }

        std::vector<ray::rpc::ObjectReference> contained_object_refs;
        std::vector<ray::ObjectID> contained_object_ids;
        auto ray_obj = std::make_shared<ray::RayObject>(
            data_buffer, metadata_buffer, contained_object_refs);

        // Use a local variable for task_output_inlined_bytes to avoid nullptr dereference
        int64_t task_output_inlined_bytes = 0;
        RAY_CHECK_OK(ray::core::CoreWorkerProcess::GetCoreWorker().AllocateReturnObject(
            result_id,
            c_obj.data_size,
            metadata_buffer,
            contained_object_ids,
            caller_address,
            &task_output_inlined_bytes,
            &result_ptr_obj));

        if (result_ptr_obj != nullptr && data_buffer != nullptr) {
          memcpy(result_ptr_obj->GetData()->Data(),
                 data_buffer->Data(),
                 static_cast<size_t>(c_obj.data_size));
        }

        RAY_CHECK_OK(ray::core::CoreWorkerProcess::GetCoreWorker().SealReturnObject(
            result_id, result_ptr_obj, ray::ObjectID::Nil(), caller_address));
      }
    }

    // Memory is automatically freed by RAII when result_ptr goes out of scope

    return ray::Status::OK();
  };
}

}  // namespace ray::go
