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

// CGO wrapper implementation for TaskSubmitter functions.
// This file contains all TaskSubmitter-related CGO functions, separated from
// native_runtime.cc for better modularity and maintainability.
// Pattern: Similar to io_ray_runtime_task_NativeTaskSubmitter.cc in Java runtime.

#include "ray/core_worker/lib/go/native_task_submitter.h"

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/core_worker_process.h"
#include "ray/core_worker/lib/go/cgo_wrapper.h"
#include "ray/core_worker/lib/go/task_argument.h"
#include "ray/core_worker/lib/go/task_submitter_ops.h"
#include "ray/util/logging.h"

using ray::go::CgoErrorHandler;
using ray::go::CgoTypeConverter;

// ============================================================================
// Helper Functions
// ============================================================================

namespace {

// Helper function to create CObjectIdArray from std::vector<ray::ObjectID>
// Uses RAII for safe memory management, then releases to caller
// Caller is responsible for freeing the returned CObjectIdArray using
// CNativeCommon_FreeCObjectIdArray Uses arena allocation (single contiguous buffer) for
// better performance. The data_buffer_start field is set so
// CNativeCommon_FreeCObjectIdArray knows to free the shared buffer once instead of
// freeing each element individually.
CObjectIdArray *CreateCObjectIdArray(const std::vector<ray::ObjectID> &ids) {
  if (ids.empty()) {
    return nullptr;
  }

  // Use RAII pointer for automatic cleanup on error
  using CObjectIdArrayPtr =
      ray::go::CgoUniquePtr<CObjectIdArray, &CNativeCommon_FreeCObjectIdArray>;
  auto result_ptr =
      CObjectIdArrayPtr(static_cast<CObjectIdArray *>(malloc(sizeof(CObjectIdArray))));
  if (!result_ptr) {
    RAY_LOG(ERROR) << "Failed to allocate memory for CObjectIdArray";
    return nullptr;
  }

  result_ptr->count = static_cast<int>(ids.size());
  result_ptr->object_ids =
      static_cast<CByteArray *>(malloc(sizeof(CByteArray) * result_ptr->count));
  if (!result_ptr->object_ids) {
    RAY_LOG(ERROR) << "Failed to allocate memory for CObjectIdArray";
    return nullptr;  // RAII will clean up result_ptr
  }

  // Calculate total data size needed for all object IDs
  size_t total_data_size = 0;
  std::vector<size_t> offsets(ids.size());
  for (size_t i = 0; i < ids.size(); ++i) {
    offsets[i] = total_data_size;
    total_data_size += ids[i].Binary().size();
  }

  // Allocate a single contiguous buffer for all object ID data (arena allocation).
  // This reduces malloc calls from N+2 to 2, improving performance and reducing
  // memory fragmentation. The data_buffer_start field is set so the free function
  // knows to free this shared buffer once instead of freeing each element individually.
  char *data_buffer = static_cast<char *>(malloc(total_data_size));
  if (!data_buffer) {
    RAY_LOG(ERROR) << "Failed to allocate contiguous data buffer of size "
                   << total_data_size;
    return nullptr;
  }

  // Fill each object ID with pointer into the contiguous buffer
  for (int i = 0; i < result_ptr->count; ++i) {
    const std::string &binary = ids[i].Binary();
    result_ptr->object_ids[i].size = static_cast<int>(binary.size());
    result_ptr->object_ids[i].data =
        data_buffer + offsets[i];  // Point into contiguous buffer
    memcpy(result_ptr->object_ids[i].data, binary.data(), binary.size());
  }

  // Save the start of the shared data buffer so the free function can release it properly
  result_ptr->data_buffer_start = data_buffer;

  // Release ownership and return raw pointer to caller
  return result_ptr.release();
}

}  // anonymous namespace
// ============================================================================
// CGO Exports - TaskSubmitter Functions
// ============================================================================

extern "C" CObjectIdArray *CNativeTaskSubmitter_SubmitTask(
    const char **function_descriptor,
    int function_descriptor_count,
    const CFunctionArg *args,
    int args_count,
    int num_returns,
    const CTaskOptions *options) {
  return CgoErrorHandler::Execute(
      "CNativeTaskSubmitter_SubmitTask", [&]() -> CObjectIdArray * {
        // Build function descriptor
        std::vector<std::string> func_desc_vec = CNativeCommon_ConvertToStringVector(
            function_descriptor, function_descriptor_count);
        if (func_desc_vec.empty()) {
          RAY_LOG(ERROR) << "Empty function descriptor";
          return nullptr;
        }

        // Build task arguments
        std::vector<std::unique_ptr<ray::go::TaskArgument>> task_args =
            ray::go::BuildTaskArgs(args, args_count);

        // Build task options
        ray::go::TaskSubmitOptions submit_options;
        if (options != nullptr) {
          // Parse resources
          submit_options.resources = ray::go::TaskSubmitterOperations::ParseResources(
              options->resources ? options->resources : "");
          submit_options.serialized_runtime_env_info =
              options->runtime_env ? options->runtime_env : "";
          submit_options.num_returns = num_returns;
          submit_options.max_retries = options->max_retries;
          if (options->placement_group_id != nullptr &&
              options->placement_group_id_size > 0) {
            submit_options.placement_group_id_hex = std::string(
                options->placement_group_id, options->placement_group_id_size);
            submit_options.bundle_index = options->bundle_index;
          }
        } else {
          submit_options.num_returns = num_returns;
        }

        // Submit task using business logic layer
        auto &ops = ray::go::TaskSubmitterOperations::GetInstance();
        std::vector<ray::rpc::ObjectReference> return_refs =
            ops.SubmitTask(func_desc_vec, task_args, submit_options);

        // Convert ObjectReferences to ObjectIDs
        std::vector<ray::ObjectID> return_ids;
        for (const auto &ref : return_refs) {
          return_ids.push_back(ray::ObjectID::FromBinary(ref.object_id()));
        }

        return CreateCObjectIdArray(return_ids);
      });
}

extern "C" CByteArray *CNativeTaskSubmitter_CreateActor(
    const char **function_descriptor,
    int function_descriptor_count,
    const CFunctionArg *args,
    int args_count,
    const CActorCreationOptions *options) {
  return CgoErrorHandler::Execute(
      "CNativeTaskSubmitter_CreateActor", [&]() -> CByteArray * {
        // Build function descriptor
        std::vector<std::string> func_desc_vec = CNativeCommon_ConvertToStringVector(
            function_descriptor, function_descriptor_count);
        if (func_desc_vec.empty()) {
          RAY_LOG(ERROR) << "Empty function descriptor for actor creation";
          return nullptr;
        }

        // Build task arguments
        std::vector<std::unique_ptr<ray::go::TaskArgument>> task_args =
            ray::go::BuildTaskArgs(args, args_count);

        // Build actor creation options
        ray::go::ActorCreateOptions actor_options;
        if (options != nullptr) {
          actor_options.max_restarts = options->max_restarts;
          actor_options.max_task_retries = options->max_task_retries;
          actor_options.resources = ray::go::TaskSubmitterOperations::ParseResources(
              options->resources ? options->resources : "");
          actor_options.name = options->name ? options->name : "";
          actor_options.namespace_ = options->namespace_ ? options->namespace_ : "";
          actor_options.serialized_runtime_env_info =
              options->runtime_env ? options->runtime_env : "";
        }

        // Create actor using business logic layer
        auto &ops = ray::go::TaskSubmitterOperations::GetInstance();
        ray::ActorID actor_id = ops.CreateActor(func_desc_vec, task_args, actor_options);

        // Convert to CByteArray
        const std::string &binary = actor_id.Binary();
        return CgoTypeConverter::StringToCByteArray(binary);
      });
}

extern "C" CObjectIdArray *CNativeTaskSubmitter_SubmitActorTask(
    const char *actor_id_data,
    int actor_id_size,
    const char **function_descriptor,
    int function_descriptor_count,
    const CFunctionArg *args,
    int args_count,
    int num_returns,
    const CTaskOptions *options) {
  return CgoErrorHandler::Execute(
      "CNativeTaskSubmitter_SubmitActorTask", [&]() -> CObjectIdArray * {
        // Parse actor ID
        if (actor_id_data == nullptr || actor_id_size <= 0) {
          RAY_LOG(ERROR) << "Invalid actor ID";
          return nullptr;
        }
        ray::ActorID actor_id =
            ray::ActorID::FromBinary(std::string(actor_id_data, actor_id_size));

        // Build function descriptor
        std::vector<std::string> func_desc_vec = CNativeCommon_ConvertToStringVector(
            function_descriptor, function_descriptor_count);
        if (func_desc_vec.empty()) {
          RAY_LOG(ERROR) << "Empty function descriptor for actor task";
          return nullptr;
        }

        // Build task arguments
        std::vector<std::unique_ptr<ray::go::TaskArgument>> task_args =
            ray::go::BuildTaskArgs(args, args_count);

        // Build task options
        ray::go::TaskSubmitOptions submit_options;
        if (options != nullptr) {
          // Parse resources
          submit_options.resources = ray::go::TaskSubmitterOperations::ParseResources(
              options->resources ? options->resources : "");
          submit_options.num_returns = num_returns;
          submit_options.max_retries = options->max_retries;
        } else {
          submit_options.num_returns = num_returns;
        }

        // Submit actor task using business logic layer
        auto &ops = ray::go::TaskSubmitterOperations::GetInstance();
        std::vector<ray::rpc::ObjectReference> return_refs =
            ops.SubmitActorTask(actor_id, func_desc_vec, task_args, submit_options);

        // Convert ObjectReferences to ObjectIDs
        std::vector<ray::ObjectID> return_ids;
        for (const auto &ref : return_refs) {
          return_ids.push_back(ray::ObjectID::FromBinary(ref.object_id()));
        }

        return CreateCObjectIdArray(return_ids);
      });
}

extern "C" int CNativeTaskSubmitter_GetActor(const char *name,
                                             const char *namespace_,
                                             CByteArray **actor_id_out,
                                             char **error_out) {
  return CgoErrorHandler::Execute("CNativeTaskSubmitter_GetActor", [&]() -> int {
    if (!name || !actor_id_out) {
      return 0;
    }

    // Get namespace from parameter or job config
    std::string ns;
    if (namespace_ != nullptr && strlen(namespace_) > 0) {
      ns = std::string(namespace_);
    } else {
      // Use job config namespace as default
      ns = ray::core::CoreWorkerProcess::GetCoreWorker().GetJobConfig().ray_namespace();
    }

    // Call CoreWorker's GetNamedActorHandle
    auto result = ray::core::CoreWorkerProcess::GetCoreWorker().GetNamedActorHandle(
        std::string(name), ns);

    const auto &status = result.second;
    if (status.IsNotFound()) {
      // Actor not found - return nil actor ID
      *actor_id_out = CgoTypeConverter::StringToCByteArray(ray::ActorID::Nil().Binary());
      return 1;
    }

    if (!status.ok()) {
      return 0;
    }

    const auto &actor_handle = result.first;
    if (!actor_handle) {
      return 0;
    }

    // Convert actor ID to CByteArray
    const std::string &binary = actor_handle->GetActorID().Binary();
    *actor_id_out = CgoTypeConverter::StringToCByteArray(binary);
    return 1;
  });
}
