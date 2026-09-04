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

// CGO wrapper header for TaskSubmitter C++ API.
// This file provides C wrappers for CoreWorker TaskSubmitter to enable CGO binding.
// Pattern: Similar to io_ray_runtime_task_NativeTaskSubmitter.h in Java runtime.

#ifndef RAY_CORE_WORKER_LIB_GO_NATIVE_TASK_SUBMITTER_H
#define RAY_CORE_WORKER_LIB_GO_NATIVE_TASK_SUBMITTER_H

#include "cgo_wrapper.h"

#ifdef __cplusplus
extern "C" {
#endif

// CTaskOptions contains task submission options.
typedef struct {
  const char* resources;         // Resource requirements (e.g., "CPU:2.0,GPU:1.0")
  int max_retries;               // Maximum number of retries for failed tasks
  const char* runtime_env;       // Serialized runtime environment
  // PlacementGroup fields
  const char* placement_group_id;  // PlacementGroup ID binary data (hex string)
  int placement_group_id_size;     // Size of placement group ID in bytes
  int bundle_index;                // Bundle index within placement group (-1 if not used)
} CTaskOptions;

// CActorCreationOptions contains actor creation options.
// Note: This struct is used to simplify the C interface.
// The C++ ActorCreationOptions has const members and is constructed from this struct.
typedef struct {
  int max_restarts;              // Maximum number of actor restarts
  int max_task_retries;          // Maximum number of task retries
  const char* resources;         // Resource requirements (e.g., "CPU:2.0,GPU:1.0")
  const char* name;              // Actor name (optional)
  const char* namespace_;        // Namespace for actor (optional)
  const char* runtime_env;       // Serialized runtime environment
} CActorCreationOptions;

// ============================================================================
// TaskSubmitter Functions
// ============================================================================

// CNativeTaskSubmitter_SubmitTask submits a remote task.
//
// Parameters:
//   function_descriptor - Array of function descriptor strings
//   function_descriptor_count - Number of elements in function_descriptor array
//   args - Array of function arguments
//   args_count - Number of arguments
//   num_returns - Number of return values
//   options - Task options (can be NULL for defaults)
//
// Returns:
//   CObjectIdArray* containing return object IDs, or NULL on failure.
//   Caller is responsible for freeing the returned CObjectIdArray.
CObjectIdArray* CNativeTaskSubmitter_SubmitTask(
    const char** function_descriptor,
    int function_descriptor_count,
    const CFunctionArg* args,
    int args_count,
    int num_returns,
    const CTaskOptions* options);

// CNativeTaskSubmitter_CreateActor creates a new actor.
//
// Parameters:
//   function_descriptor - Array of function descriptor strings
//   function_descriptor_count - Number of elements in function_descriptor array
//   args - Array of constructor arguments
//   args_count - Number of constructor arguments
//   options - Actor creation options (can be NULL for defaults)
//
// Returns:
//   CByteArray* containing actor ID binary data, or NULL on failure.
//   Caller is responsible for freeing the returned CByteArray.
CByteArray* CNativeTaskSubmitter_CreateActor(
    const char** function_descriptor,
    int function_descriptor_count,
    const CFunctionArg* args,
    int args_count,
    const CActorCreationOptions* options);

// CNativeTaskSubmitter_SubmitActorTask submits a task to an actor.
//
// Parameters:
//   actor_id_data - Binary data of actor ID
//   actor_id_size - Size of actor ID binary data
//   function_descriptor - Array of function descriptor strings
//   function_descriptor_count - Number of elements in function_descriptor array
//   args - Array of function arguments
//   args_count - Number of arguments
//   num_returns - Number of return values
//   options - Task options (can be NULL for defaults)
//
// Returns:
//   CObjectIdArray* containing return object IDs, or NULL on failure.
//   Caller is responsible for freeing the returned CObjectIdArray.
CObjectIdArray* CNativeTaskSubmitter_SubmitActorTask(
    const char* actor_id_data,
    int actor_id_size,
    const char** function_descriptor,
    int function_descriptor_count,
    const CFunctionArg* args,
    int args_count,
    int num_returns,
    const CTaskOptions* options);

// CNativeTaskSubmitter_GetActor retrieves a named actor by name and namespace.
//
// Parameters:
//   name - The name of the actor
//   namespace - The namespace of the actor (can be NULL for default namespace)
//   actor_id_out - Output parameter: pointer to CByteArray* containing actor ID binary data
//   error_out - Output parameter: pointer to error message string (NULL on success)
//
// Returns:
//   1 on success, 0 on failure.
//   On success, *actor_id_out points to allocated CByteArray (caller must free).
//   On failure, *error_out points to error message (caller must free).
int CNativeTaskSubmitter_GetActor(
    const char* name,
    const char* namespace_,
    CByteArray** actor_id_out,
    char** error_out);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // RAY_CORE_WORKER_LIB_GO_NATIVE_TASK_SUBMITTER_H
