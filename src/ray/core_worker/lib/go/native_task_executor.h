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

// CGO wrapper header for TaskExecutor C++ API.
// This file provides C wrappers for CoreWorker TaskExecutor to enable CGO binding.
// Pattern: Similar to io_ray_runtime_task_NativeTaskExecutor.h in Java runtime.

#ifndef RAY_CORE_WORKER_LIB_GO_NATIVE_TASK_EXECUTOR_H
#define RAY_CORE_WORKER_LIB_GO_NATIVE_TASK_EXECUTOR_H

#include "cgo_wrapper.h"

#ifdef __cplusplus
extern "C" {
#endif

// Note: CSerializedObject, CSerializedObjectArray, and CFunctionArg are defined in cgo_wrapper.h

// GoTaskExecutorCallback is the function pointer type for Go task executor callback.
// This callback is registered by Go runtime and called by C++ when a task is received.
//
// Parameters:
//   task_type - Type of task (matches ray::rpc::TaskType enum values)
//   function_descriptor - Array of function descriptor strings
//   function_descriptor_count - Number of elements in function_descriptor array
//   args - Array of function arguments
//   args_count - Number of arguments
//   num_returns - Number of expected return values
//   actor_id_data - Binary data of actor ID (for actor tasks, NULL for normal tasks)
//   actor_id_size - Size of actor ID binary data
//
// Returns:
//   CSerializedObjectArray* containing task execution results, or NULL on failure.
//   Caller is responsible for freeing the returned CSerializedObjectArray.
typedef CSerializedObjectArray* (*GoTaskExecutorCallback)(
    int task_type,
    char** function_descriptor,
    int function_descriptor_count,
    CFunctionArg* args,
    int args_count,
    int num_returns,
    char* actor_id_data,
    int actor_id_size);

// ============================================================================
// TaskExecutor Functions
// ============================================================================

// GoExecuteTask is the exported function from Go that executes tasks.
// This function is called by C++ to execute a task in the Go runtime.
//
// Parameters:
//   task_type - Type of task (matches ray::rpc::TaskType enum values)
//   function_descriptor - Array of function descriptor strings
//   function_descriptor_count - Number of elements in function_descriptor array
//   args - Array of function arguments
//   args_count - Number of arguments
//   num_returns - Number of expected return values
//   actor_id_data - Binary data of actor ID (for actor tasks, NULL for normal tasks)
//   actor_id_size - Size of actor ID binary data
//
// Returns:
//   CSerializedObjectArray* containing task execution results, or NULL on failure.
//   Caller is responsible for freeing the returned CSerializedObjectArray.
CSerializedObjectArray* GoExecuteTask(
    int task_type,
    char** function_descriptor,
    int function_descriptor_count,
    CFunctionArg* args,
    int args_count,
    int num_returns,
    char* actor_id_data,
    int actor_id_size);

// RegisterGoTaskExecutorCallback registers the Go task executor callback with C++.
// This is a wrapper function that calls CNativeTaskExecutor_RegisterCallback with
// the GoExecuteTask callback function (which is exported from Go).
//
// This function should be called once during Go runtime initialization.
void RegisterGoTaskExecutorCallback(void);

// CNativeTaskExecutor_RegisterCallback registers the Go task executor callback.
//
// Parameters:
//   callback - Function pointer to Go task executor callback
//
// This function should be called once during Go runtime initialization.
void CNativeTaskExecutor_RegisterCallback(GoTaskExecutorCallback callback);

// CNativeTaskExecutor_Execute executes a normal task directly.
//
// Parameters:
//   function_descriptor - Array of function descriptor strings
//   function_descriptor_count - Number of elements in function_descriptor array
//   args - Array of function arguments
//   args_count - Number of arguments
//   num_returns - Number of expected return values
//
// Returns:
//   CSerializedObjectArray* containing execution results, or NULL on failure.
//   Caller is responsible for freeing the returned CSerializedObjectArray.
CSerializedObjectArray* CNativeTaskExecutor_Execute(
    const char** function_descriptor,
    int function_descriptor_count,
    const CFunctionArg* args,
    int args_count,
    int num_returns);

// CNativeTaskExecutor_ExecuteActorTask executes an actor task.
//
// Parameters:
//   actor_id_data - Binary data of actor ID
//   actor_id_size - Size of actor ID binary data
//   function_descriptor - Array of function descriptor strings
//   function_descriptor_count - Number of elements in function_descriptor array
//   args - Array of function arguments
//   args_count - Number of arguments
//   num_returns - Number of expected return values
//
// Returns:
//   CSerializedObjectArray* containing execution results, or NULL on failure.
//   Caller is responsible for freeing the returned CSerializedObjectArray.
CSerializedObjectArray* CNativeTaskExecutor_ExecuteActorTask(
    const char* actor_id_data,
    int actor_id_size,
    const char** function_descriptor,
    int function_descriptor_count,
    const CFunctionArg* args,
    int args_count,
    int num_returns);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // RAY_CORE_WORKER_LIB_GO_NATIVE_TASK_EXECUTOR_H
