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

// CGO wrapper header for WorkerContext C++ API.
// This file provides C wrappers for CoreWorker WorkerContext to enable CGO binding.
// Pattern: Similar to io_ray_runtime_context_NativeWorkerContext.h in Java runtime.

#ifndef RAY_CORE_WORKER_LIB_GO_NATIVE_WORKER_CONTEXT_H
#define RAY_CORE_WORKER_LIB_GO_NATIVE_WORKER_CONTEXT_H

#include "cgo_wrapper.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// WorkerContext Functions
// ============================================================================

// CNativeWorkerContext_GetCurrentWorkerId returns the current worker ID.
//
// Returns:
//   CByteArray* containing worker ID binary data, or NULL on failure.
//   Caller is responsible for freeing the returned CByteArray and its data.
CByteArray *CNativeWorkerContext_GetCurrentWorkerId();

// CNativeWorkerContext_GetCurrentJobID returns the current job ID.
//
// Returns:
//   CByteArray* containing job ID binary data, or NULL on failure.
//   Caller is responsible for freeing the returned CByteArray and its data.
CByteArray *CNativeWorkerContext_GetCurrentJobID();

// CNativeWorkerContext_GetCurrentActorID returns the current actor ID.
//
// Returns:
//   CByteArray* containing actor ID binary data, or NULL if not an actor.
//   Caller is responsible for freeing the returned CByteArray and its data.
CByteArray *CNativeWorkerContext_GetCurrentActorID();

// CNativeWorkerContext_GetCurrentTaskType returns the current task type.
//
// Returns:
//   Task type as integer (matches ray::rpc::TaskType enum values).
//   Returns 0 on error.
int CNativeWorkerContext_GetCurrentTaskType();

// CNativeWorkerContext_IsCurrentTaskSet returns true if current task is set.
//
// Returns:
//   true if GetCurrentTask() would return a valid task, false otherwise.
//   Use this to check before calling GetCurrentTaskType or GetCurrentTaskId.
bool CNativeWorkerContext_IsCurrentTaskSet();

// CNativeWorkerContext_GetCurrentTaskID returns the current task ID.
//
// Returns:
//   CByteArray* containing task ID binary data, or NULL on failure.
//   Caller is responsible for freeing the returned CByteArray and its data.
CByteArray *CNativeWorkerContext_GetCurrentTaskID();

// CNativeWorkerContext_GetRpcAddress returns the RPC address of the worker.
//
// Returns:
//   CByteArray* containing serialized rpc::Address, or NULL on failure.
//   Caller is responsible for freeing the returned CByteArray and its data.
CByteArray *CNativeWorkerContext_GetRpcAddress();

// CNativeWorkerContext_GetSerializedRuntimeEnv returns the serialized runtime
// environment.
//
// Returns:
//   C string containing serialized runtime environment, or NULL on error.
//   The returned string is statically allocated and should NOT be freed by caller.
//   Use CNativeWorkerContext_HasLastError() to distinguish between empty string and
//   error.
const char *CNativeWorkerContext_GetSerializedRuntimeEnv();

// CNativeWorkerContext_HasLastError returns true if the last operation failed.
//
// Returns:
//   true if the last operation encountered an error, false otherwise.
//   This is used to distinguish between NULL (error) and empty string (valid).
bool CNativeWorkerContext_HasLastError();

// CNativeWorkerContext_GetNamespace returns the current namespace.
//
// Returns:
//   C string containing namespace, or NULL on failure.
//   The returned string is statically allocated and should NOT be freed by caller.
const char *CNativeWorkerContext_GetNamespace();

// CNativeWorkerContext_GetCurrentNodeID returns the current node ID.
//
// Returns:
//   CByteArray* containing node ID binary data, or NULL on failure.
//   Caller is responsible for freeing the returned CByteArray and its data.
CByteArray *CNativeWorkerContext_GetCurrentNodeID();

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // RAY_CORE_WORKER_LIB_GO_NATIVE_WORKER_CONTEXT_H
