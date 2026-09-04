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

// CGO wrapper header for Ray Native Runtime C++ API.
// This file provides C wrappers for C++ CoreWorker to enable CGO binding.
// This is a port of the Java nativeInitialize and nativeShutdown functions.
// Naming convention: Consistent with Java runtime (io_ray_runtime_RayNativeRuntime.cc).

#ifndef RAY_CORE_WORKER_LIB_GO_NATIVE_RUNTIME_H
#define RAY_CORE_WORKER_LIB_GO_NATIVE_RUNTIME_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Forward declarations - opaque pointers for C++ types
// Opaque pointer for C++ CoreWorker implementation
// Using abstract name to hide C++ implementation details
typedef struct CNativeRuntimeImpl CNativeRuntime;

// Worker type enumeration
// Note: Values must match ray::rpc::WorkerType enum in protobuf (common.proto)
// to ensure correct type conversion at the C++ boundary.
typedef enum {
  NATIVE_RUNTIME_TYPE_WORKER = 0,  // Must match ray::rpc::WorkerType::WORKER
  NATIVE_RUNTIME_TYPE_DRIVER = 1,  // Must match ray::rpc::WorkerType::DRIVER
} CNativeRuntimeType;

// GoObjectRefHandle is defined in go_heap_buffer.h
// Forward declaration for C compatibility when that header is not included
struct GoObjectRefHandle;

// ============================================================================
// NativeRuntime Lifecycle
// ============================================================================

// CNativeRuntimeInitializeOptions contains all initialization options for CNativeRuntime.
// This struct is used to reduce the number of parameters to CNativeRuntime_Initialize.
typedef struct {
  int worker_mode;                    // Worker type (NATIVE_RUNTIME_TYPE_DRIVER or NATIVE_RUNTIME_TYPE_WORKER)
  const char* node_ip_address;        // IP address of the Ray node
  int node_manager_port;              // Port of the raylet node manager
  const char* driver_name;            // Name of the driver
  const char* store_socket;           // Socket path for the object store
  const char* raylet_socket;          // Socket path for the raylet
  const char* job_id_hex;             // Job ID as hex string
  const char* gcs_address;            // GCS server address (format: "host:port")
  const char* cluster_id_hex;         // Cluster ID as hex string (optional, can be empty)
  const char* log_dir;                // Directory for log files
  const char* job_config_serialized;  // Serialized job configuration
  const char* worker_id_hex;          // Worker ID as hex string (worker mode; empty for driver)
  int startup_token;                  // Startup token for this worker
  int runtime_env_hash;               // Hash of the runtime environment
  bool enable_logging;                // Initialize logging if true
} CNativeRuntimeInitializeOptions;

// Initialize the Ray runtime.
// This is the Go equivalent of Java's nativeInitialize function.
//
// Parameters:
//   opts - Pointer to CNativeRuntimeInitializeOptions struct
//
// Returns:
//   Opaque handle (non-NULL on success, NULL on failure).
//   Note: This is NOT a real pointer to a C++ object - it's an opaque handle
//   used only to indicate success (non-NULL) or failure (NULL).
//   The caller should NOT dereference this handle.
//   Error details are logged and can be retrieved from log files.
CNativeRuntime* CNativeRuntime_Initialize(const CNativeRuntimeInitializeOptions* opts);

// Shutdown the Ray runtime.
// This is the Go equivalent of Java's nativeShutdown function.
void CNativeRuntime_Shutdown();

// Run the task execution loop.
// This function blocks and processes tasks from the raylet.
void CNativeRuntime_RunTaskExecutionLoop();

// ============================================================================
// Go Object Allocator CGO Callbacks
// These functions are implemented in Go and called from C++ via CGO.
// ============================================================================

// GoAllocateObject allocates object memory in Go heap.
// Parameters:
//   object_id_data - Binary data of ObjectID
//   object_id_size - Size of ObjectID binary data
//   data - Object data pointer
//   data_size - Object data size
//   metadata - Object metadata pointer
//   metadata_size - Object metadata size
//
// Returns:
//   Opaque handle to Go object reference (GoObjectRefHandle*)
//   NULL on failure
void* GoAllocateObject(char* object_id_data, int object_id_size,
                       char* data, int data_size,
                       char* metadata, int metadata_size);

// GoReleaseObjectRef releases a Go object reference.
// Parameters:
//   handle - Opaque handle returned by GoAllocateObject
void GoReleaseObjectRef(void* handle);

// GoGetObjectData returns the data pointer of a Go object.
// Parameters:
//   handle - Opaque handle returned by GoAllocateObject
//
// Returns:
//   Pointer to object data, or NULL if object has no data
void* GoGetObjectData(void* handle);

size_t GoGetObjectSize(void* handle);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // RAY_CORE_WORKER_LIB_GO_NATIVE_RUNTIME_H
