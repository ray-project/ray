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

/**
 * @file cgo_stubs.cc
 * @brief Stub implementations of CGO functions for building without Go runtime
 *
 * This file provides empty implementations of Go-exported functions to allow
 * C++ code to compile and link when the Go runtime is not yet available.
 *
 * These stubs should ONLY be used for:
 * 1. Initial C++ development before Go runtime is ready
 * 2. Testing C++ components in isolation
 *
 * DO NOT use these stubs in production builds - they will cause runtime failures.
 */

#include "cgo_wrapper.h"
#include "native_task_executor.h"

#include "ray/util/logging.h"

// ============================================================================
// CGO Stub Implementations
// ============================================================================

/**
 * @brief Stub implementation of GoAllocateObject
 *
 * This is a placeholder that returns a minimal valid handle.
 * In the real implementation, this would allocate memory in Go heap.
 *
 * @param object_id_data - Object ID data
 * @param object_id_size - Object ID size in bytes
 * @param owner_ip_address - Owner IP address
 * @param owner_port - Owner port
 * @param owner_worker_id_data - Owner worker ID data
 * @param owner_worker_id_size - Owner worker ID size
 * @return Minimal valid handle (nullptr is acceptable for stub)
 */
extern "C" void* GoAllocateObject(
    const char* object_id_data,
    int object_id_size,
    const char* owner_ip_address,
    int owner_port,
    const char* owner_worker_id_data,
    int owner_worker_id_size) {
  RAY_LOG(DEBUG) << "GoAllocateObject called (stub - returning minimal handle)";
  // Return nullptr as minimal valid handle for stub
  return nullptr;
}

/**
 * @brief Stub implementation of GoReleaseObjectRef
 *
 * This is a placeholder that does nothing. In the real implementation,
 * this would release a Go object reference.
 *
 * @param handle - Opaque handle to release
 */
extern "C" void GoReleaseObjectRef(void* handle) {
  RAY_LOG(DEBUG) << "GoReleaseObjectRef called (stub - no-op)";
  // No-op: Go runtime not available
}

// ============================================================================
// Additional stubs can be added here as needed
// ============================================================================
