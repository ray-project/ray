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

// CGO wrapper implementation for WorkerContext functions.
// This file contains all WorkerContext-related CGO functions, using the
// new architecture with separation of concerns:
// - CGO boundary (this file): C/C++ type conversion, error handling
// - Business logic (worker_context_ops.h): Pure C++ operations
// - CoreWorker access (core_worker_provider.h): Dependency injection

#include "native_worker_context.h"

#include <mutex>
#include <thread>

#include "cgo_wrapper.h"
#include "worker_context_ops.h"

using namespace ray::go;

// ============================================================================
// Thread-local and cached data
// ============================================================================

namespace {

// Thread-local Operations instance - avoids repeated creation
WorkerContextOperations &GetContextOps() {
  static thread_local WorkerContextOperations ops;
  return ops;
}

// Cache for immutable IDs (Worker ID and Job ID never change during worker lifetime)
// Using RAII wrappers for automatic memory management
thread_local CByteArrayPtr cached_worker_id;
thread_local CByteArrayPtr cached_job_id;
std::mutex cache_mutex;  // Protects cache initialization

}  // anonymous namespace

// ============================================================================
// CGO Export Functions - Minimal implementation using new architecture
// ============================================================================

extern "C" CByteArray *CNativeWorkerContext_GetCurrentWorkerId() {
  return CgoErrorHandler::Execute(
      "CNativeWorkerContext_GetCurrentWorkerId", []() -> CByteArray * {
        // Check thread-local cache first
        if (cached_worker_id != nullptr) {
          // Return a new copy for the caller to free
          return CgoTypeConverter::ToCByteArray(
              reinterpret_cast<const uint8_t *>(cached_worker_id->data),
              cached_worker_id->size);
        }

        // Cache miss - get Worker ID
        auto worker_id = GetContextOps().GetWorkerId();

        // Populate thread-local cache
        cached_worker_id = CByteArrayPtr(CgoTypeConverter::IdToCByteArray(worker_id));

        // Return a new copy for the caller to free
        return CgoTypeConverter::ToCByteArray(
            reinterpret_cast<const uint8_t *>(cached_worker_id->data),
            cached_worker_id->size);
      });
}

extern "C" CByteArray *CNativeWorkerContext_GetCurrentJobID() {
  return CgoErrorHandler::Execute(
      "CNativeWorkerContext_GetCurrentJobID", []() -> CByteArray * {
        // Check thread-local cache first
        if (cached_job_id != nullptr) {
          return CgoTypeConverter::ToCByteArray(
              reinterpret_cast<const uint8_t *>(cached_job_id->data),
              cached_job_id->size);
        }

        // Cache miss - get Job ID
        auto job_id = GetContextOps().GetJobId();

        // Populate thread-local cache
        cached_job_id = CByteArrayPtr(CgoTypeConverter::IdToCByteArray(job_id));

        // Return a new copy for the caller to free
        return CgoTypeConverter::ToCByteArray(
            reinterpret_cast<const uint8_t *>(cached_job_id->data), cached_job_id->size);
      });
}

extern "C" CByteArray *CNativeWorkerContext_GetCurrentActorID() {
  return CgoErrorHandler::Execute("CNativeWorkerContext_GetCurrentActorID",
                                  []() -> CByteArray * {
                                    auto actor_id = GetContextOps().GetCurrentActorId();
                                    return CgoTypeConverter::IdToCByteArray(actor_id);
                                  });
}

extern "C" int CNativeWorkerContext_GetCurrentTaskType() {
  return CgoErrorHandler::ExecuteInt(
      "CNativeWorkerContext_GetCurrentTaskType",
      []() -> int { return static_cast<int>(GetContextOps().GetCurrentTaskType()); });
}

extern "C" bool CNativeWorkerContext_IsCurrentTaskSet() {
  return CgoErrorHandler::Execute("CNativeWorkerContext_IsCurrentTaskSet", []() -> bool {
    return GetContextOps().IsCurrentTaskSet();
  });
}

extern "C" CByteArray *CNativeWorkerContext_GetCurrentTaskID() {
  return CgoErrorHandler::Execute("CNativeWorkerContext_GetCurrentTaskID",
                                  []() -> CByteArray * {
                                    auto task_id = GetContextOps().GetCurrentTaskId();
                                    return CgoTypeConverter::IdToCByteArray(task_id);
                                  });
}

extern "C" CByteArray *CNativeWorkerContext_GetRpcAddress() {
  return CgoErrorHandler::Execute(
      "CNativeWorkerContext_GetRpcAddress", []() -> CByteArray * {
        std::string address_bytes = GetContextOps().GetRpcAddress();
        return CgoTypeConverter::ToCByteArray(
            reinterpret_cast<const uint8_t *>(address_bytes.data()),
            static_cast<int>(address_bytes.size()));
      });
}

extern "C" const char *CNativeWorkerContext_GetSerializedRuntimeEnv() {
  // Use thread-local storage to cache the result
  static thread_local std::string cached_runtime_env;

  try {
    cached_runtime_env = GetContextOps().GetSerializedRuntimeEnv();
    return cached_runtime_env.c_str();
  } catch (const std::exception &e) {
    RAY_LOG(ERROR) << "CNativeWorkerContext_GetSerializedRuntimeEnv failed: " << e.what();
    return nullptr;
  }
}

extern "C" const char *CNativeWorkerContext_GetNamespace() {
  // Use thread-local storage to cache the result
  static thread_local std::string cached_namespace;

  try {
    cached_namespace = GetContextOps().GetNamespace();
    return cached_namespace.c_str();
  } catch (const std::exception &e) {
    RAY_LOG(ERROR) << "CNativeWorkerContext_GetNamespace failed: " << e.what();
    return nullptr;
  }
}

extern "C" bool CNativeWorkerContext_HasLastError() {
  // This function is kept for backward compatibility
  // In the new architecture, errors are logged immediately
  return false;
}

extern "C" CByteArray *CNativeWorkerContext_GetCurrentNodeID() {
  return CgoErrorHandler::Execute("CNativeWorkerContext_GetCurrentNodeID",
                                  []() -> CByteArray * {
                                    auto node_id = GetContextOps().GetCurrentNodeId();
                                    return CgoTypeConverter::IdToCByteArray(node_id);
                                  });
}
