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

#include "runtime_ops.h"

#include <atomic>
#include <cstdio>  // for sprintf, fflush
#include <memory>
#include <string>
#include <vector>

#include "go_heap_buffer.h"
#include "native_runtime.h"
#include "native_task_executor.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker_process.h"
#include "ray/util/logging.h"
#include "ray/util/path_utils.h"
#include "ray/util/time.h"
#include "runtime_callbacks.h"

namespace ray::go {

namespace {

std::atomic<bool> g_runtime_initialized{false};
std::atomic<bool> g_ray_log_initialized{false};
std::atomic<int> g_worker_mode{0};

inline ray::rpc::WorkerType ConvertToRpcWorkerType(int c_worker_type) {
  return static_cast<ray::rpc::WorkerType>(c_worker_type);
}

}  // namespace

// ============================================================================
// RuntimeOperations Class Implementation
// ============================================================================

RuntimeOperations &RuntimeOperations::GetInstance() {
  static RuntimeOperations instance;
  return instance;
}

void RuntimeOperations::Initialize(const RuntimeInitializeOptions &options) {
  if (options.gcs_address.empty()) {
    RAY_LOG(ERROR) << "RuntimeOperations::Initialize: gcs_address is required";
    return;
  }

  ray::JobID job_id = options.job_id;

  std::string gcs_server_address = options.gcs_address;
  ray::ClusterID cluster_id;
  std::string cluster_id_str = options.cluster_id;
  if (!cluster_id_str.empty()) {
    if (cluster_id_str.length() != ray::ClusterID::Size() * 2) {
      RAY_LOG(ERROR) << "Invalid ClusterID hex string length";
      return;
    }
    try {
      cluster_id = ray::ClusterID::FromHex(cluster_id_str);
    } catch (const std::exception &e) {
      RAY_LOG(ERROR) << "Failed to parse ClusterID: " << e.what();
      return;
    }
  }

  // For Worker mode: use the cluster ID passed from command line (should be non-nil)
  //   Don't fetch because we need to use the passed cluster ID to register with GCS
  // For Driver mode: fetch cluster ID from GCS if not provided
  //   This matches Java's behavior for drivers
  const bool is_driver = (options.worker_type == ray::rpc::WorkerType::DRIVER);

  // For Worker mode: use the passed cluster_id, don't fetch
  // For Driver mode: fetch cluster ID from GCS if not provided
  const bool allow_cluster_id_nil = true;
  const bool fetch_cluster_id_if_nil = is_driver && cluster_id_str.empty();

  ray::gcs::GcsClientOptions gcs_options(
      gcs_server_address, cluster_id, allow_cluster_id_nil, fetch_cluster_id_if_nil);

  ray::core::CoreWorkerOptions core_options;
  core_options.worker_type = options.worker_type;
  core_options.language = ray::Language::GO;
  core_options.store_socket = options.store_socket;
  core_options.raylet_socket = options.raylet_socket;
  core_options.node_manager_port = options.node_manager_port;
  core_options.gcs_options = gcs_options;
  core_options.job_id = job_id;
  core_options.enable_logging = options.enable_logging;
  core_options.install_failure_signal_handler = options.install_failure_signal_handler;
  core_options.interactive = options.interactive;
  core_options.metrics_agent_port = options.metrics_agent_port;
  core_options.runtime_env_hash = options.runtime_env_hash;
  core_options.debug_source = options.debug_source;

  InitializeRayLogging(options);
  InitializeCoreWorker(options);

  g_worker_mode.store(static_cast<int>(options.worker_type), std::memory_order_relaxed);
  g_runtime_initialized.store(true, std::memory_order_relaxed);
}

void RuntimeOperations::Shutdown() {
  if (!g_runtime_initialized.load(std::memory_order_relaxed)) {
    RAY_LOG(WARNING) << "Shutdown called on uninitialized runtime";
    return;
  }

  ShutdownCoreWorker();
  ShutdownRayLogging();

  g_runtime_initialized.store(false, std::memory_order_relaxed);
  g_ray_log_initialized.store(false, std::memory_order_relaxed);
  g_worker_mode.store(0, std::memory_order_relaxed);

  RAY_LOG(INFO) << "[GO-SHUTDOWN] RuntimeOperations::Shutdown completed";
}

ray::rpc::WorkerType RuntimeOperations::GetWorkerMode() const {
  return static_cast<ray::rpc::WorkerType>(g_worker_mode.load(std::memory_order_relaxed));
}

void RuntimeOperations::RunTaskExecutionLoop() {
  if (!g_runtime_initialized.load(std::memory_order_relaxed)) {
    RAY_LOG(ERROR) << "RunTaskExecutionLoop called on uninitialized runtime";
    return;
  }

  if (core_worker_) {
    core_worker_->RunTaskExecutionLoop();
  }
}

std::shared_ptr<ray::RayObject> RuntimeOperations::AllocateObject(
    const ray::RayObject &object, const ray::ObjectID &object_id) {
  // Case 1: Object only has metadata, no data
  if (!object.HasData()) {
    RAY_LOG(DEBUG) << "AllocateObject: metadata-only object, "
                   << "object_id=" << object_id.Hex();
    // Return a new RayObject with the same metadata but no data
    return std::make_shared<ray::RayObject>(object.GetMetadata(),
                                            /*data=*/nullptr,
                                            object.GetNestedRefs());
  }

  // Case 2: Object has actual data, allocate in Go heap
  const auto &data_buffer = object.GetData();
  const auto &metadata_buffer = object.GetMetadata();

  const char *data_ptr = nullptr;
  int data_size = 0;
  if (data_buffer) {
    data_ptr = reinterpret_cast<const char *>(
        reinterpret_cast<const uint8_t *>(data_buffer->Data()));
    data_size = static_cast<int>(data_buffer->Size());
  }

  const char *metadata_ptr = nullptr;
  int metadata_size = 0;
  if (metadata_buffer) {
    metadata_ptr = reinterpret_cast<const char *>(
        reinterpret_cast<const uint8_t *>(metadata_buffer->Data()));
    metadata_size = static_cast<int>(metadata_buffer->Size());
  }

  // Allocate in Go heap via CGO
  auto go_buffer = AllocateGoHeapBuffer(reinterpret_cast<const char *>(object_id.Data()),
                                        static_cast<int>(object_id.Size()),
                                        data_ptr,
                                        data_size,
                                        metadata_ptr,
                                        metadata_size);

  if (!go_buffer) {
    RAY_LOG(ERROR) << "Failed to allocate Go heap buffer for object " << object_id.Hex();
    return nullptr;
  }

  // Return RayObject that owns the Go heap buffer
  return std::make_shared<ray::RayObject>(
      std::move(go_buffer), metadata_buffer, object.GetNestedRefs());
}

std::vector<std::shared_ptr<ray::RayObject>> RuntimeOperations::AllocateObjects(
    const std::vector<std::pair<ray::RayObject, ray::ObjectID>> &objects) {
  std::vector<std::shared_ptr<ray::RayObject>> results;
  results.reserve(objects.size());

  // Batch allocation: process all objects in a single pass
  // This reduces function call overhead and CGO boundary crossings
  for (const auto &[object, object_id] : objects) {
    results.push_back(AllocateObject(object, object_id));
  }

  return results;
}

void RuntimeOperations::ReleaseObjectRef(const ray::ObjectID &object_id) {
  // Object references are managed by Go runtime through GoHeapBuffer's RAII semantics.
  // When a GoHeapBuffer is destroyed, it automatically calls GoReleaseObjectRef()
  // to decrement the Go-side reference count (ObjectRefImpl::pinCount).
  //
  // This method exists for API compatibility with other Ray language runtimes,
  // but in the Go implementation, explicit release is not needed because:
  // 1. GoHeapBuffer owns the Go object reference and releases it in destructor
  // 2. Go's GC handles memory reclamation when pinCount reaches 0
  // 3. The ObjectID parameter alone is insufficient to locate the Go object
  //    (we would need the GoObjectRefHandle* which is owned by GoHeapBuffer)
  //
  // If you encounter memory leaks, check that GoHeapBuffer instances are being
  // properly destroyed rather than calling this method.
  RAY_LOG(DEBUG) << "ReleaseObjectRef called for " << object_id.Hex()
                 << " (no-op: references managed by GoHeapBuffer RAII)";
}

// ============================================================================
// Private Helper Methods
// ============================================================================

void RuntimeOperations::InitializeRayLogging(const RuntimeInitializeOptions &options) {
  if (!options.enable_logging) {
    return;
  }

  std::string log_dir = options.log_dir;
  if (log_dir.empty()) {
    log_dir = "/tmp/ray";
  }

  std::string app_name = "ray_go";
  ray::RayLogLevel ray_log_level = ray::RayLogLevel::INFO;

  // Fix: Convert log directory to log file path using GetLogFilepathFromDirectory.
  // This matches the behavior in core_worker_process.cc, which correctly handles
  // log_dir as a directory path rather than a file path.
  const std::string log_filepath = ray::GetLogFilepathFromDirectory(log_dir, app_name);
  ray::RayLog::StartRayLog(app_name, ray_log_level, log_filepath);
  g_ray_log_initialized.store(true, std::memory_order_relaxed);
}

void RuntimeOperations::ShutdownRayLogging() {
  if (g_ray_log_initialized.load(std::memory_order_relaxed)) {
    ray::RayLog::ShutDownRayLog();
    g_ray_log_initialized.store(false, std::memory_order_relaxed);
  }
}

void RuntimeOperations::InitializeCoreWorker(const RuntimeInitializeOptions &options) {
  RAY_CHECK(core_worker_ == nullptr) << "CoreWorker already initialized";

  // For Go Worker, we need to synchronously verify GCS connection by fetching cluster ID.
  // This ensures the worker can connect to GCS before starting task execution loop.
  // Setting fetch_cluster_id_if_nil=true makes Connect() call SyncGetClusterId()
  // to verify GCS is reachable, failing fast if connection cannot be established.
  ray::gcs::GcsClientOptions gcs_options(options.gcs_address,
                                         ray::ClusterID::Nil(),
                                         /*allow_cluster_id_nil=*/true,
                                         /*fetch_cluster_id_if_nil=*/true);

  ray::core::CoreWorkerOptions core_options;
  core_options.worker_type = options.worker_type;
  core_options.language = ray::Language::GO;
  core_options.store_socket = options.store_socket;
  core_options.raylet_socket = options.raylet_socket;
  core_options.node_manager_port = options.node_manager_port;
  core_options.node_ip_address = options.node_ip_address;
  core_options.log_dir = options.log_dir;
  core_options.gcs_options = gcs_options;
  core_options.job_id = options.job_id;
  core_options.enable_logging = options.enable_logging;
  core_options.install_failure_signal_handler = options.install_failure_signal_handler;
  core_options.interactive = options.interactive;
  core_options.metrics_agent_port = options.metrics_agent_port;
  core_options.runtime_env_hash = options.runtime_env_hash;
  core_options.debug_source = options.debug_source;
  // In worker mode, register with the raylet under the worker ID it assigned
  // (passed by the raylet via --worker-id). For drivers the ID is derived from
  // the job ID in CoreWorkerProcessImpl, so leave it unset here.
  if (!options.worker_id_hex.empty()) {
    core_options.worker_id = ray::WorkerID::FromHex(options.worker_id_hex);
  }
  // Pass serialized JobConfig to core_worker
  // This will be used when registering with raylet and starting workers
  core_options.serialized_job_config = options.serialized_job_config;
  core_options.gc_collect = CreateGCCollectCallback();
  core_options.task_execution_callback = CreateTaskExecutionCallback();
  // Register object allocator callback to enable Go heap allocation for objects
  // This allows Go GC to manage object lifecycles, similar to Java runtime
  core_options.object_allocator = [](const ray::RayObject &object,
                                     const ray::ObjectID &object_id) {
    return RuntimeOperations::GetInstance().AllocateObject(object, object_id);
  };

  ray::core::CoreWorkerProcess::Initialize(core_options);
  core_worker_ = &ray::core::CoreWorkerProcess::GetCoreWorker();

  RAY_CHECK(core_worker_ != nullptr) << "Failed to initialize CoreWorker";
}

void RuntimeOperations::ShutdownCoreWorker() {
  if (core_worker_) {
    // For driver mode, we should call Disconnect() before Shutdown() to match Java
    // behavior. This ensures the raylet is properly notified before the worker shuts
    // down.
    if (g_worker_mode.load(std::memory_order_relaxed) ==
        static_cast<int>(ray::rpc::WorkerType::DRIVER)) {
      core_worker_->Disconnect(ray::rpc::WorkerExitType::INTENDED_USER_EXIT,
                               "Shutdown by ray.shutdown()");
    }

    core_worker_->Shutdown();
    core_worker_ = nullptr;
  }

  ray::core::CoreWorkerProcess::Shutdown();

  RAY_LOG(INFO) << "ShutdownCoreWorker completed";
}

}  // namespace ray::go
