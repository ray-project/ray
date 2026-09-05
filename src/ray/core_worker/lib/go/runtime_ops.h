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

#ifndef RAY_CORE_WORKER_LIB_GO_RUNTIME_OPS_H_
#define RAY_CORE_WORKER_LIB_GO_RUNTIME_OPS_H_

#include <functional>
#include <memory>
#include <string>

#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/core_worker_process.h"
#include "ray/core_worker/lib/go/cgo_wrapper.h"
#include "ray/core_worker/lib/go/core_worker_provider.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray::go {

// Options for initializing the Ray runtime
struct RuntimeInitializeOptions {
  ray::rpc::WorkerType worker_type = ray::rpc::WorkerType::WORKER;
  std::string node_ip_address;
  std::string driver_name;
  std::string gcs_address;
  int node_manager_port = 0;
  std::string store_socket;
  std::string raylet_socket;
  std::string log_dir;
  std::string serialized_job_config;
  bool enable_logging = true;
  bool install_failure_signal_handler = true;
  bool interactive = false;
  int metrics_agent_port = -1;
  int64_t startup_token = 0;
  uint64_t runtime_env_hash = 0;
  std::string debug_source;
  ray::JobID job_id;
  std::string cluster_id;
  // Worker ID as hex string. Set for worker-mode processes so the worker
  // registers with the raylet under the worker ID the raylet assigned.
  std::string worker_id_hex;
};

// Business logic layer for runtime lifecycle management
// This class provides pure C++ implementation without CGO dependencies
class RuntimeOperations {
 public:
  // Get singleton instance
  static RuntimeOperations &GetInstance();

  // Initialize the Ray runtime with the given options
  void Initialize(const RuntimeInitializeOptions &options);

  // Shutdown the Ray runtime (safe to call even if not initialized)
  void Shutdown();

  // Get the worker mode (DRIVER or WORKER)
  ray::rpc::WorkerType GetWorkerMode() const;

  // Run the task execution loop (blocks until shutdown)
  void RunTaskExecutionLoop();

  // Allocate an object in Go heap memory
  // Returns nullptr if allocation fails
  std::shared_ptr<ray::RayObject> AllocateObject(const ray::RayObject &object,
                                                 const ray::ObjectID &object_id);

  // Allocate multiple objects in Go heap memory (batch operation)
  // Returns vector of allocated objects, with nullptr for failed allocations
  std::vector<std::shared_ptr<ray::RayObject>> AllocateObjects(
      const std::vector<std::pair<ray::RayObject, ray::ObjectID>> &objects);

  // Release an object reference
  void ReleaseObjectRef(const ray::ObjectID &object_id);

 private:
  RuntimeOperations() = default;
  ~RuntimeOperations() = default;

  // Delete copy constructor and assignment operator
  RuntimeOperations(const RuntimeOperations &) = delete;
  RuntimeOperations &operator=(const RuntimeOperations &) = delete;

  // Internal helper methods
  void InitializeCoreWorker(const RuntimeInitializeOptions &options);
  void ShutdownCoreWorker();
  void ShutdownRayLogging();

 public:
  // InitializeRayLogging is public to allow early logging initialization
  // before parameter validation in CNativeRuntime_Initialize
  void InitializeRayLogging(const RuntimeInitializeOptions &options);

  // Member variables
  std::shared_ptr<ray::gcs::GcsClient> gcs_client_;
  ray::core::CoreWorker *core_worker_ = nullptr;  // Owned by CoreWorkerProcess singleton
  bool runtime_initialized_ = false;
};

}  // namespace ray::go

#endif  // RAY_CORE_WORKER_LIB_GO_RUNTIME_OPS_H_
