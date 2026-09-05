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

// CGO Boundary Layer for Ray Native Runtime.
// This file contains only C interface functions and type conversions.
// All business logic is delegated to RuntimeOperations.

#include "ray/core_worker/lib/go/native_runtime.h"

#include <cstdio>
#include <mutex>
#include <string>
#include <vector>

#include "absl/strings/escaping.h"
#include "ray/core_worker/lib/go/cgo_wrapper.h"
#include "ray/core_worker/lib/go/go_heap_buffer.h"
#include "ray/core_worker/lib/go/runtime_ops.h"
#include "ray/util/logging.h"
#include "ray/util/time.h"

namespace ray::go {

// ============================================================================
// CGO Boundary Layer - C Interface Functions
// ============================================================================

extern "C" CNativeRuntime *CNativeRuntime_Initialize(
    const CNativeRuntimeInitializeOptions *opts) {
  return ray::go::CgoErrorHandler::Execute(
      "CNativeRuntime_Initialize", [&]() -> CNativeRuntime * {
        if (opts == nullptr) {
          throw std::invalid_argument("opts is null");
        }
        RAY_LOG(INFO) << "[CNativeRuntime_Initialize] gcs_address="
                      << (opts->gcs_address != nullptr ? opts->gcs_address : "(null)")
                      << ", enable_logging=" << opts->enable_logging;
        // Convert C options to C++ options
        ray::go::RuntimeInitializeOptions options;
        options.worker_type = static_cast<ray::rpc::WorkerType>(opts->worker_mode);
        options.node_ip_address = CNativeCommon_ConvertToString(opts->node_ip_address);
        options.node_manager_port = opts->node_manager_port;
        options.driver_name = CNativeCommon_ConvertToString(opts->driver_name);
        options.store_socket = CNativeCommon_ConvertToString(opts->store_socket);
        options.raylet_socket = CNativeCommon_ConvertToString(opts->raylet_socket);
        options.job_id =
            ray::JobID::FromHex(CNativeCommon_ConvertToString(opts->job_id_hex));
        options.gcs_address = CNativeCommon_ConvertToString(opts->gcs_address);
        options.cluster_id = CNativeCommon_ConvertToString(opts->cluster_id_hex);
        options.log_dir = CNativeCommon_ConvertToString(opts->log_dir);
        options.worker_id_hex = CNativeCommon_ConvertToString(opts->worker_id_hex);

        // Go passes JobConfig as base64-encoded protobuf.
        // Decode it to raw protobuf bytes so node_manager.cc can parse it directly.
        {
          std::string base64_job_config =
              CNativeCommon_ConvertToString(opts->job_config_serialized);
          std::string decoded_job_config;
          if (!base64_job_config.empty() &&
              absl::Base64Unescape(base64_job_config, &decoded_job_config)) {
            options.serialized_job_config = decoded_job_config;
            RAY_LOG(INFO) << "JobConfig base64 decoded successfully, decoded size="
                          << decoded_job_config.size();
          } else if (!base64_job_config.empty()) {
            // Base64 decoding failed, pass as-is (might be raw protobuf from CPP/Java)
            options.serialized_job_config = base64_job_config;
            RAY_LOG(WARNING) << "JobConfig base64 decoding failed, passing as-is, size="
                             << base64_job_config.size();
          }
          // If base64_job_config is empty, options.serialized_job_config remains empty
        }

        options.startup_token = opts->startup_token;
        options.runtime_env_hash = opts->runtime_env_hash;
        options.enable_logging = opts->enable_logging;

        // Note: Logging has been initialized, so we can use RAY_LOG for structured
        // logging
        RAY_LOG(DEBUG)
            << "CNativeRuntime_Initialize: calling RuntimeOperations::Initialize, "
            << "worker_type=" << static_cast<int>(options.worker_type)
            << ", gcs_address=" << options.gcs_address
            << ", job_id=" << options.job_id.Hex()
            << ", cluster_id=" << options.cluster_id << ", serialized_job_config='"
            << options.serialized_job_config << "'";

        // Initialize runtime
        ray::go::RuntimeOperations::GetInstance().Initialize(options);

        RAY_LOG(INFO) << "CNativeRuntime_Initialize: SUCCESS";

        // Return opaque handle
        return reinterpret_cast<CNativeRuntime *>(1);
      });
}

extern "C" void CNativeRuntime_Shutdown() {
  ray::go::CgoErrorHandler::ExecuteVoid("CNativeRuntime_Shutdown", []() {
    ray::go::RuntimeOperations::GetInstance().Shutdown();
  });
}

extern "C" void CNativeRuntime_RunTaskExecutionLoop() {
  ray::go::CgoErrorHandler::ExecuteVoid("CNativeRuntime_RunTaskExecutionLoop", []() {
    ray::go::RuntimeOperations::GetInstance().RunTaskExecutionLoop();
    _Exit(0);
  });
}
}  // namespace ray::go
