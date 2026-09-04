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

#ifndef RAY_CORE_WORKER_LIB_GO_RUNTIME_CALLBACKS_H
#define RAY_CORE_WORKER_LIB_GO_RUNTIME_CALLBACKS_H

#include "cgo_wrapper.h"
#include "native_task_executor.h"
#include "ray/core_worker/core_worker_process.h"
#include "ray/core_worker/core_worker.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "src/ray/protobuf/gcs.pb.h"

#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

namespace ray::go {

/**
 * @brief Creates a GC collect callback function for the core worker.
 *
 * This callback is invoked periodically by the core worker to trigger
 * garbage collection in the Go runtime. It implements rate limiting
 * to ensure GC is not triggered more than once per second.
 *
 * @return std::function<void()> A function that triggers GC in Go runtime
 */
std::function<void()> CreateGCCollectCallback();

/**
 * @brief Creates a task execution callback function for the core worker.
 *
 * This callback is invoked by the core worker when a task needs to be
 * executed. It converts C++ task parameters to CGO-compatible format
 * and calls GoExecuteTask to execute the task in the Go runtime.
 *
 * The callback handles:
 * - Function descriptor conversion (Cpp/Python/Java types)
 * - Argument serialization and CGO boundary crossing
 * - Result deserialization and object allocation
 * - Error handling and status reporting
 *
 * @return std::function<ray::Status(...)> A function that executes tasks in Go runtime
 */
std::function<ray::Status(
    const ray::rpc::Address &caller_address,
    ray::rpc::TaskType task_type,
    const std::string task_name,
    const ray::core::RayFunction &ray_function,
    const std::unordered_map<std::string, double> &required_resources,
    const std::vector<std::shared_ptr<ray::RayObject>> &args,
    const std::vector<ray::rpc::ObjectReference> &arg_refs,
    const std::string &debugger_breakpoint,
    const std::string &serialized_retry_exception_allowlist,
    std::vector<std::pair<ray::ObjectID, std::shared_ptr<ray::RayObject>>> *returns,
    std::vector<std::pair<ray::ObjectID, std::shared_ptr<ray::RayObject>>> *dynamic_returns,
    std::vector<std::pair<ray::ObjectID, bool>> *streaming_generator_returns,
    std::shared_ptr<ray::LocalMemoryBuffer> &creation_task_exception_pb_bytes,
    bool *is_retryable_error,
    std::string *actor_repr_name,
    std::string *application_error,
    const std::vector<ray::ConcurrencyGroup> &defined_concurrency_groups,
    const std::string name_of_concurrency_group_to_execute,
    bool is_reattempt,
    bool is_streaming_generator,
    bool retry_exception,
    int64_t generator_backpressure_num_objects,
    int64_t num_objects_per_yield,
    const std::optional<std::string> &tensor_transport)> CreateTaskExecutionCallback();

}  // namespace ray::go

#endif  // RAY_CORE_WORKER_LIB_GO_GO_CALLBACKS_H
