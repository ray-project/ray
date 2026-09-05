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

#include "task_submitter_ops.h"

#include <sstream>

#include "ray/core_worker/core_worker.h"
#include "ray/util/logging.h"

namespace {

// Helper function to convert hex string to binary
// NOTE: This is intentionally a local implementation rather than reusing
// crypto_ext.cc::decodeHexString for the following reasons:
// 1. Different return type: We need std::string for PlacementGroupID::FromBinary(), not
// std::vector<unsigned char>
// 2. Different exception type: std::invalid_argument is more semantically correct for
// invalid input
// 3. Dependency isolation: Avoids coupling core_worker to crypto module
// 4. Testability: Allows independent testing of Go binding hex conversion logic
// The core algorithm is the same as decodeHexString and hex_to_uchar in ray/common/id.h.
std::string HexStringToBinary(const std::string &hex_str) {
  if (hex_str.length() % 2 != 0) {
    throw std::invalid_argument("Invalid hex string length: " +
                                std::to_string(hex_str.length()));
  }
  std::string binary;
  binary.reserve(hex_str.length() / 2);
  for (size_t i = 0; i < hex_str.length(); i += 2) {
    binary.push_back(static_cast<char>(std::stoi(hex_str.substr(i, 2), nullptr, 16)));
  }
  return binary;
}

}  // anonymous namespace

namespace ray {
namespace go {

// ============================================================================
// TaskSubmitterOperations Implementation
// ============================================================================

// Static provider - defaults to DefaultCoreWorkerProvider
static std::shared_ptr<ICoreWorkerProvider> g_core_worker_provider =
    std::make_shared<DefaultCoreWorkerProvider>();

TaskSubmitterOperations &TaskSubmitterOperations::GetInstance() {
  static TaskSubmitterOperations instance;
  return instance;
}

void TaskSubmitterOperations::SetCoreWorkerProvider(
    std::shared_ptr<ICoreWorkerProvider> provider) {
  g_core_worker_provider = provider;
}

ICoreWorkerProvider &TaskSubmitterOperations::GetCoreWorkerProvider() {
  return *g_core_worker_provider;
}

std::vector<ray::rpc::ObjectReference> TaskSubmitterOperations::SubmitTask(
    const std::vector<std::string> &function_descriptor,
    const std::vector<std::unique_ptr<TaskArgument>> &args,
    const TaskSubmitOptions &options) {
  auto &core_worker = GetCoreWorker();

  // Build RayFunction using shared helper method
  ray::core::RayFunction ray_function = BuildRayFunction(function_descriptor);

  // Convert task arguments using shared helper method
  auto task_args = ConvertTaskArgs(args);

  // Build task options
  ray::core::TaskOptions task_options = BuildTaskOptions(options);

  // Build scheduling strategy
  ray::rpc::SchedulingStrategy scheduling_strategy =
      BuildSchedulingStrategy(options.placement_group_id_hex, options.bundle_index);

  // Submit task
  std::vector<ray::rpc::ObjectReference> return_refs =
      core_worker.SubmitTask(ray_function,
                             task_args,
                             task_options,
                             options.max_retries,
                             /*retry_exceptions=*/false,
                             scheduling_strategy,
                             /*debugger_breakpoint=*/"",
                             /*serialized_retry_exception_allowlist=*/"",
                             /*call_site=*/"",
                             ray::TaskID::Nil());

  return return_refs;
}

ray::ActorID TaskSubmitterOperations::CreateActor(
    const std::vector<std::string> &function_descriptor,
    const std::vector<std::unique_ptr<TaskArgument>> &args,
    const ActorCreateOptions &options) {
  auto &core_worker = GetCoreWorker();

  // Build RayFunction using shared helper method
  ray::core::RayFunction ray_function = BuildRayFunction(function_descriptor);

  // Convert task arguments using shared helper method
  auto task_args = ConvertTaskArgs(args);

  // Build actor creation options
  ray::core::ActorCreationOptions actor_options = BuildActorOptions(options);

  // Create actor
  ray::ActorID actor_id;
  RAY_CHECK_OK(core_worker.CreateActor(ray_function,
                                       task_args,
                                       actor_options,
                                       /*extension_data=*/"",
                                       /*call_site=*/"",
                                       &actor_id));

  return actor_id;
}

std::vector<ray::rpc::ObjectReference> TaskSubmitterOperations::SubmitActorTask(
    const ray::ActorID &actor_id,
    const std::vector<std::string> &function_descriptor,
    const std::vector<std::unique_ptr<TaskArgument>> &args,
    const TaskSubmitOptions &options) {
  auto &core_worker = GetCoreWorker();

  // Build RayFunction using shared helper method
  ray::core::RayFunction ray_function = BuildRayFunction(function_descriptor);

  // Convert task arguments using shared helper method
  auto task_args = ConvertTaskArgs(args);

  // Build task options
  ray::core::TaskOptions task_options = BuildTaskOptions(options);

  // Submit actor task
  std::vector<ray::rpc::ObjectReference> return_refs;
  RAY_CHECK_OK(core_worker.SubmitActorTask(actor_id,
                                           ray_function,
                                           task_args,
                                           task_options,
                                           options.max_retries,
                                           /*retry_exceptions=*/false,
                                           /*serialized_retry_exception_allowlist=*/"",
                                           /*call_site=*/"",
                                           return_refs));

  return return_refs;
}

std::unordered_map<std::string, double> TaskSubmitterOperations::ParseResources(
    const std::string &resources_str) {
  std::unordered_map<std::string, double> resources;

  if (resources_str.empty()) {
    return resources;
  }

  std::stringstream ss(resources_str);
  std::string item;

  while (std::getline(ss, item, ',')) {
    size_t pos = item.find(':');
    if (pos != std::string::npos) {
      std::string name = item.substr(0, pos);
      double quantity = std::stod(item.substr(pos + 1));
      resources[name] = quantity;
    }
  }

  return resources;
}

std::string TaskSubmitterOperations::HexToBinary(const std::string &hex_str) {
  // Delegate to the local helper function
  // See HexStringToBinary above for why this is a local implementation
  return HexStringToBinary(hex_str);
}

ray::core::RayFunction TaskSubmitterOperations::BuildRayFunction(
    const std::vector<std::string> &descriptor) const {
  ray::FunctionDescriptor func_descriptor =
      ray::FunctionDescriptorBuilder::FromVector(ray::Language::GO, descriptor);
  return ray::core::RayFunction(ray::Language::GO, func_descriptor);
}

std::vector<std::unique_ptr<ray::TaskArg>> TaskSubmitterOperations::ConvertTaskArgs(
    const std::vector<std::unique_ptr<TaskArgument>> &args) const {
  std::vector<std::unique_ptr<ray::TaskArg>> task_args;
  task_args.reserve(args.size());  // Pre-allocate to avoid reallocations
  for (const auto &arg : args) {
    task_args.push_back(arg->ToRayTaskArg());
  }
  return task_args;
}

ray::core::TaskOptions TaskSubmitterOperations::BuildTaskOptions(
    const TaskSubmitOptions &options) const {
  ray::core::TaskOptions task_options;
  task_options.resources = options.resources;
  task_options.num_returns = options.num_returns;
  // Note: Explicitly set generator_backpressure_num_objects to -1 to indicate
  // that backpressure is not enabled. This matches Java's behavior in
  // io_ray_runtime_task_NativeTaskSubmitter.cc:164 where it hardcodes -1.
  // If this field is left uninitialized (0 or garbage value) and the task is
  // a streaming generator, it would trigger an assertion failure in
  // TaskSpecification::GeneratorBackpressureNumObjects() (RAY_CHECK_NE(result, 0)
  // in task_spec.cc:248).
  task_options.generator_backpressure_num_objects = -1;

  if (!options.serialized_runtime_env_info.empty()) {
    task_options.serialized_runtime_env_info = options.serialized_runtime_env_info;
  }

  return task_options;
}

ray::core::ActorCreationOptions TaskSubmitterOperations::BuildActorOptions(
    const ActorCreateOptions &options) const {
  // Make a copy of namespace_ because ActorCreationOptions constructor
  // expects a non-const reference (unusual API design)
  std::string namespace_copy = options.namespace_;
  // A valid scheduling strategy is mandatory: CoreWorker::CreateActor CHECK-fails
  // when actor_creation_options.scheduling_strategy is NOT_SET. Actor creation
  // has no placement-group fields, so the default scheduling strategy is used.
  ray::rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_default_scheduling_strategy();
  return ray::core::ActorCreationOptions(options.max_restarts,
                                         options.max_task_retries,
                                         1,  // initial_restarts
                                         options.resources,
                                         options.resources,
                                         {},  // required_resources
                                         std::nullopt,
                                         options.name,
                                         namespace_copy,
                                         false,  // is_detached
                                         scheduling_strategy,
                                         options.serialized_runtime_env_info,
                                         {},     // worker_capture_output
                                         false,  // is_global_publisher
                                         -1,     // max_concurrency
                                         false,  // is_asyncio_actor
                                         false,  // enable_task_events
                                         {},     // serialized_dag
                                         {},     // serialized_actor_data
                                         {}      // extension_data
  );
}

ray::rpc::SchedulingStrategy TaskSubmitterOperations::BuildSchedulingStrategy(
    const std::string &pg_id_hex, int bundle_index) const {
  ray::rpc::SchedulingStrategy scheduling_strategy;

  if (!pg_id_hex.empty()) {
    std::string pg_id_binary = HexToBinary(pg_id_hex);
    ray::PlacementGroupID pg_id = ray::PlacementGroupID::FromBinary(pg_id_binary);

    scheduling_strategy.mutable_placement_group_scheduling_strategy()
        ->set_placement_group_id(pg_id.Binary());
    scheduling_strategy.mutable_placement_group_scheduling_strategy()
        ->set_placement_group_bundle_index(bundle_index);
  } else {
    // No placement group specified - use default scheduling strategy.
    // This matches Java's behavior in io_ray_runtime_task_NativeTaskSubmitter.cc:383
    // where mutable_default_scheduling_strategy() is always called.
    scheduling_strategy.mutable_default_scheduling_strategy();
  }

  return scheduling_strategy;
}

}  // namespace go
}  // namespace ray
