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

#include "ray/core_worker/lib/go/task_submitter_ops.h"

#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ray/core_worker/core_worker.h"
#include "ray/util/logging.h"

namespace {

// A GoFunctionDescriptor is always 4 elements: [module, package, actorType,
// method]. Both convertActorCreationOptionsToC (Go side) and
// MapConcurrencyGroupsFromC (here) must agree on this fixed layout.
constexpr int kGoFunctionDescriptorElementCount = 4;

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
    ray::Language language,
    const std::vector<std::string> &function_descriptor,
    const std::vector<std::unique_ptr<TaskArgument>> &args,
    const TaskSubmitOptions &options) {
  auto &core_worker = GetCoreWorker();

  // Build RayFunction using shared helper method
  ray::core::RayFunction ray_function = BuildRayFunction(language, function_descriptor);

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
    ray::Language language,
    const std::vector<std::string> &function_descriptor,
    const std::vector<std::unique_ptr<TaskArgument>> &args,
    const ActorCreateOptions &options) {
  auto &core_worker = GetCoreWorker();

  // Build RayFunction using shared helper method
  ray::core::RayFunction ray_function = BuildRayFunction(language, function_descriptor);

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
    ray::Language language,
    const std::vector<std::string> &function_descriptor,
    const std::vector<std::unique_ptr<TaskArgument>> &args,
    const TaskSubmitOptions &options) {
  auto &core_worker = GetCoreWorker();

  // Build RayFunction using shared helper method
  ray::core::RayFunction ray_function = BuildRayFunction(language, function_descriptor);

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

ray::Status TaskSubmitterOperations::KillActor(const ray::ActorID &actor_id,
                                               bool force_kill,
                                               bool no_restart) {
  return GetCoreWorker().KillActor(actor_id, force_kill, no_restart);
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

void TaskSubmitterOperations::EnsureDefaultCPU(
    std::unordered_map<std::string, double> &resources) {
  if (resources.empty()) {
    resources["CPU"] = 1.0;
  }
}

std::string TaskSubmitterOperations::HexToBinary(const std::string &hex_str) {
  // Delegate to the local helper function
  // See HexStringToBinary above for why this is a local implementation
  return HexStringToBinary(hex_str);
}

void TaskSubmitterOperations::MapConcurrencyGroupsFromC(
    const CActorCreationOptions *options, ActorCreateOptions *out) {
  if (options == nullptr || out == nullptr) {
    return;
  }
  // Map concurrency groups from the flat C arrays. Each group's function
  // descriptors are 4-element GoFunctionDescriptor string arrays (module,
  // package, actorType, method).
  if (options->cg_count > 0) {
    for (int i = 0; i < options->cg_count; i++) {
      ConcurrencyGroupDescriptor cg;
      cg.name = options->cg_names[i] ? options->cg_names[i] : "";
      // Clamp non-positive max_concurrency to 1 (matching the max_concurrency
      // normalization convention) so a negative value from Go never wraps into
      // a huge uint32_t.
      cg.max_concurrency = options->cg_max_concurrency[i] > 0
                               ? static_cast<uint32_t>(options->cg_max_concurrency[i])
                               : 1;
      const int fd_count = options->cg_fd_counts[i];
      if (options->cg_fds[i] != nullptr && fd_count > 0) {
        for (int j = 0; j < fd_count; j++) {
          // cg_fds[i][j] points to a 4-element (module, package, actorType,
          // method) GoFunctionDescriptor string array; the C struct type
          // (const char *) cannot express that it aliases a const char *[4],
          // so reinterpret the stored pointer as the const char * const * it
          // actually is (the layout is fixed by convertActorCreationOptionsToC,
          // which allocates exactly 4 C strings per method descriptor).
          const char *const *fd =
              reinterpret_cast<const char *const *>(options->cg_fds[i][j]);
          for (int k = 0; k < kGoFunctionDescriptorElementCount; k++) {
            cg.function_descriptors.emplace_back(fd[k] ? fd[k] : "");
          }
        }
      }
      out->concurrency_groups.push_back(std::move(cg));
    }
  }
}

ray::core::RayFunction TaskSubmitterOperations::BuildRayFunction(
    ray::Language language, const std::vector<std::string> &descriptor) const {
  ray::FunctionDescriptor func_descriptor =
      ray::FunctionDescriptorBuilder::FromVector(language, descriptor);
  return ray::core::RayFunction(language, func_descriptor);
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
  // Add default CPU=1 if no resources are specified, matching Python's behavior
  // for autoscaler compatibility (it triggers pending lease reporting).
  task_options.resources = options.resources;
  EnsureDefaultCPU(task_options.resources);
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

  task_options.name = options.name;
  task_options.concurrency_group_name = options.concurrency_group_name;

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

  // Add default CPU=1 if no resources are specified, matching Python's behavior
  // for autoscaler compatibility (it triggers pending lease reporting). The
  // same map is used as placement resources, as needed by the constructor.
  std::unordered_map<std::string, double> resources = options.resources;
  EnsureDefaultCPU(resources);

  // max_concurrency: 1 means serialized (the Ray default), -1 means unlimited
  // and values >= 1 allow that many concurrent method calls. A zero value (from
  // a zero-value options struct) resolves to the serialized default instead of
  // unlimited, so an actor never silently becomes reentrant.
  //
  // The Go submitter (convertActorCreationOptionsToC) is the authoritative
  // normalization layer: it maps 0 -> 1 and any negative value -> -1 before
  // this point, so the only values reaching this C++ code are >= 1 or -1.
  const int max_concurrency = options.max_concurrency == 0 ? 1 : options.max_concurrency;

  // Build concurrency groups declared by the Go API. Each group's function
  // descriptors are 4-element Go descriptors (module, package, actorType,
  // method) built via BuildGo so they match the actor's methods registered in
  // ConcurrencyGroupManager.
  std::vector<ray::ConcurrencyGroup> concurrency_groups;
  concurrency_groups.reserve(options.concurrency_groups.size());
  for (const auto &cg : options.concurrency_groups) {
    std::vector<ray::FunctionDescriptor> fds;
    fds.reserve(cg.function_descriptors.size() / 4);
    for (size_t i = 0; i + 3 < cg.function_descriptors.size(); i += 4) {
      fds.push_back(ray::FunctionDescriptorBuilder::BuildGo(
          cg.function_descriptors[i],        // module_name
          cg.function_descriptors[i + 1],    // package_path
          cg.function_descriptors[i + 2],    // function_name (actorType)
          cg.function_descriptors[i + 3]));  // method_name
    }
    concurrency_groups.emplace_back(cg.name, cg.max_concurrency, std::move(fds));
  }

  return ray::core::ActorCreationOptions(
      options.max_restarts,
      options.max_task_retries,
      max_concurrency,
      resources,
      resources,
      {},  // dynamic_worker_options
      // Always pass the explicit value: when the actor is NON_DETACHED,
      // std::nullopt would fall back to the job's default_actor_lifetime (which
      // may be DETACHED), incorrectly creating an explicit non-detached actor as
      // detached. Passing the real bool covers the job default, matching Java.
      std::optional<bool>(options.is_detached),  // is_detached
      options.name,
      namespace_copy,
      options.is_asyncio,  // is_asyncio
      scheduling_strategy,
      options.serialized_runtime_env_info,
      std::move(concurrency_groups),  // concurrency_groups
      options.is_asyncio,             // allow_out_of_order_execution; align with Java:
                           // ActorCreationOptions.allowOutOfOrderExecution = isAsync
      options.max_pending_calls,  // max_pending_calls
      false,                      // enable_tensor_transport
      false,                      // enable_task_events
      {},                         // labels
      {},                         // label_selector
      {});                        // fallback_strategy
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
