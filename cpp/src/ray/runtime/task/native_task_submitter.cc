// Copyright 2020-2021 The Ray Authors.
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

#include "native_task_submitter.h"

#include <ray/api/ray_exception.h>

#include "../abstract_ray_runtime.h"
#include "ray/common/ray_config.h"

namespace ray {
namespace internal {

using ray::core::CoreWorkerProcess;
using ray::core::TaskOptions;

RayFunction BuildRayFunction(InvocationSpec &invocation) {
  if (invocation.remote_function_holder.lang_type_ == LangType::CPP) {
    auto function_descriptor = FunctionDescriptorBuilder::BuildCpp(
        invocation.remote_function_holder.function_name_,
        "",
        invocation.remote_function_holder.class_name_);
    return RayFunction(ray::Language::CPP, function_descriptor);
  } else if (invocation.remote_function_holder.lang_type_ == LangType::PYTHON) {
    auto function_descriptor = FunctionDescriptorBuilder::BuildPython(
        invocation.remote_function_holder.module_name_,
        invocation.remote_function_holder.class_name_,
        invocation.remote_function_holder.function_name_,
        "");
    return RayFunction(ray::Language::PYTHON, function_descriptor);
  } else if (invocation.remote_function_holder.lang_type_ == LangType::JAVA) {
    auto function_descriptor = FunctionDescriptorBuilder::BuildJava(
        invocation.remote_function_holder.class_name_,
        invocation.remote_function_holder.function_name_,
        "");
    return RayFunction(ray::Language::JAVA, function_descriptor);
  } else {
    throw RayException("not supported yet");
  }
}

template <typename T>
static BundleID GetBundleID(const T &options) {
  BundleID bundle_id = std::make_pair(PlacementGroupID::Nil(), -1);
  if (!options.group.Empty()) {
    PlacementGroupID id = PlacementGroupID::FromBinary(options.group.GetID());
    bundle_id = std::make_pair(id, options.bundle_index);
  }
  return bundle_id;
};

ObjectID NativeTaskSubmitter::Submit(InvocationSpec &invocation,
                                     const CallOptions &call_options) {
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  TaskOptions options{};
  options.name = call_options.name;
  options.resources = call_options.resources;
  options.serialized_runtime_env_info = call_options.serialized_runtime_env_info;
  options.generator_backpressure_num_objects = -1;
  std::vector<rpc::ObjectReference> return_refs;

  std::string call_site;
  if (::RayConfig::instance().record_task_actor_creation_sites()) {
    std::stringstream ss;
    ss << ray::StackTrace();
    call_site = ss.str();
  }
  if (invocation.task_type == TaskType::ACTOR_TASK) {
    // NOTE: Ray CPP doesn't support per-method max_retries and retry_exceptions
    const auto native_actor_handle = core_worker.GetActorHandle(invocation.actor_id);
    int max_retries = native_actor_handle->MaxTaskRetries();

    auto status = core_worker.SubmitActorTask(invocation.actor_id,
                                              BuildRayFunction(invocation),
                                              invocation.args,
                                              options,
                                              max_retries,
                                              /*retry_exceptions=*/false,
                                              /*serialized_retry_exception_allowlist=*/"",
                                              call_site,
                                              return_refs);
    if (!status.ok()) {
      return ObjectID::Nil();
    }
  } else {
    BundleID bundle_id = GetBundleID(call_options);
    rpc::SchedulingStrategy scheduling_strategy;
    scheduling_strategy.mutable_default_scheduling_strategy();
    if (!bundle_id.first.IsNil()) {
      auto placement_group_scheduling_strategy =
          scheduling_strategy.mutable_placement_group_scheduling_strategy();
      placement_group_scheduling_strategy->set_placement_group_id(
          bundle_id.first.Binary());
      placement_group_scheduling_strategy->set_placement_group_bundle_index(
          bundle_id.second);
      placement_group_scheduling_strategy->set_placement_group_capture_child_tasks(false);
    }
    return_refs = core_worker.SubmitTask(BuildRayFunction(invocation),
                                         invocation.args,
                                         options,
                                         1,
                                         false,
                                         scheduling_strategy,
                                         "",
                                         /*serialized_retry_exception_allowlist=*/"",
                                         call_site);
  }
  return ObjectID::FromBinary(return_refs[0].object_id());
}

ObjectID NativeTaskSubmitter::SubmitTask(InvocationSpec &invocation,
                                         const CallOptions &call_options) {
  return Submit(invocation, call_options);
}

ActorID NativeTaskSubmitter::CreateActor(InvocationSpec &invocation,
                                         const ActorCreationOptions &create_options) {
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  std::unordered_map<std::string, double> resources;
  std::string name = create_options.name;
  std::string ray_namespace = create_options.ray_namespace;
  BundleID bundle_id = GetBundleID(create_options);
  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_default_scheduling_strategy();
  if (!bundle_id.first.IsNil()) {
    auto placement_group_scheduling_strategy =
        scheduling_strategy.mutable_placement_group_scheduling_strategy();
    placement_group_scheduling_strategy->set_placement_group_id(bundle_id.first.Binary());
    placement_group_scheduling_strategy->set_placement_group_bundle_index(
        bundle_id.second);
    placement_group_scheduling_strategy->set_placement_group_capture_child_tasks(false);
  }
  std::string call_site;
  if (::RayConfig::instance().record_task_actor_creation_sites()) {
    std::stringstream ss;
    ss << ray::StackTrace();
    call_site = ss.str();
  }
  ray::core::ActorCreationOptions actor_options{
      create_options.max_restarts,
      /*max_task_retries=*/0,
      create_options.max_concurrency,
      create_options.resources,
      resources,
      /*dynamic_worker_options=*/{},
      /*is_detached=*/std::nullopt,
      name,
      ray_namespace,
      /*is_asyncio=*/false,
      scheduling_strategy,
      create_options.serialized_runtime_env_info};
  ActorID actor_id;
  auto status = core_worker.CreateActor(BuildRayFunction(invocation),
                                        invocation.args,
                                        actor_options,
                                        /*extension_data=*/"",
                                        call_site,
                                        &actor_id);
  if (!status.ok()) {
    throw RayException("Create actor error");
  }
  return actor_id;
}

ObjectID NativeTaskSubmitter::SubmitActorTask(InvocationSpec &invocation,
                                              const CallOptions &task_options) {
  return Submit(invocation, task_options);
}

ActorID NativeTaskSubmitter::GetActor(const std::string &actor_name,
                                      const std::string &ray_namespace) const {
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  const std::string ns =
      ray_namespace.empty() ? core_worker.GetJobConfig().ray_namespace() : ray_namespace;
  auto pair = core_worker.GetNamedActorHandle(actor_name, ns);
  if (!pair.second.ok()) {
    RAY_LOG(WARNING) << pair.second.message();
    return ActorID::Nil();
  }

  auto actor_handle = pair.first;
  RAY_CHECK(actor_handle);
  return actor_handle->GetActorID();
}

ray::PlacementGroup NativeTaskSubmitter::CreatePlacementGroup(
    const ray::PlacementGroupCreationOptions &create_options) {
  auto options = ray::core::PlacementGroupCreationOptions(
      create_options.name,
      (ray::core::PlacementStrategy)create_options.strategy,
      create_options.bundles,
      false);
  ray::PlacementGroupID placement_group_id;
  auto status = CoreWorkerProcess::GetCoreWorker().CreatePlacementGroup(
      options, &placement_group_id);
  if (!status.ok()) {
    throw RayException(status.message());
  }

  ray::PlacementGroup placement_group{placement_group_id.Binary(), create_options};
  placement_group.SetWaitCallbak([this](const std::string &id, int64_t timeout_seconds) {
    return WaitPlacementGroupReady(id, timeout_seconds);
  });

  return placement_group;
}

void NativeTaskSubmitter::RemovePlacementGroup(const std::string &group_id) {
  auto placement_group_id = ray::PlacementGroupID::FromBinary(group_id);
  auto status =
      CoreWorkerProcess::GetCoreWorker().RemovePlacementGroup(placement_group_id);
  if (!status.ok()) {
    throw RayException(status.message());
  }
}

bool NativeTaskSubmitter::WaitPlacementGroupReady(const std::string &group_id,
                                                  int64_t timeout_seconds) {
  auto placement_group_id = ray::PlacementGroupID::FromBinary(group_id);
  auto status = CoreWorkerProcess::GetCoreWorker().WaitPlacementGroupReady(
      placement_group_id, timeout_seconds);

  if (status.IsNotFound()) {
    throw RayException(status.message());
  }
  return status.ok();
}

}  // namespace internal
}  // namespace ray
