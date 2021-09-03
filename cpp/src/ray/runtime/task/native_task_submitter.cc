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

namespace ray {
namespace internal {

using ray::core::CoreWorkerProcess;
using ray::core::TaskOptions;

RayFunction BuildRayFunction(InvocationSpec &invocation) {
  auto function_descriptor = FunctionDescriptorBuilder::BuildCpp(
      invocation.remote_function_holder.function_name);
  return RayFunction(ray::Language::CPP, function_descriptor);
}

ObjectID NativeTaskSubmitter::Submit(InvocationSpec &invocation,
                                     const CallOptions &call_options) {
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  TaskOptions options{};
  options.name = call_options.name;
  options.resources = call_options.resources;
  std::vector<rpc::ObjectReference> return_refs;
  if (invocation.task_type == TaskType::ACTOR_TASK) {
    return_refs = core_worker.SubmitActorTask(
        invocation.actor_id, BuildRayFunction(invocation), invocation.args, options);
  } else {
    return_refs = core_worker.SubmitTask(
        BuildRayFunction(invocation), invocation.args, options, 1, false,
        std::make_pair(PlacementGroupID::Nil(), -1), true, "");
  }
  std::vector<ObjectID> return_ids;
  for (const auto &ref : return_refs) {
    return_ids.push_back(ObjectID::FromBinary(ref.object_id()));
  }
  return return_ids[0];
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
  std::string ray_namespace = "";
  ray::core::ActorCreationOptions actor_options{create_options.max_restarts,
                                                /*max_task_retries=*/0,
                                                create_options.max_concurrency,
                                                create_options.resources,
                                                resources,
                                                /*dynamic_worker_options=*/{},
                                                /*is_detached=*/false,
                                                name,
                                                ray_namespace,
                                                /*is_asyncio=*/false};
  ActorID actor_id;
  auto status = core_worker.CreateActor(BuildRayFunction(invocation), invocation.args,
                                        actor_options, "", &actor_id);
  if (!status.ok()) {
    throw RayException("Create actor error");
  }
  return actor_id;
}

ObjectID NativeTaskSubmitter::SubmitActorTask(InvocationSpec &invocation,
                                              const CallOptions &task_options) {
  return Submit(invocation, task_options);
}

ray::PlacementGroup NativeTaskSubmitter::CreatePlacementGroup(
    const ray::internal::PlacementGroupCreationOptions &create_options) {
  auto get_full_name = [](bool global, std::string name) -> std::string {
    if (name.empty()) {
      return "";
    }
    return global
               ? name
               : CoreWorkerProcess::GetCoreWorker().GetCurrentJobId().Hex() + "-" + name;
  };
  auto full_name = get_full_name(create_options.global, create_options.name);
  auto options = ray::core::PlacementGroupCreationOptions(
      std::move(full_name), (ray::core::PlacementStrategy)create_options.strategy,
      create_options.bundles, false);
  ray::PlacementGroupID placement_group_id;
  auto status = CoreWorkerProcess::GetCoreWorker().CreatePlacementGroup(
      options, &placement_group_id);
  if (!status.ok()) {
    throw RayException(status.message());
  }

  ray::PlacementGroup placement_group{placement_group_id.Binary(), create_options};
  placement_group.SetWaitCallbak([this](const std::string &id, int timeout_seconds) {
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
                                                  int timeout_seconds) {
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
