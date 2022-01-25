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

#include "native_ray_runtime.h"

#include <ray/api.h>

#include "./object/native_object_store.h"
#include "./object/object_store.h"
#include "./task/native_task_submitter.h"
#include "ray/common/ray_config.h"

namespace ray {
namespace internal {

NativeRayRuntime::NativeRayRuntime() {
  object_store_ = std::unique_ptr<ObjectStore>(new NativeObjectStore());
  task_submitter_ = std::unique_ptr<TaskSubmitter>(new NativeTaskSubmitter());
  task_executor_ = std::make_unique<TaskExecutor>();

  auto bootstrap_address = ConfigInternal::Instance().bootstrap_ip;
  if (bootstrap_address.empty()) {
    bootstrap_address = GetNodeIpAddress();
  }
  bootstrap_address =
      bootstrap_address + ":" + std::to_string(ConfigInternal::Instance().bootstrap_port);
  if (::RayConfig::instance().bootstrap_with_gcs()) {
    global_state_accessor_ =
        ProcessHelper::GetInstance().CreateGlobalStateAccessor(bootstrap_address);
  } else {
    global_state_accessor_ = ProcessHelper::GetInstance().CreateGlobalStateAccessor(
        bootstrap_address, ConfigInternal::Instance().redis_password);
  }
}

ActorID NativeRayRuntime::GetCurrentActorID() {
  return core::CoreWorkerProcess::GetCoreWorker().GetWorkerContext().GetCurrentActorID();
}

}  // namespace internal
}  // namespace ray
