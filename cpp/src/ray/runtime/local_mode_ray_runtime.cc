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

#include "local_mode_ray_runtime.h"

#include <ray/api.h>

#include "./object/local_mode_object_store.h"
#include "./object/object_store.h"
#include "./task/local_mode_task_submitter.h"

namespace ray {
namespace internal {

namespace {
const JobID kUnusedJobId = JobID::FromInt(1);
}

LocalModeRayRuntime::LocalModeRayRuntime()
    : job_id_(kUnusedJobId),
      worker_(ray::core::WorkerType::DRIVER, ComputeDriverIdFromJob(job_id_), job_id_) {
  object_store_ = std::unique_ptr<ObjectStore>(new LocalModeObjectStore(*this));
  task_submitter_ = std::unique_ptr<TaskSubmitter>(new LocalModeTaskSubmitter(*this));
}

ActorID LocalModeRayRuntime::GetNextActorID() {
  const auto next_task_index = worker_.GetNextTaskIndex();
  const ActorID actor_id =
      ActorID::Of(worker_.GetCurrentJobID(), worker_.GetCurrentTaskID(), next_task_index);
  return actor_id;
}

const WorkerContext &LocalModeRayRuntime::GetWorkerContext() { return worker_; }

std::string LocalModeRayRuntime::Put(std::shared_ptr<msgpack::sbuffer> data) {
  ObjectID object_id =
      ObjectID::FromIndex(worker_.GetCurrentTaskID(), worker_.GetNextPutIndex());
  AbstractRayRuntime::Put(data, &object_id);
  return object_id.Binary();
}

}  // namespace internal
}  // namespace ray
