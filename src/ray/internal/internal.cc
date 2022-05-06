// Copyright 2020 The Ray Authors.
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

#include "ray/internal/internal.h"

#include "ray/core_worker/core_worker.h"

namespace ray {
namespace internal {

using ray::core::CoreWorkerProcess;
using ray::core::TaskOptions;

std::vector<rpc::ObjectReference> SendInternal(const ActorID &peer_actor_id,
                                               std::shared_ptr<LocalMemoryBuffer> buffer,
                                               RayFunction &function,
                                               int return_num) {
  std::unordered_map<std::string, double> resources;
  std::string name = function.GetFunctionDescriptor()->DefaultTaskName();
  TaskOptions options{name, return_num, resources};

  char meta_data[3] = {'R', 'A', 'W'};
  std::shared_ptr<LocalMemoryBuffer> meta =
      std::make_shared<LocalMemoryBuffer>((uint8_t *)meta_data, 3, true);

  std::vector<std::unique_ptr<TaskArg>> args;
  if (function.GetLanguage() == Language::PYTHON) {
    auto dummy = "__RAY_DUMMY__";
    std::shared_ptr<LocalMemoryBuffer> dummyBuffer =
        std::make_shared<LocalMemoryBuffer>((uint8_t *)dummy, 13, true);
    args.emplace_back(new TaskArgByValue(std::make_shared<RayObject>(
        std::move(dummyBuffer), meta, std::vector<rpc::ObjectReference>(), true)));
  }
  args.emplace_back(new TaskArgByValue(std::make_shared<RayObject>(
      std::move(buffer), meta, std::vector<rpc::ObjectReference>(), true)));

  std::vector<std::shared_ptr<RayObject>> results;
  auto result = CoreWorkerProcess::GetCoreWorker().SubmitActorTask(
      peer_actor_id, function, args, options);
  if (!result.has_value()) {
    RAY_CHECK(false) << "Back pressure should not be enabled.";
  }
  return result.value();
}

const ray::stats::TagKeyType TagRegister(const std::string tag_name) {
  return ray::stats::TagKeyType::Register(tag_name);
}

const ActorID &GetCurrentActorID() {
  return CoreWorkerProcess::GetCoreWorker().GetWorkerContext().GetCurrentActorID();
}

bool IsInitialized() { return CoreWorkerProcess::IsInitialized(); }

void SetCurrentThreadWorker(const WorkerID &worker_id) {
  CoreWorkerProcess::SetCurrentThreadWorkerId(worker_id);
}

}  // namespace internal
}  // namespace ray
