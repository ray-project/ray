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

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ray/core_worker/core_worker.h"

namespace ray {
namespace internal {
// NOTE(lingxuan.zlx): This internal module is designed to export ray symbols
// to other thirdparty outside project, which makes they can access internal
// function of core worker library or native function and reduce symbols racing.

using ray::core::CoreWorkerProcess;
using ray::core::TaskOptions;

std::vector<rpc::ObjectReference> SendInternal(
    const ActorID &peer_actor_id,
    std::shared_ptr<LocalMemoryBuffer> buffer,
    RayFunction &function,
    int return_num,
    int max_retries,
    bool retry_exceptions,
    std::string serialized_retry_exception_allowlist) {
  std::unordered_map<std::string, double> resources;
  std::string name = function.GetFunctionDescriptor()->DefaultTaskName();
  TaskOptions options{name, return_num, resources};

  char meta_data[3] = {'R', 'A', 'W'};
  auto meta = std::make_shared<LocalMemoryBuffer>(
      reinterpret_cast<uint8_t *>(meta_data), 3, true);

  std::vector<std::unique_ptr<TaskArg>> args;
  if (function.GetLanguage() == Language::PYTHON) {
    auto dummy = "__RAY_DUMMY__";
    auto dummyBuffer = std::make_shared<LocalMemoryBuffer>(
        reinterpret_cast<uint8_t *>(const_cast<char *>(dummy)), 13, true);
    args.emplace_back(new TaskArgByValue(std::make_shared<RayObject>(
        std::move(dummyBuffer), meta, std::vector<rpc::ObjectReference>(), true)));
  }
  args.emplace_back(new TaskArgByValue(std::make_shared<RayObject>(
      std::move(buffer), meta, std::vector<rpc::ObjectReference>(), true)));

  std::vector<std::shared_ptr<RayObject>> results;
  std::vector<rpc::ObjectReference> return_refs;
  auto result = CoreWorkerProcess::GetCoreWorker().SubmitActorTask(
      peer_actor_id,
      function,
      args,
      options,
      max_retries,
      retry_exceptions,
      serialized_retry_exception_allowlist,
      /*call_site=*/"",
      return_refs);
  if (!result.ok()) {
    RAY_CHECK(false) << "Back pressure should not be enabled.";
  }
  return return_refs;
}

const ray::stats::TagKeyType TagRegister(const std::string tag_name) {
  return ray::stats::TagKeyType::Register(tag_name);
}

const std::string TagKeyName(stats::TagKeyType &tagkey) { return tagkey.name(); }

const ActorID &GetCurrentActorID() {
  return CoreWorkerProcess::GetCoreWorker().GetWorkerContext().GetCurrentActorID();
}

bool IsInitialized() { return CoreWorkerProcess::IsInitialized(); }

}  // namespace internal
}  // namespace ray
