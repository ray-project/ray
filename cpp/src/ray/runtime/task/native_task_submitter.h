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

#pragma once

#include "../native_ray_runtime.h"
#include "invocation_spec.h"
#include "task_submitter.h"

namespace ray {
namespace internal {

class NativeTaskSubmitter : public TaskSubmitter {
 public:
  ObjectID SubmitTask(InvocationSpec &invocation, const CallOptions &call_options);

  ActorID CreateActor(InvocationSpec &invocation,
                      const ActorCreationOptions &create_options);

  ObjectID SubmitActorTask(InvocationSpec &invocation, const CallOptions &call_options);

  ActorID GetActor(const std::string &actor_name, const std::string &ray_namespace) const;

  ray::PlacementGroup CreatePlacementGroup(
      const ray::PlacementGroupCreationOptions &create_options);
  void RemovePlacementGroup(const std::string &group_id);
  bool WaitPlacementGroupReady(const std::string &group_id, int64_t timeout_seconds);

 private:
  ObjectID Submit(InvocationSpec &invocation, const CallOptions &call_options);
};
}  // namespace internal
}  // namespace ray