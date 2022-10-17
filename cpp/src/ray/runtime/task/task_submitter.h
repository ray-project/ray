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

#pragma once

#include <ray/api/ray_runtime.h>

#include <memory>

#include "invocation_spec.h"

namespace ray {
namespace internal {

class TaskSubmitter {
 public:
  TaskSubmitter(){};

  virtual ~TaskSubmitter(){};

  virtual ObjectID SubmitTask(InvocationSpec &invocation,
                              const CallOptions &call_options) = 0;

  virtual ActorID CreateActor(InvocationSpec &invocation,
                              const ActorCreationOptions &create_options) = 0;

  virtual ObjectID SubmitActorTask(InvocationSpec &invocation,
                                   const CallOptions &call_options) = 0;

  virtual ActorID GetActor(const std::string &actor_name,
                           const std::string &ray_namespace) const = 0;

  virtual ray::PlacementGroup CreatePlacementGroup(
      const ray::PlacementGroupCreationOptions &create_options) = 0;

  virtual void RemovePlacementGroup(const std::string &group_id) = 0;

  virtual bool WaitPlacementGroupReady(const std::string &group_id,
                                       int64_t timeout_seconds) {
    return true;
  }
};
}  // namespace internal
}  // namespace ray