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
namespace api {

class TaskSubmitter {
 public:
  TaskSubmitter(){};

  virtual ~TaskSubmitter(){};

  virtual ObjectID SubmitTask(InvocationSpec &invocation) = 0;

  virtual ActorID CreateActor(InvocationSpec &invocation) = 0;

  virtual ObjectID SubmitActorTask(InvocationSpec &invocation) = 0;
};
}  // namespace api
}  // namespace ray