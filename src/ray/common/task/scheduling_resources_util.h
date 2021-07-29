// Copyright 2017 The Ray Authors.
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

#include "ray/common/constants.h"
#include "ray/common/task/task_spec.h"

namespace ray {

/// If the task need a sole actor worker assignment.
inline bool NeedSoleActorWorkerAssignment(const TaskSpecification &task_spec) {
  return task_spec.GetLanguage() == rpc::Language::PYTHON;
}

}  // namespace ray
