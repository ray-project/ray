// Copyright 2025 The Ray Authors.
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

#include <memory>
#include <string>
#include <vector>

#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/common/status.h"
#include "ray/common/task/task_spec.h"

// XXX: avoid this??
#include "ray/core_worker/reference_count.h"

namespace ray {
namespace core {

struct TaskExecutionResult {
  Status status;
  bool is_retryable_error;
  std::string application_error;
  std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> return_objects;
  std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> dynamic_return_objects;
  std::vector<std::pair<ObjectID, bool>> streaming_generator_returns;
  ReferenceCounter::ReferenceTableProto borrowed_refs;
};

}  // namespace core
}  // namespace ray
