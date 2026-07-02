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

#include <cstdint>
#include <string>
#include <utility>

namespace ray {
namespace observability {

// Identifies the task attempt an event belongs to: (task_id binary, attempt_number).
// Used as the buffer / drop-tracking key in RayTaskEventRecorder.
using TaskAttemptId = std::pair<std::string, int32_t>;

// Mix-in implemented by the task-event wrappers (definition / lifecycle / profile) so
// RayTaskEventRecorder can read the (task_id, attempt) an event belongs to. Kept separate
// from RayEventInterface so the generic recorder stays free of task-specific concepts;
// RayTaskEventRecorder dynamic_casts the events it receives to this interface.
class TaskRayEventInterface {
 public:
  virtual ~TaskRayEventInterface() = default;

  // The task attempt this event belongs to.
  virtual TaskAttemptId GetTaskAttempt() const = 0;
};

}  // namespace observability
}  // namespace ray
