// Copyright 2026 The Ray Authors.
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
#include <memory>
#include <string>

#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/observability/ray_event.h"
#include "ray/observability/task_ray_event_interface.h"
#include "src/ray/protobuf/public/events_task_definition_event.pb.h"

namespace ray {
namespace observability {

template class RayEvent<rpc::events::TaskDefinitionEvent>;

/**
 * @brief Wraps a rpc::events::TaskDefinitionEvent (the static metadata of a non-actor
 * task attempt) as a RayEventInterface for recording through RayTaskEventRecorder.
 *
 * Exactly one definition event is produced per task attempt (from the single
 * spec-carrying status event), so MergeData must never be called; therefore
 * RAY_CHECK fails.
 *
 * (claude) The event keeps a reference to the task spec and builds the proto when it is
 * serialized for export, so the conversion stays off the task's call path.
 */
class RayTaskDefinitionEvent : public RayEvent<rpc::events::TaskDefinitionEvent>,
                               public TaskRayEventInterface {
 public:
  RayTaskDefinitionEvent(std::shared_ptr<const TaskSpecification> task_spec,
                         const TaskID &task_id,
                         const JobID &job_id,
                         int32_t task_attempt,
                         std::shared_ptr<const std::string> session_name,
                         int64_t timestamp);

  std::string GetEntityId() const override;

  TaskAttemptId GetTaskAttempt() const override;

 protected:
  void MergeData(RayEvent<rpc::events::TaskDefinitionEvent> &&other) override;
  ray::rpc::events::RayEvent SerializeData() && override;

 private:
  std::shared_ptr<const TaskSpecification> task_spec_;
  TaskID task_id_;
  JobID job_id_;
  int32_t task_attempt_;
};

}  // namespace observability
}  // namespace ray
