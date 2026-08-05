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
#include <optional>
#include <string>
#include <vector>

#include "ray/common/id.h"
#include "ray/observability/ray_event.h"
#include "ray/observability/task_ray_event_interface.h"
#include "ray/observability/task_state_update.h"
#include "src/ray/protobuf/public/events_task_lifecycle_event.pb.h"

namespace ray {
namespace observability {

template class RayEvent<rpc::events::TaskLifecycleEvent>;

/**
 * @brief Wraps a rpc::events::TaskLifecycleEvent (the dynamic part of a task attempt: its
 * state transitions plus node/worker/error/log info) as a RayEventInterface so it can be
 * recorded through RayTaskEventRecorder and sent to the event aggregator.
 *
 * Multiple lifecycle events for the same task attempt are merged by the recorder into a
 * single time series via MergeData.
 *
 * (claude) Each recorded status change is kept as-is and the whole series is turned into
 * a proto when the event is serialized for export, so the conversion stays off the task's
 * call path.
 */
class RayTaskLifecycleEvent : public RayEvent<rpc::events::TaskLifecycleEvent>,
                              public TaskRayEventInterface {
 public:
  RayTaskLifecycleEvent(const TaskID &task_id,
                        const JobID &job_id,
                        int32_t task_attempt,
                        rpc::TaskStatus task_status,
                        const std::optional<const TaskStateUpdate> &state_update,
                        std::shared_ptr<const std::string> session_name,
                        int64_t timestamp);

  // Entity id is (task_id, task_attempt); shared with the task's definition event so the
  // two can be associated, and used by the recorder to merge lifecycle events of the same
  // attempt.
  std::string GetEntityId() const override;

  TaskAttemptId GetTaskAttempt() const override;

 protected:
  void MergeData(RayEvent<rpc::events::TaskLifecycleEvent> &&other) override;
  ray::rpc::events::RayEvent SerializeData() && override;

 private:
  // (claude) One recorded status change of the task attempt.
  struct StatusChange {
    rpc::TaskStatus task_status;
    int64_t timestamp;
    std::optional<TaskStateUpdate> state_update;
  };

  TaskID task_id_;
  JobID job_id_;
  int32_t task_attempt_;
  // (claude) Status changes of this attempt, in the order they were recorded.
  std::vector<StatusChange> status_changes_;
};

}  // namespace observability
}  // namespace ray
