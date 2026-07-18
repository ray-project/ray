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

#include <string>

#include "ray/observability/ray_event.h"
#include "ray/observability/task_ray_event_interface.h"
#include "src/ray/protobuf/public/events_task_lifecycle_event.pb.h"

namespace ray {
namespace observability {

template class RayEvent<rpc::events::TaskLifecycleEvent>;

// RayTaskLifecycleEvent wraps a rpc::events::TaskLifecycleEvent (the dynamic part of a
// task attempt: its state transitions plus node/worker/error/log info) as a
// RayEventInterface so it can be recorded through RayEventRecorder and sent to the event
// aggregator.
//
// Multiple lifecycle events for the same task attempt are merged by the recorder into
// a single time series via MergeData.
class RayTaskLifecycleEvent : public RayEvent<rpc::events::TaskLifecycleEvent>,
                              public TaskRayEventInterface {
 public:
  RayTaskLifecycleEvent(rpc::events::TaskLifecycleEvent data,
                        const std::string &session_name);

  // Entity id is (task_id, task_attempt); shared with the task's definition event so the
  // two can be associated, and used by the recorder to merge lifecycle events of the same
  // attempt.
  std::string GetEntityId() const override;

  TaskAttemptId GetTaskAttempt() const override;

 protected:
  void MergeData(RayEvent<rpc::events::TaskLifecycleEvent> &&other) override;
  ray::rpc::events::RayEvent SerializeData() && override;
};

}  // namespace observability
}  // namespace ray
