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

#include <string>

#include "ray/observability/ray_event.h"
#include "ray/observability/task_ray_event_interface.h"
#include "src/ray/protobuf/events_task_profile_events.pb.h"

namespace ray {
namespace observability {

template class RayEvent<rpc::events::TaskProfileEvents>;

// RayTaskProfileEvent wraps a rpc::events::TaskProfileEvents (timeline/profiling spans
// for a task attempt) as a RayEventInterface for recording through RayEventRecorder.
// Profile events for the same task attempt are merged by appending their span entries.
class RayTaskProfileEvent : public RayEvent<rpc::events::TaskProfileEvents>,
                            public TaskRayEventInterface {
 public:
  RayTaskProfileEvent(rpc::events::TaskProfileEvents data,
                      const std::string &session_name);

  std::string GetEntityId() const override;

  TaskAttemptId GetTaskAttempt() const override;

 protected:
  void MergeData(RayEvent<rpc::events::TaskProfileEvents> &&other) override;
  ray::rpc::events::RayEvent SerializeData() && override;
};

}  // namespace observability
}  // namespace ray
