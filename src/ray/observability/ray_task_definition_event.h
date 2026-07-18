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
#include "src/ray/protobuf/public/events_task_definition_event.pb.h"

namespace ray {
namespace observability {

template class RayEvent<rpc::events::TaskDefinitionEvent>;

// RayTaskDefinitionEvent wraps a rpc::events::TaskDefinitionEvent (the static metadata of
// a non-actor task attempt: function, resources, runtime env, parent, etc.) as a
// RayEventInterface for recording through RayEventRecorder.
//
// Definition events are static, so MergeData is a no-op: if more than one definition
// event is produced for the same task attempt (each spec-carrying status event produces
// one), the recorder collapses them to a single event when grouping by
// (entity_id, event_type).
//
// TODO(karticam): this proto is built EAGERLY -- the caller passes a fully-populated
// TaskDefinitionEvent, constructed at task-submission time. The legacy TaskEventBuffer
// instead deferred definition-proto building to the flush thread, keeping it off the task
// submission/execution critical path. Building the proto eagerly here might increase
// latency in the task submission time. Benchmark this and if it regresses, implement lazy
// serialization.
//
//
// Definition events are the only ones eligible for deferral since MergeData is a no-op,
// so the proto need not exist before the recorder's merge step.
// Lifecycle/profile are mergeable and must stay eager.
class RayTaskDefinitionEvent : public RayEvent<rpc::events::TaskDefinitionEvent>,
                               public TaskRayEventInterface {
 public:
  RayTaskDefinitionEvent(rpc::events::TaskDefinitionEvent data,
                         const std::string &session_name);

  std::string GetEntityId() const override;

  TaskAttemptId GetTaskAttempt() const override;

 protected:
  void MergeData(RayEvent<rpc::events::TaskDefinitionEvent> &&other) override;
  ray::rpc::events::RayEvent SerializeData() && override;
};

}  // namespace observability
}  // namespace ray
