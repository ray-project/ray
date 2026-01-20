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

#include "ray/observability/ray_task_lifecycle_event.h"

namespace ray {
namespace observability {

RayTaskLifecycleEvent::RayTaskLifecycleEvent(const TaskID &task_id,
                                             int32_t attempt_number,
                                             const JobID &job_id,
                                             const NodeID &node_id,
                                             rpc::TaskStatus task_status,
                                             int64_t timestamp_ns,
                                             const std::string &session_name)
    : RayEvent<rpc::events::TaskLifecycleEvent>(
          rpc::events::RayEvent::CORE_WORKER,
          rpc::events::RayEvent::TASK_LIFECYCLE_EVENT,
          rpc::events::RayEvent::INFO,
          "",
          session_name) {
  data_.set_task_id(task_id.Binary());
  data_.set_task_attempt(attempt_number);
  data_.set_job_id(job_id.Binary());
  data_.set_node_id(node_id.Binary());

  if (task_status != rpc::TaskStatus::NIL) {
    rpc::events::TaskLifecycleEvent::StateTransition state_transition;
    state_transition.set_state(task_status);
    state_transition.mutable_timestamp()->CopyFrom(
        AbslTimeNanosToProtoTimestamp(timestamp_ns));
    *data_.mutable_state_transitions()->Add() = std::move(state_transition);
  }
}

std::string RayTaskLifecycleEvent::GetEntityId() const {
  return data_.task_id() + "_" + std::to_string(data_.task_attempt());
}

void RayTaskLifecycleEvent::MergeData(RayEvent<rpc::events::TaskLifecycleEvent> &&other) {
  auto &&other_event = static_cast<RayTaskLifecycleEvent &&>(other);

  // Merge state transitions
  for (auto &state : *other_event.data_.mutable_state_transitions()) {
    data_.mutable_state_transitions()->Add(std::move(state));
  }

  // Merge optional fields - take value from incoming event if it has the field set
  if (other_event.data_.has_is_debugger_paused()) {
    data_.set_is_debugger_paused(other_event.data_.is_debugger_paused());
  }
  if (!other_event.data_.worker_id().empty()) {
    data_.set_worker_id(other_event.data_.worker_id());
  }
  if (other_event.data_.worker_pid() != 0) {
    data_.set_worker_pid(other_event.data_.worker_pid());
  }
  if (other_event.data_.has_ray_error_info()) {
    *data_.mutable_ray_error_info() =
        std::move(*other_event.data_.mutable_ray_error_info());
  }
  if (other_event.data_.has_actor_repr_name()) {
    data_.set_actor_repr_name(other_event.data_.actor_repr_name());
  }
}

ray::rpc::events::RayEvent RayTaskLifecycleEvent::SerializeData() && {
  ray::rpc::events::RayEvent event;
  event.mutable_task_lifecycle_event()->Swap(&data_);
  return event;
}

void RayTaskLifecycleEvent::SetIsDebuggerPaused(bool is_debugger_paused) {
  data_.set_is_debugger_paused(is_debugger_paused);
}

void RayTaskLifecycleEvent::SetErrorInfo(const rpc::RayErrorInfo &error_info) {
  *data_.mutable_ray_error_info() = error_info;
}

}  // namespace observability
}  // namespace ray
